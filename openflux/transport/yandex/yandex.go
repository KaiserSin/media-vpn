package yandex

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"universal-bypass-tool/transport"
)

const (
	maxPageBytes        = 4 << 20
	maxPacketBytes      = 64 << 10
	maxWSMessageBytes   = 512 << 10
	reconnectResetAfter = 60 * time.Second
)

type YandexDocsInfo struct {
	CookieStr, Token, DocID, CallbackURL, UserID, Origin, Host, WsURL string
	Permissions                                                       map[string]interface{}
	OpenCmd                                                           map[string]interface{}
}

type DocSession struct {
	Info                                 YandexDocsInfo
	Conn                                 *websocket.Conn
	WriteQueue                           chan []byte
	UserID                               string
	writeMu                              sync.Mutex
	socketReady, authSent, authenticated bool
	authenticatedAt                      time.Time
	documentAuthSent, authUnlockPending  bool
	handshakeNotes                       map[string]bool
	providerNotes                        map[string]bool
	channelIndex                         int
}

// Labels passed here are static protocol classifications, never remote strings.
func (s *DocSession) noteHandshake(label string) {
	if s.handshakeNotes == nil {
		s.handshakeNotes = make(map[string]bool)
	}
	if s.handshakeNotes[label] || len(s.handshakeNotes) >= 16 {
		return
	}
	s.handshakeNotes[label] = true
	if s.channelIndex > 0 {
		log.Printf("[Media] channel %02d handshake: %s", s.channelIndex, label)
	} else {
		log.Printf("[Media] document handshake: %s", label)
	}
}

func authResultLabel(raw json.RawMessage) string {
	switch strings.TrimSpace(string(raw)) {
	case "":
		return "missing"
	case "null":
		return "null"
	case "1", "true":
		return "success"
	case "0", "false":
		return "rejected"
	default:
		return "unrecognized"
	}
}

func handshakeEventLabel(event string) string {
	switch event {
	case "auth", "waitAuth", "connectState", "error", "warning", "drop", "disconnectReason", "authChanges", "documentOpen", "license", "expiredToken":
		return event
	default:
		return "other"
	}
}

func numericDiagnostic(raw json.RawMessage) string {
	value := strings.TrimSpace(string(raw))
	var number int64
	if len(value) == 0 || len(value) > 20 || (value[0] != '-' && (value[0] < '0' || value[0] > '9')) || json.Unmarshal([]byte(value), &number) != nil {
		return "unavailable"
	}
	return strconv.FormatInt(number, 10)
}

// Only whitelisted labels and integer fields reach diagnostics. Log each event
// once per session; descriptions, tokens, URLs and other remote text are ignored.
func (s *DocSession) noteProviderEvent(event string, code, result, remoteError, messages json.RawMessage) {
	var label string
	switch event {
	case "disconnectReason", "drop", "error", "warning", "expiredToken", "license", "session":
		label = event
	default:
		return
	}
	if s.providerNotes == nil {
		s.providerNotes = make(map[string]bool)
	}
	if s.providerNotes[label] || len(s.providerNotes) >= 7 {
		return
	}
	s.providerNotes[label] = true
	// Session-expiry warnings use an object in messages, unlike cursor arrays.
	if label == "session" && len(code) == 0 {
		var nested struct {
			Code json.RawMessage `json:"code"`
		}
		if json.Unmarshal(messages, &nested) == nil {
			code = nested.Code
		}
	}
	if s.channelIndex > 0 {
		log.Printf("[Media] channel %02d provider event=%s code=%s result=%s error=%s", s.channelIndex, label, numericDiagnostic(code), numericDiagnostic(result), numericDiagnostic(remoteError))
	} else {
		log.Printf("[Media] document provider event=%s code=%s result=%s error=%s", label, numericDiagnostic(code), numericDiagnostic(result), numericDiagnostic(remoteError))
	}
}

func (s *DocSession) safeWrite(kind int, data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.Conn == nil {
		return errors.New("no active websocket")
	}
	if err := s.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return errors.New("websocket write deadline failed")
	}
	if err := s.Conn.WriteMessage(kind, data); err != nil {
		return errors.New("websocket write failed")
	}
	return nil
}

type YandexDocsTransport struct {
	*transport.BaseTransport
	url         string
	session     *DocSession
	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	done        chan struct{}
	baseUserID  string
	httpClient  *http.Client
	dialer      *websocket.Dialer
	// Only package-local offline fixtures can enable loopback HTTP/WS.
	allowTestURLs          bool
	retryDelay             func(int) time.Duration
	healthySessionDuration time.Duration
	channelIndex           int
	batchMaxBytes          int
	batchFlushDelay        time.Duration
	batchStats             batchCounters
	bootstrapBody          []byte // Immutable after explicit configuration before Start.
	bootstrapDialAddress   string // Configured only before Start, alongside bootstrapBody.
}

// SetChannelIndex labels diagnostics without accepting a URL or secret string.
// Configure before Start; it does not alter the single-document wire protocol.
func (t *YandexDocsTransport) SetChannelIndex(index int) {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	if t.done == nil && index >= 1 && index <= 10 {
		t.channelIndex = index
	}
}

func NewYandexDocsTransport(docURL string, config transport.TransportConfig) *YandexDocsTransport {
	if config.MaxReconnectAttempts <= 0 || config.MaxReconnectAttempts > 8 {
		config.MaxReconnectAttempts = 8
	}
	if config.ReconnectDelay < time.Second {
		config.ReconnectDelay = time.Second
	}
	if config.ReconnectMultiplier < 2 {
		config.ReconnectMultiplier = 2
	}
	if config.MaxQueueSize <= 0 || config.MaxQueueSize > 1024 {
		config.MaxQueueSize = 1024
	}
	if config.KeepAliveInterval < time.Second {
		config.KeepAliveInterval = 10 * time.Second
	}
	jar, _ := cookiejar.New(nil)
	t := &YandexDocsTransport{
		BaseTransport: transport.NewBaseTransport(config), url: docURL, baseUserID: randUserID(),
		httpClient:             &http.Client{Timeout: 20 * time.Second, Jar: jar},
		dialer:                 &websocket.Dialer{HandshakeTimeout: 10 * time.Second},
		healthySessionDuration: reconnectResetAfter,
	}
	t.retryDelay = t.reconnectDelay
	return t
}

func (t *YandexDocsTransport) Start() error {
	if _, err := validateURL(t.url, false, t.allowTestURLs); err != nil {
		return fmt.Errorf("document URL: %w", err)
	}
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	if t.done != nil {
		select {
		case <-t.done:
		default:
			return errors.New("transport already started")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.cancel, t.done = cancel, make(chan struct{})
	if err := t.BaseTransport.Start(); err != nil {
		cancel()
		close(t.done)
		return err
	}
	go t.connectionLoop(ctx, t.done)
	return nil
}

// Cancel requests and close the socket immediately, including a blocked ReadMessage.
// The worker exits asynchronously, so Stop is also safe inside a receive callback.
func (t *YandexDocsTransport) Stop() error {
	t.lifecycleMu.Lock()
	if t.cancel != nil {
		t.cancel()
	}
	t.BaseTransport.Stop()
	t.Mu.RLock()
	s := t.session
	t.Mu.RUnlock()
	if s != nil && s.Conn != nil {
		s.Conn.Close()
	}
	t.lifecycleMu.Unlock()
	return nil
}

func (t *YandexDocsTransport) Send(data []byte) error {
	if len(data) > maxPacketBytes {
		return errors.New("packet exceeds transport limit")
	}
	if !t.IsConnected() {
		return errors.New("transport not connected")
	}
	t.Mu.RLock()
	s := t.session
	t.Mu.RUnlock()
	if s == nil {
		return errors.New("no active session")
	}
	packet := append([]byte(nil), data...)
	select {
	case s.WriteQueue <- packet:
		return nil
	default:
		return errors.New("write queue full")
	}
}

func (t *YandexDocsTransport) reconnectDelay(attempt int) time.Duration {
	delay := t.GetConfig().ReconnectDelay
	for i := 0; i < attempt; i++ {
		next := float64(delay) * t.GetConfig().ReconnectMultiplier
		if next >= float64(30*time.Second) {
			return 30 * time.Second
		}
		delay = time.Duration(next)
	}
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func (t *YandexDocsTransport) connectionLoop(ctx context.Context, done chan struct{}) {
	defer close(done)
	defer t.BaseTransport.Stop()
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return
		}
		healthy, err := t.connectOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		// The retry budget covers consecutive failures, not the lifetime of a
		// service. Short authenticated flaps still consume the bounded budget.
		if healthy {
			attempt = 0
		}
		// Errors must not include document links, JWTs, response bodies or cookies.
		if t.channelIndex > 0 {
			log.Printf("[Media] channel %02d connection attempt %d failed: %v", t.channelIndex, attempt+1, err)
		} else {
			log.Printf("[Media] document connection attempt %d failed: %v", attempt+1, err)
		}
		if attempt >= t.GetConfig().MaxReconnectAttempts {
			return
		}
		t.RecordReconnect()
		timer := time.NewTimer(t.retryDelay(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (t *YandexDocsTransport) connectOnce(ctx context.Context) (bool, error) {
	info, err := t.fetchDocInfoContext(ctx, t.url, t.baseUserID)
	if err != nil {
		return false, err
	}
	headers := http.Header{}
	headers.Set("User-Agent", "Mozilla/5.0")
	headers.Set("Origin", info.Origin)
	if info.CookieStr != "" {
		headers.Set("Cookie", info.CookieStr)
	}
	conn, resp, err := t.dialer.DialContext(ctx, info.WsURL, headers)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		if resp != nil {
			return false, fmt.Errorf("websocket handshake rejected (HTTP %d)", resp.StatusCode)
		}
		return false, errors.New("websocket connection failed")
	}
	s := &DocSession{Info: info, Conn: conn, UserID: t.baseUserID, WriteQueue: make(chan []byte, t.GetConfig().MaxQueueSize), channelIndex: t.channelIndex}
	s.noteHandshake("websocket established")
	t.Mu.Lock()
	t.session = s
	t.Mu.Unlock()
	defer func() {
		t.SetConnected(false)
		conn.Close()
		t.Mu.Lock()
		if t.session == s {
			t.session = nil
		}
		t.Mu.Unlock()
	}()
	err = t.runSession(ctx, s)
	healthy := !s.authenticatedAt.IsZero() && time.Since(s.authenticatedAt) >= t.healthySessionDuration
	return healthy, err
}

type socketRead struct {
	data []byte
	err  error
}

func (t *YandexDocsTransport) runSession(parent context.Context, s *DocSession) error {
	ctx, cancel := context.WithCancel(parent)
	reads := make(chan socketRead, 1)
	readerDone := make(chan struct{})
	s.Conn.SetReadLimit(maxWSMessageBytes)
	go func() {
		defer close(readerDone)
		for {
			s.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			kind, data, err := s.Conn.ReadMessage()
			if err == nil && kind != websocket.TextMessage {
				continue
			}
			select {
			case reads <- socketRead{data, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() { cancel(); s.Conn.Close(); <-readerDone }()
	authTimer := time.NewTimer(25 * time.Second)
	defer authTimer.Stop()
	keepAlive := time.NewTicker(t.GetConfig().KeepAliveInterval)
	defer keepAlive.Stop()
	var pending []byte
	pendingSignal := make(chan struct{})
	close(pendingSignal)
	for {
		queue := s.WriteQueue
		var readyPending <-chan struct{}
		if pending != nil {
			queue = nil
			readyPending = pendingSignal
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-authTimer.C:
			if !s.authenticated {
				return fmt.Errorf("document authentication timed out (engine_open=%t socket_ack=%t auth_request_sent=%t)", s.authSent, s.socketReady, s.documentAuthSent)
			}
		case msg := <-reads:
			if msg.err != nil {
				return errors.New("websocket closed or read timed out")
			}
			if err := t.handleMessage(s, msg.data); err != nil {
				return err
			}
			if s.authenticated {
				authTimer.Stop()
			}
		case packet := <-queue:
			if !s.authenticated {
				continue
			}
			var err error
			pending, err = t.writePacketBatch(ctx, s, packet)
			if err != nil {
				return err
			}
		case <-readyPending:
			packet := pending
			pending = nil
			var err error
			pending, err = t.writePacketBatch(ctx, s, packet)
			if err != nil {
				return err
			}
		case <-keepAlive.C:
			if s.authenticated {
				if err := s.safeWrite(websocket.TextMessage, []byte(`42["message",{"type":"cursor","cursor":"`+encodeCursor([]byte(keepAlivePayload))+`"}]`)); err != nil {
					return err
				}
			}
		}
	}
}

func (t *YandexDocsTransport) handleMessage(s *DocSession, data []byte) error {
	text := string(data)
	if strings.HasPrefix(text, "0{") {
		var open map[string]interface{}
		if json.Unmarshal(data[1:], &open) != nil {
			return errors.New("invalid Engine.IO greeting")
		}
		if s.authSent || s.socketReady {
			return nil
		}
		s.noteHandshake("Engine.IO greeting received")
		token, _ := json.Marshal(map[string]string{"token": s.Info.Token})
		s.authSent = true
		return s.safeWrite(websocket.TextMessage, append([]byte("40"), token...))
	}
	if text == "2" {
		return s.safeWrite(websocket.TextMessage, []byte("3"))
	}
	if text == "3" {
		return nil
	}
	if text == "1" {
		return errors.New("server closed document session (Engine.IO close=1)")
	}
	if text == "41" {
		return errors.New("server closed document session (Socket.IO disconnect=41)")
	}
	if strings.HasPrefix(text, "44") {
		return errors.New("Socket.IO authentication rejected")
	}
	if text == "40" || strings.HasPrefix(text, "40{") {
		if !s.authSent {
			return errors.New("Socket.IO acknowledgement before greeting")
		}
		if s.socketReady {
			return nil
		}
		s.socketReady = true
		s.noteHandshake("Socket.IO acknowledgement received")
		authData := map[string]interface{}{
			"type": "auth", "docid": s.Info.DocID, "token": "fghhfgsjdgfjs",
			"user": map[string]interface{}{"id": s.UserID}, "editorType": 0,
			"lastOtherSaveTime": -1, "permissions": s.Info.Permissions, "openCmd": s.Info.OpenCmd,
			"coEditingMode": "fast", "jwtOpen": s.Info.Token,
		}
		msg, _ := json.Marshal([]interface{}{"message", authData})
		if err := s.safeWrite(websocket.TextMessage, append([]byte("42"), msg...)); err != nil {
			return err
		}
		s.documentAuthSent = true
		s.noteHandshake("document authentication request sent")
		return nil
	}
	if !strings.HasPrefix(text, "42") {
		return nil
	}
	var envelope []json.RawMessage
	if json.Unmarshal(data[2:], &envelope) != nil || len(envelope) != 2 {
		return nil
	}
	var event string
	if json.Unmarshal(envelope[0], &event) != nil || event != "message" {
		return nil
	}
	var message struct {
		Type     string          `json:"type"`
		WaitAuth bool            `json:"waitAuth"`
		Result   json.RawMessage `json:"result"`
		Error    json.RawMessage `json:"error"`
		Code     json.RawMessage `json:"code"`
		Cursor   json.RawMessage `json:"cursor"`
		Messages json.RawMessage `json:"messages"`
	}
	if json.Unmarshal(envelope[1], &message) != nil {
		return nil
	}
	if !s.authenticated {
		s.noteHandshake("received event=" + handshakeEventLabel(message.Type))
	} else {
		s.noteProviderEvent(message.Type, message.Code, message.Result, message.Error, message.Messages)
	}
	if message.Type == "connectState" {
		if !message.WaitAuth {
			s.authUnlockPending = false
			return nil
		}
		if !s.authenticated || s.authUnlockPending {
			return nil
		}
		// The existing editor releases its own temporary join lock. No document
		// changes are sent, saved, deleted or unlocked. This mirrors OnlyOffice's
		// unLockDocument(false, true, -1, false) co-authoring transition.
		// https://github.com/ONLYOFFICE/sdkjs/blob/master/common/docscoapi.js
		unlock := []byte(`42["message",{"type":"unLockDocument","isSave":false,"unlock":true,"deleteIndex":-1,"releaseLocks":false}]`)
		if err := s.safeWrite(websocket.TextMessage, unlock); err != nil {
			return err
		}
		s.authUnlockPending = true
		s.noteHandshake("co-authoring join acknowledged without document changes")
		return nil
	}
	if message.Type == "auth" {
		s.noteHandshake("authentication result=" + authResultLabel(message.Result))
		if !s.socketReady {
			return errors.New("document authentication before Socket.IO acknowledgement")
		}
		if string(message.Result) != "1" && string(message.Result) != "true" {
			return errors.New("document authentication rejected or missing result")
		}
		if len(message.Error) > 0 && string(message.Error) != "null" && string(message.Error) != "0" && string(message.Error) != "false" {
			return errors.New("document authentication rejected")
		}
		if !s.authenticated {
			s.authenticatedAt = time.Now()
		}
		s.authenticated = true
		t.SetConnected(true)
		return nil
	}
	if message.Type != "cursor" || !s.authenticated {
		return nil
	}
	var messages []struct {
		Cursor string `json:"cursor"`
	}
	if len(message.Messages) > 0 && json.Unmarshal(message.Messages, &messages) != nil {
		return nil
	}
	var cursor string
	if json.Unmarshal(message.Cursor, &cursor) == nil {
		t.receiveCursor(cursor)
	}
	for _, m := range messages {
		t.receiveCursor(m.Cursor)
	}
	return nil
}

func (t *YandexDocsTransport) receiveCursor(cursor string) {
	// Ignore the historical malformed heartbeat without delivering it to a relay.
	if cursor == "18;---KA---" {
		return
	}
	data, err := decodeCursor(cursor)
	if err != nil || string(data) == keepAlivePayload {
		return
	}
	packets, batched, err := decodeBatch(data)
	if err != nil {
		t.batchStats.malformed.Add(1)
		return
	}
	t.RecordReceive(len(data))
	if batched {
		t.batchStats.batchesReceived.Add(1)
		t.batchStats.packetsReceived.Add(uint64(len(packets)))
		for _, packet := range packets {
			t.CallReceive(packet)
		}
		return
	}
	t.batchStats.packetsReceived.Add(1)
	t.CallReceive(data)
}

func (t *YandexDocsTransport) fetchDocInfo(docURL, userID string) (YandexDocsInfo, error) {
	return t.fetchDocInfoContext(context.Background(), docURL, userID)
}

func (t *YandexDocsTransport) fetchDocInfoContext(ctx context.Context, docURL, userID string) (YandexDocsInfo, error) {
	if err := ctx.Err(); err != nil {
		return YandexDocsInfo{}, err
	}
	if _, err := validateURL(docURL, false, t.allowTestURLs); err != nil {
		return YandexDocsInfo{}, fmt.Errorf("document URL: %w", err)
	}
	client := *t.httpClient
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return errors.New("too many document redirects")
		}
		_, err := validateURL(req.URL.String(), false, t.allowTestURLs)
		return err
	}
	t.lifecycleMu.Lock()
	body := t.bootstrapBody
	t.lifecycleMu.Unlock()
	if body == nil {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, docURL, nil)
		if err != nil {
			return YandexDocsInfo{}, errors.New("invalid document request")
		}
		req.Header.Set("User-Agent", "Mozilla/5.0")
		resp, err := client.Do(req)
		if err != nil {
			return YandexDocsInfo{}, errors.New("document request failed or timed out")
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return YandexDocsInfo{}, fmt.Errorf("document request rejected (HTTP %d)", resp.StatusCode)
		}
		body, err = io.ReadAll(io.LimitReader(resp.Body, maxPageBytes+1))
		if err != nil {
			return YandexDocsInfo{}, errors.New("cannot read document page")
		}
		if len(body) > maxPageBytes {
			return YandexDocsInfo{}, errors.New("document page exceeds size limit")
		}
	}
	info, err := parseDocInfo(body, userID, t.allowTestURLs)
	if err != nil {
		return YandexDocsInfo{}, err
	}
	if info.WsURL == "" {
		info.WsURL, err = t.discoverWSURL(ctx, &client, info)
		if err != nil {
			return YandexDocsInfo{}, err
		}
	}
	// Forward only cookies that the jar considers applicable to the websocket host.
	cookieURL, _ := url.Parse(info.Origin)
	var cookies []*http.Cookie
	if client.Jar != nil {
		cookies = client.Jar.Cookies(cookieURL)
	}
	var parts []string
	for _, c := range cookies {
		parts = append(parts, c.Name+"="+c.Value)
	}
	info.CookieStr = strings.Join(parts, "; ")
	return info, nil
}

var scriptRE = regexp.MustCompile(`(?is)<script\b([^>]*)>(.*?)</script\s*>`)
var configIDRE = regexp.MustCompile(`(?i)\bid\s*=\s*["']client-config["']`)
var versionRE = regexp.MustCompile(`^[0-9]{4}\.[0-9]+\.[0-9]+-[0-9]+$`)
var assetVersionRE = regexp.MustCompile(`/([0-9]{4}\.[0-9]+\.[0-9]+-[0-9]+)/`)

func object(m map[string]interface{}, key string) (map[string]interface{}, error) {
	v, ok := m[key].(map[string]interface{})
	if !ok || v == nil {
		return nil, fmt.Errorf("document config missing or invalid %s", key)
	}
	return v, nil
}
func requiredString(m map[string]interface{}, key string) (string, error) {
	v, ok := m[key].(string)
	if !ok || strings.TrimSpace(v) == "" {
		return "", fmt.Errorf("document config missing or invalid %s", key)
	}
	return v, nil
}
func parseDocInfo(body []byte, userID string, allowTestURLs bool) (YandexDocsInfo, error) {
	var config map[string]interface{}
	found := false
	for _, s := range scriptRE.FindAllSubmatch(body, -1) {
		if !configIDRE.Match(s[1]) {
			continue
		}
		found = true
		if json.Unmarshal(s[2], &config) != nil && json.Unmarshal([]byte(html.UnescapeString(string(s[2]))), &config) != nil {
			return YandexDocsInfo{}, errors.New("document client-config is not valid JSON")
		}
		break
	}
	if !found {
		return YandexDocsInfo{}, errors.New("document client-config missing; public editing page required")
	}
	office, err := object(config, "officeActionData")
	if err != nil {
		return YandexDocsInfo{}, err
	}
	editor, err := object(office, "editor_config")
	if err != nil {
		return YandexDocsInfo{}, err
	}
	document, err := object(editor, "document")
	if err != nil {
		return YandexDocsInfo{}, err
	}
	balancer, err := requiredString(office, "balancer_url")
	if err != nil {
		return YandexDocsInfo{}, err
	}
	endpoint, err := validateURL(balancer, false, allowTestURLs)
	if err != nil {
		return YandexDocsInfo{}, fmt.Errorf("balancer URL: %w", err)
	}
	if endpoint.RawQuery != "" || (endpoint.Path != "" && endpoint.Path != "/") {
		return YandexDocsInfo{}, errors.New("balancer URL must be an origin")
	}
	token, err := requiredString(editor, "token")
	if err != nil {
		return YandexDocsInfo{}, err
	}
	docID, err := requiredString(document, "key")
	if err != nil {
		return YandexDocsInfo{}, err
	}
	fileType, err := requiredString(document, "fileType")
	if err != nil {
		return YandexDocsInfo{}, err
	}
	docFile, err := requiredString(document, "url")
	if err != nil {
		return YandexDocsInfo{}, err
	}
	// The signed config can legitimately contain localhost backend URLs. They
	// are opaque open-command values, never fetched by this client.
	fileURL, fileErr := url.Parse(docFile)
	if fileErr != nil || fileURL.Host == "" || fileURL.User != nil || fileURL.Fragment != "" || (fileURL.Scheme != "https" && fileURL.Scheme != "http") {
		return YandexDocsInfo{}, errors.New("document config invalid backend file URL")
	}
	title, err := requiredString(document, "title")
	if err != nil {
		return YandexDocsInfo{}, err
	}
	perms := map[string]interface{}{}
	if p, exists := document["permissions"]; exists {
		var ok bool
		perms, ok = p.(map[string]interface{})
		if !ok {
			return YandexDocsInfo{}, errors.New("document config invalid permissions")
		}
	}
	wsURL, err := resolveWSURL(office, editor, endpoint, docID, body, allowTestURLs)
	if err != nil {
		return YandexDocsInfo{}, err
	}
	return YandexDocsInfo{
		Token: token, DocID: docID, UserID: userID, Origin: endpoint.Scheme + "://" + endpoint.Host, Host: endpoint.Host, WsURL: wsURL, Permissions: perms,
		OpenCmd: map[string]interface{}{"c": "open", "id": docID, "userid": userID, "format": fileType, "url": docFile, "title": title, "lcid": 25},
	}, nil
}

func resolveWSURL(office, editor map[string]interface{}, endpoint *url.URL, docID string, body []byte, allowTestURLs bool) (string, error) {
	for _, m := range []map[string]interface{}{office, editor} {
		for _, key := range []string{"ws_url", "websocket_url", "wsUrl", "websocketUrl"} {
			raw, exists := m[key]
			if !exists {
				continue
			}
			value, ok := raw.(string)
			if !ok || value == "" {
				return "", errors.New("document config invalid websocket URL")
			}
			u, err := url.Parse(value)
			if err != nil {
				return "", errors.New("document config invalid websocket URL")
			}
			if !u.IsAbs() {
				u = endpoint.ResolveReference(u)
				if u.Scheme == "https" {
					u.Scheme = "wss"
				} else if allowTestURLs {
					u.Scheme = "ws"
				}
			}
			u, err = validateURL(u.String(), true, allowTestURLs)
			if err != nil {
				return "", fmt.Errorf("websocket URL: %w", err)
			}
			if u.Host != endpoint.Host {
				return "", errors.New("websocket and balancer hosts differ")
			}
			q := u.Query()
			q.Set("EIO", "4")
			q.Set("transport", "websocket")
			u.RawQuery = q.Encode()
			return u.String(), nil
		}
	}
	version := ""
	for _, m := range []map[string]interface{}{office, editor} {
		for _, key := range []string{"version", "buildVersion", "editor_version", "editorVersion"} {
			if v, ok := m[key].(string); ok && versionRE.MatchString(v) {
				version = v
				break
			}
		}
		if version != "" {
			break
		}
	}
	if version == "" {
		if match := assetVersionRE.FindSubmatch(body); len(match) > 1 {
			version = string(match[1])
		}
	}
	if version == "" {
		// fetchDocInfo discovers the version through the editor's own API asset.
		return "", nil
	}
	return buildWSURL(endpoint, docID, version, allowTestURLs), nil
}

func buildWSURL(endpoint *url.URL, docID, version string, allowTestURLs bool) string {
	scheme := "wss"
	if allowTestURLs && endpoint.Scheme == "http" {
		scheme = "ws"
	}
	// PathEscape prevents a document key from injecting a path/query into the endpoint.
	return scheme + "://" + endpoint.Host + "/" + version + "/doc/" + url.PathEscape(docID) + "/c/?EIO=4&transport=websocket"
}

var apiVersionRE = regexp.MustCompile(`(?:[?&]_dc=|/)([0-9]{4}\.[0-9]+\.[0-9]+-[0-9]+)`)

func (t *YandexDocsTransport) discoverWSURL(ctx context.Context, baseClient *http.Client, info YandexDocsInfo) (string, error) {
	endpoint, err := validateURL(info.Origin, false, t.allowTestURLs)
	if err != nil {
		return "", errors.New("invalid editor API origin")
	}
	client := *baseClient
	client.Timeout = 10 * time.Second
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return errors.New("too many editor API redirects")
		}
		if _, err := validateURL(req.URL.String(), false, t.allowTestURLs); err != nil {
			return err
		}
		if req.URL.Host != endpoint.Host {
			return errors.New("editor API redirect changed host")
		}
		return nil
	}
	// This is the asset loaded by the official only_office frontend.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, info.Origin+"/web-apps/apps/api/documents/api.js", nil)
	if err != nil {
		return "", errors.New("invalid editor API request")
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := client.Do(req)
	if err != nil {
		return "", errors.New("editor API request failed or timed out")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("editor API rejected (HTTP %d)", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return "", errors.New("editor API response invalid or too large")
	}
	matches := apiVersionRE.FindSubmatch(data)
	if len(matches) < 2 {
		return "", errors.New("editor API version missing; endpoint discovery required")
	}
	return buildWSURL(endpoint, info.DocID, string(matches[1]), t.allowTestURLs), nil
}

func validateURL(raw string, websocketURL, allowTestURLs bool) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return nil, errors.New("invalid absolute URL")
	}
	expected := "https"
	if websocketURL {
		expected = "wss"
	}
	if allowTestURLs && (u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost" || u.Hostname() == "::1") {
		if u.Scheme == expected || (!websocketURL && u.Scheme == "http") || (websocketURL && u.Scheme == "ws") {
			return u, nil
		}
	}
	if u.Scheme != expected {
		return nil, errors.New("secure URL required")
	}
	if u.Port() != "" && u.Port() != "443" {
		return nil, errors.New("unexpected URL port")
	}
	host := strings.ToLower(u.Hostname())
	for _, suffix := range []string{"yandex.ru", "yandex.net", "yandex.com", "yandexcloud.net"} {
		if host == suffix || strings.HasSuffix(host, "."+suffix) {
			return u, nil
		}
	}
	return nil, errors.New("URL host is not a supported Yandex domain")
}

func randUserID() string {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value[:])
}
