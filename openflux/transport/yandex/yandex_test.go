package yandex

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"universal-bypass-tool/transport"
)

func pageFixture(host, wsURL string) []byte {
	office := map[string]interface{}{
		"balancer_url": host,
		"editor_config": map[string]interface{}{
			"token":    "fixture-secret-token",
			"document": map[string]interface{}{"key": "fixture-doc", "fileType": "docx", "url": host + "/fixture-file", "title": "Fixture", "permissions": map[string]interface{}{"edit": true}},
		},
	}
	if wsURL != "" {
		office["ws_url"] = wsURL
	}
	data, _ := json.MarshalIndent(map[string]interface{}{"officeActionData": office}, "", "  ")
	return []byte("<html><script type='application/json' id='client-config'>\n" + string(data) + "\n</script></html>")
}

func TestParseDocInfoFixture(t *testing.T) {
	info, err := parseDocInfo(pageFixture("https://docs.yandex.ru", "/current/doc/socket"), "fixture-user", false)
	if err != nil {
		t.Fatal(err)
	}
	if info.DocID != "fixture-doc" || info.WsURL != "wss://docs.yandex.ru/current/doc/socket?EIO=4&transport=websocket" {
		t.Fatalf("wrong parsed endpoint or document")
	}
	if info.OpenCmd["userid"] != "fixture-user" {
		t.Fatal("missing user ID")
	}
}

func TestParseDocInfoMalformed(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"missing script", "<html>login</html>", "client-config missing"},
		{"bad JSON", `<script id="client-config">SECRET_NOT_JSON</script>`, "not valid JSON"},
		{"empty config", `<script id="client-config">{}</script>`, "officeActionData"},
		{"wrong action type", `<script id="client-config">{"officeActionData":"SECRET"}</script>`, "officeActionData"},
		{"null editor", `<script id="client-config">{"officeActionData":{"editor_config":null}}</script>`, "editor_config"},
		{"missing document", `<script id="client-config">{"officeActionData":{"editor_config":{}}}</script>`, "document"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseDocInfo([]byte(tc.body), "u", false)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted %s, got %v", tc.want, err)
			}
			if strings.Contains(err.Error(), "SECRET") {
				t.Fatal("secret leaked")
			}
		})
	}
}

func TestURLValidation(t *testing.T) {
	for _, raw := range []string{"http://docs.yandex.ru/test", "https://docs.yandex.ru.evil.test/test", "https://user:secret@docs.yandex.ru/test", "https://127.0.0.1/test", "https://docs.yandex.ru:1234/test", "https://docs.yandex.ru/test#secret"} {
		if _, err := validateURL(raw, false, false); err == nil {
			t.Fatalf("accepted unsafe URL %s", raw)
		}
	}
	page := pageFixture("https://docs.yandex.ru", "wss://other.yandex.ru/socket")
	if _, err := parseDocInfo(page, "u", false); err == nil {
		t.Fatal("accepted different websocket host")
	}
	page = pageFixture("https://docs.yandex.ru", "wss://bad.invalid/socket?token=secret")
	if _, err := parseDocInfo(page, "u", false); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("endpoint should be rejected without secrets")
	}
}

func TestWSVersionFromAssetsAndDiscoveryNeeded(t *testing.T) {
	body := append(pageFixture("https://docs.yandex.ru", ""), []byte(`<script src="/2026.2.3-999/editor.js"></script>`)...)
	info, err := parseDocInfo(body, "u", false)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(info.WsURL, "/2026.2.3-999/") {
		t.Fatal("asset version ignored")
	}
	info, err = parseDocInfo(pageFixture("https://docs.yandex.ru", ""), "u", false)
	if err != nil {
		t.Fatal(err)
	}
	if info.WsURL != "" {
		t.Fatal("must discover version, not use hardcoded old endpoint")
	}
}

func testTransport(rawURL string) *YandexDocsTransport {
	tr := NewYandexDocsTransport(rawURL, transport.DefaultConfig())
	tr.allowTestURLs = true
	return tr
}

func TestFetchStatusSizeAndMissingConfig(t *testing.T) {
	for _, kind := range []string{"status", "oversize", "missing"} {
		t.Run(kind, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch kind {
				case "status":
					w.WriteHeader(403)
					fmt.Fprint(w, "SECRET")
				case "oversize":
					fmt.Fprint(w, strings.Repeat("x", maxPageBytes+1))
				default:
					fmt.Fprint(w, "<html>Login SECRET</html>")
				}
			}))
			defer srv.Close()
			tr := testTransport(srv.URL)
			_, err := tr.fetchDocInfo(srv.URL, "u")
			if err == nil || strings.Contains(err.Error(), "SECRET") {
				t.Fatal("request should fail without leaking body")
			}
		})
	}
}

func TestFetchTimeoutAndCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer srv.Close()
	tr := testTransport(srv.URL)
	tr.httpClient.Timeout = 30 * time.Millisecond
	start := time.Now()
	_, err := tr.fetchDocInfo(srv.URL, "u")
	if err == nil || time.Since(start) > time.Second {
		t.Fatal("HTTP timeout not enforced")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = tr.fetchDocInfoContext(ctx, srv.URL, "u")
	if err == nil {
		t.Fatal("cancellation ignored")
	}
}

func TestBackoffAndRetryLimit(t *testing.T) {
	tr := NewYandexDocsTransport("https://docs.yandex.ru/x", transport.DefaultConfig())
	if tr.GetConfig().MaxReconnectAttempts != 8 {
		t.Fatal("unbounded retry count")
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	for i, d := range want {
		if got := tr.reconnectDelay(i); got != d {
			t.Fatalf("attempt %d: %v != %v", i, got, d)
		}
	}
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); w.WriteHeader(503) }))
	defer srv.Close()
	cfg := transport.DefaultConfig()
	cfg.MaxReconnectAttempts = 2
	tr = NewYandexDocsTransport(srv.URL, cfg)
	tr.allowTestURLs = true
	tr.retryDelay = func(int) time.Duration { return 25 * time.Millisecond }
	start := time.Now()
	if err := tr.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-tr.done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker failed to terminate")
	}
	if requests.Load() != 3 {
		t.Fatalf("expected initial+2 retries, got %d", requests.Load())
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Fatal("backoff was not awaited")
	}
	if tr.IsConnected() || tr.IsRunning() {
		t.Fatal("transport remained active after retry budget")
	}
}

func waitFor(t *testing.T, fn func() bool) {
	t.Helper()
	limit := time.Now().Add(2 * time.Second)
	for time.Now().Before(limit) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out")
}

func TestMockWebsocketAuthCursorReconnectAndStop(t *testing.T) {
	var attempts atomic.Int32
	ackSeen := make(chan struct{}, 2)
	allowAck := make(chan struct{})
	received := make(chan []byte, 4)
	sent := make(chan string, 2)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/doc" {
			w.Write(pageFixture(srv.URL, "/socket"))
			return
		}
		if r.URL.Path != "/socket" {
			http.NotFound(w, r)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		n := attempts.Add(1)
		conn.WriteMessage(websocket.TextMessage, []byte(`0{"sid":"fixture","pingInterval":25000,"pingTimeout":20000}`))
		_, auth, err := conn.ReadMessage()
		if err != nil || !strings.HasPrefix(string(auth), "40") {
			return
		}
		conn.WriteMessage(websocket.TextMessage, []byte(`40{"sid":"fixture"}`))
		_, auth, err = conn.ReadMessage()
		if err != nil || !strings.Contains(string(auth), `"type":"auth"`) {
			return
		}
		ackSeen <- struct{}{}
		if n == 1 {
			<-allowAck
		}
		conn.WriteMessage(websocket.TextMessage, []byte(`42["message",{"type":"auth","result":1}]`))
		if n == 1 {
			// Unrelated save events must never be accepted as transport packets.
			conn.WriteMessage(websocket.TextMessage, []byte(`42["message",{"type":"saveChanges","excelAdditionalInfo":"U0VDUkVU"}]`))
			conn.WriteMessage(websocket.TextMessage, []byte(`42["message",{"type":"cursor","messages":[{"cursor":"5;aGVsbG8="}]}]`))
			_, packet, err := conn.ReadMessage()
			if err == nil {
				sent <- string(packet)
			}
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	tr := testTransport(srv.URL + "/doc")
	tr.retryDelay = func(int) time.Duration { return 25 * time.Millisecond }
	tr.Receive(func(data []byte) { received <- data })
	if err := tr.Start(); err != nil {
		t.Fatal(err)
	}
	defer tr.Stop()
	select {
	case <-ackSeen:
	case <-time.After(time.Second):
		t.Fatal("no handshake")
	}
	if tr.IsConnected() {
		t.Fatal("connected before server auth acknowledgement")
	}
	if err := tr.Send([]byte("early")); err == nil {
		t.Fatal("sent before authentication")
	}
	close(allowAck)
	waitFor(t, tr.IsConnected)
	if err := tr.Send([]byte("outbound")); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-received:
		if string(data) != "hello" {
			t.Fatalf("unexpected cursor %q", data)
		}
	case <-time.After(time.Second):
		t.Fatal("no cursor")
	}
	select {
	case data := <-sent:
		if !strings.Contains(data, base64.StdEncoding.EncodeToString([]byte("outbound"))) {
			t.Fatal("wrong outbound packet")
		}
	case <-time.After(time.Second):
		t.Fatal("no sent packet")
	}
	waitFor(t, func() bool { return attempts.Load() == 2 && tr.IsConnected() })
	if err := tr.Stop(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-tr.done:
	case <-time.After(time.Second):
		t.Fatal("Stop leaked websocket reader/worker")
	}
	if tr.IsConnected() {
		t.Fatal("connected after Stop")
	}
	select {
	case data := <-received:
		t.Fatalf("unexpected packet %q", data)
	default:
	}
}

func TestRejectAuthAndNonCursorMessages(t *testing.T) {
	tr := testTransport("http://localhost/test")
	s := &DocSession{socketReady: true}
	if err := tr.handleMessage(s, []byte(`42["message",{"type":"auth","result":0,"error":"SECRET"}]`)); err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Fatal("failed auth accepted or secret leaked")
	}
	if tr.IsConnected() {
		t.Fatal("auth failure set connected")
	}
	s.socketReady = false
	if err := tr.handleMessage(s, []byte(`42["message",{"type":"auth","result":1}]`)); err == nil {
		t.Fatal("accepted missing socket acknowledgement")
	}
}

func TestEditorAPIVersionDiscovery(t *testing.T) {
	var apiRequests atomic.Int32
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/doc":
			page := string(pageFixture(srv.URL, ""))
			page = strings.Replace(page, srv.URL+"/fixture-file", "http://localhost:12701/backend-file", 1)
			fmt.Fprint(w, page)
		case "/web-apps/apps/api/documents/api.js":
			apiRequests.Add(1)
			fmt.Fprint(w, `var cacheKey="?_dc=2026.2.1-2268";`)
		default:
			t.Error("unexpected HTTP request path")
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	tr := testTransport(srv.URL + "/doc")
	info, err := tr.fetchDocInfo(srv.URL+"/doc", "u")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(info.WsURL, "/2026.2.1-2268/doc/") || apiRequests.Load() != 1 {
		t.Fatal("did not discover exact editor version")
	}
	if info.OpenCmd["url"] != "http://localhost:12701/backend-file" {
		t.Fatal("signed backend URL was altered")
	}
}

func TestTwoParticipantsCompleteAuthenticationJoin(t *testing.T) {
	type peer struct {
		conn *websocket.Conn
		mu   sync.Mutex
	}
	write := func(p *peer, msg string) error {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.conn.WriteMessage(websocket.TextMessage, []byte(msg))
	}
	var peersMu sync.Mutex
	var first *peer
	firstReady := make(chan struct{})
	unlocked := make(chan struct{}, 1)
	invalid := make(chan string, 4)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/doc" {
			w.Write(pageFixture(srv.URL, "/socket"))
			return
		}
		if r.URL.Path != "/socket" {
			http.NotFound(w, r)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		p := &peer{conn: conn}
		write(p, `0{"sid":"fixture","pingInterval":25000,"pingTimeout":20000}`)
		_, frame, err := conn.ReadMessage()
		if err != nil || !strings.HasPrefix(string(frame), "40") {
			return
		}
		write(p, `40{"sid":"fixture"}`)
		_, frame, err = conn.ReadMessage()
		if err != nil || !strings.Contains(string(frame), `"type":"auth"`) {
			return
		}
		peersMu.Lock()
		leader := first
		isFirst := leader == nil
		if isFirst {
			first = p
		}
		peersMu.Unlock()
		if isFirst {
			write(p, `42["message",{"type":"auth","result":1}]`)
			close(firstReady)
		} else {
			<-firstReady
			write(p, `42["message",{"type":"waitAuth","lockDocument":{"id":"fixture"}}]`)
			write(leader, `42["message",{"type":"connectState","waitAuth":true,"participants":[{},{}]}]`)
			select {
			case <-unlocked:
				write(p, `42["message",{"type":"auth","result":1}]`)
			case <-time.After(time.Second):
				invalid <- "second participant remained blocked"
				return
			case <-r.Context().Done():
				return
			}
		}
		for {
			_, frame, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if !strings.HasPrefix(string(frame), "42") {
				continue
			}
			var packet []json.RawMessage
			json.Unmarshal(frame[2:], &packet)
			if len(packet) != 2 {
				continue
			}
			var message struct {
				Type         string `json:"type"`
				IsSave       bool   `json:"isSave"`
				Unlock       bool   `json:"unlock"`
				DeleteIndex  *int   `json:"deleteIndex"`
				ReleaseLocks bool   `json:"releaseLocks"`
			}
			json.Unmarshal(packet[1], &message)
			if message.Type == "unLockDocument" {
				if !isFirst || message.IsSave || !message.Unlock || message.DeleteIndex == nil || *message.DeleteIndex != -1 || message.ReleaseLocks {
					invalid <- "unsafe join acknowledgement"
					return
				}
				select {
				case unlocked <- struct{}{}:
				default:
					invalid <- "duplicate join acknowledgement"
				}
			} else if message.Type != "cursor" {
				invalid <- "unexpected document mutation command"
				return
			}
		}
	}))
	defer srv.Close()
	one, two := testTransport(srv.URL+"/doc"), testTransport(srv.URL+"/doc")
	defer func() {
		one.Stop()
		two.Stop()
		if one.done != nil {
			<-one.done
		}
		if two.done != nil {
			<-two.done
		}
	}()
	if err := one.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, one.IsConnected)
	if err := two.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, two.IsConnected)
	if !one.IsConnected() {
		t.Fatal("first participant lost authentication")
	}
	select {
	case msg := <-invalid:
		t.Fatal(msg)
	default:
	}
}

func TestDiagnosticLabelsNeverContainRemotePayload(t *testing.T) {
	if authResultLabel(json.RawMessage(`"SECRET-TOKEN"`)) != "unrecognized" {
		t.Fatal("unsafe auth label")
	}
	if handshakeEventLabel("SECRET-TOKEN") != "other" {
		t.Fatal("unsafe event label")
	}
}

func TestServerCloseClassifications(t *testing.T) {
	tr := testTransport("https://localhost/fixture")
	for _, tc := range []struct{ frame, want string }{
		{"1", "server closed document session (Engine.IO close=1)"},
		{"41", "server closed document session (Socket.IO disconnect=41)"},
	} {
		if err := tr.handleMessage(&DocSession{}, []byte(tc.frame)); err == nil || err.Error() != tc.want {
			t.Fatal("close packet classification changed or was ambiguous")
		}
	}
	for _, frame := range []string{"411", "41SECRET_MARKER", "1SECRET_MARKER"} {
		if err := tr.handleMessage(&DocSession{}, []byte(frame)); err != nil {
			t.Fatal("a remote suffix was misclassified or exposed")
		}
	}
}

func TestNumericDiagnosticsAreBoundedIntegers(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{"0", "0"}, {" -4001 ", "-4001"}, {"9223372036854775807", "9223372036854775807"},
		{"-9223372036854775808", "-9223372036854775808"},
		{"", "unavailable"}, {"null", "unavailable"}, {"true", "unavailable"},
		{`"4001"`, "unavailable"}, {`"SECRET_MARKER"`, "unavailable"},
		{`{"code":4001}`, "unavailable"}, {"1.5", "unavailable"}, {"1e3", "unavailable"},
		{"9223372036854775808", "unavailable"}, {strings.Repeat("1", 100), "unavailable"},
	} {
		if got := numericDiagnostic(json.RawMessage(tc.raw)); got != tc.want {
			t.Fatalf("numeric diagnostic mismatch: got %q want %q", got, tc.want)
		}
	}
}

func TestPostAuthenticationProviderDiagnosticsAreSafeAndBounded(t *testing.T) {
	var output bytes.Buffer
	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetOutput(&output)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() { log.SetOutput(oldWriter); log.SetFlags(oldFlags); log.SetPrefix(oldPrefix) })
	tr := testTransport("https://localhost/fixture")
	s := &DocSession{authenticated: true, channelIndex: 3}
	for _, event := range []string{"disconnectReason", "drop", "error", "warning", "expiredToken", "license"} {
		frame, _ := json.Marshal([]interface{}{"message", map[string]interface{}{
			"type": event, "code": -4001, "result": 1, "error": 2,
			"description": "SECRET_MARKER", "message": "https://secret.invalid/token", "token": "SECRET_MARKER",
		}})
		for repeat := 0; repeat < 3; repeat++ {
			if err := tr.handleMessage(s, append([]byte("42"), frame...)); err != nil {
				t.Fatal(err)
			}
		}
		if strings.Count(output.String(), "provider event="+event+" ") != 1 ||
			!strings.Contains(output.String(), "channel 03 provider event="+event+" code=-4001 result=1 error=2") {
			t.Fatal("provider diagnostic missing numeric fields or repeated without a bound")
		}
	}
	if err := tr.handleMessage(s, []byte(`42["message",{"type":"session","messages":{"code":4002,"reason":"SECRET_MARKER"}}]`)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "provider event=session code=4002 result=unavailable error=unavailable") {
		t.Fatal("nested session warning code was ignored")
	}
	before := output.String()
	for i := 0; i < 100; i++ {
		if err := tr.handleMessage(s, []byte(`42["message",{"type":"SECRET_MARKER","code":4003}]`)); err != nil {
			t.Fatal(err)
		}
	}
	if output.String() != before || len(s.providerNotes) != 7 {
		t.Fatal("unknown events reached diagnostics or grew session state")
	}
	untrusted := &DocSession{authenticated: true}
	if err := tr.handleMessage(untrusted, []byte(`42["message",{"type":"error","code":"SECRET_MARKER","result":{"token":"SECRET_MARKER"},"error":"https://secret.invalid/"}]`)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "document provider event=error code=unavailable result=unavailable error=unavailable") ||
		strings.Contains(output.String(), "SECRET_MARKER") || strings.Contains(output.String(), "https://") {
		t.Fatal("provider diagnostic exposed remote text")
	}
}
