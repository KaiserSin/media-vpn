package yandex

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"universal-bypass-tool/transport"
)

func bootstrapFixture(host, wsURL string) []byte {
	return append([]byte(nil), scriptRE.FindSubmatch(pageFixture(host, wsURL))[2]...)
}

type bootstrapRoundTripper func(*http.Request) (*http.Response, error)

func (f bootstrapRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestBootstrapConfigIsImmutableAndSkipsDocumentHTTP(t *testing.T) {
	tr := NewYandexDocsTransport("https://docs.yandex.ru/fixture", transport.DefaultConfig())
	raw := bootstrapFixture("https://docs.yandex.ru", "/fixture-socket")
	if err := tr.SetBootstrapConfig(raw); err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		raw[i] = 'x'
	}
	var requests atomic.Int32
	tr.httpClient.Transport = bootstrapRoundTripper(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return nil, errors.New("HTTP must not run")
	})
	info, err := tr.fetchDocInfoContext(context.Background(), tr.url, "session-user")
	if err != nil || info.DocID != "fixture-doc" || info.OpenCmd["userid"] != "session-user" || requests.Load() != 0 {
		t.Fatal("cached bootstrap did not preserve validated immutable config")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tr.fetchDocInfoContext(ctx, tr.url, "session-user"); !errors.Is(err, context.Canceled) {
		t.Fatal("cached bootstrap ignored cancellation")
	}
}

func TestBootstrapConfigValidationAndLifecycle(t *testing.T) {
	valid := bootstrapFixture("https://docs.yandex.ru", "/fixture-socket")
	for _, raw := range [][]byte{nil, []byte(`null`), []byte(`[]`), []byte(`{"SECRET_MARKER":`), []byte(`{}`), []byte(strings.Repeat("x", maxPageBytes+1)), bootstrapFixture("https://bad.invalid", "/socket")} {
		tr := NewYandexDocsTransport("https://docs.yandex.ru/fixture", transport.DefaultConfig())
		if err := tr.SetBootstrapConfig(raw); err == nil || strings.Contains(err.Error(), "SECRET_MARKER") || strings.Contains(err.Error(), "https://") {
			t.Fatal("bootstrap input validation or error sanitization failed")
		}
	}
	for _, field := range []string{"token", "document"} {
		var config map[string]interface{}
		if json.Unmarshal(valid, &config) != nil {
			t.Fatal("fixture invalid")
		}
		delete(config["officeActionData"].(map[string]interface{})["editor_config"].(map[string]interface{}), field)
		raw, _ := json.Marshal(config)
		tr := NewYandexDocsTransport("https://docs.yandex.ru/fixture", transport.DefaultConfig())
		if tr.SetBootstrapConfig(raw) == nil {
			t.Fatal("missing bootstrap authentication fields accepted")
		}
	}
	tr := NewYandexDocsTransport("https://docs.yandex.ru/fixture", transport.DefaultConfig())
	tr.done = make(chan struct{})
	if tr.SetBootstrapConfig(valid) == nil {
		t.Fatal("bootstrap modified after transport start")
	}
}

func TestBootstrapPreservesDiscoveryAndWebsocketAuthentication(t *testing.T) {
	var documentRequests, assetRequests, authentications atomic.Int32
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/document":
			documentRequests.Add(1)
			http.Error(w, "not an editor page", http.StatusForbidden)
		case r.URL.Path == "/web-apps/apps/api/documents/api.js":
			assetRequests.Add(1)
			_, _ = w.Write([]byte(`script.src="/2026.2.1-3364/editor.js"`))
		default:
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`0{"sid":"fixture"}`))
			_, msg, err := conn.ReadMessage()
			if err != nil || !strings.HasPrefix(string(msg), "40") {
				return
			}
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`40{"sid":"fixture"}`))
			_, msg, err = conn.ReadMessage()
			if err != nil || !strings.Contains(string(msg), `"type":"auth"`) {
				return
			}
			authentications.Add(1)
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`42["message",{"type":"auth","result":1}]`))
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}
	}))
	defer server.Close()
	tr := testTransport(server.URL + "/document")
	if err := tr.SetBootstrapConfig(bootstrapFixture(server.URL, "")); err != nil {
		t.Fatal(err)
	}
	if err := tr.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		tr.Stop()
		select {
		case <-tr.done:
		case <-time.After(time.Second):
			t.Error("bootstrap transport did not stop")
		}
	}()
	waitFor(t, tr.IsConnected)
	if documentRequests.Load() != 0 || assetRequests.Load() != 1 || authentications.Load() != 1 {
		t.Fatal("cached config changed discovery or websocket authentication behavior")
	}
}
