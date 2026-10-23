package yandex

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"universal-bypass-tool/transport"
)

// Exercise actual document and WebSocket handshakes rather than replacing the
// connection worker. The small package-private threshold keeps fixtures fast;
// production still requires a full minute after document authentication.
func TestReconnectBudgetResetsOnlyAfterSustainedAuthentication(t *testing.T) {
	for _, tc := range []struct {
		name         string
		plan         string // H: sustained auth, S: short auth, U: long unauthenticated, F: immediate failure
		resetAfter   time.Duration
		wantAttempts int32
		wantBackoffs []int
	}{
		{"healthy sessions exceed lifetime budget", "FFHHHHFF", 25 * time.Millisecond, 8, []int{0, 1, 0, 0, 0, 0, 1}},
		{"short authenticated flaps remain bounded", "SSS", time.Hour, 3, []int{0, 1}},
		{"long unauthenticated sessions remain bounded", "UUU", 25 * time.Millisecond, 3, []int{0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			var srv *httptest.Server
			srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/doc" {
					_, _ = w.Write(pageFixture(srv.URL, "/socket"))
					return
				}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				n := int(attempts.Add(1)) - 1
				if n >= len(tc.plan) {
					return // A broken retry bound is caught by the attempt assertion.
				}
				kind := tc.plan[n]
				if kind == 'F' {
					return
				}
				if err := conn.WriteMessage(websocket.TextMessage, []byte(`0{"sid":"fixture"}`)); err != nil {
					return
				}
				_, msg, err := conn.ReadMessage()
				if err != nil || !strings.HasPrefix(string(msg), "40") {
					return
				}
				if err := conn.WriteMessage(websocket.TextMessage, []byte(`40{"sid":"fixture"}`)); err != nil {
					return
				}
				_, msg, err = conn.ReadMessage()
				if err != nil || !strings.Contains(string(msg), `"type":"auth"`) {
					return
				}
				if kind != 'U' {
					if err := conn.WriteMessage(websocket.TextMessage, []byte(`42["message",{"type":"auth","result":1}]`)); err != nil {
						return
					}
				}
				if kind == 'H' || kind == 'U' {
					time.Sleep(80 * time.Millisecond)
				}
			}))
			defer srv.Close()
			cfg := transport.DefaultConfig()
			cfg.MaxReconnectAttempts = 2
			tr := NewYandexDocsTransport(srv.URL+"/doc", cfg)
			tr.allowTestURLs = true
			if tr.healthySessionDuration != time.Minute {
				t.Fatal("production recovery threshold must be one minute")
			}
			tr.healthySessionDuration = tc.resetAfter
			var backoffs []int // Only the connection worker writes; read after done closes.
			tr.retryDelay = func(attempt int) time.Duration {
				backoffs = append(backoffs, attempt)
				return time.Millisecond
			}
			if err := tr.Start(); err != nil {
				t.Fatal(err)
			}
			defer tr.Stop()
			select {
			case <-tr.done:
			case <-time.After(4 * time.Second):
				t.Fatal("connection worker did not honor the consecutive retry limit")
			}
			if got := attempts.Load(); got != tc.wantAttempts {
				t.Fatalf("connection attempts = %d, want %d", got, tc.wantAttempts)
			}
			if !reflect.DeepEqual(backoffs, tc.wantBackoffs) {
				t.Fatalf("backoff sequence = %v, want %v", backoffs, tc.wantBackoffs)
			}
			if tr.IsConnected() || tr.IsRunning() {
				t.Fatal("exhausted transport still marked active")
			}
		})
	}
}

func TestStopCancelsReconnectBackoff(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	tr := testTransport(srv.URL)
	backoffStarted := make(chan struct{}, 1)
	tr.retryDelay = func(int) time.Duration {
		backoffStarted <- struct{}{}
		return time.Hour
	}
	if err := tr.Start(); err != nil {
		t.Fatal(err)
	}
	defer tr.Stop()
	select {
	case <-backoffStarted:
	case <-time.After(time.Second):
		t.Fatal("retry backoff did not begin")
	}
	if err := tr.Stop(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-tr.done:
	case <-time.After(time.Second):
		t.Fatal("Stop failed to cancel retry backoff")
	}
	if requests.Load() != 1 || tr.IsRunning() || tr.IsConnected() {
		t.Fatal("stopped transport retried or remained active")
	}
}
