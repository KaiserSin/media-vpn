package yandex

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"universal-bypass-tool/transport"
)

func newBootstrapDialTransport(t *testing.T) *YandexDocsTransport {
	t.Helper()
	tr := NewYandexDocsTransport("https://docs.yandex.ru/fixture", transport.DefaultConfig())
	if err := tr.SetBootstrapConfig(bootstrapFixture("https://docs.yandex.ru", "")); err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestBootstrapDialAddressValidationAndLifecycle(t *testing.T) {
	for _, raw := range []string{"87.250.251.212", "2a02:6b8::1", "::ffff:87.250.251.212"} {
		if err := newBootstrapDialTransport(t).SetBootstrapDialAddress(raw); err != nil {
			t.Fatal("public IP rejected:", err)
		}
	}
	for _, raw := range []string{"", "SECRET_MARKER", "https://SECRET_MARKER", "87.250.251.212:443", "127.0.0.1", "::1", "::ffff:127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1", "fc00::1", "169.254.1.1", "fe80::1", "fe80::1%en0", "0.0.0.0", "::", "224.0.0.1", "ff02::1", "100.64.0.1", "192.0.2.1", "198.18.0.1", "198.51.100.1", "203.0.113.1", "240.0.0.1", "2001:db8::1", "2002:7f00:1::1", "64:ff9b::7f00:1"} {
		tr := newBootstrapDialTransport(t)
		client, dialer := tr.httpClient, tr.dialer
		err := tr.SetBootstrapDialAddress(raw)
		if err == nil || strings.Contains(err.Error(), "SECRET_MARKER") || strings.Contains(err.Error(), "https://") {
			t.Fatalf("unsafe address accepted or unsanitized error for fixture %q", raw)
		}
		if tr.httpClient != client || tr.dialer != dialer || tr.bootstrapDialAddress != "" {
			t.Fatal("invalid address changed configuration")
		}
	}
	tr := NewYandexDocsTransport("https://docs.yandex.ru/fixture", transport.DefaultConfig())
	if tr.SetBootstrapDialAddress("87.250.251.212") == nil {
		t.Fatal("dial address accepted without bootstrap")
	}
	tr = newBootstrapDialTransport(t)
	tr.done = make(chan struct{})
	if tr.SetBootstrapDialAddress("87.250.251.212") == nil {
		t.Fatal("dial address changed after start")
	}
	tr = newBootstrapDialTransport(t)
	tr.allowTestURLs = true
	if err := tr.SetBootstrapDialAddress("127.0.0.1"); err != nil {
		t.Fatal("explicit loopback fixture rejected")
	}
	if tr.SetBootstrapConfig(bootstrapFixture("https://other.yandex.ru", "")) == nil || tr.SetBootstrapDialAddress("87.250.251.212") == nil {
		t.Fatal("pinned origin changed after configuration")
	}
	tr = newBootstrapDialTransport(t)
	if tr.SetBootstrapConfig(bootstrapFixture("https://docs.yandex.ru", "wss://other.yandex.ru/socket")) == nil {
		t.Fatal("foreign websocket origin accepted")
	}
	tr = testTransport("https://localhost/fixture")
	if err := tr.SetBootstrapConfig(bootstrapFixture("https://localhost:8443", "")); err != nil {
		t.Fatal(err)
	}
	if tr.SetBootstrapDialAddress("87.250.251.212") == nil {
		t.Fatal("non-443 origin accepted")
	}
}

func TestBootstrapDialAddressMapsOnlyExactOriginAndClones(t *testing.T) {
	tr := newBootstrapDialTransport(t)
	var httpAddress, wsAddress string
	stop := errors.New("fixture dial stopped")
	httpBase := &http.Transport{DialContext: func(_ context.Context, _, address string) (net.Conn, error) {
		httpAddress = address
		return nil, stop
	}}
	tr.httpClient.Transport = httpBase
	tr.dialer.NetDialContext = func(_ context.Context, _, address string) (net.Conn, error) {
		wsAddress = address
		return nil, stop
	}
	client, wsBase, defaultHTTP := tr.httpClient, tr.dialer, http.DefaultTransport
	if err := tr.SetBootstrapDialAddress("2a02:6b8::1"); err != nil {
		t.Fatal(err)
	}
	if tr.httpClient == client || tr.httpClient.Transport == httpBase || tr.dialer == wsBase || http.DefaultTransport != defaultHTTP || client.Transport != httpBase {
		t.Fatal("pinning mutated a shared transport or dialer")
	}
	for _, address := range []string{"docs.yandex.ru:443", "other.yandex.ru:443", "docs.yandex.ru:80", "docs.yandex.ru:8443", "docs.yandex.ru.evil.test:443", "DOCS.YANDEX.RU:443"} {
		want := address
		if address == "docs.yandex.ru:443" {
			want = "[2a02:6b8::1]:443"
		}
		_, _ = tr.httpClient.Transport.(*http.Transport).DialContext(context.Background(), "tcp", address)
		_, _ = tr.dialer.NetDialContext(context.Background(), "tcp", address)
		if httpAddress != want || wsAddress != want {
			t.Fatal("pin mapping changed a foreign hostname or port")
		}
	}
	_, _ = httpBase.DialContext(context.Background(), "tcp", "docs.yandex.ru:443")
	_, _ = wsBase.NetDialContext(context.Background(), "tcp", "docs.yandex.ru:443")
	if httpAddress != "docs.yandex.ru:443" || wsAddress != "docs.yandex.ru:443" {
		t.Fatal("original transport or dialer was modified")
	}
	plain := newBootstrapDialTransport(t)
	if plain.httpClient.Transport != nil || plain.dialer.NetDialContext != nil || plain.bootstrapDialAddress != "" {
		t.Fatal("default DNS configuration changed without opt-in")
	}
}

func bootstrapTLSCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Fixture"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		DNSNames: []string{"docs.yandex.ru"}, BasicConstraintsValid: true, IsCA: true,
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, roots
}

func TestBootstrapDialAddressPreservesHTTPSAndWSSIdentity(t *testing.T) {
	cert, roots := bootstrapTLSCertificate(t)
	var httpRequests, wsRequests, dials atomic.Int32
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "docs.yandex.ru" || r.TLS == nil || r.TLS.ServerName != "docs.yandex.ru" {
			t.Error("pinning changed HTTP Host or TLS SNI")
			http.Error(w, "wrong host", http.StatusBadRequest)
			return
		}
		if r.URL.Path == "/web-apps/apps/api/documents/api.js" {
			httpRequests.Add(1)
			_, _ = w.Write([]byte(`script.src="/2026.2.1-3364/editor.js"`))
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		wsRequests.Add(1)
		_ = conn.WriteMessage(websocket.TextMessage, []byte("fixture"))
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	server.StartTLS()
	defer server.Close()
	dialFixture := func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "87.250.251.212:443" {
			return nil, errors.New("fixture expected exact numeric IP mapping")
		}
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	tr := newBootstrapDialTransport(t)
	tr.httpClient.Transport = &http.Transport{DialContext: dialFixture, TLSClientConfig: &tls.Config{RootCAs: roots}}
	tr.dialer.NetDialContext = dialFixture
	tr.dialer.TLSClientConfig = &tls.Config{RootCAs: roots}
	if err := tr.SetBootstrapDialAddress("87.250.251.212"); err != nil {
		t.Fatal(err)
	}
	defer tr.httpClient.CloseIdleConnections()
	info, err := tr.fetchDocInfoContext(context.Background(), tr.url, "session-user")
	if err != nil {
		t.Fatal(err)
	}
	conn, _, err := tr.dialer.DialContext(context.Background(), info.WsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, msg, err := conn.ReadMessage()
	conn.Close()
	if err != nil || string(msg) != "fixture" || httpRequests.Load() != 1 || wsRequests.Load() != 1 || dials.Load() != 2 {
		t.Fatal("pinned HTTPS discovery and WSS exchange failed")
	}
	if tr.httpClient.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify || tr.dialer.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("pinning disabled TLS verification")
	}
	// The same fixture certificate must fail when its CA is not trusted.
	for _, kind := range []string{"https", "wss"} {
		untrusted := newBootstrapDialTransport(t)
		untrusted.httpClient.Transport = &http.Transport{DialContext: dialFixture}
		untrusted.dialer.NetDialContext = dialFixture
		if err := untrusted.SetBootstrapDialAddress("87.250.251.212"); err != nil {
			t.Fatal(err)
		}
		if kind == "https" {
			_, err = untrusted.fetchDocInfoContext(context.Background(), untrusted.url, "u")
		} else {
			_, _, err = untrusted.dialer.DialContext(context.Background(), info.WsURL, nil)
		}
		untrusted.httpClient.CloseIdleConnections()
		if err == nil {
			t.Fatal("pinning accepted an untrusted TLS certificate")
		}
	}
}
