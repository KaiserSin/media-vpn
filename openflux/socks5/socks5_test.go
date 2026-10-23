package socks5

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

type dialFunc func(string) (net.Conn, error)

func (f dialFunc) DialTCP(address string) (net.Conn, error) { return f(address) }

type contextDialer struct{ entered, cancelled chan struct{} }

func (d *contextDialer) DialTCP(string) (net.Conn, error) { panic("context dialer was not used") }
func (d *contextDialer) DialTCPContext(ctx context.Context, _ string) (net.Conn, error) {
	close(d.entered)
	<-ctx.Done()
	close(d.cancelled)
	return nil, ctx.Err()
}

func pipeClient(t *testing.T, s *SOCKS5Server) (net.Conn, <-chan struct{}) {
	t.Helper()
	client, server := net.Pipe()
	client.SetDeadline(time.Now().Add(3 * time.Second))
	done := make(chan struct{})
	go func() { defer close(done); s.handleConnection(server) }()
	t.Cleanup(func() { client.Close(); s.Close(); awaitDone(t, done) })
	return client, done
}
func awaitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("connection handler did not stop")
	}
}
func send(t *testing.T, c net.Conn, data []byte) {
	t.Helper()
	if err := writeAll(c, data); err != nil {
		t.Fatal(err)
	}
}
func recv(t *testing.T, c net.Conn, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := io.ReadFull(c, b); err != nil {
		t.Fatal(err)
	}
	return b
}
func greet(t *testing.T, c net.Conn) {
	t.Helper()
	send(t, c, []byte{5, 1, 0})
	if got := recv(t, c, 2); !bytes.Equal(got, []byte{5, 0}) {
		t.Fatalf("greeting %x", got)
	}
}
func expectReply(t *testing.T, c net.Conn, code byte) []byte {
	t.Helper()
	b := recv(t, c, 10)
	if b[0] != 5 || b[1] != code || b[2] != 0 || b[3] != 1 {
		t.Fatalf("reply %x, wanted code %d", b, code)
	}
	return b
}
func ipv4Request(ip net.IP, port uint16) []byte {
	b := []byte{5, 1, 0, 1, 0, 0, 0, 0, 0, 0}
	copy(b[4:8], ip.To4())
	binary.BigEndian.PutUint16(b[8:], port)
	return b
}
func domainRequest(host string, port uint16) []byte {
	b := append([]byte{5, 1, 0, 3, byte(len(host))}, []byte(host)...)
	return append(b, byte(port>>8), byte(port))
}
func echoDialer(addresses chan<- string) Dialer {
	return dialFunc(func(address string) (net.Conn, error) {
		if addresses != nil {
			addresses <- address
		}
		a, b := net.Pipe()
		go func() { defer b.Close(); io.Copy(b, b) }()
		return a, nil
	})
}

func TestFragmentedHandshakeAndRequest(t *testing.T) {
	for _, tc := range []struct {
		name    string
		request []byte
		address string
	}{
		{"ipv4", ipv4Request(net.IPv4(192, 0, 2, 10), 443), "192.0.2.10:443"},
		{"short-domain", domainRequest("x", 80), "x:80"},
		{"domain", domainRequest("example.test", 443), "example.test:443"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addresses := make(chan string, 1)
			s := NewSOCKS5Server("127.0.0.1:0", echoDialer(addresses))
			c, _ := pipeClient(t, s)
			for _, v := range []byte{5, 2, 2, 0} {
				send(t, c, []byte{v})
			}
			if got := recv(t, c, 2); !bytes.Equal(got, []byte{5, 0}) {
				t.Fatalf("greeting %x", got)
			}
			for _, v := range tc.request {
				send(t, c, []byte{v})
			}
			expectReply(t, c, 0)
			if got := <-addresses; got != tc.address {
				t.Fatalf("dial address %q", got)
			}
			body := []byte("payload after fragmented SOCKS handshake")
			send(t, c, body)
			if got := recv(t, c, len(body)); !bytes.Equal(got, body) {
				t.Fatalf("echo %q", got)
			}
		})
	}
}

func TestPipelinedHandshakeRequestAndPayload(t *testing.T) {
	s := NewSOCKS5Server("127.0.0.1:0", echoDialer(nil))
	c, _ := pipeClient(t, s)
	payload := []byte("pipelined first application bytes")
	packet := append([]byte{5, 1, 0}, domainRequest("example.test", 443)...)
	packet = append(packet, payload...)
	written := make(chan error, 1)
	go func() { written <- writeAll(c, packet) }()
	if got := recv(t, c, 2); !bytes.Equal(got, []byte{5, 0}) {
		t.Fatalf("greeting %x", got)
	}
	expectReply(t, c, 0)
	if got := recv(t, c, len(payload)); !bytes.Equal(got, payload) {
		t.Fatalf("payload %q", got)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}

func TestMaximumMethodsAndDomainDoNotOverflow(t *testing.T) {
	addresses := make(chan string, 1)
	s := NewSOCKS5Server("127.0.0.1:0", dialFunc(func(a string) (net.Conn, error) { addresses <- a; return nil, syscall.ECONNREFUSED }))
	c, _ := pipeClient(t, s)
	methods := bytes.Repeat([]byte{2}, 255)
	methods[254] = 0
	send(t, c, append([]byte{5, 255}, methods...))
	if got := recv(t, c, 2); !bytes.Equal(got, []byte{5, 0}) {
		t.Fatalf("greeting %x", got)
	}
	host := strings.Repeat("a", 255)
	send(t, c, domainRequest(host, 443))
	expectReply(t, c, 5)
	if got := <-addresses; got != host+":443" {
		t.Fatalf("domain length/address altered: %q", got)
	}
}

func TestUnsupportedMethods(t *testing.T) {
	for _, greeting := range [][]byte{{5, 1, 2}, {5, 0}, {5, 2, 1, 2}} {
		t.Run(strconv.Itoa(int(greeting[1]))+"-methods", func(t *testing.T) {
			s := NewSOCKS5Server("127.0.0.1:0", dialFunc(func(string) (net.Conn, error) { t.Error("unexpected dial"); return nil, errors.New("unexpected") }))
			c, done := pipeClient(t, s)
			send(t, c, greeting)
			if got := recv(t, c, 2); !bytes.Equal(got, []byte{5, 255}) {
				t.Fatalf("method response %x", got)
			}
			awaitDone(t, done)
		})
	}
}

func TestMalformedAndUnsupportedRequests(t *testing.T) {
	cases := []struct {
		name    string
		request []byte
		code    byte
	}{
		{"bind", []byte{5, 2, 0, 1}, 7},
		{"udp-associate", []byte{5, 3, 0, 1}, 7},
		{"ipv6", []byte{5, 1, 0, 4}, 8},
		{"unknown-address", []byte{5, 1, 0, 99}, 8},
		{"wrong-version", []byte{4, 1, 0, 1}, 1},
		{"reserved-byte", []byte{5, 1, 1, 1}, 1},
		{"empty-domain", []byte{5, 1, 0, 3, 0}, 1},
		{"nul-domain", []byte{5, 1, 0, 3, 2, 'a', 0}, 1},
		{"zero-port", ipv4Request(net.IPv4(127, 0, 0, 1), 0), 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSOCKS5Server("127.0.0.1:0", dialFunc(func(string) (net.Conn, error) { t.Error("unexpected dial"); return nil, errors.New("unexpected") }))
			c, done := pipeClient(t, s)
			greet(t, c)
			send(t, c, tc.request)
			expectReply(t, c, tc.code)
			awaitDone(t, done)
		})
	}
}

func TestTruncatedDomainEndsWithoutPanic(t *testing.T) {
	s := NewSOCKS5Server("127.0.0.1:0", echoDialer(nil))
	c, done := pipeClient(t, s)
	greet(t, c)
	send(t, c, []byte{5, 1, 0, 3, 255, 'a', 'b'})
	c.Close()
	awaitDone(t, done)
}

func TestHandshakeTimeout(t *testing.T) {
	s := NewSOCKS5Server("127.0.0.1:0", echoDialer(nil))
	s.handshakeTimeout = 40 * time.Millisecond
	c, done := pipeClient(t, s)
	send(t, c, []byte{5})
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("incomplete handshake did not close")
	}
	awaitDone(t, done)
}

func TestDialTimeoutCancelsContext(t *testing.T) {
	d := &contextDialer{make(chan struct{}), make(chan struct{})}
	s := NewSOCKS5Server("127.0.0.1:0", d)
	s.dialTimeout = 40 * time.Millisecond
	c, done := pipeClient(t, s)
	greet(t, c)
	send(t, c, domainRequest("example.test", 443))
	expectReply(t, c, 4)
	awaitDone(t, d.cancelled)
	awaitDone(t, done)
}

func TestCloseCancelsPendingDial(t *testing.T) {
	d := &contextDialer{make(chan struct{}), make(chan struct{})}
	s := NewSOCKS5Server("127.0.0.1:0", d)
	c, done := pipeClient(t, s)
	greet(t, c)
	send(t, c, domainRequest("example.test", 443))
	awaitDone(t, d.entered)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	awaitDone(t, d.cancelled)
	awaitDone(t, done)
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("client remained open after Close")
	}
}

func TestLateLegacyDialIsClosed(t *testing.T) {
	release := make(chan struct{})
	returned := make(chan struct{})
	a, b := net.Pipe()
	defer b.Close()
	s := NewSOCKS5Server("127.0.0.1:0", dialFunc(func(string) (net.Conn, error) { <-release; close(returned); return a, nil }))
	s.dialTimeout = 40 * time.Millisecond
	c, done := pipeClient(t, s)
	greet(t, c)
	send(t, c, domainRequest("example.test", 443))
	expectReply(t, c, 4)
	close(release)
	awaitDone(t, returned)
	awaitDone(t, done)
	b.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := b.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("late connection not closed: %v", err)
	}
}

func TestCloseActiveRelay(t *testing.T) {
	s := NewSOCKS5Server("127.0.0.1:0", echoDialer(nil))
	c, done := pipeClient(t, s)
	greet(t, c)
	send(t, c, domainRequest("example.test", 443))
	expectReply(t, c, 0)
	s.Close()
	awaitDone(t, done)
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("relay client remained open")
	}
}

func TestTCPIntegrationHalfCloseAndListenerShutdown(t *testing.T) {
	upstream, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	upstreamDone := make(chan error, 1)
	go func() {
		c, err := upstream.Accept()
		if err != nil {
			upstreamDone <- err
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(3 * time.Second))
		body, err := io.ReadAll(c)
		if err == nil {
			err = writeAll(c, append([]byte("response:"), body...))
		}
		upstreamDone <- err
	}()
	s := NewSOCKS5Server("127.0.0.1:0", dialFunc(func(address string) (net.Conn, error) { return net.DialTimeout("tcp4", address, time.Second) }))
	defer s.Close()
	started := make(chan error, 1)
	go func() { started <- s.Start() }()
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for s.Addr() == nil {
		select {
		case err := <-started:
			t.Fatalf("Start: %v", err)
		case <-deadline:
			t.Fatal("no listener")
		case <-tick.C:
		}
	}
	addr := s.Addr().String()
	if err := s.Start(); err == nil {
		t.Fatal("second Start succeeded")
	}
	c0, err := net.DialTimeout("tcp4", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c0.Close()
	c := c0.(*net.TCPConn)
	c.SetDeadline(time.Now().Add(3 * time.Second))
	greet(t, c)
	target := upstream.Addr().(*net.TCPAddr)
	send(t, c, ipv4Request(target.IP, uint16(target.Port)))
	reply := expectReply(t, c, 0)
	if binary.BigEndian.Uint16(reply[8:10]) == 0 {
		t.Fatal("missing bound port")
	}
	send(t, c, []byte("request body"))
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "response:request body" {
		t.Fatalf("half-close truncated response: %q", body)
	}
	if err := <-upstreamDone; err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-started:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not stop")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if c, err := net.DialTimeout("tcp4", addr, 100*time.Millisecond); err == nil {
		c.Close()
		t.Fatal("listener remained open")
	}
}

func TestCloseBeforeStart(t *testing.T) {
	s := NewSOCKS5Server("127.0.0.1:0", echoDialer(nil))
	s.Close()
	if err := s.Start(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Start after Close: %v", err)
	}
}
