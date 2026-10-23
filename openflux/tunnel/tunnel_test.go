package tunnel

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"universal-bypass-tool/transport"
)

// Packets cross only in-memory queues; the only host socket is a test listener
// bound to loopback on an ephemeral port. No external service is contacted.
type memoryTransport struct {
	peer     *memoryTransport
	queue    chan []byte
	done     chan struct{}
	once     sync.Once
	mu       sync.RWMutex
	callback func([]byte)
	wg       sync.WaitGroup
}

func (m *memoryTransport) Start() error {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			select {
			case <-m.done:
				return
			case data := <-m.queue:
				m.mu.RLock()
				cb := m.callback
				m.mu.RUnlock()
				if cb != nil {
					cb(data)
				}
			}
		}
	}()
	return nil
}
func (m *memoryTransport) Stop() error { m.once.Do(func() { close(m.done) }); m.wg.Wait(); return nil }
func (m *memoryTransport) Send(data []byte) error {
	select {
	case <-m.done:
		return net.ErrClosed
	case <-m.peer.done:
		return net.ErrClosed
	case m.peer.queue <- append([]byte(nil), data...):
		return nil
	}
}
func (m *memoryTransport) Receive(cb func([]byte))         { m.mu.Lock(); defer m.mu.Unlock(); m.callback = cb }
func (m *memoryTransport) IsConnected() bool               { return true }
func (m *memoryTransport) Stats() transport.TransportStats { return transport.TransportStats{} }

func memoryPair(t *testing.T) (*memoryTransport, *memoryTransport) {
	t.Helper()
	a := &memoryTransport{queue: make(chan []byte, 2048), done: make(chan struct{})}
	b := &memoryTransport{queue: make(chan []byte, 2048), done: make(chan struct{})}
	a.peer = b
	b.peer = a
	a.Start()
	b.Start()
	t.Cleanup(func() { a.Stop(); b.Stop() })
	return a, b
}

func tunnelPair(t *testing.T, cfg FixedExitConfig) (*TCPTunnel, *TCPTunnel) {
	t.Helper()
	a, b := memoryPair(t)
	exit, err := NewFixedExitTunnel(b, cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClientTunnel(a)
	if err != nil {
		exit.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(); exit.Close() })
	return client, exit
}

func loopbackListener(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func TestFixedExitByteRoundtripAndHalfClose(t *testing.T) {
	listener := loopbackListener(t)
	payload := bytes.Repeat([]byte{0, 1, 2, 3, 127, 128, 254, 255}, 32768)
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		got, err := io.ReadAll(conn)
		if err != nil {
			serverErr <- err
			return
		}
		if !bytes.Equal(got, payload) {
			serverErr <- errors.New("upstream received different bytes")
			return
		}
		_, err = conn.Write(append([]byte("reply:"), got...))
		serverErr <- err
	}()
	client, _ := tunnelPair(t, FixedExitConfig{VirtualIP: "10.203.0.1", VirtualPort: 443, Upstream: listener.Addr().String()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := client.DialTCPContext(ctx, "10.203.0.1:443")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err = conn.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, append([]byte("reply:"), payload...)) {
		t.Fatalf("reply differs: %d bytes", len(got))
	}
	if err = <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestFixedExitRejectsOtherDestinations(t *testing.T) {
	l := loopbackListener(t)
	var accepted atomic.Int64
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			c.Close()
		}
	}()
	client, _ := tunnelPair(t, FixedExitConfig{VirtualIP: "10.203.0.1", VirtualPort: 443, Upstream: l.Addr().String()})
	for _, dest := range []string{"10.203.0.1:444", "10.203.0.2:443", "127.0.0.1:443", "192.0.2.10:443"} {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		c, err := client.DialTCPContext(ctx, dest)
		cancel()
		if err == nil {
			c.Close()
			t.Fatalf("forbidden destination accepted: %s", dest)
		}
	}
	if n := accepted.Load(); n != 0 {
		t.Fatalf("forbidden request reached upstream: %d", n)
	}
}

func TestFixedExitConfigurationFailsClosed(t *testing.T) {
	a, _ := memoryPair(t)
	base := FixedExitConfig{VirtualIP: "10.203.0.1", VirtualPort: 443, Upstream: "127.0.0.1:443"}
	for _, upstream := range []string{"192.0.2.10:443", "localhost:443", "127.0.0.1:0", "[::]:443", "127.0.0.1:65536"} {
		cfg := base
		cfg.Upstream = upstream
		if tun, err := NewFixedExitTunnel(a, cfg); err == nil {
			tun.Close()
			t.Fatalf("accepted upstream %q", upstream)
		}
	}
	if tun := NewTCPTunnel(a, true); tun != nil {
		tun.Close()
		t.Fatal("legacy raw exit was enabled")
	}
}

func TestCloseReleasesActiveUpstream(t *testing.T) {
	l := loopbackListener(t)
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err == nil {
			accepted <- c
		}
	}()
	client, exit := tunnelPair(t, FixedExitConfig{VirtualIP: "10.203.0.1", VirtualPort: 443, Upstream: l.Addr().String()})
	c, err := client.DialTCP("10.203.0.1:443")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var upstream net.Conn
	select {
	case upstream = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream not opened")
	}
	defer upstream.Close()
	done := make(chan struct{})
	go func() { exit.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close stalled with an active stream")
	}
	upstream.SetReadDeadline(time.Now().Add(time.Second))
	if _, err = upstream.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("upstream not closed: %v", err)
	}
}

func TestFixedExitIdleTimeout(t *testing.T) {
	l := loopbackListener(t)
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err == nil {
			accepted <- c
		}
	}()
	client, _ := tunnelPair(t, FixedExitConfig{VirtualIP: "10.203.0.1", VirtualPort: 443, Upstream: l.Addr().String(), IdleTimeout: 100 * time.Millisecond})
	c, err := client.DialTCP("10.203.0.1:443")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	select {
	case up := <-accepted:
		defer up.Close()
		up.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err = up.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			t.Fatalf("idle upstream remained open: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("upstream not opened")
	}
}

func TestFixedExitConnectionLimit(t *testing.T) {
	l := loopbackListener(t)
	accepted := make(chan net.Conn, 4)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()
	client, _ := tunnelPair(t, FixedExitConfig{
		VirtualIP: "10.203.0.1", VirtualPort: 443, Upstream: l.Addr().String(), MaxConnections: 1,
	})
	first, err := client.DialTCP("10.203.0.1:443")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	select {
	case up := <-accepted:
		defer up.Close()
	case <-time.After(time.Second):
		t.Fatal("first connection did not reach upstream")
	}
	second, err := client.DialTCP("10.203.0.1:443")
	if err == nil {
		defer second.Close()
		second.SetReadDeadline(time.Now().Add(time.Second))
		if _, err = second.Read(make([]byte, 1)); err == nil {
			t.Fatal("second stream not closed")
		}
		if e, ok := err.(net.Error); ok && e.Timeout() {
			t.Fatal("second stream remained open at capacity")
		}
	}
	select {
	case up := <-accepted:
		up.Close()
		t.Fatal("connection limit allowed a second upstream")
	default:
	}
}
