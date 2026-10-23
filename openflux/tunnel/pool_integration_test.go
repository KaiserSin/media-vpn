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
	"universal-bypass-tool/transport/docpool"
)

type poolMemoryLane struct {
	peer           *poolMemoryLane
	queue          chan []byte
	done           chan struct{}
	once           sync.Once
	wg             sync.WaitGroup
	mu             sync.RWMutex
	cb             func([]byte)
	running, up    atomic.Bool
	sent, received atomic.Uint64
}

func (m *poolMemoryLane) Start() error {
	m.running.Store(true)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			select {
			case <-m.done:
				return
			case b := <-m.queue:
				m.mu.RLock()
				cb := m.cb
				m.mu.RUnlock()
				if cb != nil && m.IsConnected() {
					m.received.Add(uint64(len(b)))
					cb(b)
				}
			}
		}
	}()
	return nil
}
func (m *poolMemoryLane) Stop() error {
	m.running.Store(false)
	m.once.Do(func() { close(m.done) })
	m.wg.Wait()
	return nil
}
func (m *poolMemoryLane) IsRunning() bool   { return m.running.Load() }
func (m *poolMemoryLane) IsConnected() bool { return m.running.Load() && m.up.Load() }
func (m *poolMemoryLane) Send(b []byte) error {
	if !m.IsConnected() || !m.peer.IsConnected() {
		return net.ErrClosed
	}
	select {
	case m.peer.queue <- append([]byte(nil), b...):
		m.sent.Add(uint64(len(b)))
		return nil
	default:
		return errors.New("bounded test queue full")
	}
}
func (m *poolMemoryLane) Receive(cb func([]byte)) { m.mu.Lock(); m.cb = cb; m.mu.Unlock() }
func (m *poolMemoryLane) Stats() transport.TransportStats {
	return transport.TransportStats{BytesSent: m.sent.Load(), BytesReceived: m.received.Load(), Connected: m.IsConnected()}
}

func TestDocumentPoolTCPRoundtrip(t *testing.T) {
	for _, variant := range []struct {
		mode string
		down bool
	}{{"flow", false}, {"stripe", false}, {"stripe", true}} {
		name := variant.mode
		if variant.down {
			name += "_one_channel_down"
		}
		t.Run(name, func(t *testing.T) {
			const count = 3
			left, right := make([]transport.Transport, count), make([]transport.Transport, count)
			for i := 0; i < count; i++ {
				a := &poolMemoryLane{queue: make(chan []byte, 128), done: make(chan struct{})}
				b := &poolMemoryLane{queue: make(chan []byte, 128), done: make(chan struct{})}
				a.peer = b
				b.peer = a
				a.up.Store(true)
				b.up.Store(true)
				if variant.down && i == 0 {
					a.up.Store(false)
					b.up.Store(false)
				}
				left[i], right[i] = a, b
			}
			ready := count
			if variant.down {
				ready--
			}
			cfg := docpool.Config{Mode: variant.mode, MinReady: ready, PoolID: [16]byte{7}}
			p, err := docpool.New(left, cfg)
			if err != nil {
				t.Fatal(err)
			}
			q, err := docpool.New(right, cfg)
			if err != nil {
				t.Fatal(err)
			}
			listener := loopbackListener(t)
			exit, err := NewFixedExitTunnel(q, FixedExitConfig{VirtualIP: "10.203.0.1", VirtualPort: 443, Upstream: listener.Addr().String()})
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewClientTunnel(p)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { p.Stop(); q.Stop(); client.Close(); exit.Close() })
			if err = p.Start(); err != nil {
				t.Fatal(err)
			}
			if err = q.Start(); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for (!p.IsConnected() || !q.IsConnected()) && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if !p.IsConnected() || !q.IsConnected() {
				t.Fatal("pool not ready")
			}
			repetitions := 65536 // 512 KiB in each direction without race instrumentation.
			if testing.Short() {
				repetitions = 8192
			} // 64 KiB for bounded race runs.
			payload := bytes.Repeat([]byte{0, 1, 2, 3, 127, 128, 254, 255}, repetitions)
			serverErr := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					serverErr <- err
					return
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(15 * time.Second))
				got, err := io.ReadAll(conn)
				if err == nil && !bytes.Equal(got, payload) {
					err = errors.New("exit bytes differ")
				}
				if err == nil {
					_, err = conn.Write(got)
				}
				serverErr <- err
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			conn, err := client.DialTCPContext(ctx, "10.203.0.1:443")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(15 * time.Second))
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
			if !bytes.Equal(got, payload) {
				t.Fatalf("reply bytes differ: %d", len(got))
			}
			if err = <-serverErr; err != nil {
				t.Fatal(err)
			}
			if variant.mode == "stripe" {
				for i, s := range p.LaneStats() {
					if variant.down && i == 0 {
						continue
					}
					if s.Transport.BytesSent == 0 || s.Transport.BytesReceived == 0 {
						t.Fatalf("lane %d not used bidirectionally", i+1)
					}
				}
			}
		})
	}
}
