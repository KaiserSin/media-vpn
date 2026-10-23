package streamrelay

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"universal-bypass-tool/transport"
)

// Synchronous delivery deliberately catches Send-under-state-mutex deadlocks.
type memoryTransport struct {
	mu   sync.Mutex
	cb   func([]byte)
	peer *memoryTransport
	hook func([]byte) ([][]byte, error)
}

func (t *memoryTransport) Start() error      { return nil }
func (t *memoryTransport) Stop() error       { return nil }
func (t *memoryTransport) IsConnected() bool { return true }
func (t *memoryTransport) Stats() transport.TransportStats {
	return transport.TransportStats{Connected: true}
}
func (t *memoryTransport) Receive(f func([]byte)) { t.mu.Lock(); t.cb = f; t.mu.Unlock() }
func (t *memoryTransport) Send(b []byte) error {
	t.mu.Lock()
	frames := [][]byte{b}
	var err error
	if t.hook != nil {
		frames, err = t.hook(b)
	}
	t.mu.Unlock()
	if err != nil {
		return err
	}
	for _, f := range frames {
		t.peer.mu.Lock()
		cb := t.peer.cb
		t.peer.mu.Unlock()
		if cb != nil {
			cb(f)
		}
	}
	return nil
}
func memoryPair() (*memoryTransport, *memoryTransport) {
	a, b := &memoryTransport{}, &memoryTransport{}
	a.peer = b
	b.peer = a
	return a, b
}
func testConfig() Config {
	return Config{Key: bytes.Repeat([]byte{0x5a}, 32), WindowBytes: 64 << 10, MaxBufferedBytes: 2 << 20, RetransmitTimeout: 25 * time.Millisecond, OpenTimeout: 2 * time.Second, IdleTimeout: 5 * time.Second, MaxRetries: 20}
}
func fixtureRelays(t *testing.T, cfg Config) (*Relay, *Relay, *memoryTransport, *memoryTransport) {
	t.Helper()
	a, b := memoryPair()
	c, e := NewClient(a, cfg)
	if e != nil {
		t.Fatal(e)
	}
	s, e := NewFixedExit(b, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close(); s.Close() })
	return c, s, a, b
}
func manualPair(t *testing.T, cfg Config) (*Conn, *Conn, *memoryTransport, *memoryTransport) {
	t.Helper()
	a, b, x, y := fixtureRelays(t, cfg)
	id := streamID{1}
	a.mu.Lock()
	c := a.newConnLocked(id)
	c.established = true
	c.remoteWindow = uint64(a.cfg.WindowBytes)
	a.mu.Unlock()
	b.mu.Lock()
	s := b.newConnLocked(id)
	s.established = true
	s.remoteWindow = uint64(b.cfg.WindowBytes)
	b.mu.Unlock()
	return c, s, x, y
}
func await(t *testing.T, p func() bool) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for !p() {
		if time.Now().After(until) {
			t.Fatal("condition timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestLossReorderDuplicateRoundtrip(t *testing.T) {
	for _, size := range []int{4096, 16384} {
		t.Run(fmt.Sprintf("chunk_%d", size), func(t *testing.T) {
			cfg := testConfig()
			cfg.ChunkSize = size
			c, s, a, b := fixtureRelays(t, cfg)
			var dials atomic.Int32
			s.dial = func(context.Context, string, string) (net.Conn, error) {
				dials.Add(1)
				x, y := net.Pipe()
				go func() { io.Copy(y, y); y.Close() }()
				return x, nil
			}
			chaos := func() func([]byte) ([][]byte, error) {
				seen := make(map[string]bool)
				var held []byte
				return func(p []byte) ([][]byte, error) {
					kind := p[4]
					off := binary.BigEndian.Uint64(p[24:32])
					key := string([]byte{kind}) + string(p[24:32])
					if (kind == kindOpen || kind == kindACK || kind == kindFIN || kind == kindData && off == 0) && !seen[key] {
						seen[key] = true
						return nil, nil
					}
					if kind == kindData && off == uint64(size) && !seen["held"] {
						seen["held"] = true
						held = append([]byte(nil), p...)
						return nil, nil
					}
					frames := [][]byte{p, p}
					if held != nil {
						frames = append(frames, held)
						held = nil
					}
					return frames, nil
				}
			}
			a.hook = chaos()
			b.hook = chaos()
			conn, e := c.DialTCP("10.203.0.1:443")
			if e != nil {
				t.Fatal(e)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			payload := bytes.Repeat([]byte("frame ordering preserves bytes / "), 12000)
			w := make(chan error, 1)
			go func() { _, e := conn.Write(payload); w <- e }()
			got := make([]byte, len(payload))
			if _, e = io.ReadFull(conn, got); e != nil {
				t.Fatal(e)
			}
			if e = <-w; e != nil {
				t.Fatal(e)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("corrupted byte stream")
			}
			if dials.Load() != 1 {
				t.Fatalf("duplicate OPEN created %d upstreams", dials.Load())
			}
			if c.Stats().Retransmits+s.Stats().Retransmits == 0 {
				t.Fatal("loss fixture did not exercise retry")
			}
		})
	}
}

func TestLostLastCreditUpdateRecovers(t *testing.T) {
	c, s, _, wire := manualPair(t, testConfig())
	c.SetDeadline(time.Now().Add(3 * time.Second))
	s.SetDeadline(time.Now().Add(3 * time.Second))
	data := bytes.Repeat([]byte{7}, 64<<10)
	if _, e := c.Write(data); e != nil {
		t.Fatal(e)
	}
	await(t, func() bool {
		c.r.mu.Lock()
		defer c.r.mu.Unlock()
		return c.acked == uint64(len(data)) && len(c.out) == 0
	})
	var dropped atomic.Bool
	wire.mu.Lock()
	wire.hook = func(p []byte) ([][]byte, error) {
		if p[4] == kindACK {
			consumed := binary.BigEndian.Uint64(p[36:44])
			// Keep the writer at a zero window until the full read has finished.
			// Otherwise a partial credit update can admit "next" before the
			// fixture ever drops the final update, making this test scheduler-dependent.
			if consumed > 0 && consumed < uint64(len(data)) {
				return nil, nil
			}
			if consumed == uint64(len(data)) && !dropped.Swap(true) {
				return nil, nil
			}
		}
		return [][]byte{p}, nil
	}
	wire.mu.Unlock()
	writeDone := make(chan error, 1)
	go func() { _, e := c.Write([]byte("next")); writeDone <- e }()
	got := make([]byte, len(data))
	if _, e := io.ReadFull(s, got); e != nil {
		t.Fatal(e)
	}
	if e := <-writeDone; e != nil {
		t.Fatal(e)
	}
	tail := make([]byte, 4)
	if _, e := io.ReadFull(s, tail); e != nil {
		t.Fatal(e)
	}
	if string(tail) != "next" || !dropped.Load() {
		t.Fatal("credit loss not recovered")
	}
}

func TestRejectedEnqueueRetainsExactFrame(t *testing.T) {
	c, s, wire, _ := manualPair(t, testConfig())
	var rejected atomic.Int32
	wire.mu.Lock()
	wire.hook = func(p []byte) ([][]byte, error) {
		if p[4] == kindData && rejected.Add(1) <= 4 {
			return nil, errors.New("queue full")
		}
		return [][]byte{p}, nil
	}
	wire.mu.Unlock()
	c.SetDeadline(time.Now().Add(time.Second))
	s.SetDeadline(time.Now().Add(time.Second))
	payload := bytes.Repeat([]byte{8}, 32768)
	if _, e := c.Write(payload); e != nil {
		t.Fatal(e)
	}
	got := make([]byte, len(payload))
	if _, e := io.ReadFull(s, got); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(got, payload) || c.r.Stats().SendErrors != 4 || c.r.Stats().Retransmits != 0 {
		t.Fatalf("local reject became network loss: %+v", c.r.Stats())
	}
}

func TestAuthenticationMalformedIsolation(t *testing.T) {
	r, _, _, _ := fixtureRelays(t, testConfig())
	id := streamID{9}
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, 65536)
	valid := encodeFrame(r.cfg.Key, frame{kind: kindOpen, direction: 1, id: id, data: data})
	bad := append([]byte(nil), valid...)
	bad[len(bad)-1] ^= 1
	r.receive(bad)
	bad = append([]byte(nil), valid...)
	bad[32] = 0xff
	r.receive(bad)
	r.receive([]byte("MDP1 unrelated document experiment"))
	r.receive([]byte("MBD1 unrelated batch"))
	oversize := encodeFrame(r.cfg.Key, frame{kind: kindData, direction: 1, id: id, data: make([]byte, maxPayload+1)})
	r.receive(oversize)
	bad = encodeFrame(r.cfg.Key, frame{kind: kindData, direction: 1, id: id, offset: ^uint64(0), data: []byte{1}})
	r.receive(bad)
	s := r.Stats()
	if s.AuthenticationFailures != 1 || s.MalformedFrames != 3 || s.ActiveStreams != 0 || s.BufferedBytes != 0 {
		t.Fatalf("unsafe frame accounting: %+v", s)
	}
	self := encodeFrame(r.cfg.Key, frame{kind: kindRST, direction: 0, id: id})
	r.receive(self)
	if r.Stats().FramesReceived != 0 {
		t.Fatal("self echo accepted")
	}
}

func TestDeadlinesCancellationAndShutdown(t *testing.T) {
	c, s, _, _ := manualPair(t, testConfig())
	c.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	if _, e := c.Read(make([]byte, 1)); !errors.Is(e, os.ErrDeadlineExceeded) {
		t.Fatalf("read deadline: %v", e)
	}
	c.SetDeadline(time.Time{})
	if _, e := c.Write(make([]byte, 64<<10)); e != nil {
		t.Fatal(e)
	}
	c.SetWriteDeadline(time.Now().Add(20 * time.Millisecond))
	if _, e := c.Write([]byte{1}); !errors.Is(e, os.ErrDeadlineExceeded) {
		t.Fatalf("write deadline: %v", e)
	}
	c.SetDeadline(time.Time{})
	done := make(chan error, 1)
	go func() { _, e := c.Write([]byte{2}); done <- e }()
	if e := c.CloseWrite(); e != nil {
		t.Fatal(e)
	}
	if e := <-done; !errors.Is(e, net.ErrClosed) {
		t.Fatalf("CloseWrite did not unblock write: %v", e)
	}
	c.r.Close()
	s.r.Close()
	if c.r.Stats().BufferedBytes != 0 || s.r.Stats().BufferedBytes != 0 {
		t.Fatal("shutdown leaked buffer budget")
	}
	a, _ := memoryPair()
	r, e := NewClient(a, testConfig())
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, e = r.DialTCPContext(ctx, "10.203.0.1:443"); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("dial cancellation: %v", e)
	}
	if r.Stats().ActiveStreams != 0 {
		t.Fatal("cancelled dial retained stream")
	}
}

func TestBudgetOverflowClosesAndReleases(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBufferedBytes = 64 << 10
	c, s, _, _ := manualPair(t, cfg)
	// Charge one relay's send budget while deliberately preventing delivery.
	c.r.mu.Lock()
	for n := 0; n < 16; n++ {
		if !c.r.allocateLocked(4096) {
			t.Fatal("fixture reserve")
		}
		c.ready = append(c.ready, &chunk{data: make([]byte, 4096), offset: uint64(n * 4096)})
	}
	c.r.mu.Unlock()
	f := encodeFrame(c.r.cfg.Key, frame{kind: kindData, direction: 1, id: c.id, data: []byte{1}})
	c.r.receive(f)
	st := c.r.Stats()
	if st.BufferedBytes != 0 || st.PeakBufferedBytes > cfg.MaxBufferedBytes || st.RejectedStreams != 1 || st.ActiveStreams != 0 {
		t.Fatalf("budget overflow did not fail bounded: %+v", st)
	}
	s.Close()
}

func TestConfigRejectsArbitraryUpstreamAndBadKey(t *testing.T) {
	a, _ := memoryPair()
	for _, up := range []string{"example.com:443", "8.8.8.8:443", "127.0.0.1:0"} {
		cfg := testConfig()
		cfg.Upstream = up
		if r, e := NewFixedExit(a, cfg); e == nil {
			r.Close()
			t.Fatalf("accepted upstream %s", up)
		}
	}
	cfg := testConfig()
	cfg.Key = make([]byte, 31)
	if _, e := NewClient(a, cfg); e == nil {
		t.Fatal("accepted short key")
	}
}

func TestFINReorderingLossAndFinalACKRetry(t *testing.T) {
	c, s, a, b := manualPair(t, testConfig())
	chaos := func() func([]byte) ([][]byte, error) {
		seen := map[byte]bool{}
		return func(p []byte) ([][]byte, error) {
			kind := p[4]
			target := kind == kindFIN || kind == kindACK && p[6]&flagFIN != 0 || kind == kindData && binary.BigEndian.Uint64(p[24:32]) == 0
			if target && !seen[kind] {
				seen[kind] = true
				return nil, nil
			}
			return [][]byte{p}, nil
		}
	}
	a.mu.Lock()
	a.hook = chaos()
	a.mu.Unlock()
	b.mu.Lock()
	b.hook = chaos()
	b.mu.Unlock()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	s.SetDeadline(time.Now().Add(3 * time.Second))
	payload := bytes.Repeat([]byte{0x33}, 32768)
	if _, e := c.Write(payload); e != nil {
		t.Fatal(e)
	}
	if e := c.CloseWrite(); e != nil {
		t.Fatal(e)
	}
	got, e := io.ReadAll(s)
	if e != nil || !bytes.Equal(got, payload) {
		t.Fatalf("FIN lost tail: %d/%d %v", len(got), len(payload), e)
	}
	if e = s.CloseWrite(); e != nil {
		t.Fatal(e)
	}
	if _, e = c.Read(make([]byte, 1)); !errors.Is(e, io.EOF) {
		t.Fatalf("reverse EOF: %v", e)
	}
	await(t, func() bool { return c.r.Stats().ActiveStreams == 0 && s.r.Stats().ActiveStreams == 0 })
	if c.r.Stats().BufferedBytes+s.r.Stats().BufferedBytes != 0 {
		t.Fatal("graceful FIN retained bytes")
	}
}

func TestMaxStreamsAndClosedOPENAreBounded(t *testing.T) {
	cfg := testConfig()
	cfg.MaxStreams = 1
	c, s, _, _ := manualPair(t, cfg)
	if _, e := c.r.DialTCP("10.203.0.1:443"); !errors.Is(e, ErrCapacity) {
		t.Fatalf("client stream limit: %v", e)
	}
	openData := make([]byte, 4)
	binary.BigEndian.PutUint32(openData, uint32(cfg.WindowBytes))
	other := encodeFrame(s.r.cfg.Key, frame{kind: kindOpen, direction: 0, id: streamID{2}, data: openData})
	s.r.receive(other)
	if got := s.r.Stats(); got.ActiveStreams != 1 || got.RejectedStreams != 1 {
		t.Fatalf("exit stream limit: %+v", got)
	}
	s.Close()
	duplicate := encodeFrame(s.r.cfg.Key, frame{kind: kindOpen, direction: 0, id: s.id, data: openData})
	s.r.receive(duplicate)
	if s.r.Stats().ActiveStreams != 0 {
		t.Fatal("closed OPEN reopened upstream")
	}
}
