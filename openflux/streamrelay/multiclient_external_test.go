package streamrelay_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"universal-bypass-tool/streamrelay"
	"universal-bypass-tool/transport"
)

// A shared document is a broadcast medium, not a point-to-point connection.
// Each bounded inbox has its own asynchronous worker; Send never invokes a
// relay callback. Self-echo is included deliberately, and frames are opaque.
type multiExternalBus struct {
	mu        sync.Mutex
	endpoints []*multiExternalEndpoint
}

type multiExternalEndpoint struct {
	bus      *multiExternalBus
	inbox    chan []byte
	stop     chan struct{}
	done     chan struct{}
	start    sync.Once
	stopOnce sync.Once
	started  atomic.Bool
	closed   atomic.Bool
	mu       sync.RWMutex
	callback func([]byte)
	sent     atomic.Uint64
	received atomic.Uint64
	bytesOut atomic.Uint64
	bytesIn  atomic.Uint64
}

var _ transport.Transport = (*multiExternalEndpoint)(nil)

func (b *multiExternalBus) endpoint(t *testing.T) *multiExternalEndpoint {
	t.Helper()
	e := &multiExternalEndpoint{bus: b, inbox: make(chan []byte, 64), stop: make(chan struct{}), done: make(chan struct{})}
	b.mu.Lock()
	b.endpoints = append(b.endpoints, e)
	b.mu.Unlock()
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		e.Stop()
		select {
		case <-e.done:
		case <-time.After(externalTimeout):
			t.Error("broadcast receive worker did not stop")
		}
	})
	return e
}

func (e *multiExternalEndpoint) Start() error {
	if e.closed.Load() {
		return net.ErrClosed
	}
	e.start.Do(func() {
		e.started.Store(true)
		go func() {
			defer close(e.done)
			for {
				select {
				case <-e.stop:
					return
				case frame := <-e.inbox:
					timer := time.NewTimer(250 * time.Microsecond)
					select {
					case <-e.stop:
						timer.Stop()
						return
					case <-timer.C:
					}
					e.mu.RLock()
					callback := e.callback
					e.mu.RUnlock()
					if callback != nil {
						e.received.Add(1)
						e.bytesIn.Add(uint64(len(frame)))
						callback(frame)
					}
				}
			}
		}()
	})
	return nil
}

func (e *multiExternalEndpoint) Stop() error {
	e.stopOnce.Do(func() {
		e.closed.Store(true)
		e.started.Store(false)
		close(e.stop)
	})
	return nil
}

func (e *multiExternalEndpoint) Receive(callback func([]byte)) {
	e.mu.Lock()
	e.callback = callback
	e.mu.Unlock()
}

func (e *multiExternalEndpoint) IsConnected() bool {
	return e.started.Load() && !e.closed.Load()
}

func (e *multiExternalEndpoint) Send(frame []byte) error {
	if !e.IsConnected() {
		return net.ErrClosed
	}
	if len(frame) > 64*1024 {
		return errors.New("broadcast fixture frame exceeds bound")
	}
	e.bus.mu.Lock()
	defer e.bus.mu.Unlock()
	// Checking capacity under the send lock makes enqueue all-or-nothing.
	// Receivers can only free capacity until this sender has enqueued.
	var recipients []*multiExternalEndpoint
	for _, peer := range e.bus.endpoints {
		if peer.IsConnected() {
			if len(peer.inbox) == cap(peer.inbox) {
				return errExternalQueueFull
			}
			recipients = append(recipients, peer)
		}
	}
	for _, peer := range recipients {
		peer.inbox <- append([]byte(nil), frame...)
	}
	e.sent.Add(1)
	e.bytesOut.Add(uint64(len(frame)))
	return nil
}

func (e *multiExternalEndpoint) Stats() transport.TransportStats {
	return transport.TransportStats{PacketsSent: e.sent.Load(), PacketsRecv: e.received.Load(),
		BytesSent: e.bytesOut.Load(), BytesReceived: e.bytesIn.Load(), Connected: e.IsConnected()}
}

func multiExternalExchange(ctx context.Context, conn net.Conn, payload []byte) error {
	written := make(chan error, 1)
	go func() { written <- externalWriteAll(conn, payload) }()
	received := make([]byte, len(payload))
	_, readErr := io.ReadFull(conn, received)
	if readErr != nil {
		conn.Close()
	}
	select {
	case writeErr := <-written:
		if readErr != nil {
			return fmt.Errorf("read concurrent echo: %w", readErr)
		}
		if writeErr != nil {
			return fmt.Errorf("write concurrent echo: %w", writeErr)
		}
	case <-ctx.Done():
		conn.Close()
		return ctx.Err()
	}
	if sha256.Sum256(received) != sha256.Sum256(payload) {
		return errors.New("shared document mixed, lost or reordered another client's bytes")
	}
	return nil
}

func TestExternalSharedDocsMultipleClientsAndRestart(t *testing.T) {
	server := externalEcho(t, nil)
	cfg := externalConfig(server.address, 16384)
	cfg.MaxBufferedBytes = 4 << 20
	// A stopped helper cannot necessarily flush a final RST. The real exit
	// must eventually expire that abandoned stream while live peers continue.
	cfg.IdleTimeout = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), externalTimeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	bus := &multiExternalBus{}
	exit, err := streamrelay.NewFixedExit(bus.endpoint(t), cfg)
	if err != nil {
		t.Fatal(err)
	}
	externalRelayCleanup(t, exit)
	newClient := func() (*streamrelay.Relay, *multiExternalEndpoint) {
		t.Helper()
		endpoint := bus.endpoint(t)
		relay, err := streamrelay.NewClient(endpoint, cfg)
		if err != nil {
			t.Fatal(err)
		}
		externalRelayCleanup(t, relay)
		return relay, endpoint
	}
	first, firstEndpoint := newClient()
	second, _ := newClient()
	idle, idleEndpoint := newClient()
	relays := []*streamrelay.Relay{exit, first, second, idle}
	check := func() {
		t.Helper()
		for _, relay := range relays {
			stats := relay.Stats()
			if err := externalCheckBudget(stats, cfg.MaxBufferedBytes); err != nil {
				t.Fatal(err)
			}
			if stats.AuthenticationFailures != 0 || stats.MalformedFrames != 0 || stats.RejectedStreams != 0 {
				t.Fatalf("valid shared-key traffic was rejected: %+v", stats)
			}
		}
		// An idle subscriber sees other clients' valid server frames. It must
		// never emit RST, ACK, or any other response for unknown stream IDs.
		if idleEndpoint.Stats().PacketsSent != 0 || idle.Stats().FramesSent != 0 ||
			idle.Stats().ActiveStreams != 0 || idle.Stats().BufferedBytes != 0 {
			t.Fatal("idle subscriber responded to or allocated a foreign stream")
		}
	}
	dial := func(relay *streamrelay.Relay) net.Conn {
		t.Helper()
		conn, err := relay.DialTCPContext(ctx, cfg.VirtualAddress)
		if err != nil {
			t.Fatalf("shared document dial: %v", err)
		}
		t.Cleanup(func() { conn.Close() })
		if err := conn.SetDeadline(deadline); err != nil {
			t.Fatal(err)
		}
		return conn
	}
	payload := func(size int) []byte {
		t.Helper()
		data := make([]byte, size)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		return data
	}
	a, b := dial(first), dial(second)
	size := 512 << 10
	if testing.Short() {
		size = 64 << 10
	}
	results := make(chan error, 2)
	for _, conn := range []net.Conn{a, b} {
		data := payload(size)
		go func(conn net.Conn, data []byte) { results <- multiExternalExchange(ctx, conn, data) }(conn, data)
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for done := 0; done < 2; {
		select {
		case err := <-results:
			done++
			if err != nil {
				t.Fatal(err)
			}
		case <-ticker.C:
			check()
		case <-ctx.Done():
			t.Fatal("simultaneous clients exceeded deadline")
		}
	}
	check()
	if idle.Stats().FramesReceived == 0 || idleEndpoint.Stats().PacketsRecv == 0 {
		t.Fatal("idle subscriber did not actually receive foreign broadcasts")
	}
	// Keep the exact second TCP connection alive and busy throughout closing
	// the first helper and creating a new client with freshly generated IDs.
	keepGoing := make(chan struct{})
	var stopOnce sync.Once
	stopTraffic := func() { stopOnce.Do(func() { close(keepGoing) }) }
	defer stopTraffic()
	progress := atomic.Uint64{}
	backgroundDone := make(chan error, 1)
	backgroundPayload := payload(32 << 10)
	go func() {
		for {
			select {
			case <-keepGoing:
				backgroundDone <- nil
				return
			default:
			}
			// Different successive messages also detect stale duplicated data.
			backgroundPayload[0]++
			if err := multiExternalExchange(ctx, b, backgroundPayload); err != nil {
				backgroundDone <- err
				return
			}
			progress.Add(1)
		}
	}()
	for progress.Load() == 0 {
		select {
		case err := <-backgroundDone:
			t.Fatalf("second client's existing connection failed: %v", err)
		case <-ticker.C:
			check()
		case <-ctx.Done():
			t.Fatal("second client made no progress before restart")
		}
	}
	beforeRestart := progress.Load()
	closed := make(chan error, 1)
	go func() { closed <- first.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("first client Close hung")
	}
	firstEndpoint.Stop()
	if stats := first.Stats(); stats.ActiveStreams != 0 || stats.BufferedBytes != 0 {
		t.Fatalf("closed client retained local streams or buffers: %+v", stats)
	}
	restarted, _ := newClient()
	relays = append(relays, restarted)
	restartedConn := dial(restarted)
	if err := multiExternalExchange(ctx, restartedConn, payload(size)); err != nil {
		t.Fatalf("restarted client: %v", err)
	}
	stopTraffic()
	select {
	case err := <-backgroundDone:
		if err != nil {
			t.Fatalf("first client's shutdown/restart broke second client's TCP stream: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("second client's ongoing transfer did not finish")
	}
	if progress.Load() <= beforeRestart {
		t.Fatal("second client made no verified progress during restart")
	}
	check()
	// A final exchange uses that same connection after the restarted client
	// has sent traffic, without redialing or silently reconnecting it.
	if err := multiExternalExchange(ctx, b, payload(4096)); err != nil {
		t.Fatalf("second client's original connection after restart: %v", err)
	}
	b.Close()
	restartedConn.Close()
	for {
		check()
		allReleased := server.active.Load() == 0
		for _, relay := range relays {
			stats := relay.Stats()
			allReleased = allReleased && stats.ActiveStreams == 0 && stats.BufferedBytes == 0
		}
		if allReleased {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("shared clients did not release relay/upstream resources: exit=%+v upstream=%d",
				exit.Stats(), server.active.Load())
		}
	}
	if server.accepted.Load() != 3 {
		t.Fatalf("upstream opened %d connections; want two initial clients plus one restart", server.accepted.Load())
	}
	// With no streams left, the broadcasts must settle rather than trigger
	// a feedback loop between clients. This includes delayed terminal frames.
	lastSent := uint64(0)
	stableSince := time.Now()
	for time.Since(stableSince) < 100*time.Millisecond {
		select {
		case <-ticker.C:
			check()
			total := uint64(0)
			for _, relay := range relays {
				total += relay.Stats().FramesSent
			}
			if total != lastSent {
				lastSent, stableSince = total, time.Now()
			}
		case <-ctx.Done():
			t.Fatal("foreign terminal frames caused an ongoing response storm")
		}
	}
}
