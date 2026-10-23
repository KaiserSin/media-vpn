package streamrelay_test

import (
	"bytes"
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

const externalTimeout = 20 * time.Second

var errExternalQueueFull = errors.New("external fixture transport queue full")

// This deliberately does not invoke Receive from Send. Each endpoint has one
// asynchronous delivery worker and a 64-frame bounded queue. Backpressure
// is returned to the relay just as it is by the real document transport.
type externalTransport struct {
	peer      *externalTransport
	inbox     chan []byte
	stop      chan struct{}
	finished  chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool
	closed    atomic.Bool
	mu        sync.RWMutex
	callback  func([]byte)
	sent      atomic.Uint64
	received  atomic.Uint64
	packets   atomic.Uint64
	got       atomic.Uint64
}

var _ transport.Transport = (*externalTransport)(nil)

func (t *externalTransport) Start() error {
	if t.closed.Load() {
		return io.ErrClosedPipe
	}
	t.startOnce.Do(func() {
		t.started.Store(true)
		go func() {
			defer close(t.finished)
			for {
				select {
				case <-t.stop:
					return
				case frame := <-t.inbox:
					// Modest artificial delivery latency exercises bounded queues
					// and the aggregate memory budget without a network dependency.
					timer := time.NewTimer(250 * time.Microsecond)
					select {
					case <-t.stop:
						timer.Stop()
						return
					case <-timer.C:
					}
					t.mu.RLock()
					cb := t.callback
					t.mu.RUnlock()
					if cb != nil {
						t.received.Add(uint64(len(frame)))
						t.got.Add(1)
						cb(frame)
					}
				}
			}
		}()
	})
	return nil
}

func (t *externalTransport) Stop() error {
	t.stopOnce.Do(func() {
		t.closed.Store(true)
		t.started.Store(false)
		close(t.stop)
	})
	return nil
}

func (t *externalTransport) IsConnected() bool {
	return t.started.Load() && !t.closed.Load() && t.peer.started.Load() && !t.peer.closed.Load()
}

func (t *externalTransport) Receive(cb func([]byte)) {
	t.mu.Lock()
	t.callback = cb
	t.mu.Unlock()
}

func (t *externalTransport) Send(frame []byte) error {
	if !t.IsConnected() {
		return io.ErrClosedPipe
	}
	if len(frame) > 64*1024 {
		return errors.New("external fixture frame exceeds 64 KiB")
	}
	copyOfFrame := append([]byte(nil), frame...)
	select {
	case <-t.stop:
		return io.ErrClosedPipe
	case <-t.peer.stop:
		return io.ErrClosedPipe
	case t.peer.inbox <- copyOfFrame:
		t.sent.Add(uint64(len(frame)))
		t.packets.Add(1)
		return nil
	default:
		return errExternalQueueFull
	}
}

func (t *externalTransport) Stats() transport.TransportStats {
	return transport.TransportStats{
		BytesSent: t.sent.Load(), BytesReceived: t.received.Load(),
		PacketsSent: t.packets.Load(), PacketsRecv: t.got.Load(), Connected: t.IsConnected(),
	}
}

func externalPair(t *testing.T) (*externalTransport, *externalTransport) {
	t.Helper()
	newEndpoint := func() *externalTransport {
		return &externalTransport{inbox: make(chan []byte, 64), stop: make(chan struct{}), finished: make(chan struct{})}
	}
	a, b := newEndpoint(), newEndpoint()
	a.peer, b.peer = b, a
	for _, endpoint := range []*externalTransport{a, b} {
		if err := endpoint.Start(); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		a.Stop()
		b.Stop()
		for _, endpoint := range []*externalTransport{a, b} {
			select {
			case <-endpoint.finished:
			case <-time.After(externalTimeout):
				t.Error("relay receive callback did not release the asynchronous transport worker")
			}
		}
	})
	return a, b
}

type externalUpstream struct {
	address  string
	accepted atomic.Int64
	active   atomic.Int64
}

func externalEcho(t *testing.T, trailer []byte) *externalUpstream {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &externalUpstream{address: listener.Addr().String()}
	var connections sync.Map
	var workers sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			server.accepted.Add(1)
			server.active.Add(1)
			connections.Store(conn, struct{}{})
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer server.active.Add(-1)
				defer connections.Delete(conn)
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(externalTimeout))
				if _, err := io.Copy(conn, conn); err == nil && len(trailer) != 0 {
					// Sent only after the client's write-half reaches real TCP EOF.
					externalWriteAll(conn, trailer)
				}
			}()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		<-acceptDone
		connections.Range(func(key, _ any) bool {
			key.(net.Conn).Close()
			return true
		})
		done := make(chan struct{})
		go func() { workers.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(externalTimeout):
			t.Error("upstream TCP workers did not terminate")
		}
	})
	return server
}

func externalConfig(upstream string, chunk int) streamrelay.Config {
	return streamrelay.Config{
		Key: bytes.Repeat([]byte{0x51}, 32), ChunkSize: chunk,
		WindowBytes: 256 * 1024, MaxBufferedBytes: 512 * 1024, MaxStreams: 128,
		Upstream: upstream, VirtualAddress: "10.203.0.1:443",
	}
}

func externalRelays(t *testing.T, cfg streamrelay.Config) (*streamrelay.Relay, *streamrelay.Relay) {
	t.Helper()
	clientTransport, exitTransport := externalPair(t)
	exit, err := streamrelay.NewFixedExit(exitTransport, cfg)
	if err != nil {
		t.Fatal(err)
	}
	externalRelayCleanup(t, exit)
	client, err := streamrelay.NewClient(clientTransport, cfg)
	if err != nil {
		t.Fatal(err)
	}
	externalRelayCleanup(t, client)
	return client, exit
}

func externalRelayCleanup(t *testing.T, relay *streamrelay.Relay) {
	t.Helper()
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() { relay.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(externalTimeout):
			t.Error("relay Close did not finish within 20 seconds")
		}
	})
}

func externalWriteAll(w io.Writer, payload []byte) error {
	for len(payload) != 0 {
		n, err := w.Write(payload)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(payload) {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}

func externalCheckBudget(stats streamrelay.Stats, budget int) error {
	if stats.BufferedBytes < 0 || stats.BufferedBytes > budget || stats.PeakBufferedBytes > budget {
		return fmt.Errorf("buffer budget exceeded: current=%d peak=%d budget=%d", stats.BufferedBytes, stats.PeakBufferedBytes, budget)
	}
	return nil
}

func TestExternalConcurrentDuplexIntegrityAndMemoryBudget(t *testing.T) {
	for _, chunk := range []int{4096, 16384} {
		t.Run(fmt.Sprintf("chunk_%d", chunk), func(t *testing.T) {
			server := externalEcho(t, nil)
			cfg := externalConfig(server.address, chunk)
			// Integrity needs room for both directions of all four windows.
			// Deliberate aggregate-budget exhaustion is tested separately below.
			cfg.MaxBufferedBytes = 4 * 1024 * 1024
			client, exit := externalRelays(t, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), externalTimeout)
			defer cancel()
			size := 512 * 1024
			if testing.Short() {
				size = 64 * 1024
			}
			// Random per-stream content detects duplication, lane mixing and
			// chunk reordering, unlike a short periodic repeated byte pattern.
			payloads := make([][]byte, 4)
			for i := range payloads {
				payloads[i] = make([]byte, size)
				if _, err := rand.Read(payloads[i]); err != nil {
					t.Fatal(err)
				}
			}
			results := make(chan error, len(payloads))
			start := make(chan struct{})
			for _, payload := range payloads {
				go func(payload []byte) {
					<-start
					conn, err := client.DialTCPContext(ctx, cfg.VirtualAddress)
					if err != nil {
						results <- err
						return
					}
					defer conn.Close()
					conn.SetDeadline(time.Now().Add(externalTimeout))
					written := make(chan error, 1)
					go func() { written <- externalWriteAll(conn, payload) }()
					received := make([]byte, len(payload))
					if _, err := io.ReadFull(conn, received); err != nil {
						results <- fmt.Errorf("read echoed payload: %w", err)
						return
					}
					if sha256.Sum256(received) != sha256.Sum256(payload) {
						results <- errors.New("SHA256 mismatch after concurrent duplex transfer")
						return
					}
					select {
					case err := <-written:
						results <- err
					case <-ctx.Done():
						results <- ctx.Err()
					}
				}(payload)
			}
			close(start)
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for completed := 0; completed < len(payloads); {
				select {
				case err := <-results:
					completed++
					if err != nil {
						t.Errorf("stream transfer: %v", err)
					}
				case <-ticker.C:
					for _, relay := range []*streamrelay.Relay{client, exit} {
						if err := externalCheckBudget(relay.Stats(), cfg.MaxBufferedBytes); err != nil {
							t.Fatal(err)
						}
					}
				case <-ctx.Done():
					t.Fatal("four concurrent streams exceeded 20-second deadline")
				}
			}
			if server.accepted.Load() != 4 {
				t.Errorf("opened %d upstream TCP connections; want 4", server.accepted.Load())
			}
			for _, relay := range []*streamrelay.Relay{client, exit} {
				stats := relay.Stats()
				if err := externalCheckBudget(stats, cfg.MaxBufferedBytes); err != nil {
					t.Error(err)
				}
				if stats.PeakBufferedBytes <= 0 {
					t.Error("public peak-buffer counter never recorded the nonempty workload")
				}
			}
		})
	}
}

func TestExternalOversubscribedBudgetRejectsAndReleases(t *testing.T) {
	server := externalEcho(t, nil)
	cfg := externalConfig(server.address, 16384)
	// Four 256 KiB windows intentionally exceed this 512 KiB shared budget.
	client, exit := externalRelays(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), externalTimeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	connections := make([]net.Conn, 0, 4)
	t.Cleanup(func() {
		for _, conn := range connections {
			conn.Close()
		}
	})
	for i := 0; i < 4; i++ {
		conn, err := client.DialTCPContext(ctx, cfg.VirtualAddress)
		if err != nil {
			t.Fatalf("open stream before applying buffer pressure: %v", err)
		}
		connections = append(connections, conn)
		if err := conn.SetDeadline(deadline); err != nil {
			t.Fatal(err)
		}
	}
	type transferResult struct {
		readErr, writeErr error
		prefixMatches     bool
	}
	results := make(chan transferResult, len(connections))
	start, readStart := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseReaders := func() { releaseOnce.Do(func() { close(readStart) }) }
	defer releaseReaders()
	for _, conn := range connections {
		payload := make([]byte, 512*1024)
		if _, err := rand.Read(payload); err != nil {
			t.Fatal(err)
		}
		go func(conn net.Conn, payload []byte) {
			<-start
			written := make(chan error, 1)
			go func() { written <- externalWriteAll(conn, payload) }()
			// Withhold application consumption until the advertised windows
			// have genuinely put pressure on the relay's shared receive budget.
			select {
			case <-readStart:
			case <-ctx.Done():
				conn.Close()
			}
			received := make([]byte, len(payload))
			n, readErr := io.ReadFull(conn, received)
			matches := sha256.Sum256(received[:n]) == sha256.Sum256(payload[:n])
			if readErr != nil {
				conn.Close() // Release a writer waiting for remote credit.
			}
			var writeErr error
			select {
			case writeErr = <-written:
			case <-ctx.Done():
				writeErr = ctx.Err()
			}
			conn.Close()
			results <- transferResult{readErr: readErr, writeErr: writeErr, prefixMatches: matches}
		}(conn, payload)
	}
	close(start)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	checkBudget := func() {
		t.Helper()
		for _, relay := range []*streamrelay.Relay{client, exit} {
			if err := externalCheckBudget(relay.Stats(), cfg.MaxBufferedBytes); err != nil {
				t.Fatal(err)
			}
		}
	}
	for client.Stats().RejectedStreams+exit.Stats().RejectedStreams == 0 {
		select {
		case <-ticker.C:
			checkBudget()
		case <-ctx.Done():
			t.Fatal("oversubscribed workload did not reject or release pressure within 20 seconds")
		}
	}
	releaseReaders()
	capacityErrors, failed, completed := 0, 0, 0
	for completed < len(connections) {
		select {
		case result := <-results:
			completed++
			if !result.prefixMatches {
				t.Error("capacity rejection corrupted the delivered payload prefix")
			}
			if result.readErr != nil || result.writeErr != nil {
				failed++
			}
			for _, err := range []error{result.readErr, result.writeErr} {
				if err == nil {
					continue
				}
				if errors.Is(err, streamrelay.ErrCapacity) {
					capacityErrors++
				}
				var timeout net.Error
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
					(errors.As(err, &timeout) && timeout.Timeout()) {
					t.Errorf("capacity pressure must terminate or recover without a timeout: %v", err)
				}
			}
		case <-ticker.C:
			checkBudget()
		case <-ctx.Done():
			t.Fatal("oversubscribed streams did not terminate within 20 seconds")
		}
	}
	if failed == 0 {
		t.Error("public capacity rejection did not reach any of the active streams")
	}
	// A peer RST need not preserve ErrCapacity across the transport. The public
	// rejection counter is the evidence for that case; no private error strings
	// or frame internals are used by this external-package test.
	t.Logf("capacity pressure: failed streams=%d explicit ErrCapacity=%d client rejects=%d exit rejects=%d",
		failed, capacityErrors, client.Stats().RejectedStreams, exit.Stats().RejectedStreams)
	waitReleased := func() {
		t.Helper()
		for {
			checkBudget()
			a, b := client.Stats(), exit.Stats()
			if a.ActiveStreams == 0 && b.ActiveStreams == 0 && a.BufferedBytes == 0 &&
				b.BufferedBytes == 0 && server.active.Load() == 0 {
				return
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatalf("resources not released after pressure: client=%+v exit=%+v upstream active=%d",
					a, b, server.active.Load())
			}
		}
	}
	waitReleased()
	if server.accepted.Load() != 4 {
		t.Fatalf("opened %d upstream connections under pressure; want four", server.accepted.Load())
	}
	// A fresh small stream verifies that the released capacity is usable,
	// rather than only becoming zero when the entire Relay is destroyed.
	probe, err := client.DialTCPContext(ctx, cfg.VirtualAddress)
	if err != nil {
		t.Fatalf("reuse capacity after overload: %v", err)
	}
	defer probe.Close()
	if err := probe.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	payload := []byte("capacity-released-and-reusable")
	if err := externalWriteAll(probe, payload); err != nil {
		t.Fatal(err)
	}
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(probe, received); err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(received) != sha256.Sum256(payload) {
		t.Fatal("post-overload probe returned corrupted data")
	}
	probe.Close()
	waitReleased()
}

func TestExternalCloseWriteDeliversTailAndEOF(t *testing.T) {
	trailer := []byte("upstream-saw-real-TCP-EOF")
	server := externalEcho(t, trailer)
	cfg := externalConfig(server.address, 4096)
	client, _ := externalRelays(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), externalTimeout)
	defer cancel()
	conn, err := client.DialTCPContext(ctx, cfg.VirtualAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	closeWriter, ok := conn.(interface{ CloseWrite() error })
	if !ok {
		t.Skip("public connection does not expose CloseWrite")
	}
	conn.SetDeadline(time.Now().Add(externalTimeout))
	payload := make([]byte, 32*1024+17)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	go func() {
		if err := externalWriteAll(conn, payload); err != nil {
			written <- err
			return
		}
		written <- closeWriter.CloseWrite()
	}()
	received := make([]byte, len(payload)+len(trailer))
	if _, err := io.ReadFull(conn, received); err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte(nil), payload...), trailer...)
	if sha256.Sum256(received) != sha256.Sum256(want) {
		t.Fatal("half-close dropped or reordered payload/tail")
	}
	var one [1]byte
	if n, err := conn.Read(one[:]); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("after tail got n=%d err=%v; want EOF", n, err)
	}
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("CloseWrite did not complete")
	}
	if n, err := conn.Write([]byte{1}); n != 0 || err == nil {
		t.Fatalf("write after CloseWrite: n=%d err=%v; want rejection", n, err)
	}
}

func TestExternalWrongKeyNeverOpensUpstream(t *testing.T) {
	server := externalEcho(t, nil)
	clientTransport, exitTransport := externalPair(t)
	exitConfig := externalConfig(server.address, 4096)
	exit, err := streamrelay.NewFixedExit(exitTransport, exitConfig)
	if err != nil {
		t.Fatal(err)
	}
	externalRelayCleanup(t, exit)
	clientConfig := exitConfig
	clientConfig.Key = bytes.Repeat([]byte{0xA7}, 32)
	client, err := streamrelay.NewClient(clientTransport, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	externalRelayCleanup(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := client.DialTCPContext(ctx, clientConfig.VirtualAddress)
	if conn != nil {
		conn.Close()
	}
	if err == nil {
		t.Fatal("wrong key unexpectedly opened a stream")
	}
	if server.accepted.Load() != 0 {
		t.Fatalf("wrong key opened %d upstream connections", server.accepted.Load())
	}
	if exitTransport.Stats().PacketsRecv == 0 || exit.Stats().AuthenticationFailures == 0 {
		t.Error("test did not deliver an authenticated-frame rejection to the exit")
	}
	if err := externalCheckBudget(exit.Stats(), exitConfig.MaxBufferedBytes); err != nil {
		t.Error(err)
	}
}
