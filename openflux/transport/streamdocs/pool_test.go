package streamdocs

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"universal-bypass-tool/transport"
)

type asyncLane struct {
	mu                   sync.RWMutex
	callback             func([]byte)
	inbox                chan []byte
	sent                 chan []byte
	stop                 chan struct{}
	done                 chan struct{}
	stopOnce             sync.Once
	running              atomic.Bool
	ready                atomic.Bool
	reject               atomic.Bool
	starts, stops, sends atomic.Int64
	sentBytes, recvBytes atomic.Uint64
	failStart            bool
	startGate            <-chan struct{}
}

func newLane() *asyncLane {
	return &asyncLane{inbox: make(chan []byte, 16), sent: make(chan []byte, 16),
		stop: make(chan struct{}), done: make(chan struct{})}
}

func (l *asyncLane) Start() error {
	l.starts.Add(1)
	if l.startGate != nil {
		<-l.startGate
	}
	if l.failStart {
		return errors.New("private child start failure")
	}
	l.running.Store(true)
	l.ready.Store(true)
	go func() {
		defer close(l.done)
		for {
			select {
			case <-l.stop:
				return
			case frame := <-l.inbox:
				l.mu.RLock()
				cb := l.callback
				l.mu.RUnlock()
				if cb != nil {
					l.recvBytes.Add(uint64(len(frame)))
					cb(frame)
				}
			}
		}
	}()
	return nil
}

func (l *asyncLane) Stop() error {
	l.stopOnce.Do(func() {
		l.stops.Add(1)
		started := l.running.Swap(false)
		l.ready.Store(false)
		close(l.stop)
		if started {
			<-l.done // Exercises a child Stop that waits for its Receive callback.
		}
	})
	return nil
}
func (l *asyncLane) IsRunning() bool         { return l.running.Load() }
func (l *asyncLane) IsConnected() bool       { return l.running.Load() && l.ready.Load() }
func (l *asyncLane) Receive(cb func([]byte)) { l.mu.Lock(); l.callback = cb; l.mu.Unlock() }
func (l *asyncLane) Send(frame []byte) error {
	l.sends.Add(1)
	if !l.IsConnected() || l.reject.Load() {
		return errors.New("private child send failure")
	}
	select {
	case l.sent <- append([]byte(nil), frame...):
		l.sentBytes.Add(uint64(len(frame)))
		return nil
	default:
		return errors.New("fixture queue full")
	}
}
func (l *asyncLane) Stats() transport.TransportStats {
	return transport.TransportStats{BytesSent: l.sentBytes.Load(), BytesReceived: l.recvBytes.Load(), Connected: l.IsConnected()}
}

func eventually(t *testing.T, f func() bool) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		if f() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func testPool(t *testing.T, lanes []*asyncLane, minReady int, stagger time.Duration) *Pool {
	t.Helper()
	children := make([]transport.Transport, len(lanes))
	for i, child := range lanes {
		children[i] = child
	}
	p, err := New(children, minReady, stagger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p.Stop()
		select {
		case <-p.Done():
		case <-time.After(3 * time.Second):
			t.Error("pool shutdown did not complete")
		}
	})
	return p
}

func take(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()
	select {
	case b := <-ch:
		return b
	case <-time.After(3 * time.Second):
		t.Fatal("frame was not delivered")
		return nil
	}
}

func TestRoundRobinOpaqueFramesAndAggregateStats(t *testing.T) {
	lanes := []*asyncLane{newLane(), newLane(), newLane()}
	p := testPool(t, lanes, 0, 0)
	if p.MinReady() != 3 || p.Size() != 3 {
		t.Fatal("default readiness changed")
	}
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	eventually(t, p.IsConnected)
	var total uint64
	for n := 0; n < 9; n++ {
		frame := bytes.Repeat([]byte{byte(n)}, n+1) // Intentionally not IP or MDP1.
		if err := p.Send(frame); err != nil {
			t.Fatal(err)
		}
		if got := take(t, lanes[n%3].sent); !bytes.Equal(got, frame) {
			t.Fatal("pool altered frame")
		}
		total += uint64(len(frame))
	}
	if s := p.Stats(); !s.Connected || s.BytesSent != total {
		t.Fatalf("aggregate counters: %+v", s)
	}
	for _, s := range p.LaneStats() {
		if s.AcceptedFrames != 3 || s.SendErrors != 0 {
			t.Fatalf("lane counters: %+v", s)
		}
	}
}

func TestFailoverOnePassAndFrameBounds(t *testing.T) {
	lanes := []*asyncLane{newLane(), newLane(), newLane()}
	p := testPool(t, lanes, 1, 0)
	p.Start()
	eventually(t, func() bool { return p.ReadyCount() == 3 })
	lanes[0].reject.Store(true)
	lanes[1].ready.Store(false)
	frame := bytes.Repeat([]byte{7}, MaxFrameBytes)
	if err := p.Send(frame); err != nil {
		t.Fatal(err)
	}
	if got := take(t, lanes[2].sent); !bytes.Equal(got, frame) {
		t.Fatal("failover changed frame")
	}
	if lanes[0].sends.Load() != 1 || lanes[1].sends.Load() != 0 || lanes[2].sends.Load() != 1 {
		t.Fatal("unexpected failover attempts")
	}
	for _, b := range [][]byte{nil, {}, make([]byte, MaxFrameBytes+1)} {
		if err := p.Send(b); !errors.Is(err, ErrFrame) {
			t.Fatalf("invalid frame: %v", err)
		}
	}
	for _, l := range lanes {
		l.ready.Store(true)
		l.reject.Store(true)
	}
	before := []int64{lanes[0].sends.Load(), lanes[1].sends.Load(), lanes[2].sends.Load()}
	if err := p.Send([]byte{1}); !errors.Is(err, ErrBusy) {
		t.Fatalf("all-busy result: %v", err)
	}
	for i, l := range lanes {
		if l.sends.Load()-before[i] != 1 {
			t.Fatal("pool retried a lane more than once")
		}
		l.ready.Store(false)
	}
	if err := p.Send([]byte{1}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("disconnected result: %v", err)
	}
}

func TestAsyncReceivePreservesArrivalOrderAndDoesNotFilterEcho(t *testing.T) {
	lanes := []*asyncLane{newLane(), newLane()}
	p := testPool(t, lanes, 0, 0)
	got := make(chan []byte, 4)
	p.Receive(func(b []byte) {
		_ = p.Stats() // Receive must not hold a pool mutex around callbacks.
		if err := p.Send([]byte("callback reply")); err != nil {
			t.Error(err)
		}
		got <- append([]byte(nil), b...)
	})
	p.Start()
	eventually(t, p.IsConnected)
	// The second-sent message can arrive first on a different lane. Ordering is
	// intentionally left to MSR offsets; this wrapper must forward immediately.
	lanes[1].inbox <- []byte("second")
	if string(take(t, got)) != "second" {
		t.Fatal("pool waited for an earlier frame")
	}
	lanes[0].inbox <- []byte("first")
	if string(take(t, got)) != "first" {
		t.Fatal("arrival order changed")
	}
	lanes[0].inbox <- []byte("first") // Self-echo/dedup belongs to authenticated MSR.
	if string(take(t, got)) != "first" {
		t.Fatal("pool silently deduplicated frame")
	}
	lanes[1].inbox <- make([]byte, MaxFrameBytes+1)
	lanes[1].inbox <- []byte("barrier")
	if string(take(t, got)) != "barrier" {
		t.Fatal("oversized frame reached callback")
	}
}

func TestStopInsideReceiveCallbackDoesNotDeadlockChildStop(t *testing.T) {
	lane := newLane()
	p := testPool(t, []*asyncLane{lane}, 0, 0)
	returned := make(chan struct{})
	p.Receive(func([]byte) { p.Stop(); close(returned) })
	p.Start()
	eventually(t, p.IsConnected)
	lane.inbox <- []byte("stop")
	select {
	case <-returned:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop blocked receive callback")
	}
	select {
	case <-p.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("child Stop deadlocked")
	}
	if p.IsRunning() || p.IsConnected() || lane.stops.Load() != 1 {
		t.Fatal("invalid final state")
	}
	if err := p.Send([]byte{1}); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
}

func TestCancelStartStaggerAndStopBeforeStart(t *testing.T) {
	lanes := []*asyncLane{newLane(), newLane(), newLane()}
	p := testPool(t, lanes, 0, time.Second)
	p.Start()
	eventually(t, func() bool { return lanes[0].starts.Load() == 1 })
	p.Stop()
	p.Stop()
	<-p.Done()
	if lanes[1].starts.Load() != 0 || lanes[2].starts.Load() != 0 {
		t.Fatal("started lane after cancellation")
	}
	for _, l := range lanes {
		if l.stops.Load() != 1 {
			t.Fatal("non-idempotent Stop")
		}
	}
	if err := p.Start(); !errors.Is(err, ErrStopped) {
		t.Fatal("restarted stopped pool")
	}
	q := testPool(t, []*asyncLane{newLane()}, 0, 0)
	q.Stop()
	<-q.Done()
	if err := q.Start(); !errors.Is(err, ErrStopped) {
		t.Fatal("Stop-before-Start was ignored")
	}
}

func TestPartialStartFailureAndConcurrentLifecycleStats(t *testing.T) {
	failed := newLane()
	failed.failStart = true
	lanes := []*asyncLane{failed, newLane()}
	p := testPool(t, lanes, 1, 0)
	p.Start()
	eventually(t, p.IsConnected)
	if failed.stops.Load() != 1 || !p.IsRunning() {
		t.Fatal("failed lane was not isolated")
	}
	if err := p.Start(); err == nil {
		t.Fatal("allowed double Start")
	}
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = p.Stats()
				_ = p.LaneStats()
				_ = p.Send([]byte{1})
				p.Receive(func([]byte) {})
			}
			p.Stop()
		}()
	}
	wg.Wait()
	<-p.Done()
	if p.IsRunning() {
		t.Fatal("pool still running")
	}
}

func TestStopWhileChildStartIsInFlight(t *testing.T) {
	gate := make(chan struct{})
	first, second := newLane(), newLane()
	first.startGate = gate
	p := testPool(t, []*asyncLane{first, second}, 0, 0)
	p.Start()
	eventually(t, func() bool { return first.starts.Load() == 1 })
	p.Stop() // Must return while a child's context-free Start is still finishing.
	close(gate)
	<-p.Done()
	if first.stops.Load() != 1 || second.starts.Load() != 0 {
		t.Fatal("start/stop race leaked a lane")
	}
}

func TestConfigurationBounds(t *testing.T) {
	for _, lanes := range [][]transport.Transport{nil, {nil}, make([]transport.Transport, 11)} {
		if _, err := New(lanes, 0, 0); err == nil {
			t.Fatal("accepted invalid lanes")
		}
	}
	for _, min := range []int{-1, 2} {
		if _, err := New([]transport.Transport{newLane()}, min, 0); err == nil {
			t.Fatal("accepted invalid readiness")
		}
	}
	for _, delay := range []time.Duration{-1, 31 * time.Second} {
		if _, err := New([]transport.Transport{newLane()}, 0, delay); err == nil {
			t.Fatal("accepted invalid stagger")
		}
	}
}

func TestSimultaneousStartAndStopStayClosed(t *testing.T) {
	for n := 0; n < 64; n++ {
		p := testPool(t, []*asyncLane{newLane()}, 0, 0)
		gate := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-gate; _ = p.Start() }()
		go func() { defer wg.Done(); <-gate; _ = p.Stop() }()
		close(gate)
		wg.Wait()
		<-p.Done()
		if p.IsRunning() || p.IsConnected() || p.ReadyCount() != 0 {
			t.Fatal("concurrent Start revived stopped pool")
		}
		if err := p.Send([]byte{1}); !errors.Is(err, ErrStopped) {
			t.Fatalf("stopped pool accepted send: %v", err)
		}
	}
}
