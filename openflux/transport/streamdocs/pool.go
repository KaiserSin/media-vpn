// Package streamdocs distributes opaque stream-relay frames across independent
// document transports. Authentication, ordering and retransmission belong to
// streamrelay; this layer adds no framing, packet queue, or delivery retries.
package streamdocs

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"universal-bypass-tool/transport"
)

const MaxLanes = 10
const MaxFrameBytes = 32 << 10
const DefaultStartStagger = 350 * time.Millisecond

var (
	ErrStopped     = errors.New("stream document pool stopped")
	ErrFrame       = errors.New("stream frame must contain 1 to 32768 bytes")
	ErrUnavailable = errors.New("no stream document channel ready")
	ErrBusy        = errors.New("no stream document channel accepted frame")
)

type lane struct {
	transport transport.Transport
	started   atomic.Bool
	failed    atomic.Bool
	stopOnce  sync.Once
	accepted  atomic.Uint64
	bytes     atomic.Uint64
	errors    atomic.Uint64
}

type LaneStats struct {
	Index                                     int
	Started, Running, Connected               bool
	AcceptedFrames, AcceptedBytes, SendErrors uint64
	Transport                                 transport.TransportStats
}

type Pool struct {
	lanes        []lane
	minReady     int
	startStagger time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	lifecycle    sync.Mutex
	started      bool
	running      atomic.Bool
	launching    atomic.Bool
	closed       atomic.Bool
	next         atomic.Uint64
	startedAt    atomic.Int64
	startDone    chan struct{}
	stopDone     chan struct{}
	receiveMu    sync.RWMutex
	callback     func([]byte)
}

var _ transport.Transport = (*Pool)(nil)

// New takes ownership of child Start/Stop and Receive registration. minReady=0
// requires all lanes. A zero stagger starts without delay; callers wanting the
// normal 350ms spacing pass DefaultStartStagger explicitly.
func New(lanes []transport.Transport, minReady int, startStagger time.Duration) (*Pool, error) {
	if len(lanes) < 1 || len(lanes) > MaxLanes {
		return nil, errors.New("stream document pool requires 1 to 10 channels")
	}
	if minReady == 0 {
		minReady = len(lanes)
	}
	if minReady < 1 || minReady > len(lanes) {
		return nil, errors.New("min-ready must be between 1 and channel count")
	}
	if startStagger < 0 || startStagger > 30*time.Second {
		return nil, errors.New("start stagger must be between zero and 30 seconds")
	}
	for _, child := range lanes {
		if child == nil {
			return nil, errors.New("nil stream document channel")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{lanes: make([]lane, len(lanes)), minReady: minReady, startStagger: startStagger,
		ctx: ctx, cancel: cancel, startDone: make(chan struct{}), stopDone: make(chan struct{})}
	for i, child := range lanes {
		p.lanes[i].transport = child
		child.Receive(p.deliver)
	}
	return p, nil
}

func (p *Pool) Start() error {
	p.lifecycle.Lock()
	defer p.lifecycle.Unlock()
	if p.closed.Load() {
		return ErrStopped
	}
	if p.started {
		return errors.New("stream document pool already started")
	}
	p.started = true
	p.startedAt.Store(time.Now().UnixNano())
	p.running.Store(true)
	p.launching.Store(true)
	go p.startLanes()
	return nil
}

func (p *Pool) startLanes() {
	defer close(p.startDone)
	defer p.launching.Store(false)
	for i := range p.lanes {
		if i > 0 && p.startStagger > 0 {
			timer := time.NewTimer(p.startStagger)
			select {
			case <-p.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		p.lifecycle.Lock()
		if p.closed.Load() {
			p.lifecycle.Unlock()
			return
		}
		l := &p.lanes[i]
		err := l.transport.Start()
		if err != nil {
			l.failed.Store(true)
		} else {
			l.started.Store(true)
		}
		p.lifecycle.Unlock()
		if err != nil {
			// Clean up a partially successful child Start without exposing its error.
			l.stopOnce.Do(func() { _ = l.transport.Stop() })
		}
	}
}

// Stop requests shutdown once and returns without waiting, including when called
// from a Receive callback. Done closes after all child Stop calls have returned.
func (p *Pool) Stop() error {
	if p.closed.CompareAndSwap(false, true) {
		p.running.Store(false)
		p.cancel()
		go p.stopLanes()
	}
	return nil
}

func (p *Pool) stopLanes() {
	defer close(p.stopDone)
	p.lifecycle.Lock()
	started := p.started
	p.lifecycle.Unlock()
	if started {
		<-p.startDone
	}
	for i := range p.lanes {
		l := &p.lanes[i]
		l.stopOnce.Do(func() { _ = l.transport.Stop() })
	}
}

func (p *Pool) Done() <-chan struct{} { return p.stopDone }

func (p *Pool) laneRunning(i int) bool {
	l := &p.lanes[i]
	if !l.started.Load() || l.failed.Load() {
		return false
	}
	if state, ok := l.transport.(interface{ IsRunning() bool }); ok {
		return state.IsRunning()
	}
	return true
}

func (p *Pool) IsRunning() bool {
	if p.closed.Load() || !p.running.Load() {
		return false
	}
	if p.launching.Load() {
		return true
	}
	for i := range p.lanes {
		if p.laneRunning(i) {
			return true
		}
	}
	return false
}

func (p *Pool) ReadyCount() int {
	if p.closed.Load() || !p.running.Load() {
		return 0
	}
	n := 0
	for i := range p.lanes {
		if p.laneRunning(i) && p.lanes[i].transport.IsConnected() {
			n++
		}
	}
	return n
}

func (p *Pool) IsConnected() bool { return p.ReadyCount() >= p.minReady }
func (p *Pool) Size() int         { return len(p.lanes) }
func (p *Pool) MinReady() int     { return p.minReady }

func (p *Pool) Send(data []byte) error {
	if p.closed.Load() || !p.running.Load() {
		return ErrStopped
	}
	if len(data) == 0 || len(data) > MaxFrameBytes {
		return ErrFrame
	}
	first := int((p.next.Add(1) - 1) % uint64(len(p.lanes)))
	ready := false
	for offset := 0; offset < len(p.lanes); offset++ {
		if p.closed.Load() || !p.running.Load() {
			return ErrStopped
		}
		i := (first + offset) % len(p.lanes)
		l := &p.lanes[i]
		if !p.laneRunning(i) || !l.transport.IsConnected() {
			continue
		}
		ready = true
		if err := l.transport.Send(data); err != nil {
			l.errors.Add(1)
			continue
		}
		l.accepted.Add(1)
		l.bytes.Add(uint64(len(data)))
		return nil
	}
	if !ready {
		return ErrUnavailable
	}
	return ErrBusy
}

func (p *Pool) Receive(cb func([]byte)) {
	p.receiveMu.Lock()
	p.callback = cb
	p.receiveMu.Unlock()
}

func (p *Pool) deliver(data []byte) {
	if p.closed.Load() || !p.running.Load() || len(data) == 0 || len(data) > MaxFrameBytes {
		return
	}
	p.receiveMu.RLock()
	cb := p.callback
	p.receiveMu.RUnlock()
	if cb != nil {
		cb(data)
	}
}

func (p *Pool) LaneStats() []LaneStats {
	result := make([]LaneStats, len(p.lanes))
	for i := range p.lanes {
		l := &p.lanes[i]
		var stats transport.TransportStats
		if l.started.Load() {
			stats = l.transport.Stats()
		}
		running := !p.closed.Load() && p.running.Load() && p.laneRunning(i)
		result[i] = LaneStats{Index: i + 1, Started: l.started.Load(), Running: running,
			Connected: running && l.transport.IsConnected(), AcceptedFrames: l.accepted.Load(),
			AcceptedBytes: l.bytes.Load(), SendErrors: l.errors.Load(), Transport: stats}
	}
	return result
}

// Stats aggregates child transport counters, which may include frames discarded
// later by streamrelay's HMAC/direction checks. It is not application goodput.
func (p *Pool) Stats() transport.TransportStats {
	s := transport.TransportStats{Connected: p.IsConnected()}
	for _, lane := range p.LaneStats() {
		s.BytesSent += lane.Transport.BytesSent
		s.BytesReceived += lane.Transport.BytesReceived
		s.PacketsSent += lane.Transport.PacketsSent
		s.PacketsRecv += lane.Transport.PacketsRecv
		s.Reconnects += lane.Transport.Reconnects
	}
	if started := p.startedAt.Load(); started != 0 {
		s.Uptime = time.Since(time.Unix(0, started))
	}
	return s
}
