// Package docpool fans independent document transports into one TCP/IP stack.
// It introduces no packet queue; each document owns a bounded nonblocking queue.
package docpool

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"universal-bypass-tool/transport"
)

const MaxLanes = 10
const MaxPacketBytes = 1500
const envelopeSize = 40

var (
	ErrStopped     = errors.New("document pool stopped")
	ErrUnavailable = errors.New("assigned document channel unavailable")
	ErrBusy        = errors.New("no document channel accepted packet")
	ErrPacket      = errors.New("pool requires an unfragmented IPv4 TCP packet within MTU")
)

type Config struct {
	Mode         string
	MinReady     int
	StartStagger time.Duration
	PoolID       [16]byte
}

type lane struct {
	transport transport.Transport
	started   atomic.Bool
	failed    atomic.Bool
	errors    atomic.Uint64
	accepted  atomic.Uint64
}

type LaneStats struct {
	Index                       int
	Started, Running, Connected bool
	AcceptedBytes, SendErrors   uint64
	Transport                   transport.TransportStats
}

type Pool struct {
	lanes      []lane
	cfg        Config
	running    atomic.Bool
	launching  atomic.Bool
	next       atomic.Uint64
	lifecycle  sync.Mutex
	cancel     context.CancelFunc
	done       chan struct{}
	startedAt  time.Time
	receiveMu  sync.RWMutex
	callback   func([]byte)
	endpointID [16]byte
}

func New(transports []transport.Transport, cfg Config) (*Pool, error) {
	if len(transports) < 1 || len(transports) > MaxLanes {
		return nil, errors.New("pool requires between 1 and 10 document channels")
	}
	if cfg.Mode != "flow" && cfg.Mode != "stripe" {
		return nil, errors.New("pool mode must be flow or stripe")
	}
	if cfg.MinReady == 0 {
		cfg.MinReady = len(transports)
	}
	if cfg.MinReady < 1 || cfg.MinReady > len(transports) {
		return nil, errors.New("min-ready must be between 1 and document count")
	}
	if cfg.StartStagger < 0 || cfg.StartStagger > 30*time.Second {
		return nil, errors.New("start-stagger must be between zero and 30 seconds")
	}
	p := &Pool{cfg: cfg, lanes: make([]lane, len(transports))}
	if _, err := rand.Read(p.endpointID[:]); err != nil {
		return nil, errors.New("cannot initialize pool endpoint identity")
	}
	for i, trans := range transports {
		if trans == nil {
			return nil, errors.New("nil document channel")
		}
		p.lanes[i].transport = trans
		trans.Receive(func(data []byte) { p.deliver(i, data) })
	}
	return p, nil
}

// Start is one-shot. Reconnection remains owned by each child transport.
func (p *Pool) Start() error {
	p.lifecycle.Lock()
	defer p.lifecycle.Unlock()
	if p.done != nil {
		return errors.New("document pool already started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel, p.done = cancel, make(chan struct{})
	p.startedAt = time.Now()
	p.running.Store(true)
	p.launching.Store(true)
	go p.startLanes(ctx)
	return nil
}

func (p *Pool) startLanes(ctx context.Context) {
	defer close(p.done)
	defer p.launching.Store(false)
	for i := range p.lanes {
		if i != 0 && p.cfg.StartStagger > 0 {
			timer := time.NewTimer(p.cfg.StartStagger)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		// Serialize Start against Stop so cancellation cannot start another lane.
		p.lifecycle.Lock()
		if ctx.Err() != nil {
			p.lifecycle.Unlock()
			return
		}
		l := &p.lanes[i]
		if err := l.transport.Start(); err != nil {
			l.failed.Store(true)
			l.errors.Add(1)
		} else {
			l.started.Store(true)
		}
		p.lifecycle.Unlock()
	}
}

func (p *Pool) Stop() error {
	p.lifecycle.Lock()
	p.running.Store(false)
	if p.cancel != nil {
		p.cancel()
	}
	// Children have asynchronous Stop, safe from a receive callback as well.
	for i := range p.lanes {
		_ = p.lanes[i].transport.Stop()
	}
	p.lifecycle.Unlock()
	return nil
}

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
	if !p.running.Load() {
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
	if !p.running.Load() {
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

func (p *Pool) IsConnected() bool { return p.ReadyCount() >= p.cfg.MinReady }
func (p *Pool) Size() int         { return len(p.lanes) }
func (p *Pool) MinReady() int     { return p.cfg.MinReady }

// FlowIndex hashes a canonical TCP four-tuple, identical in both directions.
// The fixed modulo keeps a flow on one document even after a lane disconnects;
// this needs no unbounded flow table or timers and never silently migrates it.
func FlowIndex(data []byte, count int) (int, error) {
	if count < 1 || count > MaxLanes {
		return 0, errors.New("invalid channel count")
	}
	if len(data) < 40 || len(data) > MaxPacketBytes || data[0]>>4 != 4 || data[9] != 6 {
		return 0, ErrPacket
	}
	ihl := int(data[0]&15) * 4
	total := int(binary.BigEndian.Uint16(data[2:4]))
	if ihl < 20 || total != len(data) || ihl+20 > total || binary.BigEndian.Uint16(data[6:8])&0x3fff != 0 {
		return 0, ErrPacket
	}
	tcpLen := int(data[ihl+12]>>4) * 4
	if tcpLen < 20 || ihl+tcpLen > total {
		return 0, ErrPacket
	}
	var a, b [6]byte
	copy(a[:4], data[12:16])
	copy(a[4:], data[ihl:ihl+2])
	copy(b[:4], data[16:20])
	copy(b[4:], data[ihl+2:ihl+4])
	if bytes.Compare(a[:], b[:]) > 0 {
		a, b = b, a
	}
	h := uint64(14695981039346656037)
	for _, endpoint := range [2][6]byte{a, b} {
		for _, octet := range endpoint {
			h ^= uint64(octet)
			h *= 1099511628211
		}
	}
	return int(h % uint64(count)), nil
}

func (p *Pool) trySend(i int, data []byte) error {
	l := &p.lanes[i]
	if !p.laneRunning(i) || !l.transport.IsConnected() {
		l.errors.Add(1)
		return ErrUnavailable
	}
	wire := make([]byte, envelopeSize+len(data))
	copy(wire[:4], "MDP1")
	copy(wire[4:20], p.cfg.PoolID[:])
	copy(wire[20:36], p.endpointID[:])
	wire[36], wire[37] = byte(i), byte(len(p.lanes))
	if p.cfg.Mode == "stripe" {
		wire[38] = 1
	}
	copy(wire[envelopeSize:], data)
	if err := l.transport.Send(wire); err != nil {
		l.errors.Add(1)
		return ErrBusy
	}
	l.accepted.Add(uint64(len(data)))
	return nil
}

func (p *Pool) Send(data []byte) error {
	if !p.running.Load() {
		return ErrStopped
	}
	index, err := FlowIndex(data, len(p.lanes))
	if err != nil {
		return err
	}
	if p.cfg.Mode == "flow" {
		if err := p.trySend(index, data); err != nil {
			return fmt.Errorf("channel %d: %w", index+1, err)
		}
		return nil
	}
	first := int((p.next.Add(1) - 1) % uint64(len(p.lanes)))
	for offset := 0; offset < len(p.lanes); offset++ {
		if p.trySend((first+offset)%len(p.lanes), data) == nil {
			return nil
		}
	}
	return ErrBusy
}

func (p *Pool) Receive(cb func([]byte)) { p.receiveMu.Lock(); p.callback = cb; p.receiveMu.Unlock() }
func (p *Pool) deliver(index int, wire []byte) {
	if !p.running.Load() {
		return
	}
	if len(wire) < envelopeSize+40 || len(wire) > envelopeSize+MaxPacketBytes || string(wire[:4]) != "MDP1" {
		return
	}
	if !bytes.Equal(wire[4:20], p.cfg.PoolID[:]) || bytes.Equal(wire[20:36], p.endpointID[:]) {
		return
	}
	mode := byte(0)
	if p.cfg.Mode == "stripe" {
		mode = 1
	}
	if wire[36] != byte(index) || wire[37] != byte(len(p.lanes)) || wire[38] != mode || wire[39] != 0 {
		return
	}
	data := wire[envelopeSize:]
	if _, err := FlowIndex(data, len(p.lanes)); err != nil {
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
		// Do not read BaseTransport startTime concurrently with its Start.
		var stats transport.TransportStats
		if l.started.Load() {
			stats = l.transport.Stats()
		}
		result[i] = LaneStats{Index: i + 1, Started: l.started.Load(), Running: p.running.Load() && p.laneRunning(i), Connected: p.running.Load() && p.laneRunning(i) && l.transport.IsConnected(), AcceptedBytes: l.accepted.Load(), SendErrors: l.errors.Load(), Transport: stats}
	}
	return result
}

func (p *Pool) Stats() transport.TransportStats {
	s := transport.TransportStats{Connected: p.IsConnected()}
	for _, l := range p.LaneStats() {
		s.BytesSent += l.Transport.BytesSent
		s.BytesReceived += l.Transport.BytesReceived
		s.PacketsSent += l.Transport.PacketsSent
		s.PacketsRecv += l.Transport.PacketsRecv
		s.Reconnects += l.Transport.Reconnects
	}
	p.lifecycle.Lock()
	if !p.startedAt.IsZero() {
		s.Uptime = time.Since(p.startedAt)
	}
	p.lifecycle.Unlock()
	return s
}
