package docpool

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"universal-bypass-tool/transport"
)

type fakeLane struct {
	running, connected, busy atomic.Bool
	starts                   atomic.Int32
	mu                       sync.Mutex
	frames                   [][]byte
	cb                       func([]byte)
	startErr                 bool
}

func (f *fakeLane) Start() error {
	f.starts.Add(1)
	if f.startErr {
		return errors.New("private provider detail")
	}
	f.running.Store(true)
	return nil
}
func (f *fakeLane) Stop() error       { f.running.Store(false); return nil }
func (f *fakeLane) IsRunning() bool   { return f.running.Load() }
func (f *fakeLane) IsConnected() bool { return f.running.Load() && f.connected.Load() }
func (f *fakeLane) Send(b []byte) error {
	if f.busy.Load() {
		return errors.New("private transport detail")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.frames = append(f.frames, append([]byte(nil), b...))
	return nil
}
func (f *fakeLane) Receive(cb func([]byte)) { f.mu.Lock(); f.cb = cb; f.mu.Unlock() }
func (f *fakeLane) Stats() transport.TransportStats {
	return transport.TransportStats{Connected: f.IsConnected()}
}
func (f *fakeLane) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.frames) }
func (f *fakeLane) last() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.frames[len(f.frames)-1]...)
}
func (f *fakeLane) inject(b []byte) {
	f.mu.Lock()
	cb := f.cb
	f.mu.Unlock()
	if cb != nil {
		cb(b)
	}
}

func packet(port uint16, reverse bool) []byte {
	b := make([]byte, 40)
	b[0] = 0x45
	binary.BigEndian.PutUint16(b[2:4], 40)
	b[8] = 64
	b[9] = 6
	copy(b[12:16], []byte{10, 10, 10, 2})
	copy(b[16:20], []byte{10, 203, 0, 1})
	binary.BigEndian.PutUint16(b[20:22], port)
	binary.BigEndian.PutUint16(b[22:24], 443)
	b[32] = 0x50
	if reverse {
		var tmp [4]byte
		copy(tmp[:], b[12:16])
		copy(b[12:16], b[16:20])
		copy(b[16:20], tmp[:])
		b[20], b[22] = b[22], b[20]
		b[21], b[23] = b[23], b[21]
	}
	return b
}
func newFakePool(t *testing.T, n int, cfg Config) (*Pool, []*fakeLane) {
	t.Helper()
	lanes := make([]*fakeLane, n)
	all := make([]transport.Transport, n)
	for i := range lanes {
		lanes[i] = &fakeLane{}
		lanes[i].connected.Store(true)
		all[i] = lanes[i]
	}
	p, err := New(all, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Stop() })
	return p, lanes
}
func startPool(t *testing.T, p *Pool) {
	t.Helper()
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
	case <-time.After(time.Second):
		t.Fatal("pool start hung")
	}
}

func TestFlowCanonicalStableDistribution(t *testing.T) {
	seen := map[int]bool{}
	for port := uint16(20000); port < 21000; port++ {
		a, err := FlowIndex(packet(port, false), 10)
		if err != nil {
			t.Fatal(err)
		}
		b, err := FlowIndex(packet(port, true), 10)
		if err != nil || a != b {
			t.Fatal("directions disagree")
		}
		seen[a] = true
	}
	if len(seen) != 10 {
		t.Fatalf("only %d lanes used", len(seen))
	}
	p, lanes := newFakePool(t, 10, Config{Mode: "flow"})
	startPool(t, p)
	b := packet(20123, false)
	index, _ := FlowIndex(b, 10)
	for i := 0; i < 5; i++ {
		if err := p.Send(b); err != nil {
			t.Fatal(err)
		}
	}
	lanes[index].connected.Store(false)
	if err := p.Send(b); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("missing bounded unavailable error: %v", err)
	}
	for i, l := range lanes {
		if i != index && l.count() != 0 {
			t.Fatal("flow migrated")
		}
	}
	lanes[index].connected.Store(true)
	if err := p.Send(packet(20123, true)); err != nil {
		t.Fatal(err)
	}
	if lanes[index].count() != 6 {
		t.Fatal("recovered flow did not retain affinity")
	}
}

func TestStripeRetriesOtherReadyLaneAndIsBounded(t *testing.T) {
	p, ls := newFakePool(t, 3, Config{Mode: "stripe", MinReady: 2})
	startPool(t, p)
	for i := 0; i < 9; i++ {
		if err := p.Send(packet(22000, false)); err != nil {
			t.Fatal(err)
		}
	}
	for _, l := range ls {
		if l.count() != 3 {
			t.Fatal("not round robin")
		}
	}
	ls[0].connected.Store(false)
	ls[1].busy.Store(true)
	if p.ReadyCount() != 2 || !p.IsConnected() {
		t.Fatal("ready threshold wrong")
	}
	if err := p.Send(packet(22000, false)); err != nil {
		t.Fatal(err)
	}
	if ls[2].count() != 4 {
		t.Fatal("did not skip failed and full channels")
	}
	ls[2].busy.Store(true)
	start := time.Now()
	err := p.Send(packet(22000, false))
	if !errors.Is(err, ErrBusy) || time.Since(start) > 100*time.Millisecond {
		t.Fatal("blocked or hid queue exhaustion")
	}
}

func TestEnvelopeRejectsEchoDifferentPoolLaneModeAndMalformed(t *testing.T) {
	a, al := newFakePool(t, 1, Config{Mode: "stripe", PoolID: [16]byte{1}})
	startPool(t, a)
	b, bl := newFakePool(t, 1, Config{Mode: "stripe", PoolID: [16]byte{1}})
	startPool(t, b)
	var received atomic.Int32
	b.Receive(func(got []byte) {
		if !bytes.Equal(got, packet(23456, false)) {
			t.Error("payload changed")
		}
		received.Add(1)
	})
	if err := a.Send(packet(23456, false)); err != nil {
		t.Fatal(err)
	}
	wire := al[0].last()
	bl[0].inject(wire)
	if received.Load() != 1 {
		t.Fatal("peer payload not received")
	}
	b.Send(packet(23456, false))
	bl[0].inject(bl[0].last())
	for _, off := range []int{0, 4, 36, 37, 38, 39} {
		bad := append([]byte(nil), wire...)
		bad[off] ^= 1
		bl[0].inject(bad)
	}
	bl[0].inject(wire[:10])
	bl[0].inject(packet(23456, false))
	if received.Load() != 1 {
		t.Fatal("accepted echo or foreign envelope")
	}
	b.Stop()
	bl[0].inject(wire)
	if received.Load() != 1 {
		t.Fatal("received after Stop")
	}
}

func TestStopCancelsStaggerWithoutStartingLaterChannels(t *testing.T) {
	p, ls := newFakePool(t, 10, Config{Mode: "flow", StartStagger: time.Second})
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for ls[0].starts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	p.Stop()
	select {
	case <-p.done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("stagger did not cancel")
	}
	for _, l := range ls[1:] {
		if l.starts.Load() != 0 {
			t.Fatal("started after Stop")
		}
	}
	if p.IsRunning() || p.IsConnected() || !errors.Is(p.Send(packet(20000, false)), ErrStopped) {
		t.Fatal("pool still active")
	}
	if err := p.Start(); err == nil {
		t.Fatal("allowed second Start")
	}
}

func TestConcurrentSendReceiveStatsAndStop(t *testing.T) {
	p, ls := newFakePool(t, 3, Config{Mode: "stripe"})
	startPool(t, p)
	p.Receive(func([]byte) {})
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 150; i++ {
				p.Send(packet(25000, false))
				p.Stats()
				p.ReadyCount()
				p.Receive(func([]byte) {})
			}
		}()
	}
	for i := 0; i < 100; i++ {
		ls[i%3].connected.Store(i%2 == 0)
	}
	p.Stop()
	wg.Wait()
}

func TestRejectsMalformedIPAndLimits(t *testing.T) {
	valid := packet(23456, false)
	for _, off := range []int{0, 2, 6, 9, 32} {
		bad := append([]byte(nil), valid...)
		bad[off] = 0xff
		if _, err := FlowIndex(bad, 2); err == nil {
			t.Fatalf("accepted malformed field %d", off)
		}
	}
	if _, err := FlowIndex(make([]byte, 1600), 2); err == nil {
		t.Fatal("accepted oversized packet")
	}
	for _, cfg := range []Config{{Mode: "wrong"}, {Mode: "flow", MinReady: 2}, {Mode: "flow", StartStagger: -1}} {
		if _, err := New([]transport.Transport{&fakeLane{}}, cfg); err == nil {
			t.Fatal("accepted invalid config")
		}
	}
	if _, err := New(nil, Config{Mode: "flow"}); err == nil {
		t.Fatal("accepted no lanes")
	}
}

func TestTerminalStartFailureDoesNotLeakDetails(t *testing.T) {
	p, ls := newFakePool(t, 1, Config{Mode: "flow"})
	ls[0].startErr = true
	startPool(t, p)
	if p.IsRunning() || p.IsConnected() {
		t.Fatal("failed transport remained running")
	}
	if p.LaneStats()[0].SendErrors != 1 {
		t.Fatal("start failure not counted")
	}
}
