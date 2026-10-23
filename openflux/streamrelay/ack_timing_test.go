package streamrelay

import (
	"encoding/binary"
	"testing"
	"time"
)

// No scheduler goroutine: timers and frame selection are advanced explicitly.
func offlineConn(t *testing.T) (*Relay, *Conn) {
	t.Helper()
	cfg, e := normalize(testConfig())
	if e != nil {
		t.Fatal(e)
	}
	r := &Relay{cfg: cfg, streams: make(map[streamID]*Conn), tombs: make(map[streamID]tombstone), wake: make(chan struct{}, 1), budgetChanged: make(chan struct{})}
	c := r.newConnLocked(streamID{3})
	c.established = true
	c.remoteWindow = uint64(cfg.WindowBytes)
	return r, c
}

func TestConfiguredRTOFloorSurvivesFastACKSamples(t *testing.T) {
	for _, floor := range []time.Duration{3 * time.Second, 5 * time.Second, 7 * time.Second} {
		r, c := offlineConn(t)
		r.cfg.RetransmitTimeout = floor
		for i := 0; i < 20; i++ {
			r.observeRTTLocked(c, time.Millisecond)
		}
		if c.rto != floor {
			t.Fatalf("floor %v became %v", floor, c.rto)
		}
		for i := 0; i < 20; i++ {
			r.observeRTTLocked(c, 10*time.Second)
		}
		ceiling := 5 * time.Second
		if floor > ceiling {
			ceiling = floor
		}
		if c.rto != ceiling {
			t.Fatalf("bounded adaptive RTO: got %v want %v", c.rto, ceiling)
		}
	}
}

func TestReadCoalescesSmallCreditButReopensFullWindowImmediately(t *testing.T) {
	r, c := offlineConn(t)
	r.allocateLocked(4096)
	c.ready = append(c.ready, &chunk{data: make([]byte, 4096)})
	c.recvNext = 4096
	r.requestACKLocked(c, false)
	if _, e := c.Read(make([]byte, 4096)); e != nil {
		t.Fatal(e)
	}
	if c.ackAt.IsZero() {
		t.Fatal("draining one small chunk bypassed ACK coalescing")
	}
	if _, ok := r.nextFrameLocked(c.ackAt.Add(-time.Millisecond)); ok {
		t.Fatal("ACK before flush deadline")
	}
	b, ok := r.nextFrameLocked(c.ackAt.Add(time.Millisecond))
	if !ok {
		t.Fatal("ACK not flushed")
	}
	f, e := decodeFrame(r.cfg.Key, b)
	if e != nil || f.kind != kindACK || binary.BigEndian.Uint64(f.data) != 4096 {
		t.Fatal("missing consumed credit")
	}
	if c.lastACKConsumed != 4096 {
		t.Fatal("advertised credit not recorded")
	}

	r, c = offlineConn(t)
	r.allocateLocked(4096)
	c.ready = append(c.ready, &chunk{data: make([]byte, 4096)})
	c.recvNext = uint64(r.cfg.WindowBytes)
	c.lastACK = c.recvNext
	if _, e = c.Read(make([]byte, 4096)); e != nil {
		t.Fatal(e)
	}
	if !c.ackAt.IsZero() {
		t.Fatal("reopened zero window was delayed")
	}
}

func TestACKThresholdIncludesConsumedBytes(t *testing.T) {
	r, c := offlineConn(t)
	c.recvNext = 64 << 10
	c.lastACK = c.recvNext
	c.recvConsumed = 32 << 10
	r.requestACKLocked(c, false)
	if !c.ackAt.IsZero() {
		t.Fatal("32 KiB consumed credit was delayed")
	}
}

func TestLongRTOStillRefreshesCreditWithinOneSecond(t *testing.T) {
	r, c := offlineConn(t)
	c.rto = 5 * time.Second
	c.recvNext = 4096
	c.recvConsumed = 4096
	c.lastACK = 4096
	c.lastACKConsumed = 4096
	c.ackLast = time.Now().Add(-1100 * time.Millisecond)
	b, ok := r.nextFrameLocked(time.Now())
	if !ok || b[4] != kindACK {
		t.Fatal("credit refresh waited for long data RTO")
	}
}

func TestLongRTOAllowsDocumentQueueDelayWithoutRetransmit(t *testing.T) {
	r, c := offlineConn(t)
	r.cfg.RetransmitTimeout = 3 * time.Second
	c.rto = 3 * time.Second
	now := time.Now()
	c.sendNext = 4096
	c.sentMax = 4096
	c.out = append(c.out, &chunk{offset: 0, data: make([]byte, 4096), last: now.Add(-1500 * time.Millisecond), tries: 1})
	if _, ok := r.nextFrameLocked(now); ok {
		t.Fatal("queued data retransmitted before configured floor")
	}
	b, ok := r.nextFrameLocked(now.Add(2 * time.Second))
	if !ok || b[4] != kindData || c.out[0].tries != 2 {
		t.Fatal("missing bounded retransmission after RTO")
	}
}
