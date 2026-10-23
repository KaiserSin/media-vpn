package streamrelay

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// Regression of the shared-document bug: B formerly reset A on an unknown
// server ACK, even though the devices used distinct random stream IDs.
func TestSharedKeyBroadcastSecondClientPreservesFirst(t *testing.T) {
	a, flowA := offlineConn(t)
	b, flowB := offlineConn(t)
	exit, serverFlowA := offlineConn(t)
	exit.direction = 1
	delete(b.streams, flowB.id)
	flowB.id = streamID{4}
	b.streams[flowB.id] = flowB
	ack := encodeFrame(exit.cfg.Key, exit.ackFrameLocked(serverFlowA))
	a.receive(ack)
	b.receive(ack)
	if flowA.closed || flowB.closed || serverFlowA.closed {
		t.Fatal("broadcast ACK closed a stream")
	}
	if len(b.controls) != 0 || b.Stats().IgnoredForeignFrames != 1 {
		t.Fatal("B did not quietly ignore A's ACK")
	}
	if _, ok := b.nextFrameLocked(time.Now()); ok {
		t.Fatal("B emitted a response to A's ACK")
	}

	payload := []byte("A remains connected")
	if _, err := flowA.Write(payload); err != nil {
		t.Fatal(err)
	}
	data, ok := a.nextFrameLocked(time.Now())
	if !ok {
		t.Fatal("A DATA not scheduled")
	}
	exit.receive(data)
	got := make([]byte, len(payload))
	if n, err := serverFlowA.Read(got); err != nil || n != len(payload) || !bytes.Equal(got, payload) {
		t.Fatalf("A could not continue: %d %v", n, err)
	}
	if exit.Stats().ActiveStreams != 1 || flowA.closed || flowB.closed {
		t.Fatal("A or B connection was disrupted")
	}
}

func foreignFrame(kind byte, id streamID, direction byte) frame {
	f := frame{kind: kind, id: id, direction: direction}
	switch kind {
	case kindACK:
		f.flags = flagReady
		f.data = make([]byte, 12)
		binary.BigEndian.PutUint32(f.data[8:], 64<<10)
	case kindData:
		f.data = []byte("foreign data")
	}
	return f
}

func TestClientIgnoresForeignACKDataFINAndRST(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind byte
	}{{"ACK", kindACK}, {"DATA", kindData}, {"FIN", kindFIN}, {"RST", kindRST}} {
		t.Run(tc.name, func(t *testing.T) {
			r, own := offlineConn(t)
			r.receive(encodeFrame(r.cfg.Key, foreignFrame(tc.kind, streamID{99}, 1)))
			got := r.Stats()
			if got.FramesReceived != 1 || got.IgnoredForeignFrames != 1 || got.ActiveStreams != 1 || got.BufferedBytes != 0 || got.AuthenticationFailures != 0 || len(r.controls) != 0 || own.closed {
				t.Fatalf("foreign frame disturbed client: %+v", got)
			}
		})
	}
}

func TestOwnClosedTombstonesKeepResponses(t *testing.T) {
	for _, graceful := range []bool{false, true} {
		for _, kind := range []byte{kindACK, kindData, kindFIN, kindRST} {
			r, c := offlineConn(t)
			if graceful {
				r.closeConnLocked(c, nil, false)
			} else {
				r.closeConnLocked(c, net.ErrClosed, false)
			}
			r.receive(encodeFrame(r.cfg.Key, foreignFrame(kind, c.id, 1)))
			wantKind := byte(0)
			if graceful {
				if kind == kindData || kind == kindFIN {
					wantKind = kindACK
				}
			} else if kind != kindRST {
				wantKind = kindRST
			}
			if wantKind == 0 {
				if len(r.controls) != 0 {
					t.Fatalf("unexpected own tomb response: graceful=%t kind=%d", graceful, kind)
				}
			} else if len(r.controls) != 1 || r.controls[0].kind != wantKind || r.controls[0].id != c.id {
				t.Fatalf("missing own tomb response: graceful=%t kind=%d", graceful, kind)
			}
			if r.Stats().IgnoredForeignFrames != 0 {
				t.Fatal("own tombstone was misclassified as foreign")
			}
		}
	}
}

func TestExitUnknownStreamsStillReset(t *testing.T) {
	for _, kind := range []byte{kindACK, kindData, kindFIN, kindRST} {
		r, own := offlineConn(t)
		r.direction = 1
		unknown := streamID{99}
		r.receive(encodeFrame(r.cfg.Key, foreignFrame(kind, unknown, 0)))
		if kind == kindRST {
			if len(r.controls) != 0 {
				t.Fatal("RST produced a reset loop")
			}
		} else if len(r.controls) != 1 || r.controls[0].kind != kindRST || r.controls[0].id != unknown || r.controls[0].direction != 1 {
			t.Fatalf("exit stopped rejecting unknown kind %d", kind)
		}
		if own.closed || r.Stats().IgnoredForeignFrames != 0 {
			t.Fatal("exit behavior changed")
		}
	}
}

func TestFreshOrRestartedClientDoesNotResetExistingSessions(t *testing.T) {
	r, unused := offlineConn(t)
	// Model a newly started process: no active stream and no prior tombstone.
	delete(r.streams, unused.id)
	r.order = nil
	oldID := streamID{99}
	for _, kind := range []byte{kindACK, kindData, kindFIN, kindRST} {
		r.receive(encodeFrame(r.cfg.Key, foreignFrame(kind, oldID, 1)))
	}
	if r.Stats().IgnoredForeignFrames != 4 || len(r.controls) != 0 || len(r.streams) != 0 {
		t.Fatal("new client disturbed pre-existing streams")
	}
	// A newly opened local flow still receives its own data normally; old
	// sessions are not resumed implicitly after a process restart.
	current := r.newConnLocked(streamID{100})
	current.established = true
	current.remoteWindow = uint64(r.cfg.WindowBytes)
	f := foreignFrame(kindData, current.id, 1)
	r.receive(encodeFrame(r.cfg.Key, f))
	got := make([]byte, len(f.data))
	if _, err := current.Read(got); err != nil || !bytes.Equal(got, f.data) {
		t.Fatal("fresh local flow stopped receiving")
	}
}

func TestDifferentKeyRejectsBeforeForeignStreamHandling(t *testing.T) {
	b, _ := offlineConn(t)
	exit, c := offlineConn(t)
	exit.direction = 1
	b.cfg.Key = append([]byte(nil), b.cfg.Key...)
	b.cfg.Key[0] ^= 1
	b.receive(encodeFrame(exit.cfg.Key, exit.ackFrameLocked(c)))
	if b.Stats().AuthenticationFailures != 1 || b.Stats().IgnoredForeignFrames != 0 || len(b.controls) != 0 {
		t.Fatal("wrong key bypassed authentication")
	}
}
