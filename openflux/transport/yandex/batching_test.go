package yandex

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"universal-bypass-tool/transport"
)

func newBatchTransport() *YandexDocsTransport {
	return NewYandexDocsTransport("https://disk.yandex.ru/i/fixture", transport.DefaultConfig())
}

func TestBatchCodecRoundtripAndRecordLimit(t *testing.T) {
	packets := [][]byte{[]byte{0, 1, 2, 255}, bytes.Repeat([]byte{7}, 1540), []byte("last")}
	wire, err := encodeBatch(packets, 4096)
	if err != nil {
		t.Fatal(err)
	}
	got, isBatch, err := decodeBatch(wire)
	if err != nil || !isBatch || len(got) != len(packets) {
		t.Fatal("batch decode failed")
	}
	for i := range got {
		if !bytes.Equal(got[i], packets[i]) {
			t.Fatalf("packet%d changed", i)
		}
	}
	many := make([][]byte, 256)
	for i := range many {
		many[i] = []byte{byte(i)}
	}
	wire, err = encodeBatch(many, 4096)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err = decodeBatch(wire)
	if err != nil || len(got) != 256 {
		t.Fatal("maximum count did not roundtrip")
	}
	for _, bad := range [][][]byte{append(many, []byte{1}), {nil}, {make([]byte, 65536)}} {
		if _, err = encodeBatch(bad, maxBatchBytes); err == nil {
			t.Fatal("accepted invalid encode input")
		}
	}
}

func TestMalformedBatchNeverPartiallyDelivers(t *testing.T) {
	valid, err := encodeBatch([][]byte{[]byte("first"), []byte("second")}, 4096)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{}
	mutate := func(name string, fn func([]byte)) { b := append([]byte(nil), valid...); fn(b); cases[name] = b }
	mutate("zero_count", func(b []byte) { binary.BigEndian.PutUint16(b[4:6], 0) })
	mutate("excess_count", func(b []byte) { binary.BigEndian.PutUint16(b[4:6], 257) })
	mutate("wrong_count", func(b []byte) { binary.BigEndian.PutUint16(b[4:6], 3) })
	mutate("reserved", func(b []byte) { b[7] = 1 })
	mutate("unknown_version", func(b []byte) { b[3] = '2' })
	mutate("zero_length", func(b []byte) { binary.BigEndian.PutUint16(b[8:10], 0) })
	mutate("oversized_length", func(b []byte) { binary.BigEndian.PutUint16(b[8:10], 65535) })
	cases["truncated_after_valid_record"] = valid[:len(valid)-1]
	cases["trailing_bytes"] = append(append([]byte(nil), valid...), 0)
	cases["short_header"] = []byte("MBD1")
	cases["oversized_batch"] = append(append([]byte(nil), valid...), make([]byte, maxBatchBytes)...)
	for name, wire := range cases {
		t.Run(name, func(t *testing.T) {
			tr := newBatchTransport()
			received := 0
			tr.Receive(func([]byte) { received++ })
			tr.receiveCursor(encodeCursor(wire))
			if received != 0 || tr.BatchStats().MalformedBatches != 1 {
				t.Fatal("malformed batch delivered packets or was not counted")
			}
		})
	}
}

func TestLegacy64KiBAndBatchOffRemainCompatible(t *testing.T) {
	tr := newBatchTransport()
	if err := tr.ConfigureBatching(0, 2*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	legacy := bytes.Repeat([]byte{0x45}, 65536)
	queue := make(chan []byte, 1)
	queue <- []byte("next")
	wire, pending, n, err := tr.collectBatch(context.Background(), queue, legacy)
	if err != nil || n != 1 || pending != nil || !bytes.Equal(wire, legacy) || len(queue) != 1 {
		t.Fatal("off mode changed packet or consumed queue")
	}
	var got []byte
	tr.Receive(func(b []byte) { got = append([]byte(nil), b...) })
	tr.receiveCursor(encodeCursor(legacy))
	if !bytes.Equal(got, legacy) || tr.BatchStats().BatchesReceived != 0 {
		t.Fatal("legacy64KiB rejected")
	}
	if err = tr.ConfigureBatching(4096, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	wire, pending, n, err = tr.collectBatch(context.Background(), queue, legacy)
	if err != nil || n != 1 || pending != nil || !bytes.Equal(wire, legacy) || len(queue) != 1 {
		t.Fatal("large legacy packet changed in batch mode")
	}
}

func TestBatchCollectionSplitsAtByteAndCountCaps(t *testing.T) {
	tr := newBatchTransport()
	tr.ConfigureBatching(4096, 0)
	queue := make(chan []byte, 300)
	a, b, c := bytes.Repeat([]byte{1}, 1540), bytes.Repeat([]byte{2}, 1540), bytes.Repeat([]byte{3}, 1540)
	queue <- b
	queue <- c
	wire, pending, n, err := tr.collectBatch(context.Background(), queue, a)
	if err != nil || n != 2 || !bytes.Equal(pending, c) || len(queue) != 0 {
		t.Fatal("split lost FIFO packet")
	}
	got, _, err := decodeBatch(wire)
	if err != nil || !bytes.Equal(got[0], a) || !bytes.Equal(got[1], b) {
		t.Fatal("split changed packets")
	}
	for i := 0; i < 299; i++ {
		queue <- []byte{1}
	}
	wire, pending, n, err = tr.collectBatch(context.Background(), queue, []byte{0})
	if err != nil || n != 256 || pending != nil || len(queue) != 44 {
		t.Fatal("count cap not enforced")
	}
	got, _, err = decodeBatch(wire)
	if err != nil || len(got) != 256 {
		t.Fatal("count-capped frame invalid")
	}
}

func TestBatchConfigurationAndCancelledFlush(t *testing.T) {
	tr := newBatchTransport()
	for _, cfg := range []struct {
		max   int
		delay time.Duration
	}{{-1, 0}, {32769, 0}, {4096, -1}, {4096, 3 * time.Millisecond}} {
		if err := tr.ConfigureBatching(cfg.max, cfg.delay); err == nil {
			t.Fatal("accepted unsafe batch config")
		}
	}
	for _, n := range []int{0, 4096, 8192, 16384, 32768} {
		if err := tr.ConfigureBatching(n, time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, err := tr.collectBatch(ctx, make(chan []byte), []byte("first"))
	if !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled flush continued")
	}
	tr.done = make(chan struct{})
	if err = tr.ConfigureBatching(4096, 0); err == nil {
		t.Fatal("changed config after Start")
	}
}

// A loopback WebSocket exercises the real runSession writer, batch splitting,
// pending-packet FIFO, control ping/pong, receiver callbacks and cancellation.
func TestBatchWriterWebsocketIntegration(t *testing.T) {
	for _, maxBytes := range []int{0, 4096} {
		t.Run(map[bool]string{true: "off", false: "on"}[maxBytes == 0], func(t *testing.T) {
			serverConn := make(chan *websocket.Conn, 1)
			release := make(chan struct{})
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer c.Close()
				serverConn <- c
				<-release
			}))
			defer srv.Close()
			defer close(release)
			client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			server := <-serverConn
			server.SetReadDeadline(time.Now().Add(5 * time.Second))
			tr := newBatchTransport()
			if err = tr.ConfigureBatching(maxBytes, time.Millisecond); err != nil {
				t.Fatal(err)
			}
			packets := [][]byte{bytes.Repeat([]byte{1}, 1200), bytes.Repeat([]byte{2}, 1200), bytes.Repeat([]byte{3}, 1200), bytes.Repeat([]byte{4}, 1200), bytes.Repeat([]byte{5}, 65536), bytes.Repeat([]byte{6}, 100), bytes.Repeat([]byte{7}, 100)}
			s := &DocSession{Conn: client, WriteQueue: make(chan []byte, 16), authenticated: true}
			for _, p := range packets {
				s.WriteQueue <- p
			}
			received := make(chan []byte, 16)
			tr.Receive(func(b []byte) { received <- append([]byte(nil), b...) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- tr.runSession(ctx, s) }()
			if err = server.WriteMessage(websocket.TextMessage, []byte("2")); err != nil {
				t.Fatal(err)
			}
			messages := 0
			seenPackets := 0
			pong := false
			for seenPackets < len(packets) || !pong {
				_, frame, err := server.ReadMessage()
				if err != nil {
					t.Fatal(err)
				}
				if string(frame) == "3" {
					pong = true
					continue
				}
				if !strings.HasPrefix(string(frame), "42") {
					t.Fatal("unexpected writer frame")
				}
				var events []json.RawMessage
				if err = json.Unmarshal(frame[2:], &events); err != nil || len(events) != 2 {
					t.Fatal("bad socket envelope")
				}
				var event struct {
					Cursor string `json:"cursor"`
				}
				if err = json.Unmarshal(events[1], &event); err != nil {
					t.Fatal(err)
				}
				declared, encoded, found := strings.Cut(event.Cursor, ";")
				wire, err := base64.StdEncoding.DecodeString(encoded)
				if err != nil || !found || declared != strconv.Itoa(len(wire)) {
					t.Fatal("writer used an invalid declared cursor byte length")
				}
				decoded, batched, err := decodeBatch(wire)
				if err != nil {
					t.Fatal(err)
				}
				if !batched {
					decoded = [][]byte{wire}
				}
				for _, p := range decoded {
					if seenPackets >= len(packets) || !bytes.Equal(p, packets[seenPackets]) {
						t.Fatal("writer reordered or changed packets")
					}
					seenPackets++
				}
				messages++
				if err = server.WriteMessage(websocket.TextMessage, frame); err != nil {
					t.Fatal(err)
				}
			}
			for i, want := range packets {
				select {
				case got := <-received:
					if !bytes.Equal(got, want) {
						t.Fatalf("receiver packet%d changed", i)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("receiver did not deliver all packets")
				}
			}
			if maxBytes == 0 && messages != len(packets) {
				t.Fatal("off mode batched")
			}
			if maxBytes > 0 && messages != 4 {
				t.Fatalf("expected two batches plus two legacy messages, got%d", messages)
			}
			stats := tr.BatchStats()
			if stats.PacketsSent != uint64(len(packets)) || stats.PacketsReceived != uint64(len(packets)) || stats.MessagesSent != uint64(messages) {
				t.Fatalf("incorrect counters: %+v", stats)
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("batch writer/reader failed to stop")
			}
		})
	}
}
