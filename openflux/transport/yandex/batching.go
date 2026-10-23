package yandex

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	maxBatchBytes    = 32 << 10
	maxBatchPackets  = 256
	batchHeaderBytes = 8
)

var errBatch = errors.New("invalid document packet batch")

type BatchStats struct {
	MessagesSent, PacketsSent, BatchesSent             uint64
	PacketsReceived, BatchesReceived, MalformedBatches uint64
}

type batchCounters struct {
	messagesSent, packetsSent, batchesSent      atomic.Uint64
	packetsReceived, batchesReceived, malformed atomic.Uint64
}

// ConfigureBatching must be called before Start. Zero leaves the legacy sender
// unchanged. The receiver always accepts both legacy packets and valid MBD1.
// maxBytes includes batch headers; a packet too large to fit is sent alone.
func (t *YandexDocsTransport) ConfigureBatching(maxBytes int, flushDelay time.Duration) error {
	if maxBytes < 0 || maxBytes > maxBatchBytes || flushDelay < 0 || flushDelay > 2*time.Millisecond {
		return errors.New("batch limit must be 0..32768 bytes and flush delay 0..2ms")
	}
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	if t.done != nil {
		return errors.New("configure batching before transport start")
	}
	t.batchMaxBytes, t.batchFlushDelay = maxBytes, flushDelay
	return nil
}

func (t *YandexDocsTransport) BatchStats() BatchStats {
	return BatchStats{MessagesSent: t.batchStats.messagesSent.Load(), PacketsSent: t.batchStats.packetsSent.Load(), BatchesSent: t.batchStats.batchesSent.Load(), PacketsReceived: t.batchStats.packetsReceived.Load(), BatchesReceived: t.batchStats.batchesReceived.Load(), MalformedBatches: t.batchStats.malformed.Load()}
}

// MBD1 | count:u16 | reserved:u16=0 | (length:u16 | payload)*.
// Counts, total length and every record are validated before any callback.
func encodeBatch(packets [][]byte, limit int) ([]byte, error) {
	if len(packets) < 1 || len(packets) > maxBatchPackets || limit < batchHeaderBytes || limit > maxBatchBytes {
		return nil, errBatch
	}
	total := batchHeaderBytes
	for _, p := range packets {
		if len(p) == 0 || len(p) > maxBatchBytes-batchHeaderBytes-2 {
			return nil, errBatch
		}
		total += 2 + len(p)
		if total > limit {
			return nil, errBatch
		}
	}
	wire := make([]byte, total)
	copy(wire[:4], "MBD1")
	binary.BigEndian.PutUint16(wire[4:6], uint16(len(packets)))
	offset := batchHeaderBytes
	for _, p := range packets {
		binary.BigEndian.PutUint16(wire[offset:offset+2], uint16(len(p)))
		offset += 2
		copy(wire[offset:], p)
		offset += len(p)
	}
	return wire, nil
}

func decodeBatch(wire []byte) ([][]byte, bool, error) {
	// Reserve the MBD family so an unknown version cannot become an IP packet.
	if !bytes.HasPrefix(wire, []byte("MBD")) {
		return nil, false, nil
	}
	if len(wire) < batchHeaderBytes || len(wire) > maxBatchBytes || string(wire[:4]) != "MBD1" || binary.BigEndian.Uint16(wire[6:8]) != 0 {
		return nil, true, errBatch
	}
	count := int(binary.BigEndian.Uint16(wire[4:6]))
	if count < 1 || count > maxBatchPackets {
		return nil, true, errBatch
	}
	packets := make([][]byte, 0, count)
	offset := batchHeaderBytes
	for i := 0; i < count; i++ {
		if offset+2 > len(wire) {
			return nil, true, errBatch
		}
		n := int(binary.BigEndian.Uint16(wire[offset : offset+2]))
		offset += 2
		if n == 0 || offset+n > len(wire) {
			return nil, true, errBatch
		}
		packets = append(packets, wire[offset:offset+n])
		offset += n
	}
	if offset != len(wire) {
		return nil, true, errBatch
	}
	return packets, true, nil
}

// collectBatch never waits beyond the configured <=2ms flush deadline. A
// dequeued packet that will not fit is returned as pending, preserving FIFO.
func (t *YandexDocsTransport) collectBatch(ctx context.Context, queue <-chan []byte, first []byte) ([]byte, []byte, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, 0, err
	}
	if t.batchMaxBytes == 0 || len(first) == 0 || batchHeaderBytes+2+len(first) > t.batchMaxBytes {
		return first, nil, 1, nil
	}
	packets := make([][]byte, 0, min(16, maxBatchPackets))
	packets = append(packets, first)
	total := batchHeaderBytes + 2 + len(first)
	var timer *time.Timer
	var timeout <-chan time.Time
	if t.batchFlushDelay > 0 {
		timer = time.NewTimer(t.batchFlushDelay)
		timeout = timer.C
		defer timer.Stop()
	}
	var pending []byte
collect:
	for attempts := 1; attempts < maxBatchPackets; attempts++ {
		select {
		case <-ctx.Done():
			return nil, nil, 0, ctx.Err()
		case <-timeout:
			break collect
		default:
		}
		var next []byte
		select {
		case next = <-queue:
		default:
			if timeout == nil {
				break collect
			}
			select {
			case <-ctx.Done():
				return nil, nil, 0, ctx.Err()
			case <-timeout:
				break collect
			case next = <-queue:
			}
		}
		// Empty legacy cursor payloads were already ignored by the receiver.
		if len(next) == 0 {
			continue
		}
		if total+2+len(next) > t.batchMaxBytes {
			pending = next
			break
		}
		packets = append(packets, next)
		total += 2 + len(next)
	}
	if len(packets) == 1 {
		return first, pending, 1, nil
	}
	wire, err := encodeBatch(packets, t.batchMaxBytes)
	return wire, pending, len(packets), err
}

func (t *YandexDocsTransport) writePacketBatch(ctx context.Context, s *DocSession, first []byte) ([]byte, error) {
	wire, pending, count, err := t.collectBatch(ctx, s.WriteQueue, first)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = s.safeWrite(websocket.TextMessage, []byte(`42["message",{"type":"cursor","cursor":"`+encodeCursor(wire)+`"}]`)); err != nil {
		return nil, err
	}
	t.RecordSend(len(wire))
	t.batchStats.messagesSent.Add(1)
	t.batchStats.packetsSent.Add(uint64(count))
	if count > 1 {
		t.batchStats.batchesSent.Add(1)
	}
	return pending, nil
}
