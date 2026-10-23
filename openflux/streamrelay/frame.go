// Package streamrelay carries authenticated, bounded byte streams over a
// message transport. It deliberately does not accept a destination from peers.
package streamrelay

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const (
	frameHeader      = 36
	frameMAC         = sha256.Size
	maxFrame         = 32 * 1024
	maxPayload       = maxFrame - frameHeader - frameMAC
	kindOpen    byte = 1
	kindACK     byte = 2
	kindData    byte = 3
	kindFIN     byte = 4
	kindRST     byte = 5
	flagReady   byte = 1
	flagFIN     byte = 2
)

var errFrame = errors.New("stream relay: malformed frame")
var errAuthentication = errors.New("stream relay: authentication failed")

type streamID [16]byte
type frame struct {
	kind, direction, flags byte
	id                     streamID
	offset                 uint64
	data                   []byte
}

func encodeFrame(key []byte, f frame) []byte {
	b := make([]byte, frameHeader+len(f.data)+frameMAC)
	copy(b[:4], "MSR1")
	b[4], b[5], b[6] = f.kind, f.direction, f.flags
	copy(b[8:24], f.id[:])
	binary.BigEndian.PutUint64(b[24:32], f.offset)
	binary.BigEndian.PutUint32(b[32:36], uint32(len(f.data)))
	copy(b[frameHeader:], f.data)
	m := hmac.New(sha256.New, key)
	m.Write(b[:len(b)-frameMAC])
	copy(b[len(b)-frameMAC:], m.Sum(nil))
	return b
}

// decodeFrame authenticates without copying attacker-controlled payloads.
// The returned data aliases b and must be copied before returning a callback.
func decodeFrame(key, b []byte) (frame, error) {
	var f frame
	if len(b) < frameHeader+frameMAC || len(b) > maxFrame || string(b[:4]) != "MSR1" || b[7] != 0 || b[5] > 1 {
		return f, errFrame
	}
	n := int(binary.BigEndian.Uint32(b[32:36]))
	if n != len(b)-frameHeader-frameMAC {
		return f, errFrame
	}
	m := hmac.New(sha256.New, key)
	m.Write(b[:len(b)-frameMAC])
	if !hmac.Equal(m.Sum(nil), b[len(b)-frameMAC:]) {
		return f, errAuthentication
	}
	f.kind, f.direction, f.flags = b[4], b[5], b[6]
	copy(f.id[:], b[8:24])
	f.offset = binary.BigEndian.Uint64(b[24:32])
	f.data = b[frameHeader : len(b)-frameMAC]
	if f.id == (streamID{}) {
		return frame{}, errFrame
	}
	switch f.kind {
	case kindOpen:
		if n != 4 || f.offset != 0 || f.flags != 0 {
			return frame{}, errFrame
		}
	case kindACK:
		if n != 12 || f.flags & ^byte(flagReady|flagFIN) != 0 {
			return frame{}, errFrame
		}
	case kindData:
		if n == 0 || n > maxPayload || f.flags != 0 || f.offset+uint64(n) < f.offset {
			return frame{}, errFrame
		}
	case kindFIN:
		if n != 0 || f.flags != 0 {
			return frame{}, errFrame
		}
	case kindRST:
		if n != 0 || f.offset != 0 || f.flags != 0 {
			return frame{}, errFrame
		}
	default:
		return frame{}, errFrame
	}
	return f, nil
}
