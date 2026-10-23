package streamrelay

import (
	"context"
	"crypto/rand"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"time"
)

type Conn struct {
	r                                                      *Relay
	id                                                     streamID
	readMu, writeMu                                        sync.Mutex
	notify, done                                           chan struct{}
	established, closed                                    bool
	err                                                    error
	created, activity                                      time.Time
	readDeadline, writeDeadline                            time.Time
	openLast                                               time.Time
	openTries                                              int
	rto, srtt                                              time.Duration
	out                                                    []*chunk
	sendNext, sentMax, acked, remoteConsumed, remoteWindow uint64
	incoming                                               map[uint64]*chunk
	ready                                                  []*chunk
	recvNext, recvConsumed                                 uint64
	ackDirty                                               bool
	ackAt, ackLast                                         time.Time
	lastACK, lastACKConsumed                               uint64
	writeClosed, finSent, finAck                           bool
	finLast                                                time.Time
	finTries                                               int
	recvFIN                                                bool
	recvFinal                                              uint64
}

var _ net.Conn = (*Conn)(nil)

func (c *Conn) notifyLocked() { close(c.notify); c.notify = make(chan struct{}) }

func (r *Relay) DialTCP(address string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.OpenTimeout)
	defer cancel()
	return r.DialTCPContext(ctx, address)
}

func (r *Relay) DialTCPContext(ctx context.Context, address string) (net.Conn, error) {
	a, err := netip.ParseAddrPort(address)
	if err != nil || a.String() != r.cfg.VirtualAddress || r.direction != 0 {
		return nil, &net.OpError{Op: "dial", Net: "streamrelay", Err: errProtocol}
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	var id streamID
	if _, err = rand.Read(id[:]); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, net.ErrClosed
	}
	if len(r.streams) >= r.cfg.MaxStreams {
		r.stats.RejectedStreams++
		r.mu.Unlock()
		return nil, ErrCapacity
	}
	if _, ok := r.streams[id]; ok {
		r.mu.Unlock()
		return nil, ErrCapacity
	}
	c := r.newConnLocked(id)
	r.signal()
	for {
		if c.closed {
			err = c.err
			if err == nil {
				err = net.ErrClosed
			}
			r.mu.Unlock()
			return nil, err
		}
		if c.established {
			r.mu.Unlock()
			return c, nil
		}
		changed := c.notify
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			c.Close()
			return nil, ctx.Err()
		case <-changed:
		}
		r.mu.Lock()
	}
}

func waitChange(ch, budget <-chan struct{}, deadline time.Time) error {
	if deadline.IsZero() {
		select {
		case <-ch:
		case <-budget:
		}
		return nil
	}
	d := time.Until(deadline)
	if d <= 0 {
		return os.ErrDeadlineExceeded
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ch:
		return nil
	case <-budget:
		return nil
	case <-t.C:
		return os.ErrDeadlineExceeded
	}
}

func (c *Conn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	r := c.r
	r.mu.Lock()
	for {
		if !c.readDeadline.IsZero() && !time.Now().Before(c.readDeadline) {
			r.mu.Unlock()
			return 0, os.ErrDeadlineExceeded
		}
		if len(c.ready) > 0 {
			x := c.ready[0]
			n := copy(p, x.data[x.read:])
			x.read += n
			c.recvConsumed += uint64(n)
			c.activity = time.Now()
			if x.read == len(x.data) {
				c.ready[0] = nil
				c.ready = c.ready[1:]
				r.releaseLocked(len(x.data))
			}
			// A drained local read queue is normal and does not require an ACK
			// per message. Prioritize credit only if the advertised window was
			// exhausted, or the final bytes have been consumed.
			reopened := c.recvNext-c.lastACKConsumed >= uint64(r.cfg.WindowBytes)
			r.requestACKLocked(c, reopened || c.recvFIN && c.recvConsumed == c.recvFinal)
			r.maybeCompleteLocked(c)
			r.mu.Unlock()
			return n, nil
		}
		if c.closed {
			e := c.err
			if e == nil {
				e = io.EOF
			}
			r.mu.Unlock()
			return 0, e
		}
		if c.recvFIN && c.recvConsumed == c.recvFinal {
			r.mu.Unlock()
			return 0, io.EOF
		}
		changed, deadline := c.notify, c.readDeadline
		r.mu.Unlock()
		if err := waitChange(changed, nil, deadline); err != nil {
			return 0, err
		}
		r.mu.Lock()
	}
}

func (c *Conn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	r := c.r
	r.mu.Lock()
	written := 0
	for len(p) > 0 {
		if c.closed || c.writeClosed {
			e := c.err
			if e == nil {
				e = net.ErrClosed
			}
			r.mu.Unlock()
			return written, e
		}
		if !c.writeDeadline.IsZero() && !time.Now().Before(c.writeDeadline) {
			r.mu.Unlock()
			return written, os.ErrDeadlineExceeded
		}
		n := len(p)
		if n > r.cfg.ChunkSize {
			n = r.cfg.ChunkSize
		}
		end := c.sendNext + uint64(n)
		if end < c.sendNext {
			r.closeConnLocked(c, errProtocol, true)
			r.mu.Unlock()
			return written, errProtocol
		}
		// Only reserve bytes covered by advertised peer credit. Retain outgoing
		// bytes until its cumulative ACK, not merely until transport.Send.
		if c.established && end <= c.remoteConsumed+c.remoteWindow && len(c.out) < maxChunksPerStream && r.allocateLocked(n) {
			x := &chunk{offset: c.sendNext, data: append([]byte(nil), p[:n]...)}
			c.out = append(c.out, x)
			c.sendNext = end
			c.activity = time.Now()
			p = p[n:]
			written += n
			r.signal()
			continue
		}
		changed, budget, deadline := c.notify, r.budgetChanged, c.writeDeadline
		r.mu.Unlock()
		if err := waitChange(changed, budget, deadline); err != nil {
			return written, err
		}
		r.mu.Lock()
	}
	r.mu.Unlock()
	return written, nil
}

func (c *Conn) CloseWrite() error {
	r := c.r
	r.mu.Lock()
	defer r.mu.Unlock()
	if c.writeClosed {
		return nil
	}
	if c.closed {
		if c.err != nil {
			return c.err
		}
		return net.ErrClosed
	}
	c.writeClosed = true
	c.notifyLocked()
	r.signal()
	return nil
}

func (c *Conn) Close() error {
	r := c.r
	r.mu.Lock()
	r.closeConnLocked(c, net.ErrClosed, true)
	r.mu.Unlock()
	return nil
}

type relayAddr string

func (a relayAddr) Network() string  { return "streamrelay" }
func (a relayAddr) String() string   { return string(a) }
func (c *Conn) LocalAddr() net.Addr  { return relayAddr("local") }
func (c *Conn) RemoteAddr() net.Addr { return relayAddr(c.r.cfg.VirtualAddress) }
func (c *Conn) SetDeadline(t time.Time) error {
	r := c.r
	r.mu.Lock()
	defer r.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	c.readDeadline = t
	c.writeDeadline = t
	c.notifyLocked()
	return nil
}
func (c *Conn) SetReadDeadline(t time.Time) error {
	r := c.r
	r.mu.Lock()
	defer r.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	c.readDeadline = t
	c.notifyLocked()
	return nil
}
func (c *Conn) SetWriteDeadline(t time.Time) error {
	r := c.r
	r.mu.Lock()
	defer r.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	c.writeDeadline = t
	c.notifyLocked()
	return nil
}
