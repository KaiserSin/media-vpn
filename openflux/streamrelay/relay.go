package streamrelay

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"

	"universal-bypass-tool/transport"
)

const maxChunksPerStream = 4096
const maxGlobalChunks = 65536
const tombstoneLimit = 1024

var ErrCapacity = errors.New("stream relay: capacity exhausted")
var errPeerReset = errors.New("stream relay: peer closed stream")
var errProtocol = errors.New("stream relay: invalid stream state")
var errRetries = errors.New("stream relay: retransmission limit reached")

type Config struct {
	Key                                                  []byte
	ChunkSize, WindowBytes, MaxStreams, MaxBufferedBytes int
	Upstream, VirtualAddress                             string
	RetransmitTimeout                                    time.Duration
	MaxRetries                                           int
	OpenTimeout, IdleTimeout                             time.Duration
}

type Stats struct {
	ActiveStreams, BufferedBytes, PeakBufferedBytes                  int
	BytesSent, BytesReceived, FramesSent, FramesReceived             uint64
	Retransmits, SendErrors, AuthenticationFailures, MalformedFrames uint64
	RejectedStreams, ClosedStreams, IgnoredForeignFrames             uint64
}

type chunk struct {
	offset uint64
	data   []byte
	read   int
	last   time.Time
	tries  int
}

type tombstone struct {
	id             streamID
	graceful       bool
	next, consumed uint64
}

type Relay struct {
	mu            sync.Mutex
	trans         transport.Transport
	cfg           Config
	direction     byte
	ctx           context.Context
	cancel        context.CancelFunc
	closed        bool
	streams       map[streamID]*Conn
	order         []*Conn
	cursor        int
	tombs         map[streamID]tombstone
	tombOrder     []streamID
	controls      []frame
	wake          chan struct{}
	budgetChanged chan struct{}
	wg            sync.WaitGroup
	stats         Stats
	chunks        int
	// Only tests replace this; production always dials cfg.Upstream.
	dial func(context.Context, string, string) (net.Conn, error)
}

func normalize(cfg Config) (Config, error) {
	if len(cfg.Key) != 32 {
		return cfg, errors.New("stream relay: key must contain exactly 32 bytes")
	}
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 4096
	}
	if cfg.WindowBytes == 0 {
		cfg.WindowBytes = 2 << 20
	}
	if cfg.MaxStreams == 0 {
		cfg.MaxStreams = 128
	}
	if cfg.MaxBufferedBytes == 0 {
		cfg.MaxBufferedBytes = 32 << 20
	}
	if cfg.Upstream == "" {
		cfg.Upstream = "127.0.0.1:443"
	}
	if cfg.VirtualAddress == "" {
		cfg.VirtualAddress = "10.203.0.1:443"
	}
	if cfg.RetransmitTimeout == 0 {
		cfg.RetransmitTimeout = 750 * time.Millisecond
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 12
	}
	if cfg.OpenTimeout == 0 {
		cfg.OpenTimeout = 20 * time.Second
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 2 * time.Minute
	}
	if cfg.ChunkSize < 512 || cfg.ChunkSize > maxPayload || cfg.WindowBytes < 64<<10 || cfg.WindowBytes > 4<<20 || cfg.MaxStreams < 1 || cfg.MaxStreams > 128 || cfg.MaxBufferedBytes < cfg.WindowBytes || cfg.MaxBufferedBytes > 256<<20 || cfg.RetransmitTimeout < 10*time.Millisecond || cfg.RetransmitTimeout > 10*time.Second || cfg.MaxRetries < 1 || cfg.MaxRetries > 100 || cfg.OpenTimeout < 50*time.Millisecond || cfg.OpenTimeout > 5*time.Minute || cfg.IdleTimeout < time.Second || cfg.IdleTimeout > 24*time.Hour {
		return cfg, errors.New("stream relay: configuration outside supported bounds")
	}
	up, err := netip.ParseAddrPort(cfg.Upstream)
	if err != nil || !up.Addr().IsLoopback() || up.Port() == 0 {
		return cfg, errors.New("stream relay: upstream must be a literal loopback address and port")
	}
	v, err := netip.ParseAddrPort(cfg.VirtualAddress)
	if err != nil || v.Port() == 0 {
		return cfg, errors.New("stream relay: virtual address must be a literal IP address and port")
	}
	cfg.Upstream, cfg.VirtualAddress = up.String(), v.String()
	cfg.Key = append([]byte(nil), cfg.Key...)
	return cfg, nil
}

func NewClient(t transport.Transport, cfg Config) (*Relay, error)    { return newRelay(t, cfg, 0) }
func NewFixedExit(t transport.Transport, cfg Config) (*Relay, error) { return newRelay(t, cfg, 1) }

func newRelay(t transport.Transport, cfg Config, direction byte) (*Relay, error) {
	var err error
	if cfg, err = normalize(cfg); err != nil {
		return nil, err
	}
	if t == nil {
		return nil, errors.New("stream relay: transport required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Relay{trans: t, cfg: cfg, direction: direction, ctx: ctx, cancel: cancel, streams: make(map[streamID]*Conn), tombs: make(map[streamID]tombstone), wake: make(chan struct{}, 1), budgetChanged: make(chan struct{})}
	d := &net.Dialer{Timeout: cfg.OpenTimeout}
	r.dial = d.DialContext
	t.Receive(r.receive)
	r.wg.Add(1)
	go r.run()
	return r, nil
}

func (r *Relay) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.stats
	s.ActiveStreams = len(r.streams)
	return s
}

func (r *Relay) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}
func (r *Relay) budgetNotifyLocked() { close(r.budgetChanged); r.budgetChanged = make(chan struct{}) }
func (r *Relay) allocateLocked(n int) bool {
	if n > r.cfg.MaxBufferedBytes-r.stats.BufferedBytes || r.chunks >= maxGlobalChunks {
		return false
	}
	r.stats.BufferedBytes += n
	r.chunks++
	if r.stats.BufferedBytes > r.stats.PeakBufferedBytes {
		r.stats.PeakBufferedBytes = r.stats.BufferedBytes
	}
	return true
}
func (r *Relay) releaseLocked(n int) { r.stats.BufferedBytes -= n; r.chunks--; r.budgetNotifyLocked() }

func (r *Relay) newConnLocked(id streamID) *Conn {
	c := &Conn{r: r, id: id, notify: make(chan struct{}), done: make(chan struct{}), incoming: make(map[uint64]*chunk), created: time.Now(), activity: time.Now(), rto: r.cfg.RetransmitTimeout}
	r.streams[id] = c
	r.order = append(r.order, c)
	return c
}

func (r *Relay) addTombLocked(c *Conn, graceful bool) {
	if len(r.tombOrder) == tombstoneLimit {
		delete(r.tombs, r.tombOrder[0])
		copy(r.tombOrder, r.tombOrder[1:])
		r.tombOrder = r.tombOrder[:len(r.tombOrder)-1]
	}
	r.tombOrder = append(r.tombOrder, c.id)
	r.tombs[c.id] = tombstone{id: c.id, graceful: graceful, next: c.recvNext, consumed: c.recvConsumed}
}

func (r *Relay) closeConnLocked(c *Conn, err error, reset bool) {
	if c.closed {
		return
	}
	c.closed = true
	c.err = err
	for _, b := range c.out {
		r.releaseLocked(len(b.data))
	}
	for _, b := range c.incoming {
		r.releaseLocked(len(b.data))
	}
	for _, b := range c.ready {
		r.releaseLocked(len(b.data))
	}
	c.out = nil
	c.incoming = nil
	c.ready = nil
	delete(r.streams, c.id)
	for i, x := range r.order {
		if x == c {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
	r.addTombLocked(c, err == nil)
	r.stats.ClosedStreams++
	close(c.done)
	c.notifyLocked()
	if reset {
		r.queueControlLocked(frame{kind: kindRST, id: c.id})
	}
}

func (r *Relay) queueControlLocked(f frame) {
	// A bounded best-effort queue is sufficient: OPEN/DATA/FIN have retries.
	if len(r.controls) < 256 {
		f.direction = r.direction
		r.controls = append(r.controls, f)
		r.signal()
	}
}

func (r *Relay) ackFrameLocked(c *Conn) frame {
	b := make([]byte, 12)
	binary.BigEndian.PutUint64(b, c.recvConsumed)
	binary.BigEndian.PutUint32(b[8:], uint32(r.cfg.WindowBytes))
	flags := byte(flagReady)
	if c.recvFIN && c.recvNext == c.recvFinal {
		flags |= flagFIN
	}
	return frame{kind: kindACK, direction: r.direction, flags: flags, id: c.id, offset: c.recvNext, data: b}
}

func (r *Relay) requestACKLocked(c *Conn, immediate bool) {
	wasDirty := c.ackDirty
	c.ackDirty = true
	if immediate || c.recvNext-c.lastACK >= 32<<10 || c.recvConsumed-c.lastACKConsumed >= 32<<10 {
		c.ackAt = time.Time{}
	} else if !wasDirty {
		c.ackAt = time.Now().Add(5 * time.Millisecond)
	}
	r.signal()
}

func (r *Relay) receive(b []byte) {
	if len(b) < 3 || string(b[:3]) != "MSR" {
		return
	}
	f, err := decodeFrame(r.cfg.Key, b)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	if err != nil {
		if errors.Is(err, errAuthentication) {
			r.stats.AuthenticationFailures++
		} else {
			r.stats.MalformedFrames++
		}
		return
	}
	if f.direction == r.direction {
		return
	}
	r.stats.FramesReceived++
	c := r.streams[f.id]
	if c == nil {
		if tomb, ok := r.tombs[f.id]; ok {
			if tomb.graceful && (f.kind == kindFIN || f.kind == kindData || f.kind == kindOpen) {
				data := make([]byte, 12)
				binary.BigEndian.PutUint64(data, tomb.consumed)
				binary.BigEndian.PutUint32(data[8:], uint32(r.cfg.WindowBytes))
				r.queueControlLocked(frame{kind: kindACK, flags: flagReady | flagFIN, id: f.id, offset: tomb.next, data: data})
			} else if f.kind != kindRST && (!tomb.graceful || f.kind != kindACK) {
				r.queueControlLocked(frame{kind: kindRST, id: f.id})
			}
			return
		}
		// Documents broadcast the exit's frames to every connected client.
		// A shared-key client owns only the stream IDs in its local map/tombs;
		// resetting an unknown ID would terminate another device's connection.
		// Keep the exit's unknown-stream response and our own tombstones intact.
		if r.direction == 0 {
			r.stats.IgnoredForeignFrames++
			return
		}
		if f.kind != kindOpen || r.direction != 1 {
			if f.kind != kindRST {
				r.queueControlLocked(frame{kind: kindRST, id: f.id})
			}
			return
		}
		window := int(binary.BigEndian.Uint32(f.data))
		if window < 64<<10 || window > 4<<20 {
			r.stats.MalformedFrames++
			return
		}
		if len(r.streams) >= r.cfg.MaxStreams {
			r.stats.RejectedStreams++
			r.queueControlLocked(frame{kind: kindRST, id: f.id})
			return
		}
		c = r.newConnLocked(f.id)
		c.remoteWindow = uint64(window)
		r.wg.Add(1)
		go r.connectUpstream(c)
		return
	}
	if f.kind != kindACK {
		c.activity = time.Now()
	}
	switch f.kind {
	case kindOpen:
		if r.direction != 1 || int(binary.BigEndian.Uint32(f.data)) != int(c.remoteWindow) {
			r.closeConnLocked(c, errProtocol, true)
			return
		}
		if c.established {
			r.requestACKLocked(c, true)
		}
	case kindRST:
		r.closeConnLocked(c, errPeerReset, false)
	case kindACK:
		consumed := binary.BigEndian.Uint64(f.data)
		window := uint64(binary.BigEndian.Uint32(f.data[8:]))
		if consumed > f.offset || f.offset > c.sentMax || window < 64<<10 || window > 4<<20 {
			r.closeConnLocked(c, errProtocol, true)
			return
		}
		if f.flags&flagReady != 0 && !c.established && r.direction == 0 {
			c.established = true
			c.notifyLocked()
		}
		if f.offset < c.acked || consumed < c.remoteConsumed {
			return
		} // delayed ACK is harmless
		if f.offset > c.acked || consumed > c.remoteConsumed {
			c.activity = time.Now()
		}
		if f.flags&flagFIN != 0 {
			if !c.finSent || f.offset != c.sendNext {
				r.closeConnLocked(c, errProtocol, true)
				return
			}
			c.finAck = true
		}
		c.remoteWindow = window
		c.remoteConsumed = consumed
		c.acked = f.offset
		for len(c.out) > 0 && c.out[0].offset+uint64(len(c.out[0].data)) <= f.offset {
			x := c.out[0]
			c.out[0] = nil
			c.out = c.out[1:]
			if x.tries == 1 && !x.last.IsZero() {
				r.observeRTTLocked(c, time.Since(x.last))
			}
			r.releaseLocked(len(x.data))
		}
		c.notifyLocked()
		r.signal()
		r.maybeCompleteLocked(c)
	case kindData:
		if !c.established {
			return
		}
		end := f.offset + uint64(len(f.data))
		if end <= c.recvNext {
			r.requestACKLocked(c, true)
			return
		}
		if f.offset < c.recvNext || end > c.recvConsumed+uint64(r.cfg.WindowBytes) || (c.recvFIN && end > c.recvFinal) {
			r.closeConnLocked(c, errProtocol, true)
			return
		}
		if old := c.incoming[f.offset]; old != nil {
			if len(old.data) != len(f.data) {
				r.closeConnLocked(c, errProtocol, true)
			} else {
				r.requestACKLocked(c, true)
			}
			return
		}
		for _, old := range c.incoming {
			if f.offset < old.offset+uint64(len(old.data)) && end > old.offset {
				r.closeConnLocked(c, errProtocol, true)
				return
			}
		}
		if len(c.incoming)+len(c.ready) >= maxChunksPerStream || !r.allocateLocked(len(f.data)) {
			r.stats.RejectedStreams++
			r.closeConnLocked(c, ErrCapacity, true)
			return
		}
		x := &chunk{offset: f.offset, data: append([]byte(nil), f.data...)}
		c.incoming[f.offset] = x
		r.stats.BytesReceived += uint64(len(f.data))
		for {
			x = c.incoming[c.recvNext]
			if x == nil {
				break
			}
			delete(c.incoming, c.recvNext)
			c.ready = append(c.ready, x)
			c.recvNext += uint64(len(x.data))
		}
		c.notifyLocked()
		r.requestACKLocked(c, f.offset > c.recvNext || c.recvFIN && c.recvNext == c.recvFinal)
	case kindFIN:
		if f.offset < c.recvNext || f.offset > c.recvConsumed+uint64(r.cfg.WindowBytes) || c.recvFIN && f.offset != c.recvFinal {
			r.closeConnLocked(c, errProtocol, true)
			return
		}
		for _, x := range c.incoming {
			if x.offset+uint64(len(x.data)) > f.offset {
				r.closeConnLocked(c, errProtocol, true)
				return
			}
		}
		c.recvFIN = true
		c.recvFinal = f.offset
		c.notifyLocked()
		r.requestACKLocked(c, true)
	}
}

func (r *Relay) observeRTTLocked(c *Conn, sample time.Duration) {
	if c.srtt == 0 {
		c.srtt = sample
	} else {
		c.srtt = (7*c.srtt + sample) / 8
	}
	c.rto = 3 * c.srtt
	if c.rto < r.cfg.RetransmitTimeout {
		c.rto = r.cfg.RetransmitTimeout
	}
	ceiling := 5 * time.Second
	if ceiling < r.cfg.RetransmitTimeout {
		ceiling = r.cfg.RetransmitTimeout
	}
	if c.rto > ceiling {
		c.rto = ceiling
	}
}

func (r *Relay) maybeCompleteLocked(c *Conn) {
	if c.finAck && c.recvFIN && c.recvConsumed == c.recvFinal && len(c.out) == 0 {
		// Persist a final ACK in the tombstone before releasing the state.
		r.queueControlLocked(r.ackFrameLocked(c))
		r.closeConnLocked(c, nil, false)
	}
}

// run serializes transport.Send outside the state mutex. Every cycle drains a
// bounded burst, not one frame per timer tick; controls are scanned first.
func (r *Relay) run() {
	defer r.wg.Done()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.wake:
		case <-tick.C:
		}
		for burst := 0; burst < 128; burst++ {
			r.mu.Lock()
			b, ok := r.nextFrameLocked(time.Now())
			r.mu.Unlock()
			if !ok {
				break
			}
			if !r.sendPending(b) {
				break
			}
			if burst == 127 {
				r.signal()
			}
		}
	}
}

// A locally rejected enqueue does not become a hole or wait for a network RTO.
// Keep exactly one encoded frame and retry it, leaving Receive and Close free.
func (r *Relay) sendPending(b []byte) bool {
	until := time.Now().Add(r.cfg.OpenTimeout)
	for {
		err := r.trans.Send(b)
		r.mu.Lock()
		if err == nil {
			r.stats.FramesSent++
			var id streamID
			copy(id[:], b[8:24])
			c := r.streams[id]
			if c != nil {
				now := time.Now()
				switch b[4] {
				case kindOpen:
					c.openLast = now
				case kindACK:
					c.ackLast = now
				case kindFIN:
					c.finLast = now
				case kindData:
					off := binary.BigEndian.Uint64(b[24:32])
					for _, x := range c.out {
						if x.offset == off {
							x.last = now
							break
						}
					}
				}
			}
			r.mu.Unlock()
			return true
		}
		r.stats.SendErrors++
		if time.Now().After(until) {
			var id streamID
			copy(id[:], b[8:24])
			if c := r.streams[id]; c != nil {
				r.closeConnLocked(c, errRetries, false)
			}
			r.mu.Unlock()
			return false
		}
		r.mu.Unlock()
		t := time.NewTimer(5 * time.Millisecond)
		select {
		case <-r.ctx.Done():
			t.Stop()
			return false
		case <-t.C:
		}
	}
}

func (r *Relay) nextFrameLocked(now time.Time) ([]byte, bool) {
	if r.closed {
		return nil, false
	}
	if len(r.controls) > 0 {
		f := r.controls[0]
		r.controls = r.controls[1:]
		return encodeFrame(r.cfg.Key, f), true
	}
	// Control frames have priority even if another stream has a full data window.
	for _, c := range append([]*Conn(nil), r.order...) {
		if now.Sub(c.activity) > r.cfg.IdleTimeout || !c.established && now.Sub(c.created) > r.cfg.OpenTimeout {
			r.closeConnLocked(c, errRetries, true)
			continue
		}
		if !c.established && r.direction == 0 && (c.openLast.IsZero() || now.Sub(c.openLast) >= c.rto) {
			if c.openTries >= r.cfg.MaxRetries {
				r.closeConnLocked(c, errRetries, true)
				continue
			}
			c.openLast = now
			c.openTries++
			if c.openTries > 1 {
				r.stats.Retransmits++
			}
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, uint32(r.cfg.WindowBytes))
			return encodeFrame(r.cfg.Key, frame{kind: kindOpen, direction: r.direction, id: c.id, data: b}), true
		}
		// Refresh consumed credit even when every DATA byte was already ACKed;
		// a lost final credit update must not strand a zero-window writer.
		refreshInterval := c.rto
		if refreshInterval > time.Second {
			refreshInterval = time.Second
		}
		refresh := c.recvNext > 0 && !c.ackLast.IsZero() && now.Sub(c.ackLast) >= refreshInterval
		if c.established && (c.ackDirty && !now.Before(c.ackAt) || refresh) {
			c.ackDirty = false
			c.ackAt = time.Time{}
			c.lastACK = c.recvNext
			c.lastACKConsumed = c.recvConsumed
			c.ackLast = now
			return encodeFrame(r.cfg.Key, r.ackFrameLocked(c)), true
		}
		if c.established && c.writeClosed && c.sentMax == c.sendNext && !c.finAck && (c.finLast.IsZero() || now.Sub(c.finLast) >= c.rto) {
			if c.finTries >= r.cfg.MaxRetries {
				r.closeConnLocked(c, errRetries, true)
				continue
			}
			c.finLast = now
			c.finSent = true
			c.finTries++
			if c.finTries > 1 {
				r.stats.Retransmits++
			}
			return encodeFrame(r.cfg.Key, frame{kind: kindFIN, direction: r.direction, id: c.id, offset: c.sendNext}), true
		}
	}
	for i := 0; i < len(r.order); i++ {
		if r.cursor >= len(r.order) {
			r.cursor = 0
		}
		c := r.order[r.cursor]
		r.cursor++
		if !c.established {
			continue
		}
		for _, x := range c.out {
			if !x.last.IsZero() && now.Sub(x.last) < c.rto {
				continue
			}
			if x.offset+uint64(len(x.data)) > c.remoteConsumed+c.remoteWindow {
				break
			}
			if x.tries >= r.cfg.MaxRetries {
				r.closeConnLocked(c, errRetries, true)
				break
			}
			x.last = now
			x.tries++
			if x.tries > 1 {
				r.stats.Retransmits++
			} else {
				r.stats.BytesSent += uint64(len(x.data))
			}
			if end := x.offset + uint64(len(x.data)); end > c.sentMax {
				c.sentMax = end
			}
			return encodeFrame(r.cfg.Key, frame{kind: kindData, direction: r.direction, id: c.id, offset: x.offset, data: x.data}), true
		}
	}
	return nil, false
}

func (r *Relay) connectUpstream(c *Conn) {
	defer r.wg.Done()
	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.OpenTimeout)
	defer cancel()
	// Closing a stream also cancels an outstanding upstream dial.
	dialDone := make(chan struct{})
	go func() {
		select {
		case <-c.done:
			cancel()
		case <-dialDone:
		case <-ctx.Done():
		}
	}()
	up, err := r.dial(ctx, "tcp", r.cfg.Upstream)
	close(dialDone)
	r.mu.Lock()
	if err != nil {
		r.closeConnLocked(c, errors.New("stream relay: fixed upstream unavailable"), true)
		r.mu.Unlock()
		return
	}
	if c.closed {
		r.mu.Unlock()
		up.Close()
		return
	}
	c.established = true
	c.notifyLocked()
	r.requestACKLocked(c, true)
	r.mu.Unlock()
	defer up.Close()
	results := make(chan error, 2)
	go func() {
		_, e := io.Copy(up, c)
		if e == nil {
			if h, ok := up.(interface{ CloseWrite() error }); ok {
				e = h.CloseWrite()
			}
		}
		results <- e
	}()
	go func() {
		_, e := io.Copy(c, up)
		if e == nil {
			e = c.CloseWrite()
		}
		results <- e
	}()
	completed := 0
	for completed < 2 {
		select {
		case e := <-results:
			completed++
			if e != nil {
				c.Close()
				up.Close()
			}
		case <-c.done:
			up.Close()
			for completed < 2 {
				<-results
				completed++
			}
			return
		}
	}
	// Both copies finished, but acknowledged delivery/FIN may still be pending.
	select {
	case <-c.done:
	case <-r.ctx.Done():
	}
}

func (r *Relay) Close() error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		r.cancel()
		for _, c := range append([]*Conn(nil), r.order...) {
			r.closeConnLocked(c, net.ErrClosed, false)
		}
		r.budgetNotifyLocked()
	}
	r.mu.Unlock()
	r.wg.Wait()
	return nil
}
