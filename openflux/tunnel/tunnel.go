package tunnel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"universal-bypass-tool/transport"
)

// FixedExitConfig exposes one virtual endpoint to a trusted, fixed loopback
// service. Upstream is never derived from client packets or DNS.
type FixedExitConfig struct {
	VirtualIP      string
	VirtualPort    uint16
	Upstream       string
	MaxConnections int
	DialTimeout    time.Duration
	IdleTimeout    time.Duration
}

// ExitStats contains only aggregate counters, never peer addresses or payloads.
// Active includes an admitted stream while its fixed-upstream dial is pending.
type ExitStats struct {
	Limit         int
	Active        int64
	Accepted      uint64
	RejectedLimit uint64
	DialFailed    uint64
}

// TCPStats is a cumulative, address-free snapshot of this userspace stack.
// Timeouts counts retransmission timer expirations, not HTTP request timeouts.
// Individual counters are atomic; the snapshot is not a single atomic instant.
type TCPStats struct {
	SegmentsSent           uint64
	ValidSegmentsReceived  uint64
	Retransmits            uint64
	Timeouts               uint64
	SegmentSendErrors      uint64
	SegmentsAckedWithDSACK uint64
	SpuriousRecovery       uint64
	SpuriousRTORecovery    uint64
}

type TCPTunnel struct {
	gvisorStack    *stack.Stack
	tunnelEP       *TunnelLinkEndpoint
	transport      transport.Transport
	ctx            context.Context
	cancel         context.CancelFunc
	listener       net.Listener
	wg             sync.WaitGroup
	mu             sync.Mutex
	closed         bool
	connections    map[net.Conn]struct{}
	exitLimit      int
	exitActive     atomic.Int64
	exitAccepted   atomic.Uint64
	exitRejected   atomic.Uint64
	exitDialFailed atomic.Uint64
}

func (t *TCPTunnel) ExitStats() ExitStats {
	return ExitStats{Limit: t.exitLimit, Active: t.exitActive.Load(), Accepted: t.exitAccepted.Load(), RejectedLimit: t.exitRejected.Load(), DialFailed: t.exitDialFailed.Load()}
}

// SetCongestionControl changes the default for new TCP endpoints only. Call it
// immediately after construction, before transport.Start or any DialTCP call.
// The caller must serialize this setup phase with the start of network traffic.
// Existing constructors retain gVisor's Reno default unless explicitly changed.
func (t *TCPTunnel) SetCongestionControl(name string) error {
	if name != "reno" && name != "cubic" {
		return errors.New("congestion control must be reno or cubic")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.gvisorStack == nil {
		return net.ErrClosed
	}
	s := t.gvisorStack.Stats().TCP
	if len(t.connections) != 0 || s.SegmentsSent.Value() != 0 || s.ValidSegmentsReceived.Value() != 0 {
		return errors.New("configure congestion control before starting network traffic")
	}
	option := tcpip.CongestionControlOption(name)
	if err := t.gvisorStack.SetTransportProtocolOption(tcp.ProtocolNumber, &option); err != nil {
		return fmt.Errorf("configure userspace TCP congestion control: %s", err.String())
	}
	return nil
}

func (t *TCPTunnel) TCPStats() TCPStats {
	s := t.gvisorStack.Stats().TCP
	return TCPStats{
		SegmentsSent: s.SegmentsSent.Value(), ValidSegmentsReceived: s.ValidSegmentsReceived.Value(),
		Retransmits: s.Retransmits.Value(), Timeouts: s.Timeouts.Value(),
		SegmentSendErrors: s.SegmentSendErrors.Value(), SegmentsAckedWithDSACK: s.SegmentsAckedWithDSACK.Value(),
		SpuriousRecovery: s.SpuriousRecovery.Value(), SpuriousRTORecovery: s.SpuriousRTORecovery.Value(),
	}
}

// Deprecated: use the error-returning NewClientTunnel or NewFixedExitTunnel.
// The removed unrestricted exit fails closed with nil; it never opens raw sockets.
func NewTCPTunnel(trans transport.Transport, isExitNode bool) *TCPTunnel {
	if isExitNode {
		return nil
	}
	t, _ := NewClientTunnel(trans)
	return t
}

func newStack(trans transport.Transport, local netip.Addr) (*TCPTunnel, error) {
	if trans == nil {
		return nil, errors.New("nil transport")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &TCPTunnel{transport: trans, ctx: ctx, cancel: cancel, connections: make(map[net.Conn]struct{})}
	t.gvisorStack = stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol},
	})
	t.tunnelEP = NewTunnelLinkEndpoint()
	t.tunnelEP.onOutgoingPacket = trans.Send
	if err := t.gvisorStack.CreateNIC(1, t.tunnelEP); err != nil {
		t.Close()
		return nil, fmt.Errorf("create virtual NIC: %s", err.String())
	}
	if err := t.gvisorStack.AddProtocolAddress(1, tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddressWithPrefix{Address: tcpip.AddrFrom4(local.As4()), PrefixLen: 32},
	}, stack.AddressProperties{}); err != nil {
		t.Close()
		return nil, fmt.Errorf("assign virtual address: %s", err.String())
	}
	// This route is exclusively in gVisor: no host route, firewall, interface,
	// forwarding, raw socket, or privileged operation is created.
	t.gvisorStack.SetRouteTable([]tcpip.Route{{Destination: header.IPv4EmptySubnet, NIC: 1}})
	return t, nil
}

func NewClientTunnel(trans transport.Transport) (*TCPTunnel, error) {
	t, err := newStack(trans, netip.MustParseAddr("10.10.10.2"))
	if err != nil {
		return nil, err
	}
	trans.Receive(t.tunnelEP.InjectInbound)
	return t, nil
}

func normalizeExitConfig(cfg FixedExitConfig) (FixedExitConfig, netip.Addr, error) {
	ip, err := netip.ParseAddr(cfg.VirtualIP)
	if err != nil || !ip.Is4() || ip.IsUnspecified() || ip.IsMulticast() || ip == netip.MustParseAddr("10.10.10.2") {
		return cfg, netip.Addr{}, errors.New("virtual IP must be an IPv4 unicast address distinct from the client")
	}
	if cfg.VirtualPort == 0 {
		return cfg, ip, errors.New("virtual port must be nonzero")
	}
	host, port, err := net.SplitHostPort(cfg.Upstream)
	if err != nil {
		return cfg, ip, fmt.Errorf("upstream must be literal loopback IP:port: %w", err)
	}
	upIP, err := netip.ParseAddr(host)
	portN, portErr := strconv.ParseUint(port, 10, 16)
	if err != nil || !upIP.IsLoopback() || portErr != nil || portN == 0 {
		return cfg, ip, errors.New("upstream must be literal loopback IP with a nonzero port")
	}
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 32
	}
	if cfg.MaxConnections < 1 || cfg.MaxConnections > 1024 {
		return cfg, ip, errors.New("max connections must be between 1 and 1024")
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 2 * time.Minute
	}
	if cfg.DialTimeout < 0 || cfg.IdleTimeout < 0 {
		return cfg, ip, errors.New("timeouts must be positive")
	}
	return cfg, ip, nil
}

// NewFixedExitTunnel listens only on VirtualIP:VirtualPort in the in-memory
// stack, and copies accepted TCP streams only to the fixed loopback Upstream.
// The caller owns transport Start/Stop. Close stops this stack and its proxies.
func NewFixedExitTunnel(trans transport.Transport, config FixedExitConfig) (*TCPTunnel, error) {
	cfg, ip, err := normalizeExitConfig(config)
	if err != nil {
		return nil, err
	}
	t, err := newStack(trans, ip)
	if err != nil {
		return nil, err
	}
	listener, err := gonet.ListenTCP(t.gvisorStack, tcpip.FullAddress{
		NIC: 1, Addr: tcpip.AddrFrom4(ip.As4()), Port: cfg.VirtualPort,
	}, ipv4.ProtocolNumber)
	if err != nil {
		t.Close()
		return nil, fmt.Errorf("listen on virtual endpoint: %w", err)
	}
	t.listener = listener
	t.exitLimit = cfg.MaxConnections
	t.wg.Add(1)
	go t.acceptLoop(cfg)
	trans.Receive(t.tunnelEP.InjectInbound)
	return t, nil
}

func (t *TCPTunnel) acceptLoop(cfg FixedExitConfig) {
	defer t.wg.Done()
	slots := make(chan struct{}, cfg.MaxConnections)
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			return
		}
		select {
		case slots <- struct{}{}:
		default:
			t.exitRejected.Add(1)
			conn.Close()
			continue
		}
		tracked := t.track(conn)
		if tracked == nil {
			<-slots
			return
		}
		t.exitActive.Add(1)
		t.exitAccepted.Add(1)
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			defer func() { t.exitActive.Add(-1); <-slots }()
			defer tracked.Close()
			dialer := net.Dialer{Timeout: cfg.DialTimeout}
			upstream, err := dialer.DialContext(t.ctx, "tcp", cfg.Upstream)
			if err != nil {
				if t.ctx.Err() == nil {
					t.exitDialFailed.Add(1)
				}
				return
			}
			up := t.track(upstream)
			if up == nil {
				return
			}
			defer up.Close()
			bridge(t.ctx, tracked, up, cfg.IdleTimeout)
		}()
	}
}

type trackedConn struct {
	net.Conn
	once    sync.Once
	onClose func()
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.onClose)
	return err
}

func (c *trackedConn) CloseWrite() error {
	if c, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return c.CloseWrite()
	}
	return nil
}

func (t *TCPTunnel) track(conn net.Conn) *trackedConn {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		conn.Close()
		return nil
	}
	t.connections[conn] = struct{}{}
	return &trackedConn{Conn: conn, onClose: func() {
		t.mu.Lock()
		delete(t.connections, conn)
		t.mu.Unlock()
	}}
}

// bridge preserves half-close, bounds stalled writes, and measures inactivity
// jointly across both directions, so a long one-way download is not idle.
func bridge(ctx context.Context, a, b net.Conn, idle time.Duration) {
	var last atomic.Int64
	last.Store(time.Now().UnixNano())
	results := make(chan error, 2)
	pump := func(dst, src net.Conn) {
		buf := make([]byte, 32*1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				last.Store(time.Now().UnixNano())
				for written := 0; written < n; {
					dst.SetWriteDeadline(time.Now().Add(idle))
					m, writeErr := dst.Write(buf[written:n])
					written += m
					if m > 0 {
						last.Store(time.Now().UnixNano())
					}
					if writeErr != nil {
						results <- writeErr
						return
					}
					if m == 0 {
						results <- io.ErrShortWrite
						return
					}
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					if c, ok := dst.(interface{ CloseWrite() error }); ok {
						c.CloseWrite()
					}
				}
				results <- err
				return
			}
		}
	}
	go pump(a, b)
	go pump(b, a)
	interval := min(idle/2, time.Second)
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	completed := 0
	for completed < 2 {
		select {
		case err := <-results:
			completed++
			if !errors.Is(err, io.EOF) {
				a.Close()
				b.Close()
			}
		case <-ctx.Done():
			a.Close()
			b.Close()
			for completed < 2 {
				<-results
				completed++
			}
		case <-ticker.C:
			if time.Since(time.Unix(0, last.Load())) >= idle {
				a.Close()
				b.Close()
			}
		}
	}
}

func (t *TCPTunnel) DialTCP(address string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(t.ctx, 15*time.Second)
	defer cancel()
	return t.DialTCPContext(ctx, address)
}

func (t *TCPTunnel) DialTCPContext(ctx context.Context, address string) (net.Conn, error) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(t.ctx, cancel)
	defer cancel()
	defer stop()
	if err := t.ctx.Err(); err != nil {
		return nil, net.ErrClosed
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	p, err := strconv.ParseUint(port, 10, 16)
	if err != nil || p == 0 {
		return nil, errors.New("invalid TCP port")
	}
	addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip4", host)
	if err != nil {
		return nil, fmt.Errorf("resolve IPv4: %w", err)
	}
	if len(addresses) == 0 {
		return nil, errors.New("no IPv4 address")
	}
	conn, err := gonet.DialContextTCP(ctx, t.gvisorStack, tcpip.FullAddress{
		NIC: 1, Addr: tcpip.AddrFrom4(addresses[0].Unmap().As4()), Port: uint16(p),
	}, ipv4.ProtocolNumber)
	if err != nil {
		return nil, err
	}
	tracked := t.track(conn)
	if tracked == nil {
		return nil, net.ErrClosed
	}
	return tracked, nil
}

func (t *TCPTunnel) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.cancel()
	connections := make([]net.Conn, 0, len(t.connections))
	for conn := range t.connections {
		connections = append(connections, conn)
	}
	t.mu.Unlock()
	if t.listener != nil {
		t.listener.Close()
	}
	for _, conn := range connections {
		conn.Close()
	}
	if t.tunnelEP != nil {
		t.tunnelEP.Close()
	}
	if t.gvisorStack != nil {
		t.gvisorStack.Close()
	}
	t.wg.Wait()
	if t.gvisorStack != nil {
		t.gvisorStack.Wait()
	}
	return nil
}

// Only the unused legacy platform files refer to this helper.
func getLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "192.168.1.100"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}
