package socks5

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"universal-bypass-tool/utils"
)

type Dialer interface {
	DialTCP(address string) (net.Conn, error)
}

// ContextDialer allows shutdown and the connection timeout to cancel the dial.
type ContextDialer interface {
	DialTCPContext(context.Context, string) (net.Conn, error)
}

type SOCKS5Server struct {
	listenAddr                    string
	dialer                        Dialer
	ctx                           context.Context
	cancel                        context.CancelFunc
	mu                            sync.Mutex
	listener                      net.Listener
	started, closed               bool
	conns                         map[net.Conn]struct{}
	handshakeTimeout, dialTimeout time.Duration
}

func NewSOCKS5Server(addr string, dialer Dialer) *SOCKS5Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &SOCKS5Server{listenAddr: addr, dialer: dialer, ctx: ctx, cancel: cancel,
		conns: make(map[net.Conn]struct{}), handshakeTimeout: 10 * time.Second, dialTimeout: 15 * time.Second}
}

// Start serves until Close is called. Deliberate shutdown returns nil.
// A server instance may only be started once.
func (s *SOCKS5Server) Start() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return net.ErrClosed
	}
	if s.started {
		s.mu.Unlock()
		return errors.New("SOCKS5 server already started")
	}
	s.started = true
	s.mu.Unlock()
	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		listener.Close()
		return nil
	}
	s.listener = listener
	s.mu.Unlock()
	defer s.Close()
	utils.Debugf("[SOCKS5] Listening on %s", listener.Addr())
	for {
		conn, err := listener.Accept()
		if err != nil {
			if s.ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("SOCKS5 accept: %w", err)
		}
		go s.handleConnection(conn)
	}
}

// Addr returns the bound address after Start has opened the listener.
func (s *SOCKS5Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Close is idempotent and closes the listener, active clients, and upstreams.
func (s *SOCKS5Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cancel()
	listener := s.listener
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()
	var err error
	if listener != nil {
		err = listener.Close()
		if errors.Is(err, net.ErrClosed) {
			err = nil
		}
	}
	for _, conn := range conns {
		conn.Close()
	}
	return err
}

func (s *SOCKS5Server) track(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.conns[conn] = struct{}{}
	return true
}
func (s *SOCKS5Server) release(conn net.Conn) {
	conn.Close()
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}

func (s *SOCKS5Server) handleConnection(client net.Conn) {
	if !s.track(client) {
		client.Close()
		return
	}
	defer s.release(client)
	if err := client.SetDeadline(time.Now().Add(s.handshakeTimeout)); err != nil {
		return
	}
	var greeting [2]byte
	if _, err := io.ReadFull(client, greeting[:]); err != nil || greeting[0] != 5 {
		return
	}
	methods := make([]byte, int(greeting[1]))
	if _, err := io.ReadFull(client, methods); err != nil {
		return
	}
	noAuth := false
	for _, method := range methods {
		noAuth = noAuth || method == 0
	}
	if !noAuth {
		writeAll(client, []byte{5, 0xff})
		return
	}
	if err := writeAll(client, []byte{5, 0}); err != nil {
		return
	}

	var request [4]byte
	if _, err := io.ReadFull(client, request[:]); err != nil {
		return
	}
	if request[0] != 5 || request[2] != 0 {
		s.reply(client, 1, nil)
		return
	}
	// CONNECT only; BIND and UDP ASSOCIATE are unsupported.
	if request[1] != 1 {
		s.reply(client, 7, nil)
		return
	}
	var host string
	switch request[3] {
	case 1:
		var ip [4]byte
		if _, err := io.ReadFull(client, ip[:]); err != nil {
			return
		}
		host = net.IP(ip[:]).String()
	case 3:
		var size [1]byte
		if _, err := io.ReadFull(client, size[:]); err != nil {
			return
		}
		if size[0] == 0 {
			s.reply(client, 1, nil)
			return
		}
		domain := make([]byte, int(size[0]))
		if _, err := io.ReadFull(client, domain); err != nil {
			return
		}
		host = string(domain)
		if strings.ContainsAny(host, "\x00\r\n") {
			s.reply(client, 1, nil)
			return
		}
	default:
		s.reply(client, 8, nil)
		return
	}
	var portBytes [2]byte
	if _, err := io.ReadFull(client, portBytes[:]); err != nil {
		return
	}
	port := binary.BigEndian.Uint16(portBytes[:])
	if port == 0 {
		s.reply(client, 1, nil)
		return
	}
	if err := client.SetDeadline(time.Time{}); err != nil {
		return
	}
	address := net.JoinHostPort(host, strconv.Itoa(int(port)))
	utils.Debugf("[SOCKS5] CONNECT %s", address)
	upstream, err := s.dial(address)
	if err != nil {
		utils.Debugf("[SOCKS5] Dial failed: %v", err)
		s.reply(client, replyCode(err), nil)
		return
	}
	if !s.track(upstream) {
		upstream.Close()
		return
	}
	defer s.release(upstream)
	if err := s.reply(client, 0, upstream.LocalAddr()); err != nil {
		return
	}
	if err := client.SetDeadline(time.Time{}); err != nil {
		return
	}
	relay(client, upstream)
}

func (s *SOCKS5Server) dial(address string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.dialTimeout)
	defer cancel()
	// Legacy dialers cannot be interrupted. Stop waiting on timeout and close
	// late connections. New dialers should implement ContextDialer.
	type result struct {
		conn net.Conn
		err  error
	}
	results := make(chan result)
	go func() {
		var conn net.Conn
		var err error
		if dialer, ok := s.dialer.(ContextDialer); ok {
			conn, err = dialer.DialTCPContext(ctx, address)
		} else {
			conn, err = s.dialer.DialTCP(address)
		}
		select {
		case results <- result{conn, err}:
		case <-ctx.Done():
			if conn != nil {
				conn.Close()
			}
		}
	}()
	select {
	case res := <-results:
		if res.err != nil && res.conn != nil {
			res.conn.Close()
		}
		if res.err == nil && res.conn == nil {
			return nil, errors.New("SOCKS5 dialer returned no connection")
		}
		return res.conn, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *SOCKS5Server) reply(conn net.Conn, code byte, bound net.Addr) error {
	if err := conn.SetWriteDeadline(time.Now().Add(s.handshakeTimeout)); err != nil {
		return err
	}
	response := []byte{5, code, 0, 1, 0, 0, 0, 0, 0, 0}
	if addr, ok := bound.(*net.TCPAddr); ok && addr.IP.To4() != nil {
		copy(response[4:8], addr.IP.To4())
		binary.BigEndian.PutUint16(response[8:10], uint16(addr.Port))
	}
	return writeAll(conn, response)
}

func replyCode(err error) byte {
	var dnsError *net.DNSError
	var netError net.Error
	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return 5
	case errors.Is(err, syscall.ENETUNREACH):
		return 3
	case errors.Is(err, syscall.EHOSTUNREACH), errors.As(err, &dnsError):
		return 4
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netError) && netError.Timeout():
		return 4
	default:
		return 1
	}
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func relay(client, upstream net.Conn) {
	var wg sync.WaitGroup
	copyDirection := func(dst, src net.Conn) {
		defer wg.Done()
		_, err := io.Copy(dst, src)
		if err == nil {
			if half, ok := dst.(interface{ CloseWrite() error }); ok {
				if half.CloseWrite() == nil {
					return
				}
			}
		}
		// Generic connections cannot express a half-close. Closing both also
		// unblocks the opposite copy after an error.
		client.Close()
		upstream.Close()
	}
	wg.Add(2)
	go copyDirection(upstream, client)
	go copyDirection(client, upstream)
	wg.Wait()
}
