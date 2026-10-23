package tunnel

import (
	"net"
	"testing"
	"time"
)

func waitExitStats(t *testing.T, exit *TCPTunnel, ready func(ExitStats) bool) ExitStats {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		s := exit.ExitStats()
		if ready(s) {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("exit counters did not settle: %+v", exit.ExitStats())
	return ExitStats{}
}

func TestExitCapacityCountersAndSlotReuse(t *testing.T) {
	listener := loopbackListener(t)
	accepted := make(chan net.Conn, 8)
	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()
	client, exit := tunnelPair(t, FixedExitConfig{VirtualIP: "10.203.0.1", VirtualPort: 443, Upstream: listener.Addr().String(), MaxConnections: 3})
	clients, upstreams := []net.Conn{}, []net.Conn{}
	t.Cleanup(func() {
		for _, c := range clients {
			c.Close()
		}
		for _, c := range upstreams {
			c.Close()
		}
	})
	open := func() {
		t.Helper()
		c, err := client.DialTCP("10.203.0.1:443")
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, c)
		select {
		case up := <-accepted:
			upstreams = append(upstreams, up)
		case <-time.After(4 * time.Second):
			t.Fatal("admitted connection did not reach upstream")
		}
	}
	for i := 0; i < 3; i++ {
		open()
	}
	s := waitExitStats(t, exit, func(s ExitStats) bool { return s.Active == 3 && s.Accepted == 3 })
	if s.Limit != 3 || s.RejectedLimit != 0 || s.DialFailed != 0 {
		t.Fatalf("unexpected initial counters: %+v", s)
	}
	// A connection beyond capacity may complete TCP, then must close without
	// reaching the fixed TLS upstream: the previously silent failure mode.
	extra, err := client.DialTCP("10.203.0.1:443")
	if err == nil {
		defer extra.Close()
		extra.SetReadDeadline(time.Now().Add(4 * time.Second))
		_, err = extra.Read(make([]byte, 1))
		if err == nil {
			t.Fatal("over-limit stream stayed usable")
		}
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			t.Fatal("over-limit stream was not closed promptly")
		}
	}
	waitExitStats(t, exit, func(s ExitStats) bool { return s.RejectedLimit == 1 && s.Active == 3 })
	select {
	case c := <-accepted:
		c.Close()
		t.Fatal("rejected stream reached upstream")
	default:
	}
	clients[0].Close()
	upstreams[0].Close()
	waitExitStats(t, exit, func(s ExitStats) bool { return s.Active == 2 })
	open()
	s = waitExitStats(t, exit, func(s ExitStats) bool { return s.Active == 3 && s.Accepted == 4 })
	if s.RejectedLimit != 1 || s.DialFailed != 0 {
		t.Fatalf("slot reuse miscounted: %+v", s)
	}
	if err = exit.Close(); err != nil {
		t.Fatal(err)
	}
	if s = exit.ExitStats(); s.Active != 0 || s.Accepted != 4 || s.RejectedLimit != 1 {
		t.Fatalf("shutdown counters: %+v", s)
	}
}

func TestExitDialFailureReleasesCapacityAndCountsOnce(t *testing.T) {
	listener := loopbackListener(t)
	address := listener.Addr().String()
	listener.Close()
	client, exit := tunnelPair(t, FixedExitConfig{VirtualIP: "10.203.0.1", VirtualPort: 443, Upstream: address, MaxConnections: 1, DialTimeout: time.Second})
	for n := uint64(1); n <= 2; n++ {
		c, err := client.DialTCP("10.203.0.1:443")
		if err == nil {
			c.SetReadDeadline(time.Now().Add(4 * time.Second))
			_, err = c.Read(make([]byte, 1))
			c.Close()
			if err == nil {
				t.Fatal("unreachable upstream delivered data")
			}
		}
		s := waitExitStats(t, exit, func(s ExitStats) bool { return s.Active == 0 && s.DialFailed == n })
		if s.Accepted != n || s.RejectedLimit != 0 {
			t.Fatalf("dial failure leaked slot or wrong counter: %+v", s)
		}
	}
}

func TestExitLimitDefaultAnd128Validation(t *testing.T) {
	base := FixedExitConfig{VirtualIP: "10.203.0.1", VirtualPort: 443, Upstream: "127.0.0.1:443"}
	cfg, _, err := normalizeExitConfig(base)
	if err != nil || cfg.MaxConnections != 32 {
		t.Fatal("default32 changed")
	}
	base.MaxConnections = 128
	_, exit := tunnelPair(t, base)
	if s := exit.ExitStats(); s.Limit != 128 || s.Active != 0 {
		t.Fatalf("128 not configured: %+v", s)
	}
	for _, limit := range []int{-1, 1025} {
		base.MaxConnections = limit
		if _, _, err := normalizeExitConfig(base); err == nil {
			t.Fatalf("accepted invalid limit%d", limit)
		}
	}
}
