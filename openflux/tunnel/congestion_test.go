package tunnel

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
)

func stackCongestion(t *testing.T, tun *TCPTunnel) string {
	t.Helper()
	var option tcpip.CongestionControlOption
	if err := tun.gvisorStack.TransportProtocolOption(tcp.ProtocolNumber, &option); err != nil {
		t.Fatal(err.String())
	}
	return string(option)
}

func endpointCongestion(t *testing.T, tun *TCPTunnel, local, remote net.Addr) string {
	t.Helper()
	l, r := local.(*net.TCPAddr), remote.(*net.TCPAddr)
	id := stack.TransportEndpointID{
		LocalAddress: tcpip.AddrFrom4([4]byte(l.IP.To4())), LocalPort: uint16(l.Port),
		RemoteAddress: tcpip.AddrFrom4([4]byte(r.IP.To4())), RemotePort: uint16(r.Port),
	}
	ep, ok := tun.gvisorStack.FindTransportEndpoint(ipv4.ProtocolNumber, tcp.ProtocolNumber, id, 1).(*tcp.Endpoint)
	if !ok {
		t.Fatal("connected TCP endpoint missing")
	}
	var option tcpip.CongestionControlOption
	if err := ep.GetSockOpt(&option); err != nil {
		t.Fatal(err.String())
	}
	return string(option)
}

func TestCongestionDefaultAndInvalidConfiguration(t *testing.T) {
	a, _ := memoryPair(t)
	tun, err := NewClientTunnel(a)
	if err != nil {
		t.Fatal(err)
	}
	defer tun.Close()
	if got := stackCongestion(t, tun); got != "reno" {
		t.Fatalf("default changed to %q", got)
	}
	if got := tun.TCPStats(); got != (TCPStats{}) {
		t.Fatalf("new stack has traffic counters: %+v", got)
	}
	for _, name := range []string{"", "bbr", "CUBIC", " reno", "reno\n"} {
		if err := tun.SetCongestionControl(name); err == nil {
			t.Fatalf("accepted unsupported setting %q", name)
		}
		if got := stackCongestion(t, tun); got != "reno" {
			t.Fatalf("invalid setting changed default to %q", got)
		}
	}
	tun.Close()
	if err := tun.SetCongestionControl("cubic"); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("configuration after Close: %v", err)
	}
	// Counter snapshots remain readable during shutdown and afterwards.
	_ = tun.TCPStats()
}

func TestCongestionAppliedToClientAndAcceptedExit(t *testing.T) {
	for _, name := range []string{"reno", "cubic"} {
		t.Run(name, func(t *testing.T) {
			listener := loopbackListener(t)
			upstreamReady := make(chan struct{})
			serverResult := make(chan error, 1)
			payload := bytes.Repeat([]byte("congestion-control-roundtrip"), 512)
			go func() {
				c, err := listener.Accept()
				if err != nil {
					serverResult <- err
					return
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(4 * time.Second))
				close(upstreamReady)
				_, err = io.CopyN(c, c, int64(len(payload)))
				serverResult <- err
			}()
			client, exit := tunnelPair(t, FixedExitConfig{
				VirtualIP: "10.203.0.1", VirtualPort: 443, Upstream: listener.Addr().String(),
			})
			for _, tun := range []*TCPTunnel{client, exit} {
				if got := stackCongestion(t, tun); got != "reno" {
					t.Fatalf("constructor changed default: %q", got)
				}
				if err := tun.SetCongestionControl(name); err != nil {
					t.Fatal(err)
				}
			}
			c, err := client.DialTCP("10.203.0.1:443")
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(4 * time.Second))
			select {
			case <-upstreamReady:
			case <-time.After(4 * time.Second):
				t.Fatal("upstream was not connected")
			}
			if got := endpointCongestion(t, client, c.LocalAddr(), c.RemoteAddr()); got != name {
				t.Fatalf("client endpoint uses %q, want %q", got, name)
			}
			if got := endpointCongestion(t, exit, c.RemoteAddr(), c.LocalAddr()); got != name {
				t.Fatalf("accepted endpoint uses %q, want %q", got, name)
			}
			for _, tun := range []*TCPTunnel{client, exit} {
				if err := tun.SetCongestionControl("reno"); err == nil {
					t.Fatal("allowed congestion change after traffic started")
				}
			}
			if _, err := c.Write(payload); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(c, got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("TCP payload changed")
			}
			if err := <-serverResult; err != nil {
				t.Fatal(err)
			}
			for _, tun := range []*TCPTunnel{client, exit} {
				if s := tun.TCPStats(); s.SegmentsSent == 0 || s.ValidSegmentsReceived == 0 {
					t.Fatalf("TCP traffic absent from counters: %+v", s)
				}
			}
		})
	}
}
