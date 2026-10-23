package tunnel

import (
	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"sync"
)

type TunnelLinkEndpoint struct {
	mu               sync.RWMutex
	dispatcher       stack.NetworkDispatcher
	onOutgoingPacket func([]byte) error
	closed           bool
	inbound          sync.WaitGroup
}

func NewTunnelLinkEndpoint() *TunnelLinkEndpoint { return &TunnelLinkEndpoint{} }

func (e *TunnelLinkEndpoint) InjectInbound(data []byte) {
	if len(data) < 20 || len(data) > 65535 {
		return
	}
	e.mu.RLock()
	if e.closed || e.dispatcher == nil {
		e.mu.RUnlock()
		return
	}
	dispatcher := e.dispatcher
	e.inbound.Add(1)
	e.mu.RUnlock()
	defer e.inbound.Done()
	// gVisor validates packets; this endpoint owns the reference it creates.
	pkt := stack.NewPacketBuffer(stack.PacketBufferOptions{
		Payload: buffer.MakeWithData(append([]byte(nil), data...)),
	})
	defer pkt.DecRef()
	dispatcher.DeliverNetworkPacket(ipv4.ProtocolNumber, pkt)
}

func (e *TunnelLinkEndpoint) WritePackets(pkts stack.PacketBufferList) (int, tcpip.Error) {
	e.mu.RLock()
	closed, send := e.closed, e.onOutgoingPacket
	e.mu.RUnlock()
	if closed {
		return 0, &tcpip.ErrClosedForSend{}
	}
	n := 0
	for _, pkt := range pkts.AsSlice() {
		view := pkt.ToView()
		data := append([]byte(nil), view.AsSlice()...)
		view.Release()
		if send != nil {
			if err := send(data); err != nil {
				return n, &tcpip.ErrNoBufferSpace{}
			}
		}
		n++
	}
	return n, nil
}

func (e *TunnelLinkEndpoint) MTU() uint32                    { return 1500 }
func (e *TunnelLinkEndpoint) MaxHeaderLength() uint16        { return 0 }
func (e *TunnelLinkEndpoint) LinkAddress() tcpip.LinkAddress { return "\x02\x00\x00\x00\x00\x01" }
func (e *TunnelLinkEndpoint) Capabilities() stack.LinkEndpointCapabilities {
	return stack.CapabilityNone
}
func (e *TunnelLinkEndpoint) Attach(d stack.NetworkDispatcher) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dispatcher = d
}
func (e *TunnelLinkEndpoint) IsAttached() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.dispatcher != nil && !e.closed
}
func (e *TunnelLinkEndpoint) Wait()                                   {}
func (e *TunnelLinkEndpoint) ARPHardwareType() header.ARPHardwareType { return header.ARPHardwareNone }
func (e *TunnelLinkEndpoint) AddHeader(*stack.PacketBuffer)           {}
func (e *TunnelLinkEndpoint) Close()                                  { e.mu.Lock(); e.closed = true; e.mu.Unlock(); e.inbound.Wait() }
func (e *TunnelLinkEndpoint) SetMTU(uint32)                           {}
func (e *TunnelLinkEndpoint) SetLinkAddress(tcpip.LinkAddress)        {}
func (e *TunnelLinkEndpoint) ParseHeader(*stack.PacketBuffer) bool    { return true }
func (e *TunnelLinkEndpoint) SetOnCloseAction(func())                 {}
