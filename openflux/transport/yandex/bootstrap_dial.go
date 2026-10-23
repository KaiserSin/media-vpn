package yandex

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"time"
)

// These special-use ranges are not public Internet destinations, even where
// netip considers them global unicast. Private, link-local and loopback ranges
// are rejected separately below.
var nonPublicDialPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
}

func bootstrapDialIP(raw string, allowTestURLs bool) (netip.Addr, error) {
	ip, err := netip.ParseAddr(raw)
	if err != nil || ip.Zone() != "" {
		return netip.Addr{}, errors.New("invalid document bootstrap dial address")
	}
	ip = ip.Unmap()
	// Only package-local tests can opt into loopback fixture destinations.
	if allowTestURLs && ip.IsLoopback() {
		return ip, nil
	}
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() ||
		(ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip)) {
		return netip.Addr{}, errors.New("document bootstrap dial address must be public unicast")
	}
	for _, prefix := range nonPublicDialPrefixes {
		if prefix.Contains(ip) {
			return netip.Addr{}, errors.New("document bootstrap dial address must be public unicast")
		}
	}
	return ip, nil
}

type contextDialFunc func(context.Context, string, string) (net.Conn, error)

func pinBootstrapDial(dial contextDialFunc, originHostPort, ipHostPort string) contextDialFunc {
	if dial == nil {
		dial = (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if address == originHostPort {
			address = ipHostPort
		}
		return dial(ctx, network, address)
	}
}

// SetBootstrapDialAddress pins only this bootstrap's validated balancer origin
// on port 443 to an operator-supplied numeric public IP. URLs, HTTP Host, TLS
// SNI and certificate verification continue to use the original hostname.
// Configure the bootstrap first, then this address, before Start.
func (t *YandexDocsTransport) SetBootstrapDialAddress(raw string) error {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	if t.done != nil {
		return errors.New("configure document bootstrap dial address before transport start")
	}
	if t.bootstrapBody == nil || t.bootstrapDialAddress != "" {
		return errors.New("configure document bootstrap before its dial address, once")
	}
	ip, err := bootstrapDialIP(raw, t.allowTestURLs)
	if err != nil {
		return err
	}
	info, err := parseDocInfo(t.bootstrapBody, t.baseUserID, t.allowTestURLs)
	if err != nil {
		return errors.New("invalid document bootstrap config fields or endpoints")
	}
	origin, err := url.Parse(info.Origin)
	if err != nil || origin.Scheme != "https" || (origin.Port() != "" && origin.Port() != "443") {
		return errors.New("document bootstrap dial origin must use HTTPS port 443")
	}
	base := t.httpClient.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	httpTransport, ok := base.(*http.Transport)
	if !ok || httpTransport == nil || httpTransport.DialTLS != nil || httpTransport.DialTLSContext != nil {
		return errors.New("document bootstrap dial address requires a standard HTTP transport")
	}
	originHostPort := net.JoinHostPort(origin.Hostname(), "443")
	ipHostPort := net.JoinHostPort(ip.String(), "443")
	pinnedHTTP := httpTransport.Clone()
	pinnedHTTP.DialContext = pinBootstrapDial(httpTransport.DialContext, originHostPort, ipHostPort)
	client := *t.httpClient
	client.Transport = pinnedHTTP
	dialer := *t.dialer
	// A custom TLS dialer would skip the WebSocket library's normal TLS checks.
	if dialer.NetDialTLSContext != nil {
		return errors.New("document bootstrap dial address requires a standard websocket TLS dialer")
	}
	baseDial := contextDialFunc(dialer.NetDialContext)
	if baseDial == nil && dialer.NetDial != nil {
		baseDial = func(_ context.Context, network, address string) (net.Conn, error) {
			return dialer.NetDial(network, address)
		}
	}
	dialer.NetDialContext = pinBootstrapDial(baseDial, originHostPort, ipHostPort)
	t.httpClient, t.dialer = &client, &dialer
	t.bootstrapDialAddress = ip.String()
	return nil
}
