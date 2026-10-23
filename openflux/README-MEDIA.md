# Media document transport pilot

Private modified build of p1neappleXpress/OpenFlux, upstream commit 6394eafd6bed308436b59afd04af692e45bdec72.

This pilot uses an existing encrypted Xray VLESS+WS+TLS connection inside a Yandex Docs cursor transport. It is not evidence that this route is available under any mobile allowlist. Live compatibility and speed must be measured.

The modified exit uses gVisor TCP in userspace and connects only to configured literal loopback upstream. Do not apply the upstream README iptables rule. No root, raw sockets, Linux forwarding or host interface changes are needed for the program.

Mac client: media-openflux --client --url-file /private/path/document.url
VDS exit: media-openflux --exit-node --url-file /private/path/document.url

Defaults: Mac SOCKS 127.0.0.1:19080; virtual destination 10.203.0.1:443; VDS upstream 127.0.0.1:443. The document URL is a capability; keep it private. The pilot is one Mac client per document because the virtual client address is fixed. Do not use a document containing personal information.

The document transport itself is not end-to-end encryption. Preserve the inner Xray encryption and TLS certificate validation. Exclude document bootstrap and WebSocket destinations from the client VPN routing to avoid recursion before importing into Happ.

Tests cover fragmented SOCKS requests, actual localhost relays, cancellation, fixed-destination restrictions, binary TCP roundtrip, timeout, connection limits, parser errors, reconnect budget, mock WebSocket authentication and shutdown. Offline test success does not establish live Yandex support.
