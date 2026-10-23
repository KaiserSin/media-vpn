# Source provenance

This directory is an archive of the modified Media OpenFlux source used
by the user's Media Test 10 transport. It is not an unmodified upstream release.

- Upstream repository: <https://github.com/p1neappleXpress/OpenFlux>
- Exact upstream commit: `6394eafd6bed308436b59afd04af692e45bdec72`
- The local upstream checkout used for comparison had this HEAD and the origin
  `https://github.com/p1neappleXpress/OpenFlux.git`.
- The modified source copy had no `.git` directory or local commit history.
- `README.md`, `go.mod`, and `go.sum` are preserved byte for byte from upstream.

## License status

No standalone `LICENSE` or `COPYING` file was present in the upstream commit.
The upstream README's complete License section reads:

> Educational use only. Test on your own machines and networks.

This archive preserves that notice and does not assign a new license to the
upstream code or the local modifications. No public redistribution permission
is inferred from a public upstream repository.

The dependency modules retain their separate licenses. Direct module license
files in the pinned local module cache identify Gorilla WebSocket as BSD
2-Clause, gVisor as Apache-2.0, LZ4 as BSD 3-Clause, Pion WebRTC as MIT,
`wlynxg/anet` as BSD 3-Clause, and `golang.org/x/sys` as BSD 3-Clause. This source
archive references these dependencies through the original `go.mod`/`go.sum`;
it does not vendor dependency code or include compiled binaries.

## Local changes relative to that commit

Five existing source files differ:

- `main.go`: private document file inputs, restricted loopback SOCKS listener,
  fixed exit configuration, startup/readiness monitoring, document pools,
  batching/congestion experiments, and optional stream relay entry point.
- `socks5/socks5.go`: bounded request parsing, fragmented/pipelined input,
  timeouts, cancellation, and connection shutdown handling.
- `transport/yandex/yandex.go`: bounded document bootstrap parsing, endpoint
  validation, editor/WebSocket discovery and authentication, reconnect budget,
  lifecycle handling, bounded queues, batching hooks, diagnostic labels, and
  the explicit captured-bootstrap option described below.
- `tunnel/endpoint.go`: lifecycle and packet handling for the in-memory NIC.
- `tunnel/tunnel.go`: userspace gVisor TCP relay to one configured literal
  loopback upstream, destination restriction, connection limits, counters,
  shutdown, and congestion controls.

Added directories and supporting files:

- `transport/docpool/`: flow/stripe document pool with bounded routing and
  versioned per-pool framing, plus tests.
- `transport/streamdocs/`: opaque-frame transport pool, plus tests.
- `streamrelay/` and `stream_main.go`: experimental authenticated reliable byte
  streams, bounded buffering, flow control/retransmission, multi-client
  isolation, and tests. This is an optional mode.
- `transport/yandex/batching.go` and Yandex parser/authentication/reconnect/
  batching tests.
- `main_test.go`, `pool_config_test.go`, `speed_config_test.go`, SOCKS5 tests,
  and tunnel destination/lifecycle/capacity/congestion/pool tests.
- `README-MEDIA.md` and `README-POOL.md` describe the earlier pilot and pool.
- `bootstrap.go`, `bootstrap_test.go`, and `transport/yandex/bootstrap*.go` add
  opt-in private bootstrap-file loading and offline validation/integration tests.
- `transport/yandex/cursor.go` and `cursor_test.go` implement bounded cursor
  length framing and validate the sender/receiver and heartbeat behavior.

The added source/documentation files and five modified upstream files are
described above. The MAX transport, raw-socket platform files, network
checksum code, transport interface, debug helper, and Android/iOS build scripts
are unchanged upstream copies. They are preserved as source; their presence
does not mean those modes or mobile builds were verified.

## Reproducible source build

Go **1.26.4** is specified by `go.mod`. From this directory:

```sh
go test -mod=readonly -count=1 -timeout=5m ./...
go build -mod=readonly -trimpath -o media-openflux .
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=readonly -trimpath -o media-openflux-linux-amd64 .
```

The complete Go source directories, root `.go` files, `go.mod`, and `go.sum`
are sufficient for these builds with the pinned dependencies available. Do not
add runtime document URL lists, stream keys, Xray private configuration,
subscription links, service state, caches, logs, or prebuilt executables.

The original `README.md` contains upstream raw-socket/root/iptables usage.
For this modified restricted exit, follow `README-MEDIA.md` and
`README-POOL.md`; the restricted userspace mode does not require that rule.
The original mobile scripts were preserved but were not executed or repaired.

## Explicit browser-captured bootstrap option

`--doc-bootstrap-file /private/path/bootstrap.json` is accepted only with
`--stream-relay`. Omitting it preserves the existing document HTTP-fetch mode.
The private file has this envelope:

```json
{"schemaVersion":1,"documents":[{"url":"https://disk.yandex.ru/i/fixture","editorUrl":"https://docs.yandex.ru/edit/d/fixture","clientConfig":{}}]}
```

The empty `clientConfig` above only illustrates the envelope; it must contain
the real JSON configuration obtained by opening the operator's own document
in the normal browser editor. At minimum the existing parser requires
`officeActionData.editor_config` and `officeActionData.balancer_url` with valid
document, token, permissions, and backend fields. `editorUrl` is retained as
capture metadata and is not fetched. The `url` keys must exactly match the
requested document URLs, independently of file order. Duplicate, missing or
unexpected document entries are rejected. The file is bounded to 16 MiB and
1–10 documents. No file contents, paths, URLs or tokens appear in loader errors.

Each transport copies and validates its captured JSON before Start, then uses
that immutable configuration for subsequent document sessions. It skips the
document-page HTTP request; existing editor-asset version discovery, endpoint
checks, TLS certificate validation, WebSocket authentication and relay-key
authentication remain in place. No browser automation, cookie import or
JavaScript execution is added. Invalid or expired captured credentials fail
through the existing authentication/reconnect path. There is no automatic
refresh of the private file: the operator must obtain a fresh normal browser
capture when needed and restart with that file. Keep the bundle outside Git
with restrictive permissions; its URLs and tokens grant document access.

Each document entry may also supply an optional `dialAddress` string containing
a numeric public unicast IP. It is associated with that entry's exact `url` key,
independently of bundle order. After validating the captured configuration, the
transport clones its HTTP transport and WebSocket dialer and maps only that
configuration's original balancer hostname on port 443 to the supplied IP.
HTTP Host, requested URLs, TLS SNI and normal certificate verification retain
the original hostname. Other hosts and ports use their existing dialing behavior;
the HTTP transport retains its existing proxy behavior. Private, loopback,
link-local, multicast, unspecified and special-use destinations are rejected.
Package-local tests alone can enable loopback fixture addresses. Configuration
must finish before Start, and the captured configuration cannot change after
its dial address is installed. Omitting the field preserves ordinary DNS.
No global resolver, hosts file, shared HTTP transport or cookie state is changed.
An operator still needs to exclude the actual dial IP from the client tunnel
route to prevent VPN recursion. A valid captured `officeActionData.version`
continues to skip the optional editor API version lookup through the existing
version parser.

## Cursor wire-format correction

The original transport sent the literal prefix `18;` for every Base64 cursor
payload. The current editor protocol declares the actual decoded byte length.
The sender now emits that decimal length; the receiver accepts only canonical
positive decimal lengths from 1 to 65,536, bounds the encoded input, and requires
the decoded length to match exactly. A correctly framed 18-byte legacy payload
still works. Malformed historical mismatches are rejected. Heartbeats use
their actual 8-byte length and valid Base64; the receiver ignores the reserved
decoded marker and the previous malformed heartbeat before relay delivery.

The CLI stream chunk default is now 1,024 bytes; the generic relay library
default remains 4,096. An existing deployment that explicitly sets a larger
chunk must change its own arguments. A 1,024-byte DATA chunk produces a
1,092-byte authenticated relay frame, below the measured working sizes.

## Verification scope

On 2026-10-23, the following checks passed against this archive using the
previously cached Go 1.26.4 toolchain and pinned dependency modules, with
dependency downloads disabled (`GOPROXY=off`, `GOSUMDB=off`):

- Complete existing test suite: `go test -mod=readonly -count=1 -timeout=5m ./...`
- Race detector: `go test -mod=readonly -race -short -count=1 -timeout=5m ./...`
- macOS arm64 source build with `-mod=readonly -trimpath`.
- Linux amd64 cross-build with `CGO_ENABLED=0`, `GOOS=linux`, and `GOARCH=amd64`.

All seven packages containing tests passed; the four remaining packages had
no test files. The test listener restriction in the initial sandboxed attempt
required rerunning with permission for ephemeral loopback listeners. Both
final test commands completed successfully. Build outputs and Go caches stayed
outside this source archive. Of the 49 files copied from the local modified
source, 41 remain byte for byte identical. `main.go`, `stream_main.go` and
`transport/yandex/yandex.go` were updated for the opt-in bootstrap feature.
`transport/yandex/batching.go`, its tests, and `yandex_test.go` now use the
correct declared cursor length instead of the historical constant prefix.
`tunnel/tunnel_test.go` replaces two occurrences of a real public endpoint
fixture with RFC 5737 `192.0.2.10`, preserving non-loopback rejection tests.
`streamrelay/relay_test.go` makes the lost-final-credit test deterministic by
suppressing intermediate credit updates before dropping the first final
update; production relay code is unchanged. Six new Go files implement and
test the bootstrap and framing features, and this provenance note is additional archive
documentation. `go.mod` and `go.sum` remain unchanged.

After adding per-entry `dialAddress` support, the affected root and Yandex
packages, complete `go test ./...` suite, and complete `go test -race ./...`
suite passed with the same cached toolchain and downloads disabled. The local
HTTPS/WSS fixtures verify exact hostname-and-port mapping, unchanged Host/SNI,
successful certificate verification with a trusted fixture CA, rejection of
an untrusted certificate, foreign-host isolation, public-address validation,
configuration lifecycle and bundle-order independence. macOS arm64 and Linux
amd64 builds with `CGO_ENABLED=0` and `-trimpath` succeeded; their executables
remain outside the source archive. `transport/yandex/bootstrap_dial.go` and
`bootstrap_dial_test.go` implement and test the new opt-in behavior.

The archive also includes a later diagnostic-only change in
`transport/yandex/yandex.go` and `yandex_test.go`. Exact text packets `1` and
`41` now have distinct Engine.IO-close and Socket.IO-disconnect labels.
After authentication, seven whitelisted provider events can log only their
fixed event names and bounded integer code/result/error fields, once per event
per session. Session warnings also support the nested `messages.code` field.
Descriptions, URLs, tokens and other remote text are never logged. Cursor
message arrays retain their existing validation and delivery order; provider
warnings can separately use an object in `messages`. No routing, authentication,
heartbeat, pacing or relay behavior is changed by these diagnostics.

For this diagnostic-only change, fresh `go test -count=1 ./transport/yandex`
and `go test -race -count=1 ./transport/yandex` runs passed. Both macOS arm64
and Linux amd64 builds with `CGO_ENABLED=0` and `-trimpath` succeeded, using
the same cached toolchain and dependencies with downloads disabled. The
executables remain outside this archive. The installed-executable hashes in
[the deployment status report](../docs/status-2026-10-23.md) identify the
earlier pinned-address build, before these diagnostic-only source changes;
they are not hashes of executables built from the later diagnostic source.
These checks do not establish long-term stability or guarantee native VPN
behavior under other workloads or network conditions.

Tests use in-memory document transports, mocked local HTTP/WebSocket servers,
and ephemeral loopback TCP listeners. They do not contact live shared Yandex
documents. A successful source test run does not establish current Yandex
compatibility, mobile allowlist reachability, or production throughput.

Separately, bounded live scratch tests on the operator's own document proved
that captured bootstrap JSON authenticates to the real WebSocket service. Two
peers authenticated with different session user indices and editing privileges.
With the old fixed `18;` header a 13-byte probe was not forwarded; the same
probe was delivered in both directions after correcting its declared length.
The same two-peer size probe delivered 72, 128, 256, 384, 512, 580, 768, 1,024,
1,536 and 2,048-byte payloads in both directions. Correctly framed 3,072 and
4,096-byte payloads were not forwarded; a separate 32,068-byte probe also
failed. The exact provider limit is unknown. Consequently the previous
32,000-byte stream chunk setting is unsuitable for the observed provider, and
the conservative CLI choice is `--stream-chunk-bytes 1024`. Local 4 KiB/32 KiB
codec tests validate serialization, not the live provider's message allowance.
These scratch tests do not change document contents, production services, or
the archived unit tests, and do not establish allowlist reachability.

The source archive was reviewed for embedded private keys, access tokens,
VPN subscription links, and real document capability URLs. Runtime document
inputs and the stream authentication key are loaded from external private
files. Test fixtures contain deliberately fake credentials and short dummy
document paths. The inherited document-protocol placeholder auth string is
part of the upstream protocol code, not a user credential.
