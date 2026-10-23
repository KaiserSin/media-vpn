# Experimental document pool

This is a separate copy of the single-document prototype. It retains the same restricted userspace exit: `10.203.0.1:443` maps only to the configured literal loopback upstream, default `127.0.0.1:443`. It does not change Xray, operating-system routes, firewall rules or existing services.

Use the same set of 1–10 shared editing URLs on both sides, in a private newline-delimited file. Input is bounded, whitespace is trimmed, duplicates rejected, and URLs sorted before channel numbers and pool identity are assigned. Logged channel numbers refer to that canonical order, not document names. Never publish this file or its contents.

Example CLI shape, using an existing private file:

```text
media-openflux-pool --exit-node --url-list-file /private/path/documents.urls --pool-mode flow --min-ready 10 --ready-timeout 90s
media-openflux-pool --client --url-list-file /private/path/documents.urls --pool-mode flow --min-ready 10 --ready-timeout 90s --socks5 127.0.0.1:19082
```

Use the same `--pool-mode` at both ends. For a one-channel comparison, use a list containing one document and `--min-ready 1`; this keeps the same pool envelope. The older `--url-file` and `MEDIA_DOCUMENT_URL` paths retain the single-document wire format and cannot communicate with a `--url-list-file` endpoint.

## Scheduling

- `flow`: canonical symmetric IPv4/TCP four-tuple hash modulo the fixed number of documents. Both directions use the same document. No flow table, unbounded memory, idle expiry or silent reassignment. If its document disconnects or queue fills, Send reports an error; gVisor handles TCP retransmission, and the flow keeps its affinity. Several parallel TCP connections are needed to use several documents. The pool does not add Xray Mux; the benchmark caller must ensure an existing Mux setting does not collapse all work into one outer TCP flow.
- `stripe`: each complete IPv4/TCP packet chooses the next live channel. If the selected channel is unavailable or its queue is full, the call immediately tries the remaining channels once; if all reject it, Send returns an error. gVisor receives packets from all lanes and performs normal TCP reordering/retransmission. Reordering and varying RTT can reduce throughput; no linear speedup is promised.

Each pool frame has a 40-byte versioned envelope containing the pool identifier, random endpoint identifier, channel index, count and mode. Self echoes, mismatched pools, wrong indices/modes, malformed packets and over-MTU payloads are discarded. The pool identifier is derived from the canonical document list and is never logged. This is an isolation/sanity envelope, not a cryptographic authentication protocol. The existing inner TLS/Xray endpoint remains responsible for authenticating tunnel clients and protecting payload contents.

## Bounds and readiness

There is one gVisor stack per process and no extra packet queue in the pool. Each document has its own existing bounded queue, default `--queue-packets 128` (allowed1–256). At10channels, queued wire packets occupy at most approximately2MiB before Go/channel/WebSocket metadata and transient copies. This is not a total process-memory bound. Input packet MTU is1500; envelope adds40bytes.

Starts are staggered by `--start-stagger 350ms` (allowed0–30s). A document keeps its existing8-retry reconnect limit and1–30s backoff. `--min-ready 0` means all listed documents. Ready counts measure authenticated document WebSockets, not proof that the remote pool is already exchanging payloads. Both endpoints must be ready and an end-to-end transfer must succeed before measurement.

`--stats-interval 10s` reports channel indices, readiness, accepted payload bytes, actual child-transport sent/received wire bytes, send errors and reconnects. No URL, room token or endpoint/pool identifier is printed. A down lane is visible even when a lower min-ready threshold lets other lanes continue.

## Verification

Unit fixtures cover symmetric mapping/distribution, stable flow affinity across disconnection/reconnection, stripe backpressure, bounded failure, envelope echo/mismatch filtering, invalid IP packets, startup cancellation, failed child startup and concurrent lifecycle/stats calls.

Offline gVisor tests send512KiB each way through a local loopback echo service in flow, stripe and stripe-with-one-channel-down modes. `-short` uses64KiB for race runs; lifecycle/routing tests remain unchanged. The existing single-document, SOCKS5, restricted-exit and mocked WebSocket tests remain in the copied project.

No network throughput result can be inferred from these local tests.

## Fixed-exit capacity

`--max-connections` configures the number of simultaneous admitted TCP streams
to the fixed loopback upstream. Default32 preserves the previous behavior; the
allowed range is1–1024. For the128-connection experiment, add
`--max-connections 128` to the **server** unit's existing ExecStart arguments.
Client pool mode, document list and wire format do not change.

The exit prints one aggregate line every `--stats-interval` (default10s):
`Exit active=... limit=... accepted=... rejected_limit=... dial_failed=...`.
Active includes upstream dials in progress. Rejected-limit means the connection
was closed before an upstream socket was opened. Dial-failed counts failed
upstream connections, excluding shutdown cancellation. No per-request messages,
addresses or payloads are logged.

The capacity change does not resize TCP buffers or alter idle timeouts. Raising
the limit can raise live memory and CPU consumption; keep observing the existing
service memory limit and active/rejected counters. Saturation still closes new
connections immediately and is now observable.
