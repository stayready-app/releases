# SRDT Stage 21 — capability negotiation, variable control sizes and bounded stream adapter

Stage 21 keeps the Stage 20 UDP reliability/congestion machinery and changes the
session/application boundary. It is still a laboratory protocol, not a production VPN.

## 1. Authenticated V1 capability negotiation

A V1 HELLO carries an authenticated core containing protocol version, offered capabilities
and required capabilities. The server intersects optional capabilities with its supported
set and rejects unknown required capabilities. The selected set is returned inside the
authenticated/encrypted WELCOME.

Current capability bits cover packet tracking, packet ACK batching, receive-window credits,
control padding, streaming, adaptive RTO and congestion control. There is no timeout-driven
fallback. Legacy Stage 20 HELLO is accepted only when the server is started with
`--allow-legacy-handshake`; a Stage 21 client uses it only with `--legacy-handshake`.

The V1 HELLO has a semantically ignored random tail. The tail is deliberately not part of
the authenticated transcript: it can change on retry without changing ECDH keys or session
semantics. Version/capability/key material remains authenticated. An on-path party may alter
or remove the tail, but this cannot grant capabilities or change keys; it can already drop
the datagram entirely.

A repeated valid V1 HELLO does not create a new crypto session or reset counters. The server
reuses the existing session and emits a freshly encrypted WELCOME with a new nonce and
padding. Tests block the exact observed size of the first HELLO or WELCOME and verify that
retries with a different size establish the same session.

## 2. Control packet size variation

Authenticated envelopes now support all control frames, not only receive-window controls.
DATA sizing remains the responsibility of the UDP scheduler. Control padding is selected
from rotating broad bands with secret-derived jitter, so retries do not depend on repeatedly
choosing a lucky random size.

This is traffic-shaping, not a DPI-resistance proof. Packet direction, timing, volume,
server IP, encryption behavior and many other features remain observable. A sufficiently
broad size filter can still block the carrier.

## 3. Bounded streaming adapter

`ExchangeStream` is the first byte-stream adapter over the existing reliable-message
substrate. Defaults:

- 16 KiB logical chunks;
- 64 KiB maximum queued application bytes;
- at most four active transactions;
- responses written to the destination in original byte-stream order.

It requires V1 and the streaming capability. The source is read incrementally; the whole
stream is not retained in memory. A 100 MiB process test completed with a measured server
RSS peak of 9788 KiB in the current Linux loopback environment.

This adapter still targets the echo application. It is **not** yet a generic Xray/sing-box
bridge. One owner goroutine performs source reads and network progress, therefore a source
that blocks indefinitely can stall network processing. A real duplex socket bridge needs a
poll/event-loop integration or carefully bounded I/O workers.

## 4. Echo-level delivery proof

In the laboratory echo application, receiving the authenticated response for request N is
proof that the server reconstructed request N. The client therefore retires local request
state even if the separate packet receipt for the last request copy was filtered.

This optimization does not fabricate a packet receipt, RTT sample or congestion-window
increase. It is application-specific and cannot be assumed for an arbitrary Xray response.

## 5. Resource model

The server remains a single UDP socket/shared event loop with no goroutine or timer per
session. Stage 21 adds capability fields to session state. The in-process 1000-idle-session
test measured about 2461 retained heap bytes/session versus about 2413 bytes/session for
Stage 20 in back-to-back runs on this host. This is not RSS and varies with the allocator.

A process test with 1000 sessions performing 16 x 4096-byte request/echo exchanges completed
with 15,532 KiB sampled peak server RSS, 4,939,264 peak accounted payload bytes and 983,040
peak packet-state bytes. It does not prove the same result on the target 2-GB VPS or with
Xray running in the same cgroup.

## 6. Compatibility

- Stage 21 client -> Stage 20 server: explicit `--legacy-handshake`; tested PASS.
- Stage 20 client -> Stage 21 server: explicit server `--allow-legacy-handshake`; tested PASS.
- No timeout-driven silent downgrade exists.
- Old packet/credit compatibility flags remain available for older protocol stages.

## 7. Remaining blockers

Production use still requires at least:

- reviewed key provisioning, authorization, revocation and epoch rotation;
- a generic duplex stream bridge and Xray/sing-box integration;
- reliable close/shutdown semantics;
- native Windows tests;
- target 2-GB node + Xray measurements and soak tests;
- carrier alternatives;
- broader congestion/fairness work;
- updated supported Go toolchain and `govulncheck`;
- real operator/DPI testing before making any resistance claim.
