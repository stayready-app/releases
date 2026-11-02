> **Текущее состояние: Stage 17.** Актуальное дополнение: [STAGE17.md](STAGE17.md),
> измерения: [BUILD-REPORT.md](BUILD-REPORT.md). Ниже сохранены исторические решения;
> точный OPEN/GRANT не является новым режимом по умолчанию. Скользящее окно не меняет
> экспериментальную криптографию и не делает прототип пригодным для публичного доступа.

# Current architecture: Stage 16 — bounded receiver admission

Current specification: [STAGE16.md](STAGE16.md). Results: [BUILD-REPORT.md](BUILD-REPORT.md).
The Stage 15 description below is retained as history; Stage 16 supersedes its flow-control
and compatibility sections. Other Stage 15 resource/cryptographic limitations still apply.

```
Sender Queue -> OPEN(ID,Total) -> receiver reserves Budget -> GRANT -> DATA
                                                         -> reassembly
                                 retained RX ownership -> echo Queue -> OPEN
                                                                        |
Client reserves reply <- server waits for client GRANT <-----------------+
             DATA/ACK -> response reservation released -> wake fair waiters
```

Admission is symmetric and message-granular. Permission reserves payload, bitmap and bounded
retry metadata before DATA; arrays themselves are allocated lazily. Repacking a retransmission
does not consume another grant. FIFO per session and a bounded round-robin wait list schedule
admission when memory becomes available. Unsuccessful full passes stop instead of polling.
One event loop, one timer heap entry per session; no extra goroutines or per-message timers.

Default server requires grants. Old DATA-first clients need explicit `--allow-legacy-data`
on the new multi-server. DATA/ACK formats are unchanged; four new authenticated control frame
types extend the wire protocol. New clients do not silently fall back. The payload size limit
remains 1200 bytes per UDP envelope. Control sizes are NOT obfuscated yet.

Receive flow control is implemented for this bounded message/echo model. Adaptive RTO, network
congestion control, production streaming, audited keys and additional carriers are NOT done.
Per-message grants add latency/control traffic: see the paired WAN-delay measurements rather
than extrapolating the loopback CPU improvement to every path.

---

# Current architecture: Stage 15 — audited prototype

The sections after the separator are **historical design/stage notes**, not assertions
about the current implementation. Current source, AUDIT-STAGE15 and BUILD-REPORT take precedence.

`netreliable.Server` owns one UDP socket, one event loop and one timer-heap entry per session.
No per-session or per-packet goroutine/timer is created by this server. The load generator
intentionally uses per-client goroutines and is a separate process.

## Current pipeline

```
UDP socket -> bounded envelope check -> session/address lookup -> AEAD -> reliable frame
  DATA -> limits/admission -> atomic reassembly -> retained-buffer ownership transfer
       -> response Queue -> fair retransmission/fresh frames -> AEAD -> UDP
  ACK  -> validate actually sent range -> release credited bytes/buffers
```

The secure envelope still uses the experimental X25519 + PSK + AES-256-GCM scheme,
HELLO/WELCOME and a 1200-byte datagram cap. This is NOT the final zero-RTT or
ChaCha20-Poly1305 design. Logical DATA/ACK wire format is unchanged in Stage 15.
One ACK frame acknowledges one contiguous byte range; multi-range SACK is not implemented.

## Memory and ownership

A process-shared `reliable.Budget` reserves retained payload + per-byte bitmap + a bounded
allowance for original-transmission descriptors. `BufferCharge(N)` is now
`N + 8*ceil(N/64) + 64*min(N,1024,max(8,ceil(N/256)))`. A 32-KiB message reserves 44 KiB.
The descriptor allowance is reserved already on receive, so RX-to-TX ownership transfer
cannot require extra global credit. Descriptors themselves are lazy allocations.

`AcceptRetained` transfers a `Received` owner; `EnqueueReceived` transfers it again without
copying payload/bitmap. `Release`, queue completion and session close release the reservation.
Do not retain/change/reslice Data after transfer. `Accept` is a convenience API that releases
credit on return: returned caller-owned bytes are outside this budget. Production server
uses the retained API. The budget is NOT RSS: maps, cipher state, allocator classes, runtime,
transient allocation overlap and kernel buffers remain outside it and need separate measurement.

Queues cap both bytes and object count (64 messages at the generic library level).
Original transmission descriptors are bounded; retry repacking moves cursors, never creates
new descriptors. Bitmap operations work in 64-bit words. Only original ranges overlapping
an ACK are revisited. Scratch codec/AEAD buffers belong to the event-loop owner and are reused
only after a synchronous send returns. Session, Queue and Receiver are not concurrent APIs.

## Lifetime, replay and backpressure

Same-Identity HELLO re-accept is rejected by `secureudp.AcceptHello`, including when another
Server is constructed with that Identity object. An active repeated HELLO only resends cached
WELCOME ciphertext. Identity must not be copied/reconstructed with old private material and
an empty replay registry. Replay registries fail closed at their configured bounds. Durable
key epochs, key rotation and authenticated individual client authorization remain unimplemented.

Completed message IDs use bounded history plus a retirement watermark. Generic receiver IDs
must be monotonic/non-reused with bounded lag: an unknown ID below the watermark returns
ErrStale. An already active older message can finish. The echo server adds contiguous request
retirement and a bounded 128-request lookahead. This is at-most-once delivery during the live
session, not durable exactly-once delivery across process restart/new sessions.

Only semantically valid authenticated progress refreshes progress state. Shared event-loop
work is bounded per pass (64 scheduled sessions, 32 DATA frames and a 2-ms wall-work target;
one syscall can extend that target). Read deadlines always allow reading, even when timers
are overdue. The next retry wakeup is scheduled, rather than polling every queued session.

The sender does NOT yet know negotiated receive credits; resource pressure still causes
rejection/retransmission. RTO remains fixed at 80 ms. There is no proper path congestion
control/pacing. These are release blockers for weak CPUs at sustained high concurrency.

## Compatibility and current scope

`Serve` and `ServeBatch` are now bounded finite wrappers around the same server as `--multi`.
CLI options are retained. Raw `ServeEcho` remains a separate, single-peer unreliable lab harness,
with its replay/injection handling fixed; it is not a reliable transport API.
`ExchangeBatch` checks a finite dataset with bounded active groups, but retains the supplied
client dataset and completion map. A truly streaming production API is still future work.

The Go project remains separate from StayReady/sing-box/Xray. No current StayReady source
archive/configuration has been assumed or changed. TCP bridge, carriers beyond UDP, migration,
4–8-KiB datagrams/IP-fragmentation experiments and DPI claims are not implemented/verified.

Real socket tests now include a separate test-only UDP relay with loss, delay/reordering,
duplicates, corruption, injection, size filtering, dropped WELCOME and ACK blackout.
Seed fixes emulator decisions; real socket scheduling is not fully deterministic. This relay
is not yet a standalone Windows/Ubuntu executable.

See [AUDIT-STAGE15.md](AUDIT-STAGE15.md), [BUILD-REPORT.md](BUILD-REPORT.md) and
[SECURITY.md](SECURITY.md) for exact fixes, measurements and limitations.

---

# SRDT — architecture baseline (stage 1)

## Purpose
General-purpose, encrypted, bidirectional message transport. It does not parse TCP/IP packets. Initially it will carry test messages; later a local TCP bridge will carry a pre-existing sing-box/Xray-compatible connection. Xray VLESS/REALITY does **not** accept arbitrary raw IP packets at its TCP listener. Verify actual StayReady gate/signbox port mapping before deployment.

## Layers
1. Application adapter: test messages, then TCP bridge.
2. Logical session: message ID, channel, delivery receipts, bounded buffering, epochs and session resume.
3. Security: client-specific authorization, server identity, key versions, per-session keys, directional AEAD, anti-replay, authenticated control and reset. Preprovisioned public keys and optional first-packet data are design goals, not a finalized cryptographic construction. Never share one symmetric server secret across all clients. Prefer established audited key exchange.
4. Delivery: message framing; UDP-specific fragmentation, reassembly, ACK ranges, loss detection, retransmission with new packet sizes, deduplication and congestion control. Logical message IDs are independent of transmission IDs. Reliable stream carriers should not duplicate packet-level retransmission.
5. Carrier: UDP first; TCP/TLS, WebSocket, HTTP/2 and QUIC later. True protocol implementations are preferred to superficial impersonation.

## UDP constraints and experiments
Most datagrams should fit path MTU; optional minority of 4–8 KiB datagrams for experiments. Large UDP datagrams can undergo IP fragmentation, and some paths drop them systematically. Size distribution is configurable and adaptive, not an intrinsic protocol invariant. IP fragmentation and its interaction with DPI are hypotheses requiring measurements, not proven bypass mechanisms. Transport does not guarantee delivery under full blocking.

## Network adversary
Assume injection, tampering, replay, duplication, reordering, selective size-based drops, delayed ACK, outages, and carrier blocking. Invalid unauthenticated packets must not mutate session state. Control messages, including RESET, must be authenticated. Bound memory, CPU and retransmission work per session. Full carrier blocking is unrecoverable without an alternate path.

## Testing strategy
Deterministic data generator and exact per-message SHA-256 verification; virtual-time bidirectional emulator with independent seed, size filters, random loss, duplication, corruption and delay. Add replay, injection, bandwidth limits, bursts, blackouts and real UDP relay in later stages. Keep unit tests, fuzz tests, race tests, Windows cross-compilation and Ubuntu integration tests. Every bug becomes a regression scenario. Do not mistake testkit's unencrypted framing for production protocol.

## Stages / acceptance gates
1. **Foundation** (this archive): architecture, deterministic testkit, basic datagram emulator, CLI demonstration. Go build, unit tests, race tests and vet pass. No production transport yet.
2. **Secure session**: reviewed cryptographic handshake and identity, authenticated messages, UDP carrier, Windows/Ubuntu binaries; bidirectional real socket tests.
3. **Reliable UDP**: selective ACK, bounded retransmission, reassembly, deduplication, congestion control; fault matrix and timeouts.
4. **Adaptive UDP**: aggregation, size profiles, repackaging, selective size-drop recovery, real-path MTU experiments.
5. **Integration**: TCP bridge to the verified StayReady Gate/Xray entrypoint; concurrency, backpressure, resource caps, long-run tests.
6. **Other carriers**: real TLS/WebSocket/HTTP2/QUIC adapters and resume tests; compare latency, throughput and blocking resilience.

Every stage must ship the full project archive with build and test results. No claims of cross-platform runtime tests without actually running them.

## Stage 2 experimental secure UDP echo (2026-11-01)

Implemented `secureudp` prototype: ephemeral client X25519 + server static X25519, 32-byte client PSK authenticating initial hello; HMAC-SHA256/HKDF-style derivation, separate AES-256-GCM directional keys, monotonically increasing packet counters, 64-packet receive replay window, encrypted server welcome, UDP echo. AES-GCM is used because the standard Go distribution here does not ship ChaCha20-Poly1305; algorithm selection is pending. **This is experimental, unaudited crypto, not production-ready.** No client identity database, server private-key persistence, multi-session management, authenticated retry, hello replay cache, forward-secret server rekey policy, anti-amplification, congestion control, retransmission, carrier migration, or Xray integration. Replaying a previously valid ClientHello can currently replace server state in the single-session echo harness. The handshake transcript and derivation need an independent security review and a standard vetted handshake before production use. A successful echo is not a reliability test.

Stage 2 does NOT satisfy the originally proposed production-quality authenticated handshake criterion. It is an experimental transport integration milestone. Server generates a new private key on each start; public key is printed and must be pinned by the client for this process lifetime. PSK is supplied on the command line for demonstration only and may be exposed in process listings. Avoid real credentials. For any deployed version use protected configuration, durable server identity, authenticated per-client authorization, rate limits and an established key exchange.

## Stage 3 — reliability prototype (2026-11-01)

`reliable` is a transport-independent, correctness-first library. A message is
identified by a 64-bit ID; DATA frames carry total length, byte offset and
payload. ACK frames acknowledge an offset plus a 32-bit length. The sender
retains unacknowledged bytes and can resegment retransmissions with a different
`maxPayload`. The receiver supports out-of-order fragments and suppresses
redelivery of completed messages. Frames must be carried inside authenticated
and encrypted sessions; their wire format is NOT secure on its own.

**Important outstanding design work:** the current ACK format is a single
contiguous range per frame (not multi-range SACK); the sender uses O(message
size) per-byte bookkeeping and fixed RTO; there is no congestion control,
flow control, expiry for completed-message IDs, durable resumption, or
production network wiring for the reliable layer. The existing stage-2 echo
CLI still sends only one unreliable encrypted datagram. The 1200-byte limit
in `secureudp` remains. Session reinitialization/replay weaknesses in the
stage-2 echo server remain unresolved. No production deployment is safe.

Next: integrate reliability with actual UDP endpoints; implement bounded
range-based state, congestion control, secure multi-session management, then
size adaptation and network-emulator end-to-end tests.
