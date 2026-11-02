# SRDT Stage 23 — independent bidirectional streams and fixed TCP upstream bridge

## Goal

Stage 22 isolated blocking client application I/O from SRDT network progress, but the remote
application protocol was still echo-oriented: request message `N` implied response `N+1`.
Stage 23 removes that semantic dependency and introduces two independent logical byte streams:

- client -> server/upstream uses odd logical message IDs;
- server/upstream -> client uses even logical message IDs.

Either side may produce data independently of the other. TCP half-close is carried explicitly.
This makes the protocol usable for a laboratory fixed TCP upstream bridge such as an Xray/Gate
listener on `127.0.0.1:<port>`.

This remains a laboratory protocol. It is not a production VPN transport.

## Wire additions

V1 adds `CapabilityIndependentStreams`. A server configured with `--upstream` requires:

- `CapabilityStreaming`;
- `CapabilityIndependentStreams`;
- `CapabilityWindowCredits`.

The following authenticated control frames are added:

- `STREAM_OPEN` / `STREAM_OPEN_ACK`;
- `STREAM_FIN` / `STREAM_FIN_ACK`;
- `STREAM_RESET`.

`STREAM_FIN` is directional. A client FIN means no more odd client->server messages; a server FIN
means no more even server->client messages. FIN cannot precede already observed data in that
direction. Conflicting FIN state is rejected.

The older echo modes remain available. Independent-stream semantics are never enabled by timeout
or inferred from old traffic.

## Client API

`ExchangeIndependentDuplex` accepts a half-close capable `io.ReadWriteCloser`. It keeps the Stage
22 bounded application pumps but removes request/response coupling:

- client application reads are chunked into the odd stream;
- independently received even messages are written to the application;
- local EOF/`CloseWrite` produces a reliable client->server FIN;
- remote FIN is applied to the local TCP endpoint only after all preceding even messages are
  written.

Default stream bounds remain 16 KiB chunks, 64 KiB queued application data and four active logical
messages.

The CLI adds:

```text
--bridge-listen 127.0.0.1:19080
```

This is distinct from the older `--duplex-listen`, which keeps the Stage 22 echo semantics.

## Server fixed TCP upstream

The multi-client server accepts:

```text
--upstream 127.0.0.1:18443
```

No destination is accepted from an untrusted client. The configured address is the only upstream.
For StayReady this can later point at the local Xray/Gate listener.

A live bridge uses two bounded blocking goroutines:

1. TCP upstream read pump;
2. TCP upstream write pump.

The shared UDP side still has one socket/event loop and no per-DATA goroutine or timer. TCP dials
are asynchronously bounded by `--max-upstream-dials` (16 by default), so a burst of new clients
cannot create an unbounded set of blocked dial goroutines.

The upstream socket requests a 16 KiB read and 16 KiB write buffer by default. These are kernel
socket buffers and are not represented by Go RSS/accounting exactly; Linux may account more than
the requested value. `TCP_NODELAY` is enabled.

## Global memory/backpressure fixes found during Stage 23

### Pending upstream read after same-session memory release

A real deadlock-like stall was reproduced with as few as ten bridge clients. Upstream data could
arrive while the session's bounded resource limit was occupied by client->server data. The read was
retained as pending. Once the write to upstream finished and released memory, the pending read was
not reconsidered.

The write-completion path now immediately retries the pending upstream read. A concurrent
regression test reproduces the old condition.

### Cross-session starvation at the global budget

At 1000 active clients the 64 MiB global buffer budget can legitimately become full. A server->client
upstream read in session A could fail admission while memory was owned by session B; releasing
memory in B did not wake A.

Bridge reads blocked by the **global** budget now enter the existing bounded fair waiter list. When
any session releases storage, the scheduler gives waiting bridge output a bounded chance to
progress without scanning every session. Session-local blocking does not pollute this global list.

This fixes the long tail seen when the global 64 MiB budget was saturated.

### Shared UDP receive buffer

The Linux default UDP socket receive buffer in the test environment was too small for a burst of
1000 clients and caused a large avoidable retransmission storm. Stage 23 requests one shared
2 MiB UDP receive buffer and one shared 1 MiB send buffer by default.

This is **one global socket resource**, not per-client memory. It materially reduced burst loss in
the process tests. The actual kernel allocation is outside the Go heap/RSS accounting used by the
protocol budgets and may differ from the requested value.

## Process-level bridge tests

The load generator, SRDT server and TCP upstream echo service run as separate processes so the
load/upstream allocations are not charged to SRDT server RSS.

### 1000 active bridges, 64 KiB per client in each direction

Configuration:

- 1000 clients;
- each client sends `16 * 4096 = 65536` bytes;
- fixed TCP upstream echoes the stream;
- total verified client->server/upstream bytes: 65,536,000;
- the same amount returns server->client;
- `GOMAXPROCS=2`, `GOMEMLIMIT=128MiB` for the server process.

Final result:

- 1000/1000 clients PASS;
- load wall time: 7.589 s;
- sampled SRDT server CPU: 3.09 s;
- peak sampled SRDT server RSS: 29,888 KiB (~29.2 MiB);
- peak accounted message-buffer budget: 9,371,648 bytes;
- peak packet-state budget use: 1,294,336 bytes;
- `ResourceDrops = 0`;
- bridge upstream bytes read/written: 65,536,000 / 65,536,000;
- peak fair waiters: 86.

This is a short Linux loopback test on the current host, not proof that a weak 2 GiB production node
with Xray will behave identically.

An earlier high-pressure intermediate build did saturate the 64 MiB global budget and exposed the
cross-session starvation bug described above. The final run no longer saturates that budget; the
final scheduler/backpressure path peaks at about 9.37 MiB of accounted message storage in this
workload.

### CLI bridge smoke

Final Linux binaries were used with a separate TCP echo upstream. A local application connected to
`srdt-client --bridge-listen`, sent 1 MiB, half-closed its write side, then read the stream back.

- sent: 1,048,576 bytes;
- received: 1,048,576 bytes;
- exact byte comparison: PASS;
- input/output SHA-256:
  `1a0a2e9fbff171a4933522c4335912a2e2d245148912e6e57d1085cd6023ac71`;
- upstream read/written bytes: 1,048,576 / 1,048,576;
- `ResourceDrops = 0`.

## Fault and semantic tests

Stage 23 tests cover:

- server push before the client sends any application data;
- independent client and server half-close;
- FIN consistency and ordering;
- explicit upstream dial failure;
- packet loss, delay, duplication, forged datagrams and size filtering through the relay;
- the same-session pending-read wakeup bug;
- preservation of Stage 21/22 echo/credits/congestion behavior.

## Current limitations

- Production authentication/key lifecycle/revocation are not designed.
- `STREAM_RESET` is not acknowledged/retried like FIN. A lost reset may appear to the peer as a
  timeout instead of immediate failure.
- There is no keepalive yet. The default 60 s server idle timeout can close an otherwise valid but
  silent long-lived TCP bridge. Production integration must add keepalive/path liveness or choose
  a justified timeout.
- The upstream is a fixed TCP address only. No arbitrary destination, SOCKS/TUN adapter or Xray
  control integration exists.
- Two blocking goroutines are used per active upstream TCP bridge. 1000 active bridges therefore
  imply roughly 2000 bridge I/O goroutines in addition to test/client processes. This was acceptable
  in the short test but still needs target-node/soak validation.
- Kernel TCP/UDP socket-buffer memory is not included in the protocol's Go buffer budgets and was
  not measured with kernel memory accounting.
- No native Windows runtime test yet; Windows is cross-compiled only.
- No real 2 GiB StayReady node test together with Xray/sign-box, no operator WAN/TSPU test and no
  long soak.
- The available toolchain is Go 1.23.2; a current `govulncheck` was not available.
