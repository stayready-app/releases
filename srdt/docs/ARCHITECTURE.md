# Архитектура SRDT — Stage 24

Текущие изменения: [STAGE24.md](STAGE24.md). Архитектура Stage 23 сохранена как
[ARCHITECTURE-STAGE23.md](ARCHITECTURE-STAGE23.md).

## Уровни

```text
Local TCP/application endpoint
  bounded Read pump / bounded Write pump
        |
Independent SRDT byte streams
  odd IDs:  client -> server/upstream
  even IDs: server/upstream -> client
  OPEN / directional FIN / RESET
        |
Reliable logical messages
  receive-window admission / retransmission / deduplication
        |
Physical DATA attempts
  PacketNumber / packet ledger
  congestion window / pacing / adaptive RTO
        |
secureudp V1
  authenticated capabilities / AEAD / variable control padding
  authenticated PATH_PING / PATH_PONG
        |
One shared UDP socket per SRDT server
        |
Internet
        |
Server bridge
  bounded dial admission
  TCP upstream read pump / write pump
        |
Fixed TCP upstream (e.g. local Xray/Gate)
```

Logical message IDs, DATA PacketNumber, path-liveness token and AEAD nonce/counter are independent
namespaces.

## Independent streams

Client->server stream uses odd message IDs; server->client uses even IDs. Neither direction requires
a message in the other direction. `STREAM_OPEN` begins bridge semantics, each direction has its own
`STREAM_FIN`, and FIN cannot move behind already observed DATA.

`STREAM_RESET` is authenticated but still best-effort.

## Path liveness

Capability `path_liveness` is explicitly negotiated in V1. Modern clients send authenticated
`PATH_PING` only after the path has been otherwise silent; server answers `PATH_PONG`. Any valid
inbound authenticated traffic updates receive-liveness, so normal DATA/ACK traffic suppresses pings.

Default timing:

```text
keepalive interval        15 s
liveness timeout          45 s
server idle timeout       60 s
completed bridge drain     1 s
```

Keepalive period receives stable per-session jitter to reduce synchronization of large client sets.
No per-session ticker is created: liveness deadlines are scheduled by the existing network owner.

If the reverse direction dies while client->server remains usable, the client sends a best-effort
authenticated RESET before locally failing. If both directions die, server cleanup still relies on
its bounded timeout.

## Client endpoint

`ExchangeIndependentDuplex` owns the local half-close-capable connection. Blocking application I/O
uses the bounded Stage 22 pumps. The network owner remains sole mutator of Queue, Receiver,
cryptographic, liveness and congestion state.

Defaults:

- chunk: 16 KiB;
- queued application bytes: 64 KiB;
- active logical messages: 4.

No goroutine/timer exists per DATA packet.

## Server bridge model

The server owns one UDP socket and one SRDT event loop. A fixed upstream address is configured by the
operator; the client cannot choose arbitrary destinations.

One active upstream TCP bridge uses two blocking Go goroutines (Read/Write). TCP dial concurrency is
bounded (default 16). Protocol state is still modified only by the shared server event loop.

Normally completed bridges enter a bounded 1-second drain for duplicate FIN/ACK recovery, then their
session state is removed immediately instead of being retained for the general 60-second idle
lifetime.

## Memory and backpressure

Separate bounds remain:

1. global BufferBudget — retained payload/bitmap/range metadata;
2. per-session payload/active-message limits;
3. PacketStateBudget — physical-transmission ledger;
4. bounded application stream queues;
5. one shared UDP socket buffer pair;
6. kernel TCP upstream socket buffers.

Kernel socket memory and Go runtime overhead are outside BufferBudget and must be measured on the
actual 2 GiB node.

The combined per-session payload allowance is directionally fair: one direction cannot consume all
storage and starve the reverse full-duplex stream.

## Idle scaling

A final Stage 24 Linux-loopback process test keeps 1000 real TCP bridges application-idle for 4 s
while server idle timeout is only 2 s. Stress keepalive is 500 ms (30x more frequent than default).
All 1000 survive and close normally; peak sampled SRDT-server RSS is 30,812 KiB, server CPU ~1.31 s,
and no resource drops occur.

This is evidence that liveness does not require per-session polling at high frequency. It is not a
production capacity guarantee: Xray, kernel socket memory, WAN conditions and long soak remain to be
measured.
