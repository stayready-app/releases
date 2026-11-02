# Архитектура SRDT — Stage 23

Текущая спецификация изменений: [STAGE23.md](STAGE23.md). Архитектура Stage 22 сохранена как
[ARCHITECTURE-STAGE22.md](ARCHITECTURE-STAGE22.md).

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
        |
One shared UDP socket per SRDT server
        |
Internet
        |
Stage 23 server bridge
  bounded dial admission
  TCP upstream read pump / write pump
        |
Fixed TCP upstream (e.g. local Xray/Gate)
```

Logical message IDs, DATA PacketNumber and AEAD nonce/counter remain independent namespaces.

## Independent stream ownership

Client->server stream uses odd message IDs. Server->client uses even IDs. Neither direction implies
that a message must exist in the other direction. This is the essential difference from the Stage
22 echo application.

A stream starts with authenticated `STREAM_OPEN`. Each direction ends independently with
`STREAM_FIN`; FIN is acknowledged and cannot move before already observed data in that direction.
A reset is authenticated but currently does not have reliable acknowledgement semantics.

## Client endpoint

`ExchangeIndependentDuplex` owns the local half-close-capable connection. Blocking application I/O
is isolated in the Stage 22 bounded pumps. The network owner remains the sole mutator of Queue,
Receiver, cryptographic and congestion state.

Default application bounds:

- chunk: 16 KiB;
- queued application bytes: 64 KiB;
- active logical messages: 4.

There are no per-DATA goroutines or timers.

## Server bridge model

The server still owns a single UDP socket and a single SRDT event loop. No UDP session has a
dedicated network goroutine or timer.

When `ServerConfig.Upstream` is configured and independent-stream capabilities are negotiated, a
bridge dials that **fixed** TCP address. The address is not controlled by the remote client.

An active bridge uses two blocking goroutines:

- upstream TCP reader;
- upstream TCP writer.

Dial attempts are separately bounded (`MaxUpstreamDials`, default 16). Completed I/O is returned to
the shared event loop through a bounded event channel; protocol state is still owned by the event
loop.

The default requested upstream socket buffer is 16 KiB per direction to reduce kernel memory for
large session counts. The actual kernel memory footprint is platform-dependent and is not identical
to this request.

## Memory and backpressure

There are multiple deliberately separate limits:

1. **global BufferBudget** — retained logical payload, bitmap and bounded range metadata;
2. **per-session SessionBufferBytes / MaxMessages**;
3. **PacketStateBudget** — bounded physical-transmission ledger metadata;
4. application stream queue bounds;
5. shared UDP socket buffers;
6. kernel TCP upstream socket buffers.

The first four are explicit SRDT accounting. Socket-buffer memory is kernel-owned and not counted by
those budgets.

When a server->client upstream chunk cannot acquire global BufferBudget, the bridge enters the same
bounded fair waiter structure used for global receive admission. Memory release in any session can
therefore wake a bridge blocked in another session without an O(number-of-sessions) scan.

If a chunk is blocked only by that session's own active-message/byte limit, it is retried when a
same-session write/release event makes progress rather than joining the global waiter list.

## Shared UDP socket

Stage 23 requests a 2 MiB receive buffer and 1 MiB send buffer on the one shared UDP socket. This
was required to avoid burst loss when 1000 clients transmit concurrently. It is global memory, not
per-session allocation.

## Half-close mapping

- client application EOF -> client `STREAM_FIN` -> server waits until all preceding odd messages
  have been written to upstream -> `upstream.CloseWrite()`;
- upstream EOF -> server waits until preceding even messages are queued -> server `STREAM_FIN` ->
  client applies `CloseWrite()` to its local application endpoint after all preceding even data has
  been written.

This preserves TCP half-close semantics without coupling the two logical byte streams.

## Scaling result, not a guarantee

A process test with 1000 active bridges and 64 KiB per client in each direction completed with
~29.2 MiB sampled SRDT server RSS and no protocol `ResourceDrops`. This is encouraging for a 2 GiB
node but is not a production capacity claim: kernel socket memory, Xray memory/CPU, real WAN loss,
soak behavior and native Windows operation remain unmeasured.
