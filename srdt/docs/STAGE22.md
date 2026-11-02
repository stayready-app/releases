# Stage 22 — bounded asynchronous duplex application I/O

## Goal

Stage 21 had a bounded stream adapter, but its network owner directly called `src.Read` and
`dst.Write`. A blocking application endpoint therefore stopped UDP receive, ACK generation,
retransmission and receive-window maintenance. This is unacceptable for a TCP/sing-box/Xray
bridge.

Stage 22 separates application I/O from protocol ownership without changing the V1 wire format.

## Architecture

`ExchangeStream` now has three owners:

1. **network owner** — the caller goroutine; it alone mutates Queue/Receiver/session state;
2. **reader pump** — one goroutine that may block in application `Read`;
3. **writer pump** — one goroutine that may block in application `Write`.

There are no goroutines or timers per DATA packet. The reader pump owns one reusable
`ChunkSize` buffer and does not read the next application chunk until the network owner confirms
that the previous borrowed bytes were copied into bounded Queue storage. The writer pump takes
ownership of a completed `reliable.Received` object and releases it after the application write;
there is no second payload copy.

The pumps wake a UDP `ReadFrom` blocked in the network owner by moving the socket read deadline to
`time.Now()`. This avoids a polling loop and removed most of the initial throughput regression.

## APIs

`ExchangeStream` remains the finite Reader/Writer API and does not take ownership of endpoints.

`ExchangeDuplex` accepts an `io.ReadWriteCloser`, owns it for the call, and closes it on return.
Closing a normal `net.Conn` unblocks application Read/Write during cancellation. It is the intended
pre-bridge API for local TCP tests.

The CLI adds:

```
--duplex-listen 127.0.0.1:19080
```

It accepts one local TCP connection and runs it through the current SRDT echo application.

## Backpressure and bounds

Default bounds remain:

- chunk: 16 KiB;
- queued outbound application data: 64 KiB;
- active transactions: 4;
- receiver state: bounded by the existing Receiver/window-credit limits.

If local output blocks, the writer pump blocks but the network owner continues until the bounded
reply reservations/receiver state are full. At that point protocol receive-window admission
naturally applies backpressure; buffers do not grow without limit.

If local input blocks, ACK/retransmission/network progress continues independently.

## Tests added

- blocked application Read does not prevent the already-sent chunk from being echoed;
- blocked application Write does not prevent server receipt/ACK progress for the bounded active
  window;
- 512 KiB duplex traffic through loss, delay, duplication, size filtering and forged packets;
- tests run repeatedly and with Go race detector;
- process-level TCP listener smoke test uses a real TCP half-close and verifies a 1 MiB echo.

## Performance

The first asynchronous implementation showed a reproducible client-side polling penalty because
the network owner could remain blocked in UDP `ReadFrom` after an application pump became ready.
The pumps now wake the socket loop by moving its read deadline to `time.Now()`.

In the final interleaved local comparison (three 100 MiB runs per version), pristine Stage 21 had
a 4.021 s median and Stage 22 had a 3.977 s median (~1.1% lower). Individual runs overlap, so this
should be interpreted as **no demonstrated material throughput regression or speedup**, not as a
1.1% optimization claim. The shared server event loop is unchanged by this stage.

The earlier pre-wakeup implementation and both final comparison logs are retained under
`docs/logs/` so the optimization history is auditable.

## Important limitation

This is full-duplex **application I/O scheduling**, not yet a generic bidirectional server
application protocol. The current server still maps request ID N to an echo response ID N+1 and
therefore knows reply capacity in advance. An arbitrary Xray/TCP upstream can produce bytes with
sizes/timing unrelated to client writes. The next bridge layer must give each direction an
independent logical stream/window and preserve the same bounded resource model.
