# SRDT Stage 22 — build, correctness and performance report

Date: 2026-11-02

## Scope

Stage 22 changes the client-side bounded stream adapter. Blocking application `Read` and `Write`
are moved out of the protocol/network owner so that ACK generation, receive processing,
retransmission, congestion scheduling and receive-window maintenance continue while local
application I/O blocks.

The shared multi-client UDP server path is intentionally unchanged. This stage is preparation for
a future TCP/Xray bridge; the current remote application remains echo-oriented.

## Toolchain

- Go: `go1.23.2 linux/amd64`
- Host kernel: Linux 6.18.57 x86_64
- Native runtime validation: Linux amd64
- Cross compilation: Windows amd64
- Current Go toolchain is outside supported upstream release branches; current `govulncheck` was
  not available in this environment. Produced binaries remain laboratory artifacts.

## Implementation checks

### Application-I/O isolation

New regression tests prove:

1. after one local TCP chunk is written, blocking the next application `Read` does not prevent the
   already queued data from being ACKed and echoed;
2. blocking local application `Write` does not stop server receive/ACK progress through the bounded
   active window;
3. a 512 KiB duplex exchange completes through a UDP relay with loss, delay, duplication, forged
   packets and a size filter;
4. cancelling an owned `ExchangeDuplex` connection closes the endpoint and unblocks otherwise idle
   blocking I/O.

`TestStage22*` was run three times and once under the race detector.

### Bounded ownership

- reader pump: one reusable `ChunkSize` buffer (16 KiB default);
- retained outbound application payload: max 64 KiB by default;
- max active transactions: 4 by default;
- writer pump receives existing `reliable.Received` ownership and does not copy the payload again;
- no per-DATA goroutine or timer is introduced;
- exactly two application pump goroutines are introduced per active client-side duplex endpoint.

The future server-side upstream model for thousands of Xray/TCP connections is **not** assumed to
use this exact per-endpoint pump design; it must be measured independently.

## Full test results

After the final Stage 22 changes:

| Check | Result |
|---|---|
| `gofmt -l` | PASS (no output) |
| `go build ./...` | PASS |
| `go test -p 1 -count=1 -timeout=180s ./...` | PASS |
| full `netreliable` package | PASS, 59.595 s |
| `go test -race -p 1 -count=1 -timeout=240s ./...` | PASS |
| full `netreliable` race package | PASS, 65.737 s |
| `go vet ./...` | PASS |
| `TestStage22* -count=3` | PASS |
| `TestStage22*` under race detector | PASS |
| Fuzz `reliable.Decode` | PASS, 3 s |
| Fuzz packet receipts | PASS, 3 s |
| Fuzz authenticated `secureudp.Open` path | PASS, 3 s |
| Linux amd64 builds (4 applications) | PASS |
| Windows amd64 cross-builds (4 applications) | PASS |

The current `netreliable` package contains 60 top-level tests (subtests additional).

Relevant logs are under `docs/logs/stage22-*`.

## Process-level duplex TCP smoke test

Final Linux binaries were used, not `go test` helpers:

1. `srdt-server --multi` listened on UDP;
2. `srdt-client --duplex-listen 127.0.0.1:0` accepted a real local TCP connection;
3. test application sent 1 MiB and used TCP `shutdown(SHUT_WR)`;
4. it read the complete echoed byte stream;
5. SHA-256 input/output matched exactly.

Result:

- sent: 1,048,576 bytes;
- received: 1,048,576 bytes;
- exact match: PASS;
- 66 outbound and 66 inbound logical chunks;
- peak active transactions: 4;
- peak queued outbound payload: 65,536 bytes.

See `docs/logs/stage22-duplex-cli-smoke.txt`.

## 100 MiB Stage 21 vs Stage 22 comparison

Both versions used their own pristine binaries and the same host/loopback scenario. Each client
streamed 100 MiB as 16 KiB chunks through the same echo semantics.

Final **interleaved** runs:

| Version | Runs (s) | Median |
|---|---|---:|
| Stage 21 | 3.7541, 4.1238, 4.0210 | 4.0210 s |
| Stage 22 | 3.9767, 4.0136, 3.7745 | 3.9767 s |

Stage 22 median is ~1.1% lower in this small sample, but the individual ranges overlap. No material
throughput improvement is claimed. More importantly, the final asynchronous implementation no
longer shows the ~5–10% slowdown observed before the explicit UDP-loop wakeup was added.

See:

- `docs/logs/stage22-stream-comparison.txt`
- `docs/logs/stage22-stream-comparison-interleaved.txt`

## Important findings during verification

### UDP event-loop wakeup was necessary

Simply moving application I/O into goroutines initially reduced throughput because the network
owner still waited on a 20 ms UDP read deadline before observing pump readiness. Stage 22 now wakes
that owner with `SetReadDeadline(time.Now())`. This keeps a single UDP reader and avoids polling or
a third client-side network goroutine.

### Monolithic test invocations are awkward in this execution harness

A full package test and full race test both completed successfully, but the surrounding tool call
can return its own timeout while the spawned `go test` process continues. The final results were
collected from the process logs/exit codes. `scripts/verify.sh` now uses explicit Go package
timeouts and full-package checks; it is suitable for a normal shell/CI environment.

One earlier grouped fault test invocation also showed a long tail, while the exact test and the
final complete package run both passed. This is retained as a reason to keep explicit package
timeouts rather than silently accepting unlimited test duration.

## Known limitations

- current remote application is still echo-oriented; arbitrary server-originated bytes are not
  implemented;
- reliable CLOSE / half-close signaling is not part of the SRDT wire protocol yet;
- `ExchangeStream` does not own arbitrary Reader/Writer endpoints; permanently blocking
  non-closable application objects can leave a pump blocked after return. `ExchangeDuplex` owns
  and closes a `ReadWriteCloser` and is the intended bridge-facing API;
- production key lifecycle/client authorization is not designed;
- no native Windows runtime test;
- no target 2 GiB node test together with Xray/sing-box;
- no long soak/operator WAN/TSPU validation;
- alternate carriers and 4–8 KiB UDP fragmentation experiments remain future work.

## Conclusion

Stage 22 fixes the main correctness problem of Stage 21 stream handling: local application I/O no
longer owns protocol progress. It retains bounded queues and passes fault/race/process tests. It is
now a suitable **client-side** foundation for the next step: independent bidirectional logical
stream semantics and a real server-side TCP/Xray bridge. It is not production-ready.
