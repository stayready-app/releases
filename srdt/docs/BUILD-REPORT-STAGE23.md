# SRDT Stage 23 — build, correctness, bridge and scaling report

Date: 2026-11-02

## Scope

Stage 23 replaces the echo-only remote application semantics with two independent logical streams and
adds a fixed TCP upstream bridge. It also contains two correctness fixes discovered by real concurrent
bridge load and one shared-socket tuning change required for burst traffic.

## Toolchain / host

- Go: go1.23.2 linux/amd64
- Native runtime validation: Linux amd64
- Windows: cross-compiled amd64 only
- Current Go toolchain is not suitable as a release baseline; current `govulncheck` was unavailable.

## Correctness fixes found during Stage 23

### Same-session pending upstream read was never retried

When incoming client data consumed the session budget and an upstream TCP read completed concurrently,
the read was retained. After client data was written upstream and its storage released, the retained
read was not retried. Ten concurrent bridges were sufficient to reproduce clients that wrote all data
but received zero echo.

The write completion path now retries the pending upstream read immediately. Regression:
`TestStage23PendingUpstreamReadResumesAfterInboundWrite`.

### Global buffer-budget release did not wake another bridge session

At 1000 active clients, BufferBudget can reach the configured 64 MiB. A server->client read in session
A could wait for global memory owned by session B, but release in B did not wake A. This produced a
long tail/starvation under pressure.

Bridge output blocked specifically by global budget now joins the bounded fair waiter queue. A storage
release in any session can advance one waiter without an O(N) full-session scan.

### Full-duplex session budget could starve the reverse direction

A long single bridge exposed a more serious full-duplex issue that short tests did not show.
Client->server receive reservations could occupy the whole 64 KiB per-session payload budget. The
upstream reader then had no room to enqueue server->client data, so the reverse stream made almost no
progress and the session eventually hit the idle timeout.

Stage 23 now keeps a bounded directional share inside the same combined per-session/global budgets.
With the default 64 KiB session cap and 16 KiB stream chunks, each direction can retain at most 32 KiB
of payload while both directions are active. This does **not** preallocate another buffer and does not
increase BufferBudget. The long regression `TestStage23LongSingleBridgeNoDirectionalStarvation` keeps
this behavior covered.

A process-level single bridge then transferred 104,857,600 bytes client->upstream and the same amount
upstream->client in **3.851 s**, with exact byte verification, `ResourceDrops=0`, no DATA retries and
no session expiry. Server metrics report 104,857,600 upstream bytes read and written.

### FIN cannot precede already observed DATA

FIN is directional and carries the last logical message ID. A lower FIN arriving after a higher DATA
ID is inconsistent even if the frame is authenticated. Both endpoints now reject/reset that state
instead of half-closing while a later chunk is already buffered. Regression:
`TestStage23RejectsFinBeforeObservedData`.

### Shared UDP socket was too small for high-concurrency bursts

The host default receive buffer was roughly 208 KiB. With 1000 clients this caused avoidable packet
loss and retransmission storms before protocol congestion control could react. Stage 23 requests one
shared 2 MiB receive buffer and 1 MiB send buffer. This is global kernel memory, not per-client state.

## Test results

After the Stage 23 protocol/server changes the split normal netreliable groups passed:

- `^TestAudit`
- `^Test(Congestion|Credits)`
- `^Test(Batch|RealUDP|Packet|Multi)`
- `^TestStage(21|22|23)`
- `^Test(Stage16WAN|Window)`

The same five groups also passed under the race detector. Core `reliable`, `secureudp`, `testkit` and
`emulator` packages passed under `-race`.

After the final load-tool cleanup:

- `gofmt`: PASS
- `go build ./...`: PASS
- `go vet ./...`: PASS
- `TestStage23* -count=3`: PASS
- `TestStage23*` under race: PASS
- core race packages: PASS

The final netreliable package has 67 top-level tests, including the long-directional-starvation and FIN-consistency regressions. The verification script deliberately uses
independent groups instead of one monolithic network test process because this execution harness has
previously timed out while an underlying `go test` process continued running.

Fuzz runs already completed on the Stage 23 source:

- `reliable.FuzzDecode`: PASS, 3 s
- packet receipt fuzz: PASS, 3 s
- authenticated `secureudp.Open` fuzz path: PASS, 3 s

## Process-level fixed-upstream bridge smoke

Separate processes:

1. `srdt-upstream-echo` TCP service;
2. `srdt-server --multi --upstream <echo>`;
3. `srdt-client --bridge-listen`;
4. local test application.

Result:

- sent: 1,048,576 bytes;
- received: 1,048,576 bytes;
- exact match: PASS;
- SHA-256 input/output:
  `1a0a2e9fbff171a4933522c4335912a2e2d245148912e6e57d1085cd6023ac71`;
- server upstream read bytes: 1,048,576;
- server upstream written bytes: 1,048,576;
- `ResourceDrops = 0`.

## 1000 active bridge process test

The load generator, SRDT server and upstream echo are separate processes. Server was run with
`GOMAXPROCS=2` and `GOMEMLIMIT=128MiB`.

Workload:

- 1000 concurrent independent-stream clients;
- 16 messages * 4096 bytes = 65,536 bytes per client;
- 65,536,000 verified bytes client->server/upstream;
- same byte volume returned upstream->server->client;
- TCP half-close verified per client.

Result:

| Metric | Result |
|---|---:|
| clients | 1000 / 1000 PASS |
| load wall time | 7.589 s |
| sampled SRDT server CPU | 3.09 s |
| sampled SRDT server peak RSS | 29,888 KiB (~29.2 MiB) |
| peak BufferBudget accounting | 9,371,648 bytes |
| peak PacketStateBudget accounting | 1,294,336 bytes |
| bridge upstream read/written | 65,536,000 / 65,536,000 bytes |
| ResourceDrops | 0 |
| peak global waiters | 86 |
| DATA retries | 932 |
| timeout losses | 562 |
| threshold losses | 120 |

An earlier intermediate high-pressure run saturated the 64 MiB budget and exposed the cross-session
starvation bug fixed in Stage 23. The final 1000-client run above peaks at about 9.37 MiB of accounted
message storage, with no protocol resource drops.

These figures are short Linux loopback measurements. They do not include the load generator or
upstream process, but they also do not include exact kernel socket-memory accounting. They are not a
production capacity guarantee for a 2 GiB StayReady node running Xray/sign-box.

## Resource model relevant to weak servers

- one shared SRDT UDP socket/event loop;
- no per-DATA goroutine or timer;
- two blocking upstream TCP I/O goroutines per active bridge;
- bounded concurrent dials (16 default);
- 16 KiB requested upstream TCP read/write buffers per connection;
- one shared 2 MiB UDP receive and 1 MiB UDP send buffer;
- 64 MiB default global message-buffer budget;
- 16 MiB default packet-state budget;
- 64 KiB per-session combined message payload cap;
- 8 active logical messages per session.

The TCP/UDP socket buffer requests are kernel resources, not exact RSS limits.

## Known validation gaps

- no native Windows runtime execution;
- no target 2 GiB VPS test together with real Xray/sign-box;
- no multi-hour soak;
- no operator WAN/TSPU run;
- no production cryptographic/key lifecycle audit;
- no kernel socket-memory accounting;
- no current `govulncheck` on a supported Go toolchain.

## Verification harness note

A single invocation of `scripts/verify.sh` was also attempted in this execution environment. The
outer tool wrapper terminated that long-running shell while the second network group was starting.
This is recorded as `docs/logs/stage23-final-verify-wrapper-timeout.txt` and is **not** counted as a
PASS. The exact verification groups were then run independently and all completed successfully; the
`stage23-final-*` group logs are the authoritative results. In a normal CI/shell environment the
script runs the same grouped commands sequentially.
