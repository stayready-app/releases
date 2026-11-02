# SRDT Stage 21 — build, test and resource report

Date: 2026-11-01. Toolchain available in this environment: `go1.23.2 linux/amd64`.
Stage 21 is a laboratory build and is **not production ready**.

## Verification approach

The `netreliable` package contains many real-UDP tests with deliberate delay/loss. A single
monolithic invocation can exceed the execution window of this environment without exposing
intermediate package output. For Stage 21 the suite was therefore executed in deterministic
functional groups. Every listed test belongs to exactly one of the group expressions below;
core packages were tested directly. Race runs use the same grouping.

Normal groups, all PASS:

- core: `secureudp`, `reliable`, `emulator`, `testkit`;
- `^(TestAudit|TestCongestion|TestCredits)`;
- `^(TestBatch|TestRealUDP|TestPacket)`;
- `^TestMulti`;
- `^(TestStage21|TestStage16WANComparison)`;
- `^TestWindow`.

Race detector, all PASS using the same split. After the final fault-harness correction, the
changed WAN/fault tests plus the new V1 HELLO/WELCOME exact-size tests were rerun under
`-race` and also PASS.

Additional checks:

| Check | Result |
|---|---|
| `go build ./...` | PASS |
| `go vet ./...` | PASS |
| gofmt clean | PASS |
| Stage 21 tests x3 | PASS |
| control-padding size-filter test x3 | PASS |
| V1 exact-first-HELLO-size filter | PASS |
| V1 exact-first-WELCOME-size filter | PASS |
| `FuzzDecode`, 3 s / 2 workers | PASS, ~119k execs |
| `FuzzPacketReceipt`, 3 s / 2 workers | PASS, ~8k execs |
| `FuzzAuditOpen`, 3 s / 2 workers | PASS, ~96k execs |
| Linux amd64 builds | PASS |
| Windows amd64 cross-builds | PASS |
| Stage 21 client -> Stage 20 server | PASS with explicit `--legacy-handshake` |
| Stage 20 client -> Stage 21 server | PASS with explicit server `--allow-legacy-handshake` |

The fault relay separately exercises loss, delay/reordering, duplication, corruption,
forged packets, selective size blackholes, lost WELCOME, reverse-path blackout and control
loss. The previous test named `ack-blackout` depended on old fixed ACK sizes and had become
ineffective after variable control padding. Stage 21 replaces it with a real reverse-path
blackout and logs direct relay drops.

## New V1 handshake tests

Two focused tests learn the exact wire size of the first V1 HELLO or WELCOME, drop that size,
and keep dropping later packets of exactly that size. Both connections recover because
retries use a different external size while keeping the authenticated handshake semantics and
session identity unchanged. Direct relay drops are observed in both tests.

This demonstrates recovery from those *specific* size filters. It is not a claim that a broad
filter or DPI classifier cannot identify/block the carrier.

## Bounded stream test: 100 MiB

One Stage 21 V1 session streamed 100 MiB through `ExchangeStream` using 16 KiB chunks,
64 KiB application buffering and four active transactions. The server echoed the bytes and
the client verified the deterministic byte sequence.

| Metric | Result |
|---|---:|
| One-direction verified bytes | 104,857,600 |
| Logical chunks | 6,400 |
| Wall time | 4.023 s |
| Server CPU | 2.98 s |
| Sampled peak server RSS | 9,788 KiB |
| Peak accounted payload/bitmap/range budget | 90,112 B |
| Peak packet-state budget | 8,192 B |
| Resource drops | 0 |

This is Linux loopback, not the target 2-GB VPS and not an Xray bridge.

## 1000-session active process test

1000 sessions each transferred 16 x 4096-byte request messages and their echo responses.

| Metric | Result |
|---|---:|
| Verified request bytes | 65,536,000 |
| Successful clients | 1000 / 1000 |
| Wall time | 7.261 s |
| Server CPU | 2.37 s |
| Sampled peak server RSS | 15,532 KiB |
| Peak accounted payload budget | 4,939,264 B |
| Peak packet-state budget | 983,040 B |
| Resource drops | 0 |

The test proves bounded behavior for this specific workload only. It does not mean all 1000
sessions simultaneously hold their maximum 64-KiB per-session allowance.

## 1000-session idle process test

1000 sessions performed a one-byte verified exchange and then held their sockets open for a
short idle period.

- sampled peak server RSS: 11,964 KiB;
- server CPU: 0.32 s over the workload;
- peak sessions: 1000;
- resource drops: 0.

The in-process manager heap measurement was ~2461 retained bytes/session for Stage 21 versus
~2413 bytes/session for Stage 20 in back-to-back runs. This ~48-byte increase is the measured
cost of additional V1/capability state in that test; it is not RSS.

## Stage 20 vs Stage 21 A/B

Three alternating runs per version were performed with the same compiler and workload:
200 clients, 128 messages/client, 4096 bytes/message = 100 MiB request data plus echo.
All clients completed and verified every response.

Median results:

| Metric | Stage 20 | Stage 21 | Delta |
|---|---:|---:|---:|
| Wall time | 4.372 s | 4.977 s | +13.8% |
| Server CPU | 4.11 s | 4.25 s | +3.4% |
| Sampled peak RSS | 21,024 KiB | 21,136 KiB | +0.5% |
| Server sent datagrams | 329,094 | 331,828 | +0.8% |
| Server DATA retries | 52,395 | 54,555 | +4.1% |

Wall time is noisy: individual runs for both versions contained multi-second tail variation.
The small CPU/RSS/datagram regressions are more credible than treating the wall-time delta as
a universal throughput regression. Stage 21 prioritizes authenticated negotiation and a
bounded stream interface in this iteration; these costs must not be ignored on weak nodes.

## Compatibility

Compatibility is explicit, not timeout-driven:

- Stage 21 client -> Stage 20 server: `--legacy-handshake`, PASS.
- Stage 20 client -> Stage 21 server: Stage 21 server `--allow-legacy-handshake`, PASS.
- Stage 21 server default rejects fixed Stage 20 HELLO.
- Streaming requires V1 and the streaming capability; legacy handshake is rejected by the
  streaming API.

## Important limitations

1. The crypto/key provisioning design is still experimental and unaudited.
2. `ExchangeStream` is echo-oriented and single-owner; a permanently blocking `io.Reader`
   can stall network progress. It is not yet a generic full-duplex Xray bridge.
3. Control/handshake size variation is traffic shaping, not evidence of DPI/TSPU resistance.
4. No native Windows run, 2-GB target node + Xray run, long soak, operator WAN or TSPU test.
5. No reliable CLOSE, durable resumption, NAT migration or production authorization/rotation.
6. Current UDP DATA cap remains 1200 B; 4–8 KiB IP-fragmentation experiments are not yet
   implemented.
7. The available Go 1.23.2 toolchain is out of current support; updated toolchain and current
   `govulncheck` were unavailable in this environment.

Raw logs and process result JSON are under `docs/logs/`, `docs/stage21-ab/`,
`docs/stage21-stream-100m/`, `docs/stage21-active1000/`, `docs/stage21-idle1000/` and
`docs/stage21-compat/`.
