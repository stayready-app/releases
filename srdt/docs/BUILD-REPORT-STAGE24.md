# SRDT Stage 24 — build, correctness, liveness and scaling report

Date: 2026-11-02  
Toolchain available in this environment: `go1.23.2 linux/amd64`

Stage 24 adds authenticated path liveness to the Stage 23 independent TCP bridge and changes normal
bridge cleanup so completed sessions are not retained for the full idle lifetime.

The project remains **experimental and not production ready**.

## Correctness changes

### Silent valid bridges no longer expire as idle

Modern V1 independent-stream sessions negotiate `path_liveness`. A client sends authenticated
`PATH_PING` only after otherwise silent traffic; the server replies with authenticated `PATH_PONG`.
Any valid authenticated inbound frame refreshes receive-liveness, so active DATA/ACK traffic does not
create keepalive traffic.

The modern bridge no longer applies one absolute client `Options.Timeout` to its whole lifetime.
That timeout remains setup/handshake/open control. Runtime termination uses application half-close,
context cancellation, explicit/reset semantics and path-liveness.

### Asymmetric dead path releases upstream sooner

When server->client traffic is blackholed but client->server still works, the client detects liveness
failure and sends a best-effort authenticated `STREAM_RESET`. The server then closes/removes the
bridge instead of retaining its upstream TCP until the normal 60 s idle timeout.

If both directions are dead, the reset cannot arrive and server timeout cleanup is still required.

### Normal completion has bounded drain

A clean bridge enters a short drain for duplicate FIN/ACK handling. Final default:

```text
BridgeDrainTimeout = 1 s
```

After the drain it is counted as completed and removed. It is not counted as an expired session.

### Variable keepalive control sizes

`PATH_PING/PATH_PONG` use V1 authenticated control padding. Regression tests require multiple wire
sizes from repeated logically equivalent controls. This prevents only a trivial single-size rule; it
is not proof of DPI resistance.

## Final unit / regression verification

The wrapper `scripts/verify.sh` is deliberately split into bounded groups. A single wrapper invocation
in this harness reached the external command window after successfully finishing build/vet/core/audit;
the remaining groups were then run independently with their own exit status.

Final source after all changes passed:

| Check | Result |
|---|---|
| gofmt | PASS |
| `go build ./...` | PASS |
| `go vet ./...` | PASS |
| core tests: reliable/secureudp/testkit/emulator | PASS |
| netreliable Audit group | PASS |
| Congestion/Credits group | PASS |
| Batch/RealUDP/Packet/Multi group | PASS |
| Stage21/22/23/24 group | PASS |
| Window/Stage16WAN group | PASS |
| core race tests | PASS |
| Audit race group | PASS |
| Congestion/Credits race group | PASS |
| transport race group | PASS |
| Stage21/22/23/24 race group | PASS |
| Window/WAN race group | PASS |
| Stage23 regressions, 3 repeats | PASS |
| Stage24 regressions, 3 repeats | PASS |
| `FuzzDecode`, 3 s / 2 workers | PASS |
| `FuzzPacketReceipt`, 3 s / 2 workers | PASS |
| `FuzzAuditOpen`, 3 s / 2 workers | PASS |

Detailed outputs are under `docs/logs/stage24-final/`.

## Stage 24 liveness regression coverage

Stage 24 tests exercise:

- path-control encode/decode validation;
- completely silent bridge surviving multiple server idle lifetimes;
- dead-server/path detection by the client;
- explicit legacy-liveness negotiation only;
- asymmetric server->client blackout followed by authenticated client reset;
- normal completion through short drain rather than idle expiration;
- exact Stage 23 capability compatibility path;
- PING/PONG encrypted wire-size variation.

Fault relay remains used by the existing suite for loss, delay, duplication, corruption/forgery and
size filters.

## Process test: 1000 silent real TCP bridges

This final run was made after the 1 s drain default was applied.

Configuration:

```text
clients                  1000
mode                     bridge-idle
application hold            4 s
server idle timeout          2 s
keepalive interval         500 ms
path timeout                 2 s
GOMAXPROCS                   2
GOMEMLIMIT              128 MiB
```

The keepalive interval is intentionally **30 times more frequent** than the 15 s default to stress
scheduler/control processing.

Result:

```text
clients passed                  1000 / 1000
workload reported time          5.718 s
sampled SRDT server CPU         1.31 s
peak sampled SRDT server RSS   30,812 KiB
PATH_PING received              7484
PATH_PONG sent                  7484
peak BufferBudget bytes         1556476
peak PacketStateBudget bytes    1810432
ResourceDrops                   0
CompletedBridgeSessions         1000
ExpiredSessions                 0
active_sessions after drain     0
```

This includes 1000 real local upstream TCP connections. Kernel socket memory is not included in the
SRDT BufferBudget and cannot be inferred from Go RSS alone.

## Process test: 100 MiB full-duplex active bridge

One real fixed-upstream bridge, 6400 chunks x 16 KiB:

```text
client -> upstream          104,857,600 B
upstream -> client          104,857,600 B
verification                byte-for-byte PASS
workload reported time           5.796 s
sampled SRDT server CPU          5.00 s
peak sampled SRDT server RSS     9,644 KiB
peak BufferBudget bytes          90112
peak PacketStateBudget bytes     8192
ResourceDrops                    0
DATA retries                     0
PATH_PING/PONG                    0/0
```

Active traffic suppresses keepalive as intended.

### Performance interpretation

Historical Stage 23 numbers collected earlier in the conversation differ strongly from a fresh Stage
23 rerun on the current host. Therefore Stage 24 does **not** claim a throughput improvement relative
to Stage 23 from these isolated loopback measurements. The acceptance criterion here is no liveness
regression, bounded resource use and correctness under silence/failure.

## Diagnostic failures retained conceptually

During Stage 24 development very aggressive sub-second liveness settings under 1000-client single-host
stress caused false client timeouts. This drove two changes:

1. elimination of unnecessary 20 ms polling during true idle;
2. realistic production defaults of 15 s / 45 s, while retaining an aggressive 500 ms / 2 s stress
   case that passes 1000/1000.

A temporary unconditional 500 ms network-owner sleep also caused a large active-stream regression.
The final implementation is hybrid: long sleep only for true idle, short responsiveness while real
protocol work exists.

These intermediate failures are not treated as successful results.

## Cross-platform build

Final release build creates five tools for Linux/amd64 and Windows/amd64:

- `srdt-server`
- `srdt-client`
- `srdt-load`
- `srdt-test`
- `srdt-upstream-echo`

Windows builds are cross-compiled only; no native Windows runtime test has been performed.

## Remaining production blockers

See [SECURITY.md](SECURITY.md). Highest-priority items remain:

- production cryptographic identity/auth/key rotation and review;
- reliable reset / session recovery semantics;
- target 2 GiB StayReady node test together with Xray/sign-box;
- kernel TCP/UDP socket-memory measurement;
- native Windows execution and soak;
- real WAN/operator/TSPU tests;
- NAT rebinding/path migration;
- supported current Go toolchain plus current vulnerability scan.

## Final binary smoke

After the final Linux/Windows rebuild, `bin/srdt-test-linux-amd64 --messages 1000 --seed 42`
completed with `Status=PASS`, `Verified=1000`, `Invalid=0`. All five Windows/amd64 binaries were
cross-built successfully but were not executed natively.
