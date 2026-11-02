# SRDT Stage 23 — security and production limitations

SRDT is an experimental transport. Successful tests do **not** make it a production security
protocol or prove resistance to a specific DPI/TSPU deployment.

## Blocking issues before production

1. **Handshake/key provisioning remain experimental.** There is no audited per-client production
   authentication, revocation, durable server identity/epoch rotation or external cryptographic
   review.
2. **DoS/path validation is incomplete.** Admission, rate and memory limits are bounded, but they are
   not a complete anti-DDoS design.
3. **Congestion/loss control is heuristic.** Fairness relative to TCP/QUIC and progress under an
   arbitrary active filter are not proven.
4. **Traffic shaping is not protocol impersonation.** Direction, timing, volume, server IP and many
   other features remain observable. There has been no real TSPU/DPI validation.
5. **Budgets are not process/cgroup limits.** Go heap/RSS, goroutine stacks and kernel TCP/UDP socket
   buffers exist outside BufferBudget/PacketStateBudget. Target 2 GiB node tests with Xray are still
   mandatory.
6. **No durable session recovery.** Exactly-once/replay protections are bounded to the live process
   and cryptographic lifetime; crash recovery, durable resumption and NAT migration are absent.
7. **STREAM_RESET is best effort.** FIN has acknowledgement/retry semantics; reset currently does
   not. A lost reset may degrade into timeout.
8. **No keepalive/path liveness.** Default server idle timeout is 60 s. A silent but valid long-lived
   TCP bridge can expire unless production integration adds liveness or a justified longer timeout.
9. **Fixed upstream only.** The remote client cannot choose a destination, which is intentional for
   the current lab bridge. There is no generic SOCKS/TUN/Xray control protocol yet.
10. **Server upstream concurrency model needs target validation.** Each active TCP upstream bridge
    uses two blocking Go goroutines. A short 1000-active test passes, but multi-hour behavior and
    scheduler/stack cost on weak CPUs are not established.
11. **Kernel socket memory is separately significant.** Upstream TCP buffers are requested at 16 KiB
    per direction and the single UDP socket at 2 MiB receive / 1 MiB send. Actual Linux kernel
    allocation/accounting can exceed requested values and was not measured here.
12. **No native Windows runtime, real 2 GiB StayReady+Xray run, soak, operator WAN or TSPU test.**
13. **Toolchain:** current environment uses Go 1.23.2. A supported current toolchain and current
    `govulncheck` must be used before release.
14. UDP DATA remains capped by the current carrier policy; discussed 4–8 KiB fragmentation profiles
    and alternate QUIC/HTTP/WebSocket carriers are not implemented yet.

## Stage 23 stream controls

`STREAM_OPEN`, `STREAM_FIN`, `STREAM_FIN_ACK` and `STREAM_RESET` are carried inside the authenticated
SRDT session. A forged unauthenticated packet cannot alter stream state. FIN direction, ID parity and
ordering are validated.

A repeated/invalid control frame must not allocate unbounded state or extend a session indefinitely.
The independent-stream capability is negotiated explicitly; old echo traffic never silently gains
bridge semantics.

## Fixed TCP upstream boundary

The server is configured with one fixed upstream address. It does not trust the client to supply an
IP/port. This avoids accidentally turning the laboratory server into an arbitrary open proxy while
the authentication model is incomplete.

Upstream dial failures and stream errors are reported to the peer through the experimental reset/error
path where possible, then bounded session cleanup applies.

## Directional buffer invariant

The combined per-session payload limit is also partitioned fairly between active bridge directions.
One direction must not consume all session payload storage and prevent the reverse TCP stream from
making progress. This is a liveness/resource invariant, not extra reserved memory.

Authenticated FIN is checked against the highest DATA ID already observed in that direction. A FIN
that precedes buffered DATA is rejected/reset rather than silently truncating the stream.

## Resource ownership

Queue/Receiver/session state remains single-owner and is not advertised concurrent-safe. Blocking TCP
I/O is isolated in bounded pumps. Payload ownership is transferred rather than copied where possible;
all retained payload must remain covered by the global/per-session budget until released.
