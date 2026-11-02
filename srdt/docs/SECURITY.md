# SRDT Stage 24 — security and production limitations

SRDT is experimental. Passing tests does **not** make it an audited security protocol or prove
resistance to a specific DPI/TSPU deployment.

## Blocking issues before production

1. **Handshake/key provisioning remain experimental.** There is no audited per-client production
   authentication, revocation, durable server identity/epoch rotation or external cryptographic
   review.
2. **DoS/path validation is incomplete.** Admission/rate/memory limits are bounded, but this is not
   a complete anti-DDoS design.
3. **Congestion/loss control is heuristic.** Fairness against TCP/QUIC and behavior under arbitrary
   active filtering are not proven.
4. **Traffic shaping is not protocol impersonation.** Variable sizes do not hide direction, timing,
   volume, server IP or all flow statistics. No real TSPU/DPI validation has been completed.
5. **Budgets are not process/cgroup limits.** Go heap/RSS, goroutine stacks and kernel TCP/UDP socket
   memory remain outside BufferBudget/PacketStateBudget.
6. **No durable session recovery or migration.** Crash recovery, durable resumption, NAT rebinding,
   endpoint migration and multi-path recovery are absent.
7. **STREAM_RESET remains best-effort.** Stage 24 uses it to release server resources after an
   asymmetric liveness failure, but a lost reset still degrades to server timeout cleanup.
8. **Path liveness is connection-health detection, not path authentication/migration.** PING/PONG are
   authenticated inside the live session but do not validate a new network address or authorize
   migration.
9. **Fixed upstream only.** The remote client cannot choose arbitrary IP/port. There is no generic
   SOCKS/TUN/Xray control protocol yet.
10. **Two blocking Go goroutines per active upstream TCP bridge** still require multi-hour target-node
    validation on weak CPUs.
11. **Kernel socket memory is separately significant.** Requested buffer sizes are not identical to
    actual Linux kernel accounting. It must be measured on the target node.
12. **No native Windows runtime, real 2 GiB StayReady+Xray soak, operator WAN or TSPU test.**
13. **Toolchain:** the available environment uses Go 1.23.2. A supported current Go version and
    current `govulncheck` are required before release.
14. UDP DATA carrier still uses current packet-size policy; discussed 4–8 KiB fragmentation profiles
    and alternate QUIC/HTTP/WebSocket carriers are not implemented.

## Stage 24 liveness controls

`PATH_PING/PATH_PONG` are AEAD-authenticated and use variable control padding. Forged unauthenticated
packets cannot keep a session alive. A valid inbound authenticated frame updates receive-liveness;
repeated invalid packets do not.

The client is currently the liveness initiator. Server PONG is bounded control work and does not
allocate application payload state. Stable per-session keepalive jitter reduces timer synchronization.

The default interval/timeout are 15 s / 45 s. Tests with 500 ms / 2 s are deliberately aggressive
stress conditions, not recommended production defaults.

## Completed bridge cleanup

Normal full-duplex completion enters a 1-second bounded drain for duplicate FIN/ACK recovery. This is
much shorter than the normal idle lifetime but deliberately non-zero for lossy/high-latency paths.
Drain state is still bounded by session-count and resource budgets.

## Fixed upstream security boundary

Server upstream address is configured locally; client cannot supply a destination. This prevents the
laboratory bridge from becoming an arbitrary open proxy while authentication is incomplete.

## Resource ownership

Queue/Receiver/session state is single-owner and not advertised concurrent-safe. Blocking TCP I/O is
isolated in bounded pumps. Payload ownership is transferred rather than copied where possible; all
retained payload remains accounted until release. Liveness adds no per-packet/session goroutine or
unbounded history.
