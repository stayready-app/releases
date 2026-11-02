# Stage 9 — multiple messages over a single real UDP session

## Implemented

- `netreliable.ServeBatch`: fixed-size batch of independently identified logical requests in one authenticated UDP session; echoes responses using distinct IDs.
- `netreliable.ExchangeBatch`: queues several messages, processes acknowledgments and response fragments concurrently, verifies exact echoed payloads.
- Shared 64 KiB in-flight window and bounded queue supplied by existing `reliable.Queue`.
- Deterministic size-based outgoing drop test, including retransmission with smaller fragments.
- Two real loopback UDP integration test scenarios, 12 messages each.

## Validation

See `stage9-test-output.txt`. Linux build, full unit tests, vet, race tests of `netreliable` and `reliable`, cross-compilation of Windows client and Linux server all passed.

## Known limitations / next stage

- Batch APIs are library functions; existing CLI still uses one-message echo. CLI integration is next.
- `ServeBatch` serves one client and terminates after the fixed batch; no production multi-session server.
- Queue memory bound is a bound on submitted message bytes, not a complete bound on process memory.
- Receiver retains all completed IDs indefinitely and must be bounded before long-running operation.
- No adaptive RTT/RTO or congestion control; current fixed RTO and polling are unsuitable for production.
- ACK validation against unsent bytes requires hardening.
- Session establishment replay resistance and cryptographic review remain outstanding. Do not deploy publicly.
- No 100 MiB real-socket test yet; Windows binary was cross-compiled, not executed on Windows.
