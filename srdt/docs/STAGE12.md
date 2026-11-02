# Stage 12 — low-memory groundwork

This stage intentionally prioritizes correcting existing resource behavior before adding streaming or more protocol features.

## Changes

- Receiver now supports explicit per-session limits through `NewReceiverWithLimits`.
- `LowMemoryReceiverLimits` sets 8 active messages, 256 KiB unfinished payload reservation, and 128 completed IDs per receiver. The batch server now uses these limits.
- Batch server queue allocation ceiling is 1 MiB instead of `count * 8 MiB` (this is a per-session cap, not a global cap).
- Queue tracks outstanding distinct bytes incrementally instead of summing all senders on each scheduling iteration. Duplicate ACKs do not release bytes twice.
- Added regression tests for receiver budgets and queue accounting.

## Limitations / next steps

- Batch client still enqueues all messages before transmission and can allocate considerable memory; new server caps may reject oversized batches rather than stream them.
- No global process-wide memory budget, eviction/expiry policy, congestion control, streaming API, or multi-peer server yet.
- Sender retains whole messages until ACK; completed-ID replay cache eviction is still unsafe for unlimited session lifetimes.
- Existing experimental cryptographic handshake and server session replacement are not production safe.
- Linux tests and Windows cross-compilation were run; no native Windows execution or remote Windows-to-Ubuntu test.
- These defaults are experimental and require load tests alongside Xray and signbox.

See `stage12-test-output.txt` for actual commands/results.
