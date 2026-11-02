# Stage 7 — bounded sender in-flight data (partial milestone)

## Delivered

- Added `Sender.SetWindow(bytes)` to cap distinct transmitted-but-unacknowledged payload bytes.
- Added `Sender.InFlight()` for diagnostics and tests.
- The initial send path stops at the window boundary; successful ACKs reopen the window.
- Retransmissions of previously sent bytes remain possible while the window is full.
- Duplicate ACKs cannot reopen the window more than once.
- Added unit tests for window boundaries, retransmission under a full window, duplicate ACKs, and full bounded delivery.
- No wire-format changes; legacy senders keep unlimited in-flight data until configured.

## Important limitations

- This is **flow-window plumbing**, NOT congestion control. It is not an RTT-based congestion window, pacing, or a receive-advertised window.
- The network `netreliable` package has not yet been switched to the window API; it still sends one logical message per session. There is no 100 MiB real UDP benchmark.
- The sender still copies the entire logical message into memory and uses byte-granularity ACK bitmaps; MaxMessageSize is 8 MiB.
- No multi-message network session, adaptive RTO, external socket-level emulator, or production-safe session authentication yet.
- Existing cryptographic and single-session limitations remain. Do not expose the server publicly.
- Windows binaries were cross-compiled but not executed on Windows.

## Next steps

1. Build multi-message streaming queues with a bounded total memory budget and fair scheduling.
2. Integrate sender flow limits into the real UDP session.
3. Add RTT estimation, RTO backoff and pacing/congestion control.
4. Add an independent UDP emulator/proxy and real-socket 100 MiB integrity tests.
5. Address session replay, key management and multi-client isolation before public deployment.

See `stage7-test-output.txt` for executed checks.
