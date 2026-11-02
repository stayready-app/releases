# Stage 10 — receiver limits and ACK validation

This is a focused hardening stage, **not** the planned continuous UDP transport stage.

Changes:
- Sender rejects acknowledgments for bytes that have never been transmitted.
- Receiver caps simultaneous incomplete message allocation at 32 MiB (in addition to 64 active messages).
- Receiver caps completed-message deduplication cache at 4096 IDs.
- Regression tests cover all three behaviors.

## Important limitations

Completed-message cache eviction makes old message IDs eligible for delivery again if the sender reuses or replays an evicted ID in the same authenticated session. Anti-replay of physical encrypted packets alone does not prevent a previously authenticated sender from issuing a new packet with a retired logical ID. The protocol must enforce monotonic message IDs with an appropriate receive window or retain session-scoped replay state before production deployment.

The 32 MiB active cap does not bound all process memory, and does not include outgoing queues or cryptographic state. The existing network batch API remains a fixed-count experimental interface; CLI still does not use it. There is no congestion control or adaptive RTO. Do not expose the server publicly.

See `stage10-test-output.txt` for actual command results.
