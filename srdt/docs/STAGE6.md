# Stage 6 — range-based transmission timestamps

## Implemented

- Removed one `time.Time` per payload byte from `reliable.Sender`.
- Added transmission-range records (`start`, `end`, `at`) and split-on-retry behavior.
- Retained per-byte acknowledgment bitmaps and wire frame compatibility.
- Added deterministic 100 MiB logical transfer test (25 × 4 MiB messages) with systematic loss of 4096-byte initial frames, retransmitted in 768-byte frames, SHA-256 checked per message.
- All standard Go tests and race detector tests pass. Linux and Windows amd64 binaries cross-built.

## Scope and limitations

- 100 MiB result is a **simulated logical transfer**, not a real UDP throughput test.
- No multi-message network-session API, congestion control, RTT-derived RTO, bounded in-flight window, or external network emulator integration yet.
- Retry selection scans transmission ranges; ACK still updates bitmap byte by byte. Performance and memory bounds need further engineering.
- Existing single-session crypto handshake limitations remain. Not production safe.
- Windows binaries were cross-compiled, not executed on Windows.

## Reproduce

```sh
go test ./reliable -run TestHundredMiBRangeRecovery -count=1 -v
go test -race ./...
go vet ./...
```

See `docs/stage6-test-output.txt` for full suite results.
