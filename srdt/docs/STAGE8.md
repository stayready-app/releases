# Stage 8: multi-message queue (partial)

## Implemented

- `reliable.Queue`: multiple message senders with independent IDs and round-robin scheduling.
- Shared outstanding-byte budget (`maxInFlight`) across all messages.
- Bounded queued payload bytes (`maxQueued`), duplicate ID rejection, completed-message eviction.
- Expired retransmissions are allowed even when the shared window is full (`Sender.NextRetry`).
- Tests for selective loss, repackaging, integrity, shared window, queue limits and invalid ACK.

## Verification

See `docs/stage8-test-output.txt`. Go 1.23.2 Linux amd64. `go build`, `go test`, `go test -race`, `go vet`, Linux/Windows cross-build passed. Windows binaries were not executed on Windows.

## Not yet implemented

The queue is not yet connected to `netreliable` UDP applications: those still exchange one message. No sustained real-socket 100 MiB test, adaptive RTO, congestion control, or independent external network emulator. Existing `secureudp` handshake and session-lifecycle limitations remain. The receiver retains completed IDs and delivered data without bounded eviction; avoid untrusted/public deployment.

## Design caution

`Queue` is not goroutine-safe; its owner must serialize operations. `maxQueued` bounds queued payload bytes, not all memory allocations or receiver memory. Acknowledgment validation is inherited from `Sender` and needs strengthening against ACKs for never-transmitted ranges. Window accounting is logical outstanding bytes, not a congestion-control algorithm. `NextRetry` may scan expired ranges; scalability must be benchmarked.

## Next milestone

Wire `Queue` into a persistent bidirectional UDP session, cap receiver memory and completion history, then test sustained multi-message delivery with a real-socket emulator and integrity checking.
