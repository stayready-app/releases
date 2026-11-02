# Stage 5 — incremental reliability improvements (partial)

This checkpoint is **not** the completed long-transfer milestone.

## Implemented
- Sender ACK bitmap is now packed in uint64 words, reducing ACK storage approximately 8x compared to `[]bool` (Go bool occupies one byte).
- Sender tracks the count of newly acknowledged bytes, eliminating the full message scan after each ACK; duplicate ACKs do not increase the count.
- Receiver presence bitmap is packed in uint64 words. The receiver still retains payload bytes and completed message IDs for the lifetime of the receiver.
- Deterministic 8 MiB unit test uses eight independent 1 MiB logical messages, selectively drops all initial 4096-byte frames, retransmits as 768-byte frames, and checks exact contents and SHA-256.

## Verification
- `go build ./...`: PASS
- `go test -count=1 ./...`: PASS, large test ~11 seconds.
- `go vet ./...`: PASS
- `go test -race -skip TestEightMiBDeterministic ./...`: PASS.
- Full race suite including large test: NOT COMPLETED (timed out; do not count as pass).
- Linux amd64 build and Windows amd64 cross-build: PASS. Windows executable not run on Windows.
- 100 MiB integration test: NOT COMPLETED. A first 100 MiB in-memory test proved too slow and was reduced to 8 MiB.

## Remaining blockers
- Real UDP multi-message streaming, 100 MiB end-to-end verification, RTT/RTO, congestion control, ACK batching, selective-size filter wired to the full network emulator, robust multi-session lifecycle, and cryptographic protocol review.
- Current receiver has unbounded completed-ID retention and sender retains per-byte send timestamps: not suitable for sustained streams.
- Current real UDP server still handles only one echo message and then exits.
- Do not deploy publicly.
