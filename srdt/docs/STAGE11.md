# Stage 11 — Batch CLI integration (partial milestone)

This stage connects the pre-existing `netreliable.ExchangeBatch` and `ServeBatch` library functions to the Windows/Linux command-line tools. It does **not** implement a persistent unbounded stream, adaptive RTO, congestion control, or 100 MiB real-network transfer.

## Usage

Run server with `--batch 12 --psk <64 hex chars> --listen 127.0.0.1:19443`. Copy `SERVER_PUBLIC` from its output. Run client with `--batch 12 --bytes 4096 --psk <same PSK> --server-public <hex> --server 127.0.0.1:19443`. Both peers must use the same batch count. `--timeout` sets a per-run timeout. Optional `--drop-min 800 --drop-max 1100` on both sides simulates size-based loss.

## Verified in this environment

- `go build ./...`, `go test -count=1 ./...`, `go test -race -count=1 ./...`, `go vet ./...` passed.
- Windows amd64 client and server cross-compiled; not executed on Windows.
- Linux localhost real UDP CLI round trip: 12 messages x 4096 bytes, exact echo comparison passed.
- Linux localhost size-filter CLI test: 12 messages x 4096 bytes, encrypted datagrams in [800,1100] dropped on both sides, passed.
- See `stage11-test-output.txt` and `stage11-cli-smoke.txt`.

## Outstanding risks

Experimental crypto handshake, one active peer, no congestion control, batch-only session, no sustained 100 MiB UDP transfer, no external network impairment emulator, and no replay-safe session restart. Do not expose this service publicly. Batch server terminates after completing the expected number of messages; it is not a long-running multi-client server.
