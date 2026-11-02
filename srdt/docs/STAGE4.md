# Stage 4 — real UDP reliability integration

## Implemented
- `netreliable` integrates the existing `secureudp` session and `reliable` framing with actual UDP sockets.
- Full logical message transfer in both directions, per-fragment ACK, timeout-driven retry, out-of-order reassembly and exact echo verification.
- Application-level deterministic selective encrypted datagram size drop on both directions, with a smaller retry fragment after 120 ms.
- `srdt-server --reliable`, `srdt-client --reliable --bytes N` command-line switches.
- Loopback UDP integration tests with and without selective size filtering.

## Known limitations — important
- **Not production-ready. Do not expose publicly.** Stage-2 handshake is experimental and server identity is ephemeral; replay of a previously valid hello remains an unresolved security issue.
- Single active client/session; one message exchange; no concurrency or TCP bridge.
- The size filter is in the application send path, not a real network middlebox. No actual IP fragmentation or 4–8 KiB UDP datagrams are tested.
- Stage-3 per-byte ACK tracking is inefficient; no congestion control, adaptive RTT or bounded session-wide memory accounting.
- The retry-size adaptation is time-based, not inferred from measured loss statistics.
- Client's successful return does not guarantee the server received its final ACK; no session close handshake.
- Cross-compiled Windows executables have not been executed on Windows; Ubuntu-to-Windows remote test has not been performed.
- The test covers 3,800-byte payloads, **not** the previously proposed 100 MiB scenario.

## Usage (test network only)
Start server with a 32-byte hex PSK:
`./bin/srdt-server-linux-amd64 --listen 127.0.0.1:19443 --psk <64-hex-chars> --reliable --drop-min 800 --drop-max 1100`
Copy printed SERVER_PUBLIC to client:
`./bin/srdt-client-linux-amd64 --server 127.0.0.1:19443 --psk <same> --server-public <hex> --reliable --bytes 3800 --drop-min 800 --drop-max 1100`

## Verification
See `docs/stage4-test-output.txt` for the actual build/test logs.
