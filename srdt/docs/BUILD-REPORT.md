# SRDT Stage 25 — personal verification harness report

Date: 2026-11-02

Stage 25 does **not** change SRDT protocol or Go implementation. All 75 `.go` source files were
byte-for-byte SHA-256 equivalent to Stage 24. This stage adds operator-facing scripts for a simple
Ubuntu-server / Windows-client verification workflow.

## Added files

- `scripts/srdt-test-server.sh` — start/status/logs/stop helper for Ubuntu/Linux.
- `scripts/Test-SRDT.ps1` — Windows verification runner.
- `scripts/Test-SRDT.cmd` — thin Windows wrapper around the PowerShell runner.
- `docs/STAGE25.md` — usage and scope.

The Linux helper starts the bundled fixed TCP echo upstream and one SRDT multi-client UDP server,
generates a fresh laboratory PSK, captures the server public key, and emits one opaque
`SRDT_TEST_TOKEN`. The token contains the advertised host, UDP port, test PSK, and pinned public key.

The Windows runner accepts that token and automatically executes:

1. real bridge smoke: 16 × 4096-byte chunks;
2. 100 MiB exact byte verification: 6400 × 16384 bytes;
3. client-side selective encrypted-datagram drop for sizes 800..1100 bytes;
4. four seconds of application silence while the server idle timeout is two seconds, using
   500 ms authenticated keepalive and 2 s liveness timeout;
5. optional `-StressClients N` concurrent bridge test.

Every test checks the `srdt-load` JSON result and exact verified byte count. The Windows runner exits
nonzero if any test fails and writes a timestamped JSON report.

## End-to-end harness validation performed in this Linux environment

The new Linux helper was exercised exactly through `start -> token -> status -> tests -> stop`.
The following equivalent client workload was executed with the bundled Linux load binary against the
server started by the helper:

| Test | Result | Verified bytes one direction | Measured workload time |
|---|---:|---:|---:|
| smoke | PASS | 65,536 | 0.319 s |
| 100 MiB bulk | PASS | 104,857,600 | 13.287 s |
| size drop 800..1100 | PASS | 262,144 | 6.769 s |
| silent keepalive | PASS | 1 | 4.310 s |
| 1000 concurrent bridge clients | PASS 1000/1000 | 65,536,000 | 7.355 s |

These are loopback/Linux measurements and are not performance expectations for Windows-to-Ubuntu WAN.
Raw non-secret outputs are in `docs/logs/stage25/`.

## Build/source checks

- `bash -n scripts/srdt-test-server.sh`: PASS.
- PowerShell script structural/bracket validation: PASS.
- Native PowerShell execution: **not available in this Linux environment**; Windows runtime execution
  is intentionally the next user-side validation requested by this stage.
- `go build ./...`: PASS before the long network test suite entered the execution-window timeout.
- `go vet ./...`: PASS.
- quick core package tests (`emulator`, `testkit`, `reliable`, `secureudp`): PASS.
- 75/75 Go source files byte-identical to fully verified Stage 24: PASS.
- five Linux/amd64 command binaries rebuilt from current sources: PASS.
- five Windows/amd64 command binaries freshly cross-built from current sources: PASS.
- Linux `srdt-test --messages 1000`: PASS 1000/1000.

A monolithic `go test -p 1 ./...` was also started but the invocation exceeded the available execution
window while entering the long `netreliable` network suite. This is not reported as a PASS. Stage 25
contains no Go changes; the corresponding Stage 24 protocol code had already passed its full normal,
race, regression, fuzz, and release-archive checks. See `docs/BUILD-REPORT-STAGE24.md`.

## Security/operations notes

The connection token contains a **temporary laboratory PSK** and must be treated as a secret for the
lifetime of the test server. `srdt-test-server.sh stop` terminates both helper processes. The helper
does not modify host or cloud firewall rules automatically. Only the selected UDP test port needs to
be reachable from the Windows host.

This remains an experimental, unaudited transport and is not production-ready.
