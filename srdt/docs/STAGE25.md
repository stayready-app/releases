# SRDT Stage 25 — one-command personal verification harness

Stage 25 does not change the SRDT wire protocol. It adds a reproducible two-host verification workflow
for the current Stage 24 transport so a developer can validate the Windows runtime against an Ubuntu
server without manually assembling many commands.

## Linux

From the extracted project root:

```bash
./scripts/srdt-test-server.sh start
```

The script starts the bundled fixed TCP echo upstream and the SRDT UDP server, generates a fresh
laboratory PSK, captures the generated pinned server public key, and prints one `SRDT_TEST_TOKEN`.

If automatic address detection is wrong:

```bash
./scripts/srdt-test-server.sh stop
./scripts/srdt-test-server.sh start --advertise YOUR_REACHABLE_IP
```

Useful lifecycle commands:

```bash
./scripts/srdt-test-server.sh status
./scripts/srdt-test-server.sh logs
./scripts/srdt-test-server.sh stop
```

The server deliberately uses a 2-second idle timeout in this harness so the Windows keepalive test
proves that authenticated path liveness actually preserves a silent bridge.

## Windows

Copy the token printed by Linux and run from the extracted Stage 25 directory:

```powershell
.\scripts\Test-SRDT.cmd -Token 'SRDT1....'
```

The script automatically runs:

1. one-client Windows -> Ubuntu fixed-TCP bridge smoke test;
2. 100 MiB exact byte verification through one bridge session;
3. selective client-side loss of encrypted UDP datagrams whose sizes are 800..1100 bytes;
4. a four-second application-silent bridge while the server idle timeout is only two seconds.

An optional stress run is:

```powershell
.\scripts\Test-SRDT.cmd -Token 'SRDT1....' -StressClients 1000
```

The Linux harness sets `max-sessions=1200` and `max-per-ip=1200` specifically so the optional
single-Windows-host stress test is not rejected by the normal per-IP protection.

Every Windows run writes a timestamped JSON report and exits nonzero if any test fails.

## Security scope

The token contains a laboratory PSK. Treat it as temporary test material. The current SRDT
cryptographic/key-lifecycle design is still experimental and the test server should not be exposed as
a production public service. The helper does not modify the Ubuntu/cloud firewall automatically.
