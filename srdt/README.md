# SRDT — Stage 25

Экспериментальный Go-транспорт поверх UDP. **Лабораторный прототип; не готовый публичный
VPN/proxy-сервис.**


## One-command personal verification (Stage 25)

Linux server:

```bash
./scripts/srdt-test-server.sh start
```

Copy the printed `SRDT_TEST_TOKEN` to Windows and run:

```powershell
.\scripts\Test-SRDT.cmd -Token 'SRDT1....'
```

The Windows script runs smoke, 100 MiB exact transfer, size-selective loss recovery and silent
keepalive tests automatically and writes a JSON summary. Optional 1000-client stress: add
`-StressClients 1000`. See `docs/STAGE25.md`.

Stage 24 добавляет аутентифицированный path keepalive/liveness для длительно живущего TCP bridge,
устраняет абсолютный lifetime timeout modern bridge после handshake и сокращает удержание уже
закрытых bridge-сессий. Это продолжение Stage 23 independent bidirectional streams и fixed TCP
upstream bridge.

Подробности: [docs/STAGE24.md](docs/STAGE24.md). Архитектура:
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Проверки:
[docs/BUILD-REPORT.md](docs/BUILD-REPORT.md). Ограничения:
[docs/SECURITY.md](docs/SECURITY.md).

## Что изменилось

- capability `path_liveness` согласуется аутентифицированно в V1 handshake;
- полностью тихая modern bridge-сессия отправляет аутентифицированный `PATH_PING`, сервер отвечает
  `PATH_PONG`;
- любой корректный аутентифицированный входящий пакет подтверждает, что путь в этом направлении
  жив, поэтому активный DATA-трафик не создаёт лишний keepalive;
- modern bridge после handshake/open больше не ограничен абсолютным `--timeout`; этот параметр
  остаётся setup/handshake timeout;
- при потере server->client пути клиент после liveness timeout посылает best-effort
  аутентифицированный `STREAM_RESET` по оставшемуся client->server пути, чтобы сервер быстрее
  освободил upstream TCP;
- нормально завершённый bridge удерживается только короткий drain-период (default 1 s) для
  повторных FIN/ACK, а не весь server idle timeout;
- PING/PONG используют существующий authenticated variable control padding; повторные keepalive
  не обязаны иметь фиксированный внешний размер.

Defaults:

```text
keepalive interval       15 s
path-liveness timeout    45 s
server idle timeout      60 s
completed bridge drain    1 s
```

Keepalive не создаёт goroutine или timer на packet/session: ближайший срок включается в существующий
network scheduler. На сервере по-прежнему один shared UDP socket/event loop.

## Серверная ресурсная модель

Основные defaults для слабых нод не увеличены:

- global BufferBudget: 64 MiB;
- PacketStateBudget: 16 MiB;
- combined payload per session: 64 KiB;
- active messages per session: 8;
- fixed upstream TCP socket buffer request: 16 KiB read + 16 KiB write;
- shared UDP receive buffer request: 2 MiB;
- shared UDP send buffer request: 1 MiB.

Для каждого активного fixed-upstream TCP bridge по-прежнему используются две blocking Go goroutine
(upstream Read/Write). Kernel socket memory не входит в Go RSS/BufferBudget и требует отдельного
измерения на целевой Linux node.

## Laboratory fixed-upstream bridge

Server:

```bash
GOMAXPROCS=2 GOMEMLIMIT=128MiB ./bin/srdt-server-linux-amd64 \
  --multi --listen 0.0.0.0:19443 --psk "$SRDT_TEST_PSK" \
  --upstream 127.0.0.1:18443
```

Client local TCP listener:

```bash
./bin/srdt-client-linux-amd64 \
  --bridge-listen 127.0.0.1:19080 \
  --server SERVER_IP:19443 --server-public "$SERVER_PUBLIC" \
  --psk "$SRDT_TEST_PSK"
```

Default modern bridge может оставаться application-idle дольше `server --idle-timeout`, пока
аутентифицированный path остаётся живым. Для явной Stage 23 compatibility без liveness существуют
`--legacy-liveness` и `--allow-legacy-liveness`; автоматического downgrade нет.

## Финальные process-level проверки Stage 24

На финальном коде после всех lifecycle изменений:

- **1000 тихих real TCP bridge sessions**: 1000/1000 PASS; application silence 4 s при server idle
  timeout 2 s и стресс-настройке keepalive 500 ms / liveness 2 s; peak sampled SRDT-server RSS
  **30,812 KiB**, server CPU ~**1.31 s**, `ResourceDrops=0`, после drain `active_sessions=0`,
  `CompletedBridgeSessions=1000`, `ExpiredSessions=0`;
- **одна full-duplex bridge session, 100 MiB в каждую сторону**: byte-for-byte PASS,
  ~**5.80 s workload**, peak sampled server RSS **9,644 KiB**, `ResourceDrops=0`, DATA retries=0.

Это Linux loopback. Первый тест намеренно использует keepalive в 30 раз чаще production default и
не является прогнозом production packet rate. Это не проверка на реальной 2 GiB StayReady node +
Xray, WAN или ТСПУ.

## Сборка

```bash
go build ./...
go vet ./...
./scripts/verify.sh
```

В текущем окружении Go 1.23.2. Windows/amd64 binaries cross-compiled, но нативно в Windows ещё не
запускались.

## Не готово

Production key lifecycle/auth/revocation, reliable RESET, NAT/path migration, real Xray node soak,
kernel socket-memory accounting, native Windows runtime и реальный WAN/TSPU test остаются
обязательными. Traffic shaping и variable padding не являются доказательством обхода DPI.
