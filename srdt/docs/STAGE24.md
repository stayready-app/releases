# SRDT Stage 24 — authenticated path liveness and bridge lifecycle

## Цель этапа

Stage 23 сделал настоящий независимый TCP bridge, но тихое корректное TCP-соединение выглядело для
SRDT так же, как умерший путь: server idle timeout мог удалить bridge, а client CLI дополнительно
имел общий absolute timeout. Для Xray/прокси это неприемлемо — соединение может законно молчать
минуты.

Stage 24 вводит отдельную liveness-семантику пути, не смешивая её с application activity и не
создавая per-session timer/goroutine.

## Protocol additions

V1 capability:

```text
path_liveness
```

Новые аутентифицированные control frames:

```text
PATH_PING(token)
PATH_PONG(token)
```

Они проходят через тот же AEAD и variable control padding, что остальные V1 control frames.
Подделка неаутентифицированного ping/pong не продлевает сессию.

Client является инициатором liveness probe. Server отвечает PONG и не создаёт ping-pong loop.
Любой корректный аутентифицированный входящий frame обновляет `lastRx`; поэтому при нормальном DATA/
ACK/control traffic отдельный keepalive не нужен.

Defaults:

- keepalive interval: 15 s;
- liveness timeout: 45 s;
- stable per-session jitter примерно 90–110% для keepalive period, чтобы множество клиентов не
  синхронизировались на одной миллисекунде.

Client-side options:

```text
--keepalive
--keepalive-timeout
--legacy-liveness
```

Server compatibility option:

```text
--allow-legacy-liveness
```

Совместимость только явная; timeout не используется как скрытый downgrade signal.

## Removal of absolute bridge lifetime

Для modern independent bridge `Options.Timeout` теперь ограничивает setup/handshake/open, но не весь
runtime соединения. После успешного открытия lifetime определяется application half-close/reset,
context cancellation и path liveness.

Legacy-liveness mode сохраняет старое поведение, чтобы явно выбранная совместимость Stage 23 не
создала бесконечно зависающее соединение.

## Asymmetric path failure

Если server->client больше не работает, но client->server ещё жив, client после liveness timeout
пытается отправить аутентифицированный `STREAM_RESET` перед завершением локального bridge.

Server принимает такой RESET только внутри корректной negotiated independent-stream session и
немедленно освобождает fixed upstream TCP. Это сокращает удержание ресурсов по сравнению с ожиданием
60-секундного idle timeout.

RESET остаётся best-effort: если потеряны оба направления, server освобождает state только по своему
idle/progress timeout.

## Completed bridge drain

До Stage 24 нормально завершившийся TCP bridge мог оставаться в session table до общего idle
lifetime. Теперь после корректного half-close сессия переходит в короткий drain:

```text
BridgeDrainTimeout default = 1 s
```

Drain сохраняет достаточно state для повторных FIN/ACK, после чего session удаляется как completed,
а не expired. Значение bounded и конфигурируется `--bridge-drain-timeout` (50 ms..5 s).

## Scheduler and CPU model

Никаких отдельных ticker/timer goroutine на сессию не добавлено. Keepalive deadlines включаются в
существующий network owner's nearest-deadline calculation.

При настоящем idle основной loop больше не обязан просыпаться каждые 20 ms. Pumps уже умеют явно
будить network owner через socket deadline. Короткий poll сохраняется только когда есть реальная
protocol work (pending DATA/ACK/FIN/queue state). Это важно для тысяч тихих клиентов.

## Variable wire sizes

`PATH_PING/PONG` используют authenticated variable control padding. Regression test создаёт по 8
PING/PONG и требует несколько различных размеров encrypted datagram.

Это защищает только от тривиального fixed-size rule. Direction, timing, volume, server IP и другие
статистические свойства остаются наблюдаемыми.

## Resource/lifecycle regressions covered

Stage 24 tests explicitly cover:

- silent bridge surviving multiple server idle lifetimes;
- detection of completely dead server/path;
- asymmetric reverse-path blackout and fast authenticated reset;
- completed bridge removal through drain instead of idle expiry;
- explicit Stage 23 liveness compatibility;
- PING/PONG encode/decode validation;
- changing encrypted wire sizes for repeated PING/PONG.

The existing Stage 21–23 fault, stream, memory, congestion and full-duplex tests remain enabled.

## Process-level results

Final current-code runs:

### 1000 silent bridges

Configuration deliberately stresses keepalive:

```text
clients               1000
application idle       4 s
server idle timeout    2 s
keepalive             500 ms
liveness timeout        2 s
GOMAXPROCS              2
GOMEMLIMIT          128 MiB
```

Result:

```text
1000/1000 PASS
workload time             5.718 s
sampled server CPU        1.31 s
peak sampled server RSS  30,812 KiB
PATH_PING received        7,484
PATH_PONG sent            7,484
ResourceDrops                 0
CompletedBridgeSessions   1,000
ExpiredSessions               0
```

The 500 ms stress interval is 30x more frequent than the 15 s production default.

### 100 MiB full-duplex active bridge

```text
client -> upstream     104,857,600 B
upstream -> client     104,857,600 B
byte-for-byte          PASS
workload                 5.796 s
sampled server CPU        5.00 s
peak sampled RSS         9,644 KiB
ResourceDrops                0
DATA retries                 0
PATH_PING/PONG                0
```

Active traffic suppresses keepalive as intended.

## Known limitations

Stage 24 does not implement endpoint migration/NAT rebinding, multi-path probing, reliable RESET,
production cryptographic identity/key rotation, operator-specific DPI validation or target-node
soak. See [SECURITY.md](SECURITY.md).
