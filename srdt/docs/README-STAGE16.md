# SRDT — Stage 16: согласование приёма и backpressure

Экспериментальный транспорт StayReady на Go. **Не готов к публичной эксплуатации.**
Полный снимок исходников, 4 приложения для Linux/Windows amd64, тесты и журналы.
Библиотеки сторонних Go-модулей не используются. В этой среде сборка/запуск выполнены с
Go 1.23.2; это устаревший toolchain, бинарники только лабораторные. Для релиза требуется
поддерживаемый Go с security fixes и повторная проверка (см. SECURITY.md).

## Что изменилось

Отправитель теперь запрашивает OPEN на конкретное сообщение. Получатель резервирует весь
объём BufferCharge и только после этого отвечает GRANT. Пока памяти нет, ждут небольшие
управляющие запросы, а не повторно пересылаемые DATA. Разрешения используются в обоих
направлениях. Ожидающие клиенты обслуживаются общей ограниченной очередью без новых
per-session goroutine/таймеров. RX->TX ownership сохраняет исходный резерв без копирования.

[Спецификация этапа](docs/STAGE16.md) · [Проверки и сравнение](docs/BUILD-REPORT.md) ·
[Безопасность](docs/SECURITY.md) · [Архитектура](docs/ARCHITECTURE.md) ·
[Предыдущий аудит](docs/AUDIT-STAGE15.md).
Исторические документы и журналы сохранены, но не описывают текущее состояние целиком.

**Совместимость:** обновляйте обе стороны. Старый клиент Stage 15 допускается новым
`--multi` только с явным `--allow-legacy-data`; новый клиент не делает скрытого downgrade.
DATA/ACK остались прежними, добавлены четыре типа фреймов. Старые CLI options сохранены.

## Текущий работающий режим

`--multi`: несколько клиентов на одном UDP-сокете, один цикл обработки, общая heap-очередь
таймеров. Нет goroutine/таймера на каждого клиента/фрагмент. Сервер не требует числа сообщений;
тестовый клиент ExchangeBatch пока получает конечный набор данных и поддерживает до четырёх активных транзакций с независимым пополнением слотов.
`--reliable` и `--batch` сохранены и теперь используют тот же bounded сервер. Raw echo остаётся
отдельным ненадёжным лабораторным режимом.

Общий Budget резервирует сохраняемые payload, bitmap и ограниченные дескрипторы повторов.
RX-to-TX передача ownership не копирует payload/bitmap. Лимиты выделяются по фактической
потребности, не при подключении клиента. Budget **не ограничивает RSS** процесса.

## Сборка и проверка

```bash
go build ./...
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
bash scripts/verify.sh
python3 scripts/integration.py --bulk
python3 scripts/cli_smoke.py
```

verify.sh также повторяет регрессии, запускает короткий fuzz двух парсеров и собирает все
четыре приложения для обеих ОС. Integration требует Linux /proc, Python 3 и собранные bin.
Он запускает сервер и генератор отдельно, пишет docs/stage16-*.json/log и проверяет ответы.
Windows-бинарники кросс-компилировались, но не запускались на Windows.

Новые реальные socket-тесты с fault-relay:

```bash
go test -v ./netreliable -run '^(TestAudit|TestCredits)' -count=1
```

Relay расположен между endpoint-сокетами, моделирует нарушения в обоих направлениях.
Это компонент тестов, не отдельное приложение для внешней WAN. Проверяются loss, reordering,
replay/duplicates, corruption, forged input, selective sizes, dropped WELCOME, ACK/control blackout и потеря OPEN/GRANT.

## Лабораторный запуск

Терминал Ubuntu/Linux:

```bash
PSK=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
printf 'LAB_PSK=%s\n' "$PSK"
./bin/srdt-server-linux-amd64 \
  --multi --listen 127.0.0.1:19443 --psk "$PSK" \
  --metrics-file server-stats.json
```

В другом терминале задайте тот же PSK и SERVER_PUBLIC, напечатанный сервером:

```bash
./bin/srdt-client-linux-amd64 \
  --server 127.0.0.1:19443 --psk "$PSK" --server-public "$SERVER_PUBLIC" \
  --batch 12 --bytes 4096 --timeout 30s
```

Windows-клиент: `bin/srdt-client-windows-amd64.exe` с теми же параметрами. Для межмашинного
теста укажите доступный тестовый адрес Ubuntu и ограничьте UDP firewall по клиентскому IP.
Такая межмашинная проверка в этом архиве не выполнена. PSK в CLI виден в списке процессов;
не используйте production-секреты. После перезапуска сервера изменяется SERVER_PUBLIC.

## Default multi-server limits

| Ресурс | Значение |
|---|---:|
| Одновременные сессии | 1024 |
| Сессии с одного IP (CGNAT!) | 64 |
| Общий Budget payload + bitmap + reserved descriptors | 64 МиБ |
| RX + TX payload одной сессии | 64 КиБ |
| Активные RX + TX сообщения одной сессии | 8 |
| Максимальное логическое сообщение | 32 КиБ |
| Окно неподтверждённых исходящих payload | 16 КиБ |
| Handshake / idle / no-progress timeout | 5 / 60 / 15 с |
| HELLO ID за срок Server | 65536 |
| Дополнительный lifetime guard объекта Identity | 1 000 000 ID |
| Максимальная внешняя датаграмма | 1200 байт |

Ключевые CLI flags не удалены; `--help` перечисляет настройки. В Stage 15 BufferCharge
учитывает больше памяти, чем раньше (32-КиБ сообщение резервирует 44 КиБ). Счётчики прошлых
этапов не являются сопоставимым heap/RSS. При заполнении HELLO registry новые сессии
отклоняются; не очищайте реестр под прежним private key.

## Дополнительное сравнение

```bash
python3 scripts/compare.py --baseline /path/to/extracted/srdt-stage15 --runs 3
```

Оба дерева должны содержать собранные server/load. Скрипт чередует версии, сохраняет все
прогоны, сравнивает CPU/RSS/время и отдельный сценарий с задержкой 40 мс в каждую сторону.
`--bulk` в integration.py дополнительно включает pressure: 128 клиентов, Budget=8192 байта.

## Ещё не выполнено

Adaptive RTO, proper congestion control/pacing, проверенный key exchange и key rotation,
долговременная exactly-once семантика, bounded streaming client API, TCP bridge к Xray,
4–8-КиБ датаграммы/IP-fragmentation, carriers кроме UDP, Windows/Ubuntu/WAN/weak-VPS/soak
испытания и обновление toolchain. Первичная выдача GRANT стоит дополнительного обмена;
результат на длинном RTT нельзя выводить из loopback goodput. Контрольные пакеты пока имеют
постоянную длину 58 байт — постоянная блокировка этого размера приводит к timeout.

Здесь реализовано резервирование по сообщениям, а не полное производственное согласование
всех транспортных параметров. Цифры и ограничения — в BUILD-REPORT.md.
