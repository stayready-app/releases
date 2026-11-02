# SRDT — Stage 15: аудит и исправления

Экспериментальный транспорт StayReady на Go. **Не готов к публичной эксплуатации.**
Полный снимок исходников, 4 приложения для Linux/Windows amd64, тесты и журналы.
Библиотеки сторонних Go-модулей не используются. В этой среде сборка/запуск выполнены с
Go 1.23.2; это устаревший toolchain, бинарники только лабораторные. Для релиза требуется
поддерживаемый Go с security fixes и повторная проверка (см. SECURITY.md).

## Что изменилось

Исправлены повторное создание session keys/nonce в старых API, нетранзакционное принятие
конфликтующих фрагментов, повторная доставка после eviction ID, неограниченное число мелких
объектов, метаданные retransmit вне бюджета, starvation UDP-read таймерами, некорректное
обновление сессии по ACK, чувствительность к инъекциям/ошибкам отдельных датаграмм и несколько
panic/overflow. Ускорены bitmap/ACK/retry и переиспользование scratch-буферов.

[Подробный аудит](docs/AUDIT-STAGE15.md) · [Проверки и измерения](docs/BUILD-REPORT.md) ·
[Безопасность и блокеры](docs/SECURITY.md) · [Архитектура](docs/ARCHITECTURE.md).
Исторические STAGE1–14 и старые журналы сохранены, но не описывают текущее состояние.

## Текущий работающий режим

`--multi`: несколько клиентов на одном UDP-сокете, один цикл обработки, общая heap-очередь
таймеров. Нет goroutine/таймера на каждого клиента/фрагмент. Сервер не требует числа сообщений;
тестовый клиент ExchangeBatch пока получает конечный набор данных и отправляет bounded groups.
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
```

verify.sh также повторяет регрессии, запускает короткий fuzz двух парсеров и собирает все
четыре приложения для обеих ОС. Integration требует Linux /proc, Python 3 и собранные bin.
Он запускает сервер и генератор отдельно, пишет docs/stage15-*.json/log и проверяет ответы.
Windows-бинарники кросс-компилировались, но не запускались на Windows.

Новые реальные socket-тесты с fault-relay:

```bash
go test -v ./netreliable -run '^TestAudit' -count=1
```

Relay расположен между endpoint-сокетами, моделирует нарушения в обоих направлениях.
Это компонент тестов, не отдельное приложение для внешней WAN. Проверяются loss, reordering,
replay/duplicates, corruption, forged input, selective sizes, dropped WELCOME и ACK blackout.

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

## Ещё не выполнено

Negotiated receive credits/backpressure, adaptive RTO, proper congestion control/pacing,
проверенный key exchange и key rotation, долговременная exactly-once семантика, bounded
streaming client API, TCP bridge к Xray, 4–8-КиБ датаграммы/IP-fragmentation, carriers кроме UDP,
производственные Windows/Ubuntu/WAN/weak-VPS/soak испытания. Параллельная нагрузка стала
дешевле, но ResourceDrops остаются; числа приведены в BUILD-REPORT без заявления готовности.
