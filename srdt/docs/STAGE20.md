# Stage 20 — ограничение ACK traffic, раннее обнаружение потерь и ленивый packet-state

Stage 20 продолжает Stage 19 и не меняет его основную модель: логические сообщения
и физические DATA-попытки нумеруются независимо, приёмные буферы резервируются до DATA,
а физическая передача ограничивается pacing/congestion window. Цель этапа — уменьшить
CPU/сетевой overhead точных receipts и не держать 8-КиБ журнал физического полёта у
простаивающей сессии.

Это лабораторный UDP echo-транспорт, не готовый VPN и не доказательство устойчивости
к DPI/ТСПУ.

## 1. ACK небольшими ограниченными пакетами

Добавлен `TypePacketAckBatch = 11`. Один такой frame содержит только номера
аутентифицированных физических DATA-передач: от 1 до 4 `uint64`. Message ID, Offset,
Total и длина не дублируются — отправитель получает эти значения из своего ограниченного
packet ledger. Перед изменением состояния весь batch валидируется. Неизвестный будущий
номер делает batch недействительным; уже вытесненный аутентифицированный номер считается
устаревшим и не получает authority над логическими данными.

У Receiver нет динамического списка ACK: используются фиксированные массивы на четыре
номера и 32 байта payload. Частичная пачка имеет deadline 5 мс и обслуживается существующим
общим scheduler — отдельной goroutine/таймера на сессию или packet нет. Первые два DATA
в направлении подтверждаются немедленно, чтобы не задерживать первичную RTT/pacing оценку.

Практически batching оказался **асимметричным**:

* сервер по умолчанию агрегирует receipts на DATA клиента;
* клиент по умолчанию отправляет exact ACK сразу;
* `srdt-client`/`srdt-load --batch-packet-acks` явно включает client-side batching;
* `--legacy-packet-acks` сохраняет Stage 19 single-packet ACK совместимость.

Причина такого default измерена: batching ACK на клиенте уменьшает количество пакетов,
но лишает общий серверный event loop естественных входящих wakeups и в текущем pacer
существенно увеличивает задержку длинного server->client потока. Эта оптимизация остаётся
opt-in до дальнейшей переработки scheduler/pacer.

## 2. Packet-threshold loss

Receipt более нового номера может показать, что ранняя физическая попытка с большой
вероятностью потеряна. Stage 20 объявляет in-flight запись потерянной, когда подтверждённый
номер опережает её как минимум на пять. Логический диапазон сразу становится доступным
для другой упаковки; ждать RTO не обязательно.

Алгоритм сканирует не более 128 записей bounded ledger только при новых receipts. Это
не RFC 9002 loss recovery и не доказательство правильности порога во всех сетях.
Сильная reordering может вызвать преждевременный повтор. Полная блокировка одного размера,
при которой вообще нет более новых receipts, всё равно требует RTO.

## 3. Ленивое packet-state

Stage 19 держал `[128]packetRecord` после первого DATA до закрытия сессии. Stage 20 хранит
указатель и резервирует `PacketStateCharge = 8192` только на фактическом DATA send path.
Расчёт wake/deadline и ожидание receiver grant журнал не выделяют.

После последнего in-flight DATA и окончания очереди ring не освобождается немедленно:
установлен idle grace 100 мс. Короткая пауза между сообщениями переиспользует память и не
создаёт allocate/free churn; настоящий простой отдаёт 8192 байта обратно общему
`PacketStateBudget`. PacketNumber при освобождении ring не сбрасывается. Старый ACK после
освобождения не получает authority и возвращается как stale.

Серверная метрика `peak_packet_state_bytes` исправлена: high-water теперь снимается после
реального выделения ring, а не при включении packet mode.

## 4. Scheduler

Минимальный шаг event loop уменьшен до 100 мкс для уже рассчитанного близкого wake.
Это не busy-spin: при отсутствии готовой работы используются UDP read deadlines и
дедлайны существующих очередей. Credit scan сохраняет более крупную собственную
гранулярность. Никаких goroutine/таймеров на packet/session не добавлено.

## 5. Ресурсы

Жёсткие ограничения предыдущих этапов сохранены:

| Ресурс | Значение по умолчанию / cap |
|---|---:|
| Payload + bitmap + bounded range metadata | 64 МиБ global budget |
| Packet-state | отдельный 16 МиБ global budget |
| Packet ledger | 128 записей / 8192 байт на активное отправляющее направление |
| Packet ledger idle grace | 100 мс |
| Одновременные server sessions | 1024 |
| RX+TX payload одной сессии | 64 КиБ |
| RX+TX активные сообщения | 8 |
| Сообщение multi-server | до 32 КиБ |
| ACK batch | 1..4 номера, threshold 2, flush до 5 мс |

Ни один budget не является RSS cap. Go runtime, maps, crypto, socket buffers и allocator
требуют дополнительной памяти.

## 6. Измерения

Парное сравнение Stage 19/20 выполнено тремя чередующимися запусками одной конфигурации:
200 клиентов, 32 × 16 КиБ на клиента = 100 МиБ в одну сторону плюс эхо. Все ответы
проверены побайтово.

| Медиана 3 запусков | Stage 19 | Stage 20 |
|---|---:|---:|
| Wall time | 3.815 с | 4.058 с |
| CPU сервера | 4.03 с | 3.43 с |
| Максимальный sampled RSS | 39,128 КиБ | 38,536 КиБ |
| Sent datagrams сервера | 350,782 | 266,909 |
| Received datagrams сервера | 318,824 | 313,567 |
| DATA retries сервера | 59,163 | 44,127 |

Stage 20 в этой серии: CPU примерно -14.9%, server sent datagrams -23.9%, retries -25.4%,
RSS около -1.5%, но wall time **+6.4%**. Поэтому это улучшение стоимости обслуживания,
а не универсальное ускорение.

Длинная одна сессия, 100 МиБ + эхо: Stage 19 исторически 3.678 с; финальный Stage 20
3.562 с. Вариант с client-side ACK batching был значительно хуже (~6.9–7.2 с), поэтому
он не default.

1000 активных клиентов, 62.5 МиБ + эхо: PASS, sampled server RSS 102,632 КиБ, peak
payload budget 67,088,384 bytes, peak packet-state 5,193,728 bytes, ResourceDrops=0.
Это короткий Linux loopback test, не целевая 2-ГБ VPS.

1000 клиентов с маленьким обменом и коротким idle: PASS, sampled RSS 14,172 КиБ,
packet-state после hold 0; peak packet-state 2,220,032 bytes из-за 100-мс grace.

Фильтр encrypted UDP datagrams 800–1100 байт в обе стороны: 32 клиента, 1.5 МиБ + echo,
PASS; 1536 датаграмм намеренно отброшены, данные восстановлены.

## 7. Совместимость

Stage 20 client по умолчанию отправляет Stage 19 single-packet ACK, поэтому проверенный
обмен Stage 20 client -> Stage 19 server проходит без дополнительного ACK-флага.
Stage 19 client -> Stage 20 server требует серверный `--legacy-packet-acks`, поскольку
Stage 19 не понимает `TypePacketAckBatch`. Без явного флага тест заканчивается ошибкой;
скрытого downgrade по timeout нет.

## 8. Нерешённые вопросы

* client-side ACK batching пока opt-in из-за measured latency regression;
* threshold=5 эвристический и может давать spurious retransmission при сильной reordering;
* журнал всё ещё ограничен 128 живыми physical attempts и может ограничивать high-BDP link;
* нет ACK ranges, ECN, PTO, проверенной fairness с TCP/QUIC и общего capacity controller;
* HELLO остаётся фиксированного размера 81 байт; single ACK и другие controls также имеют
  распознаваемые форматы/размеры; DPI-устойчивость не доказана;
* crypto provisioning/epochs/authorization остаются экспериментальными;
* нет streaming API, Xray/sing-box bridge, надёжного CLOSE, alternate carriers;
* Нет authenticated capability/version negotiation; межверсионный packet-ACK режим пока выбирается явными flags.
* Windows binaries только cross-compiled; нет реального Windows runtime, слабой VPS 2 ГБ
  вместе с Xray, WAN/ТСПУ и длительного soak test.

Полные проверки и методика измерений — [BUILD-REPORT.md](BUILD-REPORT.md).
