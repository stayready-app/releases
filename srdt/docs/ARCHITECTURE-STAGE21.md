# Архитектура SRDT — Stage 21

Текущая спецификация: [STAGE21.md](STAGE21.md). Архитектура Stage 20 сохранена как
[ARCHITECTURE-STAGE20.md](ARCHITECTURE-STAGE20.md).

## Уровни

```text
Application / bounded stream adapter
        |
Reliable logical messages
  IDs / ordering / receive reservations
  retransmission / repacking / deduplication
        |
Physical DATA attempts
  PacketNumber / bounded packet ledger
  congestion window / pacing / adaptive RTO
        |
secureudp V1 session
  authenticated capability negotiation
  AEAD + variable control padding
        |
UDP carrier
```

Message ID/Offset адресуют логические данные. PacketNumber адресует конкретную физическую
DATA-попытку. AEAD nonce/counter является отдельным пространством и не равен PacketNumber.

## V1 session

По умолчанию клиент предлагает возможности и перечисляет обязательные. Сервер выбирает
пересечение optional capabilities и отклоняет неизвестные required capabilities. Выбранный
набор аутентифицирован WELCOME. Скрытого fallback после timeout нет.

Повтор V1 HELLO не создаёт новый session state и не сбрасывает AEAD counters. HELLO/WELCOME
могут менять внешний размер при повторе. Неавторитетный HELLO tail не влияет на ключи,
версию или capabilities.

## Управление памятью

Payload/bitmap/range metadata учитываются BufferBudget. Physical packet ledger имеет
отдельный PacketStateBudget. Ledger не хранит payload, bounded 128 slots и выделяется
лениво. Один UDP socket и shared event loop обслуживают все sessions; per-session goroutine
и per-packet timers отсутствуют.

Stage 21 streaming defaults дополнительно ограничивают application-side buffering 64 KiB
и четырьмя активными транзакциями. Эти лимиты не являются RSS cap: Go runtime, maps,
crypto и kernel socket buffers учитываются отдельно только измерениями процесса.

## Streaming

`ExchangeStream` читает `io.Reader` порциями и выдаёт эхо-ответы в исходном byte-stream
порядке. Сейчас это ограниченный лабораторный adapter поверх echo semantics, а не generic
full-duplex carrier. Blocking source может остановить network pump; перед Xray bridge это
должно быть переработано.

## Carrier separation

Message/reliability layer не должен зависеть от UDP. TCP/TLS, WebSocket, HTTP/2 и QUIC
остаются будущими carrier adapters. Надёжный carrier не должен автоматически наследовать
UDP retransmission/congestion logic.

Подробные тесты и измерения: [BUILD-REPORT.md](BUILD-REPORT.md). Ограничения безопасности:
[SECURITY.md](SECURITY.md).
