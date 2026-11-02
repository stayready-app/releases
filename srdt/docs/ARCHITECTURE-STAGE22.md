# Архитектура SRDT — Stage 22

Текущая спецификация: [STAGE22.md](STAGE22.md). Архитектура Stage 21 сохранена как
[ARCHITECTURE-STAGE21.md](ARCHITECTURE-STAGE21.md).

## Уровни

```text
Application endpoint
  bounded asynchronous Read pump
  bounded asynchronous Write pump
        |
SRDT network owner
  message queue / ordering / receive windows
        |
Reliable logical messages
  IDs / retransmission / repacking / deduplication
        |
Physical DATA attempts
  PacketNumber / packet ledger
  congestion window / pacing / adaptive RTO
        |
secureudp V1 session
  authenticated capabilities / AEAD / control padding
        |
UDP carrier
```

Message ID/Offset адресуют логические данные. PacketNumber адресует конкретную физическую
DATA-попытку. AEAD nonce/counter является отдельным пространством.

## Ownership

На клиентском stream path состояние протокола имеет одного владельца — network goroutine,
вызвавшая `ExchangeStream/ExchangeDuplex`. Queue, Receiver, session counters и congestion state
не разделяются между application pumps.

Blocking application `Read` и `Write` вынесены в две bounded goroutine. Reader использует один
переиспользуемый chunk buffer и не меняет его до подтверждения network owner. Writer получает
ownership собранного `Received` и освобождает его после записи. Per-packet goroutine/timer нет.

Pump сообщает о готовности network owner через изменение UDP read deadline на `time.Now()`, чтобы
не ждать polling interval и не создавать отдельную UDP-reader goroutine.

`ExchangeDuplex` владеет `io.ReadWriteCloser` и закрывает его при выходе, что прерывает blocking
I/O у обычного `net.Conn`. `ExchangeStream` не закрывает произвольные Reader/Writer.

## Backpressure

Outbound application buffering ограничен `MaxBuffered` (по умолчанию 64 KiB) и
`MaxActive` (по умолчанию 4 транзакции). Reader не читает следующий borrowed chunk, пока текущий
не принят bounded Queue.

Если application Writer заблокирован, network owner продолжает ACK/retransmit/receive progress.
Completed replies остаются в уже существующем bounded Receiver/response state. После исчерпания
разрешённого receive window дальнейшее поступление естественно останавливается без неограниченного
роста памяти.

## Server model

Shared UDP server Stage 21 не изменён: один UDP socket/event loop, без per-session/per-packet
goroutine/timer. Stage 22 client pumps **не являются** выбранной моделью будущего server-side
upstream для тысяч TCP/Xray соединений. Этот слой ещё должен быть спроектирован и измерен отдельно.

## V1 / wire compatibility

Wire format и capability set Stage 21 не изменены. `CapabilityStreaming` покрывает новый API,
поскольку изменения относятся к локальному scheduling/ownership, а не к wire semantics.

## Текущее ограничение

Удалённая application semantic всё ещё echo-oriented: request ID N порождает response ID N+1 и
клиент заранее знает максимальную ёмкость ответа. Это ещё не arbitrary independent server->client
stream. Следующий bridge layer должен разделить направления логического потока и корректно
реализовать TCP half-close/close semantics.

Подробные тесты и измерения: [BUILD-REPORT.md](BUILD-REPORT.md). Ограничения безопасности:
[SECURITY.md](SECURITY.md).
