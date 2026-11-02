# Архитектура SRDT — Stage 20

Текущая спецификация: [STAGE20.md](STAGE20.md). Предыдущая версия сохранена как
[ARCHITECTURE-STAGE19.md](ARCHITECTURE-STAGE19.md).

## Текущий UDP pipeline

```text
Application message
  -> bounded logical Queue + receiver reservation/window
  -> fragmentation / repacking
  -> exact physical PacketNumber
  -> bounded packet ledger (lazy, 128 slots)
  -> congestion window + pacing
  -> secureudp AEAD session
  -> UDP
  -> authenticated decode
  -> bounded Receiver
  -> exact ACK / bounded ACK batch
  -> logical completion
```

Message ID/Offset описывают логические данные; PacketNumber описывает конкретную
физическую DATA-попытку. Повтор может иметь другой размер и новый PacketNumber. AEAD
counter отдельный и не равен PacketNumber.

## Управление памятью

Payload/bitmap/range metadata учитываются общим BufferBudget. Packet ledgers имеют
отдельный PacketStateBudget, чтобы уже выданный приёмный reserve не отнимал память,
необходимую для обязательного ответа. Ledger не хранит payload и имеет максимум 128
записей. Stage 20 выделяет его только на actual DATA send path; scheduler/grant wait
не создаёт ring. После 100 мс настоящего idle ring освобождается, но PacketNumber не
сбрасывается.

Один server UDP socket и shared event loop обслуживают все sessions; нет goroutine или
таймера на packet/session. Ограниченная работа за проход предотвращает monopolization
одной сессией. ACK/credit control не проходит через DATA pacer, чтобы освобождение
ресурсов не зависело от заполненного data window.

## ACK и loss

Server Receiver по умолчанию собирает до 4 exact PacketNumber в один authenticated
`TypePacketAckBatch`. Threshold для обычной пачки — 2, partial batch flush до 5 мс.
Первые два receipts immediate. Client Receiver по умолчанию batching не использует из-за
измеренного взаимодействия с server scheduler/pacer; opt-in существует отдельно.

Receipt более нового PacketNumber может пометить in-flight copy как lost при gap >= 5.
Это bounded эвристика fast recovery, не полноценный QUIC/RFC9002 loss detector. RTO
сохраняется для blackhole/случаев без последующих receipts.

## Carrier separation

Верхний message protocol принципиально не должен зависеть от UDP. TCP/TLS, WebSocket,
HTTP/2 и QUIC могут быть carriers в будущем. Для reliable carrier нельзя автоматически
дублировать UDP retransmission/congestion control. Сейчас реализован только UDP carrier.

Ротация нескольких физических соединений, долгие connection profiles и camouflage
относятся к уровню carrier/session orchestration и не должны загрязнять message format.

## Реально реализовано vs планы

Реализован bounded message-based UDP echo transport и load/fault harness. Это не TUN,
не SOCKS/VLESS proxy и не Xray bridge. Batch API удерживает переданный приложением набор
messages и результаты; production memory-bounded streaming API ещё отсутствует.

Crypto остаётся экспериментальным X25519 + лабораторный PSK + AES-GCM. Нет durable
server identity/epoch provisioning, per-client authorization/revocation, crash resume,
NAT migration и production key rotation.

Подробные measured limits и тесты: [BUILD-REPORT.md](BUILD-REPORT.md).
