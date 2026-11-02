# Безопасность и эксплуатационные ограничения — Stage 20

**Лабораторный прототип; не публиковать как VPN/proxy.** Успешные race/fuzz/network
регрессии не являются криптографическим аудитом или доказательством DPI-устойчивости.
Предыдущий аудит: [SECURITY-STAGE19.md](SECURITY-STAGE19.md),
[AUDIT-STAGE15.md](AUDIT-STAGE15.md).

## Что проверяется реализацией

* State меняется только после успешной AEAD authentication и frame validation.
* Повтор HELLO активной session не пересоздаёт ключи со сброшенным nonce.
* Receiver проверяет конфликтующий fragment до mutation.
* Неотправленные/несовпадающие ACK не подтверждают данные; duplicate receipt не освобождает
  window второй раз.
* Packet ACK batch валидируется как единица до mutation. Sender доверяет Message ID/Offset/
  length только своей bounded ledger record, а не непроверенным полям сети.
* PacketNumber не переиспользуется после idle release/reallocation ledger.
* DATA не создаёт unlimited receive state: используются receiver window, per-session caps,
  global BufferBudget и отдельный PacketStateBudget.
* Packet ledgers bounded 128 slots; lazy allocation не позволяет grant-wait sessions
  автоматически резервировать 8 KiB каждая.
* Invalid/injected packets не должны продлевать session progress или сбрасывать state.

## Оставшиеся блокирующие вопросы

1. Handshake/provisioning остаются экспериментальными. Общий lab PSK и ephemeral identity
   не дают production per-client authorization/revocation/epoch rotation. Нужен отдельный
   проверенный дизайн и аудит. Реальные секреты в CLI не использовать.
2. Нет полноценного anti-amplification/path validation и DDoS protection. Rate/admission
   limits ограничивают часть state, но не являются доказанной защитой публичного сервера.
3. Threshold loss gap=5 может создать spurious retransmission при сильной reordering.
   Атакующий может selectively drop packets и уменьшать throughput; RTO/threshold не
   гарантируют progress при полном блокировании carrier.
4. Bounded ACK batching server-side уменьшает packet count, но client-side batching пока
   opt-in из-за latency regression. ACK control не проходит через DATA pacer и не является
   общим network rate limiter.
5. `BufferBudget` и `PacketStateBudget` не ограничивают RSS, CPU, Go runtime/maps/crypto и
   kernel socket memory. 2-ГБ target требует реального node+cgroup/soak measurement.
6. Exactly-once относится к живой bounded session, не durable application commit. Нет
   crash recovery, reliable final CLOSE, NAT migration или durable resumption.
7. Fixed/structured control traffic остаётся распознаваемым. HELLO — фиксированный 81-byte
   UDP payload; single ACK и другие controls также имеют характерные размеры. ACK batching
   не делает протокол неотличимым от QUIC/HTTP. Реальный DPI/ТСПУ не тестировался.
8. UDP cap пока 1200 bytes; обсуждавшиеся 4–8 KiB fragmentation profiles не реализованы.
9. Нет Windows runtime test, слабой VPS 2 ГБ совместно с Xray, WAN/operator/soak test.
10. Нет authenticated capability/version negotiation. Неподдерживаемый новый frame type может
    привести к частичному приёму данных и timeout; межверсионный режим сейчас выбирается явным flag.
11. Среда использует Go 1.23.2; обновлённый toolchain и `govulncheck` здесь не получены.

Queue/Receiver/session state имеют единственного владельца и не заявлены concurrent-safe.
Borrowed scratch slices нельзя удерживать после следующей операции. Test PSK `11...` и
нулевые ключи в fixtures публичны намеренно.
