# Безопасность и эксплуатационные ограничения — Stage 21

**Лабораторный прототип; не публиковать как VPN/proxy.** Успешные unit/race/fuzz/network
тесты не являются криптографическим аудитом и не доказывают DPI-устойчивость.

## Что улучшено в Stage 21

- V1 version/capability negotiation аутентифицирован; unknown required capability fail-closed.
- Нет timeout-driven silent downgrade. Legacy handshake включается явно с обеих нужных сторон.
- Повтор valid HELLO не пересоздаёт crypto session и не сбрасывает nonce/counters.
- HELLO/WELCOME retries меняют внешний размер без изменения аутентифицированной семантики.
- Все control frames могут получать authenticated encrypted padding.
- Control padding выбирается из разных size bands, а не из одного случайного диапазона.
- Stream adapter ограничивает число активных транзакций и application buffering.
- Echo-level completion может освободить request state без ложного packet receipt/RTT/cwnd credit.

## Неавторитетный V1 HELLO tail

Random tail HELLO намеренно не аутентифицирован и игнорируется при согласовании. Это
позволяет менять длину retransmit без изменения handshake transcript. Изменение tail
атакующим не может изменить version/capabilities/keys, однако может изменить наблюдаемый
размер или привести к drop. На эту часть нельзя помещать security-sensitive поля.

## Блокирующие вопросы

1. Handshake/key provisioning всё ещё экспериментальны. Нет production per-client
   authorization, revocation, durable server identity/epoch rotation и внешнего аудита.
2. Rate/admission limits не являются полноценной anti-DDoS/path-validation схемой.
3. Congestion/loss эвристики bounded, но не доказана fairness относительно TCP/QUIC и
   progress при произвольном активном фильтре.
4. Traffic-shaping не делает SRDT неотличимым от QUIC/HTTP. Direction/timing/volume/IP и
   другие признаки остаются наблюдаемыми. Реальный ТСПУ/DPI не тестировался.
5. BufferBudget/PacketStateBudget/stream limits не являются RSS/CPU/cgroup limits.
   Обязательны тесты на целевой 2-GB node совместно с Xray.
6. Exactly-once действует в пределах живой bounded session; нет crash recovery/durable
   commit, reliable final CLOSE, NAT migration или durable resumption.
7. Streaming adapter пока echo-oriented и не является безопасным generic duplex proxy API.
   Blocking source может остановить сетевой progress.
8. UDP datagram cap пока 1200 bytes; обсуждавшиеся 4–8 KiB fragmentation profiles не
   реализованы.
9. Нет native Windows runtime, длительного soak, WAN/operator tests и Xray/sing-box bridge.
10. Среда использует Go 1.23.2; новый supported toolchain и актуальный `govulncheck` здесь
    недоступны.

Queue/Receiver/session state имеют одного владельца и не заявлены concurrent-safe. Borrowed
scratch slices нельзя удерживать после следующей операции. Test secrets в fixtures публичны
намеренно и не должны использоваться в эксплуатации.
