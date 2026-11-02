# Stage 13 — shared receive allocation budget

This stage is deliberately limited to a tested resource-management refactor, not a continuous transport implementation.

- Added atomic process-shareable `reliable.Budget` with compare-and-swap reservations.
- `Receiver.SetBudget` charges full declared message size *before* allocating a reassembly buffer; completion and `Close` release the reservation.
- Ubuntu batch server attaches a 64 MiB shared receive-payload budget. It currently serves only one peer, so multi-peer integration is **not yet implemented**.
- Tests cover concurrent reservation, refusal at capacity, close cleanup and completion cleanup.

## Remaining caveats

- This is a payload accounting cap, **not an RSS cap**. Bitmaps, maps, Go runtime, encrypted packets, sender queues, socket buffers, and returned completed payloads are excluded.
- `Receiver` is single-owner and must not be used concurrently with `Close`.
- The budget needs to be injected by a future multi-session server manager; the current server is single-session.
- Cryptographic session handling remains experimental and unsafe for public exposure.
- No adaptive RTO, congestion control, true continuous session, or real Windows execution is claimed.
