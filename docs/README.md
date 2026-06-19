# Design Documentation

Planning docs for this telemetry pipeline — written before implementation, to think through
the problem and commit to a simple, defensible design.

| Document | What it covers |
|---|---|
| [`PROBLEM_ANALYSIS.md`](./PROBLEM_ANALYSIS.md) | What's actually being asked, why it's non-trivial (sparse → dense resampling), the transform spec, explicit assumptions, verify-before-coding checks, and the definition of done. |
| [`DESIGN.md`](./DESIGN.md) | The proposed architecture, the queue contract, the core writer algorithm (event-time grid · forward-fill · watermark · idempotent upsert), data model, reliability, config, tech choices, testing, tradeoffs, and milestones. |

## TL;DR

The task isn't "MQTT → queue → DB"; it's reconstructing a **gap-free, 5-second, complete**
time series from **sparse, single-field, out-of-order** telemetry.

The design splits cleanly on a **stateless / stateful** seam:

- **`collector.ts`** — stateless: parse topic, convert units (gear→int, m/s→km/h), publish a
  normalized event per message.
- **`writer.ts`** — stateful: event-time 5 s bucketing + last-observation-carried-forward +
  capacity-weighted SoC + **idempotent upsert** on `(car_id, time)`.

One idea carries the whole thing: **at-least-once delivery + idempotent writes =
effectively-once persistence**, so correctness never depends on perfect ordering. Three
dependencies (`mqtt`, `amqplib`, `pg`); no ORM, no stream framework.
