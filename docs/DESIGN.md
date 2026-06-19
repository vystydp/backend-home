# Design — EV Telemetry Pipeline

> MQTT → RabbitMQ → Postgres telemetry pipeline
> Companion document: [`PROBLEM_ANALYSIS.md`](./PROBLEM_ANALYSIS.md)

## 1. Design principles

The spec asks for **modern best practices with a focus on simplicity**. Concretely:

1. **Stateless edges, stateful core.** Per-message transforms live at the edge (collector);
   all cross-message state (resampling, forward-fill, weighted SoC) lives in one place
   (writer). Each file has a single, crisp responsibility.
2. **Idempotency over coordination.** Rather than chase exactly-once delivery, we make writes
   idempotent (`UPSERT` on `(car_id, time)`). At-least-once delivery + idempotent writes =
   **effectively-once persistence** — and correctness no longer depends on perfect ordering or
   a perfectly tuned watermark.
3. **Event-time processing.** Bucket by *when the reading happened*, not when it arrived. This
   is the only model that survives out-of-order data and produces a meaningful `time` column.
4. **Minimal dependencies.** Three well-worn libraries (`mqtt`, `amqplib`, `pg`). No ORM, no
   stream framework, no scheduler. Less to learn, less to break, easy to review.
5. **Pure, testable transforms.** Every conversion is a pure function with no I/O, so the
   tricky logic is unit-tested in milliseconds.
6. **12-factor config & fail-soft I/O.** Config from env; reconnect to MQTT/AMQP/PG; flush and
   close cleanly on shutdown.

## 2. Architecture

```mermaid
flowchart LR
  H["delta-helper<br/>(EV simulator)"] -- "per-field msgs<br/>QoS 1, out-of-order" --> M[("Mosquitto<br/>MQTT")]
  M -- "subscribe car/1/#" --> C["collector.ts<br/>parse + stateless transform"]
  C -- "normalized events<br/>{carId, metric, ts, value}" --> Q[("RabbitMQ<br/>durable queue")]
  Q -- "manual ack" --> W["writer.ts<br/>event-time resample · LOCF<br/>weighted SoC · watermark"]
  W -- "idempotent UPSERT<br/>(car_id, time)" --> P[("Postgres<br/>car_state")]
```

| Component | Responsibility | State |
|---|---|---|
| **`collector.ts`** | Subscribe to `car/1/#`; parse topic → structured event; apply **stateless** transforms (gear→int, m/s→km/h); publish one normalized event per message to RabbitMQ. | none |
| **`writer.ts`** | Consume events; keep rolling per-field state; on a 5 s grid, forward-fill a complete snapshot, compute weighted SoC, and **upsert** into `car_state`. | per-car rolling state |

**Why this boundary?** Stateless normalization is cheap, parallelizable, and belongs next to
the protocol it adapts. Stateful aggregation must be single-writer to keep event ordering and
the 5 s grid coherent. Splitting on the *stateless / stateful* seam (rather than, say, putting
all logic in the writer or all in the collector) gives each file one job and makes the queue
carry clean **domain events** instead of raw MQTT strings.

## 3. The queue contract

**Message** (JSON; the queue carries normalized domain events, decoupled from MQTT specifics):

```jsonc
{
  "carId": 1,
  "metric": "speed" | "gear" | "latitude" | "longitude" | "soc" | "capacity",
  "batteryIndex": 0,        // present only for "soc" and "capacity"
  "value": 44.3,            // post stateless-transform (speed km/h, gear int); soc raw %
  "ts": 1718800000000       // event time, epoch ms (UTC)
}
```

> **Guard at the edge.** The collector parses defensively: non-numeric values, unknown gear
> symbols, and unparseable topics are logged and **dropped, never enqueued** — malformed input
> dies at the boundary instead of poisoning the writer's state. Plain guards suffice; no schema
> library is warranted for single scalar payloads.

**Topology — deliberately the simplest thing that works:**

- One **durable** queue `car-events`, published via the default exchange.
- **Single consumer** (one writer) ⇒ FIFO preserved ⇒ the event-time state machine sees a
  coherent stream. (Scaling to many cars later = a topic exchange keyed by `carId`; see §12.)
- Capacity topics **are forwarded** so the writer can learn each battery's capacity from the
  stream at runtime (the spec's "read it from the topic, save it as a constant"), with a
  configured fallback until the first capacity message arrives.

**Delivery semantics:** MQTT subscribe at **QoS 1**; publish to RabbitMQ with **persistent**
messages + **publisher confirms**; consume with **manual ack** (ack only after a successful DB
upsert). End-to-end **at-least-once**; duplicates are harmless because writes are idempotent.

## 4. Writer — the core algorithm

This is the heart of the pipeline. Four ideas compose into something small.

### 4.1 Event-time 5-second grid

```ts
const BUCKET_MS = 5_000;
const bucketStart = (ts: number) => Math.floor(ts / BUCKET_MS) * BUCKET_MS;
```

Aligning to the epoch makes grid timestamps deterministic and reproducible regardless of when
the process starts.

### 4.2 Forward-fill (LOCF) — completeness from sparse data

Maintain the **latest value per field** in event-time order. The car's state at grid tick `T`
is the most recent observation of each field at or before `T`. Fields that didn't change carry
forward — which is exactly why gear (published only on change) and a quiet battery still appear
in every row.

```
ts:        gear=2        speed=30      gear=3
           │             │             │
grid:  ┌────────┬────────┬────────┬────────┬────────┐
T0     │ gear=2 │ gear=2 │ gear=2 │ gear=3 │ gear=3 │   ← forward-filled, no gaps
```

> **Fill horizon (staleness guard).** Forward-fill assumes the last reading still represents
> reality. If the source goes silent for minutes, blindly carrying an old speed forward is a
> confident lie. Cap it: if the newest observation for a field is older than `MAX_STALENESS_MS`
> (e.g. a few minutes), emit `NULL` for that field instead of a stale value. One constant, one
> comparison — keeps forward-fill honest without buffering or interpolation.

### 4.3 "No gaps" — emit on a clock, not on arrival

Rows are produced by **advancing the grid**, not by message arrival. We emit every tick up to a
**watermark** = `maxEventTimeSeen − GRACE_MS`. The grid advances *with the data* (so it pauses
when the source pauses and is replay-friendly), and the grace window lets most out-of-order
messages settle before the first write.

### 4.4 Out-of-order — watermark first, upsert as the safety net

`GRACE_MS` (e.g. 10 s) absorbs ordinary lateness *before* writing. Anything later still lands
correctly because each write is an idempotent **upsert on `(car_id, time)`** — a late message
recomputes and overwrites the affected bucket. **Correctness does not depend on the grace value
being perfect**, which is the whole point: the simple mechanism is also the robust one.

### 4.5 Weighted state of charge

```ts
// soc/cap are keyed by battery index; capacities are learned from the stream.
const weightedSoc = (soc: Map<number, number>, cap: Map<number, number>) => {
  let num = 0, den = 0;
  for (const [i, s] of soc) { const c = cap.get(i); if (c) { num += s * c; den += c; } }
  return den > 0 ? Math.round(num / den) : null;
};
```

SoC uses **whichever batteries have a known, non-stale reading** (the spec notes battery data
"can come only for one battery at a time"), so a single reporting battery still yields a value;
it is `NULL` only when none qualify. Capacities are cached from the `capacity` topic at runtime,
falling back to a configured default until the first capacity message lands.

### 4.6 Put together (pseudocode)

```ts
state = { lat?, lon?, gear?, speed?, soc: {0?, 1?} }  // latest value per field
maxTs = 0; nextTick = undefined

onEvent(e):
  applyIfNewer(state, e)                 // ignore stale per-field (out-of-order guard)
  maxTs = max(maxTs, e.ts)
  flushUpTo(maxTs - GRACE_MS)

flushUpTo(watermark):
  for each grid tick T from nextTick..watermark step 5s:
    row = snapshot(state, T)             // forward-filled, weighted SoC, converted units
    upsert(car_state, row)               // ON CONFLICT (car_id, time) DO UPDATE
    ack(messagesUpToThisPoint)
    nextTick = T + 5s
```

**Restart behaviour:** in-memory state is rebuilt from the incoming stream; previously written
rows persist untouched, and late corrections still apply via upsert. *(Optional hardening:
hydrate `state` from the last `car_state` row on startup — listed as a stretch, not required.)*

## 5. Data model

Use the schema as given; add **only** an additive unique constraint to enable idempotent writes.

```sql
CREATE TABLE IF NOT EXISTS car_state (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  car_id          INTEGER          NOT NULL,
  time            TIMESTAMPTZ      NOT NULL,   -- store UTC
  state_of_charge INTEGER,
  latitude        DOUBLE PRECISION,
  longitude       DOUBLE PRECISION,
  gear            INTEGER,
  speed           DOUBLE PRECISION,
  UNIQUE (car_id, time)
);
```

```sql
INSERT INTO car_state (car_id, time, state_of_charge, latitude, longitude, gear, speed)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (car_id, time) DO UPDATE SET
  state_of_charge = EXCLUDED.state_of_charge,
  latitude  = EXCLUDED.latitude,
  longitude = EXCLUDED.longitude,
  gear      = EXCLUDED.gear,
  speed     = EXCLUDED.speed;
```

We use `DO UPDATE`, **not** `DO NOTHING`: a late, out-of-order message must be able to *correct*
an already-written bucket. `DO NOTHING` would silently discard those corrections — fine for an
append-only feed, wrong for our self-correcting resampler.

> If `delta-helper` already creates `car_state`, we conform to its definition and add just the
> `UNIQUE (car_id, time)` index (still additive). Confirm via the §7 check in the analysis doc.

## 6. Reliability & delivery

| Hop | Mechanism | Failure handling |
|---|---|---|
| MQTT → collector | QoS 1 subscribe, auto-reconnect | redelivery on reconnect; dupes tolerated downstream |
| collector → RabbitMQ | persistent msgs + publisher confirms | publish only after confirm; reconnect channel |
| RabbitMQ → writer | durable queue, manual ack, bounded prefetch | unacked msgs requeued on crash |
| writer → Postgres | pooled `pg`, parameterized upsert, retry on transient errors | idempotent ⇒ safe to retry/redeliver |

**Effectively-once** persistence = at-least-once delivery + idempotent upsert.
**Graceful shutdown** on `SIGINT/SIGTERM`: stop consuming, flush, close channel/connection/pool.

## 7. Configuration (12-factor)

| Env var | Host value (running via `pnpm`) | In-compose value |
|---|---|---|
| `MQTT_URL` | `mqtt://localhost:51883` | `mqtt://mosquitto:1883` |
| `RABBITMQ_URL` | `amqp://admin:admin@localhost:55672` | `amqp://admin:admin@rabbitmq:5672` |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:55432/postgres` | `postgres://postgres:postgres@postgres:5432/postgres` |
| `CAR_ID` | `1` | `1` |
| `GRACE_MS` | `10000` | `10000` |
| `MAX_STALENESS_MS` | `300000` | `300000` |

> **Port gotcha:** the helper runs *inside* the compose network and uses container ports
> (`1883/5432/5672`); our scripts run on the *host* and must use the mapped ports
> (`51883/55432/55672`). Defaults target the host path since that's how `pnpm run` executes.

## 8. Tech choices

| Need | Choice | Why (and what we avoid) |
|---|---|---|
| MQTT client | `mqtt` (mqtt.js) | de-facto standard, reconnect built in |
| AMQP client | `amqplib` | minimal, battle-tested; avoid heavier `rascal` for this scope |
| Postgres | `pg` + raw SQL | one upsert statement; **avoid an ORM** (Prisma/TypeORM) — migrations/codegen are overhead here |
| Test runner | `node:test` (or `vitest`) | zero/!near-zero deps; pure functions need no framework |
| Runtime | `tsx` (already present) | run TS directly, no build step |

Total added runtime deps: **three**. That is a deliberate ceiling.

**Compiler strictness (free correctness).** `tsconfig` runs in `strict` mode with
`noUncheckedIndexedAccess` — the resampler indexes battery arrays (`soc[0]`, `capacities[i]`),
which is exactly where an unchecked index access would otherwise hide a bug. Zero runtime cost.

## 9. Project structure

Keep the two required entrypoints thin; factor pure logic into small, testable modules.

```
src/
  collector.ts        # subscribe → normalize → publish   (entrypoint)
  writer.ts           # consume → resample → upsert        (entrypoint)
  lib/
    config.ts         # env parsing, typed config
    topics.ts         # parse "car/1/battery/0/soc" → {carId, metric, batteryIndex}
    transforms.ts     # gearToInt, msToKmh, weightedSoc, bucketStart   (pure)
    resampler.ts      # rolling state + LOCF + watermark flush          (pure-ish, no I/O)
    queue.ts          # amqp connect/publish/consume helpers
    db.ts             # pg pool + ensureSchema + upsertRow
  types.ts            # TelemetryEvent, CarState
tests/
  transforms.test.ts
  resampler.test.ts   # out-of-order, gaps, forward-fill
```

The spec says "implement two files"; these stay the only entrypoints. The `lib/` split exists
purely so the hard logic is isolated and unit-tested — a readability win, not extra surface.

## 10. Testing strategy

- **Unit (the high-value core):** `gearToInt` (incl. `N`), `msToKmh`, `weightedSoc` (unequal
  capacities, single-battery → NULL), `bucketStart` alignment. Include adversarial inputs:
  **negative** latitude/longitude (valid — southern/western hemisphere), zeros, and duplicate
  readings.
- **Resampler behaviour:** feed out-of-order and sparse events; assert one row per 5 s, no
  gaps, correct forward-fill, and that a late event upserts the right bucket.
- **Integration (smoke):** `docker compose up`, run both services, then assert in SQL:
  ```sql
  SELECT count(*) FILTER (WHERE gap <> 5) AS bad_gaps
  FROM (SELECT EXTRACT(EPOCH FROM time - lag(time) OVER (ORDER BY time)) AS gap
        FROM car_state WHERE car_id = 1) s;   -- expect 0
  ```

## 11. Observability & ops

Structured logs (plain `console` with timestamps, or `pino` if preferred): messages
in/out, last bucket written, current lag (`maxTs − lastFlushed`), reconnect events. Enough to
see the pipeline is healthy and how far behind real time it is — no metrics stack for this scope.

## 12. Tradeoffs & alternatives considered

| Decision | Alternative | Why we chose ours |
|---|---|---|
| Bucketing in **writer** | Bucket in collector, dumb writer | Keeps state single-writer & ordered; matches the file responsibilities |
| **Watermark + upsert** | Buffer/sort everything, write once | Bounded memory; correctness independent of tuning; far simpler |
| Raw `pg` SQL | Prisma / an ORM | One statement vs. a migration/codegen toolchain; simpler to review |
| Single queue, single consumer | Per-metric queues / many consumers | Preserves ordering; nothing here needs the throughput |
| Learn capacities from stream, cache as constant | Hardcode a literal | Same "constant" the spec asks for, but correct without me hand-editing values; configured fallback covers startup |

## 13. Risks

| Risk | Mitigation |
|---|---|
| Payload has **no event timestamp** (breaks event-time model) | `readPayload` accepts both JSON-with-timestamp and bare scalars; falls back to receipt time. Confirm shape via analysis §7 |
| Helper **owns the schema** (it creates `car_state`) | Writer DDL is additive — `CREATE TABLE IF NOT EXISTS` + `CREATE UNIQUE INDEX IF NOT EXISTS`; it conforms and only adds the `(car_id, time)` index |
| Real **capacities differ from the fallback** | Writer caches the real values from the `capacity` topic; only the spec pre-first-message window uses the fallback |
| Grace window too short → premature rows | Upsert corrects them; tune `GRACE_MS` if needed |

## 14. Path to production (not built now)

Multi-car via a **topic exchange** keyed by `carId` and one writer (or consumer group) per
partition; dynamic battery discovery from `capacity` topics; a small query API; schema
migrations (`node-pg-migrate`); metrics (Prometheus) and alerting on pipeline lag. The current
design reaches each of these by extension, not rewrite.

## 15. Implementation milestones

1. **Explore** — `mosquitto_sub` the live stream; confirm payload shape, timestamp, capacities (analysis §7).
2. **Pure core** — `transforms.ts` + `resampler.ts` with unit tests (no I/O yet).
3. **Collector** — subscribe, parse, normalize, publish with confirms.
4. **Writer** — consume, resample, `ensureSchema`, upsert, manual ack.
5. **Reliability** — reconnects, graceful shutdown, structured logs.
6. **Verify** — integration smoke + the no-gaps SQL assertion; re-run to prove idempotency.
