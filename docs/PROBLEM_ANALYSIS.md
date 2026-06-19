# Problem Analysis — EV Telemetry Pipeline

> MQTT → RabbitMQ → Postgres telemetry pipeline
> Companion document: [`DESIGN.md`](./DESIGN.md)

## 1. Goal in one sentence

Continuously ingest electric-vehicle telemetry from an unstable MQTT source, route it
through RabbitMQ, and persist it to Postgres as a **gap-free, 5-second time series** where
every row is a **complete, normalized snapshot** of the car's state.

## 2. What we are given

The repository ships the full runtime via `docker-compose.yml`; we only write two files
(`src/collector.ts`, `src/writer.ts`), both currently empty.

| Service | Image | Host port → container | Credentials |
|---|---|---|---|
| MQTT broker | `eclipse-mosquitto:2` | `51883 → 1883` | anonymous (`allow_anonymous true`) |
| Postgres | `postgres:18` | `55432 → 5432` | `postgres / postgres`, db `postgres` |
| RabbitMQ | `rabbitmq:4-management` | `55672 → 5672`, `55673 → 15672` | `admin / admin` |
| `delta-helper` | vendor image | — | the EV simulator: **publishes** telemetry to MQTT and is wired to Postgres |

**MQTT topics** (one value per message, per vehicle):

| Topic | Unit / domain | Notes |
|---|---|---|
| `car/{carId}/location/latitude` | degrees | |
| `car/{carId}/location/longitude` | degrees | |
| `car/{carId}/speed` | m/s | |
| `car/{carId}/gear` | `N,1,2,3,4,5,6` | **published only on change** |
| `car/{carId}/battery/{batteryIndex}/soc` | 0–100 % | per battery |
| `car/{carId}/battery/{batteryIndex}/capacity` | Wh | **constant** |

**Target table `car_state`:** `id`, `car_id`, `time` (timestamp), `state_of_charge` (int),
`latitude`, `longitude`, `gear` (int), `speed` (double precision).

## 3. Reframing — what makes this non-trivial

A naïve reading is "forward MQTT messages to a queue, then `INSERT` them." That fails the
spec, because the **shape of the input and the shape of the required output do not match**:

| | Input (MQTT) | Required output (`car_state`) |
|---|---|---|
| Granularity | irregular, whenever a sensor fires | exactly one row / **5 s** |
| Completeness | one **field** per message | a **full snapshot** (all fields) per row |
| Ordering | asynchronous, **out of order** | rows ordered & continuous in time |
| Density | sparse, bursty, with gaps | **no gaps** |

So the core problem is **stream resampling**: reconstruct a dense,
regular, complete time series from sparse, single-field, out-of-order observations. The
established technique is **event-time bucketing + last-observation-carried-forward (LOCF /
forward-fill)**, made robust with **idempotent upserts**. (Full mechanism in `DESIGN.md §4`.)

## 4. The hard parts, made explicit

| # | Challenge | Why it's hard | Implication for the design |
|---|---|---|---|
| H1 | **Out-of-order delivery** | A later message may describe an *earlier* moment | Process by **event time**, not arrival time; tolerate lateness |
| H2 | **Sparse, partial updates** | Each message sets one field; a 5 s row needs all of them | Maintain rolling per-field state; **forward-fill** the rest |
| H3 | **"No gaps"** | Some 5 s windows receive no message at all | Emit rows on a **5 s clock**, not on message arrival |
| H4 | **Gear only on change** | Long stretches with no gear message | Forward-fill is mandatory, not optional, for gear |
| H5 | **Multi-battery SoC** | Two batteries → one number | **Capacity-weighted** average (energy-correct) |
| H6 | **Unstable source** | Drops, reconnects, duplicates | Reconnect logic + **at-least-once + idempotent writes** |
| H7 | **Restart safety** | Re-running must not corrupt/duplicate rows | `UNIQUE(car_id, time)` + `UPSERT` ⇒ replay-safe |

## 5. Transformation spec (precise)

| Field | Source | Rule | Output type |
|---|---|---|---|
| `gear` | `.../gear` | `N → 0`, `'1'..'6' → 1..6` | integer |
| `speed` | `.../speed` | km/h `= m/s × 3.6` | double precision |
| `state_of_charge` | `.../battery/{i}/soc` + capacity | `round( Σ(socᵢ·capᵢ) / Σ(capᵢ) )` | integer |
| `latitude` / `longitude` | `.../location/*` | pass-through | double precision |
| `time` | event time | floor to 5 s grid (UTC) | timestamp |

**Why capacity-weighted SoC:** SoC is a *percentage of each pack's energy*. The pack-level
SoC that means "fraction of total stored energy" is the energy-weighted mean
`Σ(socᵢ·capᵢ)/Σ(capᵢ)` — a plain average would be wrong whenever the two capacities differ.

## 6. Assumptions (stated, to be confirmed empirically)

1. **A1 — Event time is available.** "Out of order" is only meaningful if a message carries
   an event timestamp distinct from arrival time. We assume the payload (or an MQTT property)
   provides one; if not, we fall back to receipt time and document the limitation. *This is
   the single most important thing to verify first.*
2. **A2 — Scope is `car_id = 1`, two batteries (`0`, `1`), constant capacities.** Per the
   spec's "read it from the topic, save it as a constant", the writer caches each capacity
   from the `capacity` topic at runtime (with a configured fallback) rather than a hand-edited
   literal — confirmed 20000 Wh / 15000 Wh live (§7).
3. **A3 — We own the `car_state` schema** unless the helper pre-creates it. The writer
   ensures the table idempotently (`CREATE TABLE IF NOT EXISTS`) and adds only an *additive*
   `UNIQUE(car_id, time)` constraint. If the helper owns it, we conform and add the index.
4. **A4 — Timestamps stored in UTC.**
5. **A5 — Services run on the host** (`pnpm run collector` / `writer`) and therefore connect
   via the **host-mapped ports** (`51883 / 55432 / 55672`), not the in-container ports the
   helper uses.

## 7. Verify-before-coding checklist — resolved against the live stack

The empirical checks below were run before/while implementing; findings are recorded here.

- [x] **Real payloads** (`mosquitto_sub … -t 'car/#' -v`): JSON `{"value": 37.77…}` — **no
      timestamp** (resolves A1). So the event-time model uses **receipt time**; `readPayload`
      still accepts an embedded `timestamp`/`ts` if a future source adds one.
- [x] **DB**: the helper **creates `car_state`** with the spec's schema (`id serial`, `time
      timestamp without time zone`, no `(car_id,time)` constraint) (resolves A3). The writer adds
      only the additive unique index and stores an explicit **UTC** literal.
- [x] **Capacities**: read live as **battery 0 = 20000 Wh, battery 1 = 15000 Wh** (they differ,
      so weighting genuinely matters). The writer caches them from the `capacity` topic at
      runtime, so no hand-edited constant is needed (resolves A2).
- [x] **Cadence**: the helper publishes **1000 points then stops** (~1 point / 2–3 s), which is
      why the idle watchdog matters; the 10 s grace window sits comfortably above the inter-point
      interval.

## 8. Out of scope (deliberately)

Multiple cars, dynamic battery discovery, a REST/query API, dashboards, auth, historical
backfill beyond what the helper replays, and horizontal scaling. The design keeps a clean
path to these (see `DESIGN.md §12`) without building them now.

## 9. Definition of done

- `docker compose up` brings up infra + helper; `pnpm run collector` and `pnpm run writer`
  run the pipeline.
- `car_state` fills for `car_id = 1` with **exactly one row per 5 s**, **no gaps**, ordered by
  `time`.
- Every row is complete and correctly transformed (gear int, speed km/h, capacity-weighted
  integer SoC, UTC time).
- **Re-running is safe** — no duplicate or conflicting rows (idempotent upsert).
- Pure transform functions are **unit-tested**; an integration check asserts 5 s spacing and
  no gaps.
- Out-of-order messages do not produce wrong rows (late data corrects via upsert).
