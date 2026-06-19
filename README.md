# backend-home

A small data pipeline that ingests electric-vehicle telemetry from an MQTT broker,
routes it through RabbitMQ, and persists a gap-free **5-second** time series to Postgres.

## Components

- **`src/collector.ts`** — MQTT → RabbitMQ. Subscribes to the car's topics, normalizes each
  message into a domain event (gear → integer, speed → km/h), and publishes it. Malformed
  messages are dropped at the edge.
- **`src/writer.ts`** — RabbitMQ → Postgres. Resamples events onto a 5 s grid with
  last-observation forward-fill, a capacity-weighted state of charge, a watermark for
  out-of-order data, and an idempotent `INSERT … ON CONFLICT (car_id, time) DO UPDATE`.

The interesting logic lives in small, pure, unit-tested modules under `src/lib/`.

## Run

```sh
docker compose up -d        # infra + data simulator
pnpm install
pnpm run collector          # terminal 1: MQTT -> RabbitMQ
pnpm run writer             # terminal 2: RabbitMQ -> Postgres
pnpm test                   # unit tests for the transforms and resampler
```

Inspect the result — one row per 5 s, no gaps:

```sql
SELECT * FROM car_state WHERE car_id = 1 ORDER BY time;

-- no-gaps assertion (expect bad_gaps = 0):
SELECT count(*) FILTER (WHERE gap_s IS NOT NULL AND gap_s <> 5) AS bad_gaps
FROM (SELECT extract(epoch FROM time - lag(time) OVER (ORDER BY time)) AS gap_s
      FROM car_state WHERE car_id = 1) t;
```