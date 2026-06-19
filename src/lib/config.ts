/**
 * Centralised, environment-driven configuration.
 *
 * Every value defaults to the host-mapped ports from `docker-compose.yml`, so
 * `pnpm run collector` / `pnpm run writer` work with no setup. Override via env
 * to run inside the compose network or to tune behaviour.
 */
export const config = {
  /** The single car in scope. */
  carId: Number(process.env.CAR_ID ?? 1),

  mqttUrl: process.env.MQTT_URL ?? 'mqtt://localhost:51883',
  rabbitUrl: process.env.RABBITMQ_URL ?? 'amqp://admin:admin@localhost:55672',
  databaseUrl:
    process.env.DATABASE_URL ??
    'postgres://postgres:postgres@localhost:55432/postgres',

  /** Durable queue the collector publishes to and the writer consumes from. */
  queueName: process.env.QUEUE_NAME ?? 'car-events',

  /** Output granularity: one row every 5 seconds. */
  bucketMs: Number(process.env.BUCKET_MS ?? 5_000),

  /**
   * How long to wait past a 5s tick before writing it, letting ordinary
   * out-of-order messages settle. Arrivals later than this still self-correct
   * via the idempotent upsert, so this value only needs to be roughly right.
   */
  graceMs: Number(process.env.GRACE_MS ?? 10_000),

  /**
   * Carry-forward (LOCF) horizon. A value older than this is no longer
   * trustworthy, so the field is written as NULL instead of a stale reading —
   * this is also how a stalled source eventually shows up in the data.
   */
  maxStalenessMs: Number(process.env.MAX_STALENESS_MS ?? 5 * 60_000),

  /**
   * If no message arrives for this long, log that the source has gone quiet
   * ("if it stops sending data we must be able to eventually recognize it").
   */
  idleTimeoutMs: Number(process.env.IDLE_TIMEOUT_MS ?? 30_000),

  /**
   * Fallback battery capacities (Wh), used only until the real values are read
   * from the `car/<id>/battery/<i>/capacity` topic at runtime. Equal defaults
   * make the weighted average degrade gracefully to a simple average.
   */
  fallbackCapacitiesWh: [
    Number(process.env.BATTERY_0_CAPACITY_WH ?? 1000),
    Number(process.env.BATTERY_1_CAPACITY_WH ?? 1000),
  ] as number[],
} as const;
