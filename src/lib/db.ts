import pg from 'pg';
import { config } from './config';
import type { CarStateRow } from '../types';

const pool = new pg.Pool({ connectionString: config.databaseUrl });

/**
 * Format a Date as a UTC timestamp literal (no offset) for the
 * `timestamp without time zone` column, so storage is deterministic and
 * independent of the host timezone:
 * `2026-06-19T10:54:50.000Z` -> `"2026-06-19 10:54:50.000"`.
 */
function toUtcTimestamp(date: Date): string {
  return date.toISOString().replace('T', ' ').replace('Z', '');
}

/**
 * Ensure the table and the (car_id, time) uniqueness the upsert relies on.
 * The helper already creates `car_state`, so both statements are additive
 * no-ops in practice — they just make the writer runnable on its own too.
 */
export async function ensureSchema(): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS car_state (
      id              serial primary key,
      car_id          integer,
      time            timestamp,
      state_of_charge integer,
      latitude        double precision,
      longitude       double precision,
      gear            integer,
      speed           double precision
    )
  `);
  await pool.query(
    `CREATE UNIQUE INDEX IF NOT EXISTS car_state_car_id_time_key ON car_state (car_id, time)`,
  );
}

const UPSERT = `
  INSERT INTO car_state (car_id, time, state_of_charge, latitude, longitude, gear, speed)
  VALUES ($1, $2, $3, $4, $5, $6, $7)
  ON CONFLICT (car_id, time) DO UPDATE SET
    state_of_charge = EXCLUDED.state_of_charge,
    latitude        = EXCLUDED.latitude,
    longitude       = EXCLUDED.longitude,
    gear            = EXCLUDED.gear,
    speed           = EXCLUDED.speed
`;

/** Insert-or-correct one row. Idempotent: safe to replay, late data overwrites. */
export async function upsertRow(row: CarStateRow): Promise<void> {
  await pool.query(UPSERT, [
    row.carId,
    toUtcTimestamp(row.time),
    row.stateOfCharge,
    row.latitude,
    row.longitude,
    row.gear,
    row.speed,
  ]);
}

export async function closeDb(): Promise<void> {
  await pool.end();
}
