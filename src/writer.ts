import { config } from './lib/config';
import { Resampler } from './lib/resampler';
import { createConsumer } from './lib/queue';
import { ensureSchema, upsertRow, closeDb } from './lib/db';

/**
 * Writer: consume normalized events, resample them onto a dense 5s grid
 * (forward-filling sparse fields and computing the capacity-weighted SoC), and
 * upsert each row into `car_state`. The upsert key (car_id, time) makes the
 * whole pipeline idempotent and self-correcting under out-of-order data.
 */
async function main(): Promise<void> {
  await ensureSchema();
  const resampler = new Resampler();

  let lastMessageAt = Date.now();
  const watchdog = setInterval(() => {
    const idleMs = Date.now() - lastMessageAt;
    if (idleMs > config.idleTimeoutMs) {
      console.warn(
        `[writer] no events for ${Math.round(idleMs / 1000)}s — source may have stopped`,
      );
    }
  }, config.idleTimeoutMs);

  const consumer = await createConsumer(async (event) => {
    lastMessageAt = Date.now();
    resampler.apply(event);
    await resampler.flush(async (row) => {
      await upsertRow(row);
      console.log(
        `[writer] ${row.time.toISOString()} soc=${row.stateOfCharge} gear=${row.gear} ` +
          `speed=${row.speed === null ? null : row.speed.toFixed(1)}`,
      );
    });
  });

  console.log('[writer] consuming…');

  const shutdown = async () => {
    clearInterval(watchdog);
    await consumer.close();
    // Drop the grace window so the last buffered ticks are persisted on a clean stop.
    await resampler.flush((row) => upsertRow(row), { final: true });
    await closeDb();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('[writer] fatal', err);
  process.exit(1);
});
