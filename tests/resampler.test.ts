import { describe, it, expect } from 'vitest';
import { Resampler } from '../src/lib/resampler';
import type { CarStateRow, TelemetryEvent } from '../src/types';

const ev = (metric: TelemetryEvent['metric'], value: number, ts: number, batteryIndex?: number): TelemetryEvent => ({
  carId: 1,
  metric,
  value,
  ts,
  ...(batteryIndex === undefined ? {} : { batteryIndex }),
});

/** Feed events, then flush, collecting the emitted rows. */
async function run(events: TelemetryEvent[], options = {}): Promise<CarStateRow[]> {
  const resampler = new Resampler({ carId: 1, bucketMs: 5000, graceMs: 0, maxStalenessMs: 60_000, ...options });
  const rows: CarStateRow[] = [];
  for (const e of events) resampler.apply(e);
  await resampler.flush((row) => void rows.push(row), { final: true });
  return rows;
}

describe('Resampler', () => {
  it('produces dense 5s rows with forward-fill and weighted SoC', async () => {
    const rows = await run([
      ev('latitude', 50, 0),
      ev('longitude', 14, 0),
      ev('gear', 2, 0),
      ev('speed', 36, 0),
      ev('soc', 80, 0, 0),
      ev('capacity', 1000, 0, 0),
      ev('soc', 60, 0, 1),
      ev('capacity', 3000, 0, 1),
      ev('speed', 72, 7000),
      ev('gear', 3, 12000),
    ]);

    // Ticks at 0, 5000, 10000 (12000 is the last event; its bucket is 10000).
    expect(rows.map((r) => r.time.getTime())).toEqual([0, 5000, 10000]);

    // SoC is capacity-weighted: (80*1000 + 60*3000) / 4000 = 65.
    expect(rows.every((r) => r.stateOfCharge === 65)).toBe(true);

    // Gear and position carry forward; gear change at 12000 isn't visible until its tick.
    expect(rows.map((r) => r.gear)).toEqual([2, 2, 2]);
    expect(rows.every((r) => r.latitude === 50 && r.longitude === 14)).toBe(true);

    // Speed forward-fills 36 until the 7000 reading lands in the 10000 bucket.
    expect(rows.map((r) => r.speed)).toEqual([36, 36, 72]);
  });

  it('is order-independent: shuffled arrival yields the same rows', async () => {
    const events = [
      ev('latitude', 50, 0),
      ev('speed', 36, 0),
      ev('speed', 72, 7000),
      ev('gear', 2, 0),
    ];
    const inOrder = await run(events);
    const shuffled = await run([events[2]!, events[0]!, events[3]!, events[1]!]);
    expect(shuffled).toEqual(inOrder);
  });

  it('leaves no gaps even across windows with no data', async () => {
    const rows = await run([ev('speed', 18, 0), ev('speed', 36, 20000)]);
    expect(rows.map((r) => r.time.getTime())).toEqual([0, 5000, 10000, 15000, 20000]);
    // The quiet middle windows still carry the last known speed forward.
    expect(rows.map((r) => r.speed)).toEqual([18, 18, 18, 18, 36]);
  });

  it('suppresses leading all-null rows before any data is observed', async () => {
    // The first reading lands at ts=3000, which floors into the 0 bucket; that
    // 0 tick has nothing as-of it yet and must not be emitted as an empty row.
    const rows = await run([ev('speed', 10, 3000), ev('speed', 20, 12000)]);
    expect(rows.map((r) => r.time.getTime())).toEqual([5000, 10000]);
    expect(rows[0]!.speed).toBe(10);
  });

  it('writes NULL once a value passes the staleness horizon', async () => {
    const rows = await run(
      [ev('speed', 18, 0), ev('speed', 36, 20000)],
      { maxStalenessMs: 5000 },
    );
    // From tick 10000 the last speed (ts 0) is >5s old -> NULL, until the
    // ts=20000 reading lands in the 20000 bucket. (The horizon outlives a single
    // missed reading but not a prolonged silence.)
    expect(rows.map((r) => r.speed)).toEqual([18, 18, null, null, 36]);
  });

  it('holds back unsettled ticks until the grace window passes', async () => {
    const resampler = new Resampler({ carId: 1, bucketMs: 5000, graceMs: 10_000, maxStalenessMs: 60_000 });
    const rows: CarStateRow[] = [];
    resampler.apply(ev('speed', 36, 0));
    resampler.apply(ev('speed', 72, 6000));
    await resampler.flush((row) => void rows.push(row)); // maxTs=6000, watermark=-4000

    expect(rows).toHaveLength(0); // nothing has settled yet
  });
});
