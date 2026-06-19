import { config } from './config';
import { bucketStart, weightedSoc } from './transforms';
import type { CarStateRow, TelemetryEvent } from '../types';

/** True when a snapshot carries no known data for any field. */
function isEmpty(row: CarStateRow): boolean {
  return (
    row.stateOfCharge === null &&
    row.latitude === null &&
    row.longitude === null &&
    row.gear === null &&
    row.speed === null
  );
}

interface Sample {
  ts: number;
  value: number;
}

/**
 * A bounded, time-ordered history of one field's observations. Keeps enough
 * recent history to answer "value as of tick T" correctly even when messages
 * arrive out of order, and prunes everything older than the carry-forward
 * horizon so memory stays flat.
 */
class Series {
  private samples: Sample[] = [];

  add(ts: number, value: number): void {
    const s = this.samples;
    const last = s[s.length - 1];

    // Fast path: in-order append (the common case).
    if (last === undefined || ts > last.ts) {
      s.push({ ts, value });
      return;
    }
    if (ts === last.ts) {
      s[s.length - 1] = { ts, value }; // replace duplicate timestamp
      return;
    }
    // Out-of-order: find the insertion point scanning back from the end.
    let i = s.length - 1;
    while (i >= 0 && s[i]!.ts > ts) i--;
    if (i >= 0 && s[i]!.ts === ts) s[i] = { ts, value };
    else s.splice(i + 1, 0, { ts, value });
  }

  /** The most recent sample at or before `t`, or null if none. */
  valueAsOf(t: number): Sample | null {
    let result: Sample | null = null;
    for (const sample of this.samples) {
      if (sample.ts <= t) result = sample;
      else break;
    }
    return result;
  }

  /** Drop samples older than `before`, keeping the latest one ≤ before as the carry-forward anchor. */
  prune(before: number): void {
    const s = this.samples;
    let keepFrom = 0;
    for (let i = 0; i < s.length; i++) {
      if (s[i]!.ts <= before) keepFrom = i;
      else break;
    }
    if (keepFrom > 0) this.samples = s.slice(keepFrom);
  }
}

export interface ResamplerOptions {
  carId?: number;
  bucketMs?: number;
  graceMs?: number;
  maxStalenessMs?: number;
}

/**
 * Reconstructs a dense, regular, complete time series from sparse, single-field,
 * out-of-order telemetry.
 *
 *   • event-time bucketing onto a 5s grid,
 *   • last-observation-carried-forward (so quiet fields still fill every row),
 *   • a watermark + grace window to absorb ordinary lateness,
 *   • a staleness horizon so a stalled source becomes NULLs rather than lies.
 *
 * Anything later than the grace window still lands correctly downstream because
 * the writer's upsert is keyed on (car_id, time).
 */
export class Resampler {
  private readonly carId: number;
  private readonly bucketMs: number;
  private readonly graceMs: number;
  private readonly maxStalenessMs: number;

  private readonly latitude = new Series();
  private readonly longitude = new Series();
  private readonly gear = new Series();
  private readonly speed = new Series();
  private readonly soc = new Map<number, Series>();

  /** Battery capacities (Wh), learned from the stream with a configured fallback. */
  private readonly capacities = new Map<number, number>();

  private minTs = Infinity;
  private maxTs = 0;
  private nextTick: number | undefined;
  private hasEmittedData = false;

  constructor(options: ResamplerOptions = {}) {
    this.carId = options.carId ?? config.carId;
    this.bucketMs = options.bucketMs ?? config.bucketMs;
    this.graceMs = options.graceMs ?? config.graceMs;
    this.maxStalenessMs = options.maxStalenessMs ?? config.maxStalenessMs;
  }

  /** Ingest one event, updating rolling state. */
  apply(event: TelemetryEvent): void {
    if (event.carId !== this.carId) return;
    this.minTs = Math.min(this.minTs, event.ts);
    this.maxTs = Math.max(this.maxTs, event.ts);

    switch (event.metric) {
      case 'latitude':
        this.latitude.add(event.ts, event.value);
        break;
      case 'longitude':
        this.longitude.add(event.ts, event.value);
        break;
      case 'gear':
        this.gear.add(event.ts, event.value);
        break;
      case 'speed':
        this.speed.add(event.ts, event.value);
        break;
      case 'soc':
        this.socSeries(event.batteryIndex ?? 0).add(event.ts, event.value);
        break;
      case 'capacity':
        if (event.batteryIndex !== undefined) this.capacities.set(event.batteryIndex, event.value);
        break;
    }
  }

  /**
   * Emit every 5s tick that has settled. With `final: true` the grace window is
   * dropped so all remaining buffered ticks flush (used on graceful shutdown).
   * `emit` is awaited once per ready row, in time order.
   */
  async flush(
    emit: (row: CarStateRow) => Promise<void> | void,
    options: { final?: boolean } = {},
  ): Promise<void> {
    if (this.minTs === Infinity) return;
    if (this.nextTick === undefined) this.nextTick = bucketStart(this.minTs, this.bucketMs);

    const watermark = options.final ? this.maxTs : this.maxTs - this.graceMs;
    const lastTick = bucketStart(watermark, this.bucketMs);

    for (let t = this.nextTick; t <= lastTick; t += this.bucketMs) {
      const row = this.snapshot(t);
      // Suppress leading all-null rows (ticks before any data has been observed).
      // Once real data has started, emit every tick to keep the series dense.
      if (this.hasEmittedData || !isEmpty(row)) {
        this.hasEmittedData = true;
        await emit(row);
      }
      this.nextTick = t + this.bucketMs;
    }
    this.pruneAll();
  }

  /** Build the complete, forward-filled snapshot for grid tick `t`. */
  snapshot(t: number): CarStateRow {
    return {
      carId: this.carId,
      time: new Date(t),
      latitude: this.fresh(this.latitude.valueAsOf(t), t),
      longitude: this.fresh(this.longitude.valueAsOf(t), t),
      gear: this.fresh(this.gear.valueAsOf(t), t),
      speed: this.fresh(this.speed.valueAsOf(t), t),
      stateOfCharge: this.socAsOf(t),
    };
  }

  private socSeries(batteryIndex: number): Series {
    let series = this.soc.get(batteryIndex);
    if (series === undefined) {
      series = new Series();
      this.soc.set(batteryIndex, series);
    }
    return series;
  }

  /** A carried-forward value, or null if older than the staleness horizon. */
  private fresh(sample: Sample | null, t: number): number | null {
    if (sample === null) return null;
    if (t - sample.ts > this.maxStalenessMs) return null;
    return sample.value;
  }

  private socAsOf(t: number): number | null {
    const socs = new Map<number, number>();
    const caps = new Map<number, number>();
    for (const [index, series] of this.soc) {
      const value = this.fresh(series.valueAsOf(t), t);
      if (value === null) continue;
      socs.set(index, value);
      caps.set(index, this.capacities.get(index) ?? config.fallbackCapacitiesWh[index] ?? 0);
    }
    return weightedSoc(socs, caps);
  }

  private pruneAll(): void {
    if (this.nextTick === undefined) return;
    const before = this.nextTick - this.maxStalenessMs - this.bucketMs;
    this.latitude.prune(before);
    this.longitude.prune(before);
    this.gear.prune(before);
    this.speed.prune(before);
    for (const series of this.soc.values()) series.prune(before);
  }
}
