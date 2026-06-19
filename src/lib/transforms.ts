/**
 * Pure value transforms. No I/O, no state — the easy-to-test heart of the rules
 * the spec defines for the `car_state` output.
 */

/** Gear label to integer: `N` -> 0, `'1'..'6'` -> 1..6; anything else -> null. */
export function gearToInt(raw: string): number | null {
  const g = raw.trim().toUpperCase();
  if (g === 'N') return 0;
  const n = Number(g);
  return Number.isInteger(n) && n >= 1 && n <= 6 ? n : null;
}

/** Metres-per-second to kilometres-per-hour. */
export function msToKmh(ms: number): number {
  return ms * 3.6;
}

/**
 * Energy-weighted mean state of charge across batteries:
 *
 *     round( Σ(socᵢ · capᵢ) / Σ(capᵢ) )
 *
 * Uses only batteries that have a known SoC and a positive capacity, so a
 * single reporting battery still yields a result. Returns null when no battery
 * qualifies. Weighting by capacity (not a plain mean) gives the true pack-level
 * SoC whenever the two capacities differ.
 */
export function weightedSoc(
  socByBattery: ReadonlyMap<number, number>,
  capacityByBattery: ReadonlyMap<number, number>,
): number | null {
  let weightedSum = 0;
  let totalCapacity = 0;
  for (const [index, soc] of socByBattery) {
    const capacity = capacityByBattery.get(index);
    if (capacity === undefined || capacity <= 0) continue;
    weightedSum += soc * capacity;
    totalCapacity += capacity;
  }
  if (totalCapacity <= 0) return null;
  return Math.round(weightedSum / totalCapacity);
}

/** Floor a timestamp to the start of its bucket on an epoch-aligned grid. */
export function bucketStart(ts: number, bucketMs: number): number {
  return Math.floor(ts / bucketMs) * bucketMs;
}
