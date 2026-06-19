import { parseTopic } from './topics';
import { gearToInt, msToKmh } from './transforms';
import type { TelemetryEvent } from '../types';

/**
 * Turn one raw MQTT message into a normalized {@link TelemetryEvent}, applying
 * the stateless transforms (gear -> int, m/s -> km/h). Returns `null` for topics
 * we don't persist or for unparseable payloads — malformed input dies here at the
 * edge instead of poisoning the writer's state.
 *
 * `now` is injectable so the function stays pure and unit-testable.
 */
export function messageToEvent(
  topic: string,
  payload: Buffer,
  now: number = Date.now(),
): TelemetryEvent | null {
  const parsed = parseTopic(topic);
  if (parsed === null) return null;

  const { value: raw, ts } = readPayload(payload, now);
  if (raw === null) return null;

  let value: number | null;
  switch (parsed.metric) {
    case 'gear':
      value = gearToInt(raw);
      break;
    case 'speed': {
      const n = numeric(raw);
      value = n === null ? null : msToKmh(n);
      break;
    }
    default: // latitude, longitude, soc, capacity
      value = numeric(raw);
  }
  if (value === null || !Number.isFinite(value)) return null;

  const event: TelemetryEvent = { carId: parsed.carId, metric: parsed.metric, value, ts };
  if (parsed.batteryIndex !== undefined) event.batteryIndex = parsed.batteryIndex;
  return event;
}

/**
 * MQTT payloads may be a bare value (`"12.3"`, `"N"`) or JSON carrying an event
 * timestamp (`{ "value": 12.3, "timestamp": "..." }`). Support both, and fall
 * back to receipt time when no event time is present.
 */
export function readPayload(payload: Buffer, now: number): { value: string | null; ts: number } {
  const text = payload.toString().trim();

  if (text.startsWith('{')) {
    try {
      const obj = JSON.parse(text) as Record<string, unknown>;
      const v = obj.value ?? obj.v ?? null;
      const tsRaw = obj.timestamp ?? obj.ts ?? obj.time ?? null;
      return {
        value: v === null ? null : String(v),
        ts: tsRaw === null ? now : toEpochMs(tsRaw, now),
      };
    } catch {
      return { value: null, ts: now };
    }
  }

  return { value: text.length > 0 ? text : null, ts: now };
}

function toEpochMs(raw: unknown, now: number): number {
  if (typeof raw === 'number') return raw < 1e12 ? raw * 1000 : raw; // seconds vs ms
  const parsed = Date.parse(String(raw));
  return Number.isNaN(parsed) ? now : parsed;
}

function numeric(raw: string): number | null {
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}
