import { describe, it, expect } from 'vitest';
import { messageToEvent } from '../src/lib/parse';

const buf = (s: string) => Buffer.from(s);
const NOW = 1_700_000_000_000;

describe('messageToEvent', () => {
  it('applies stateless transforms (gear -> int, speed -> km/h)', () => {
    expect(messageToEvent('car/1/gear', buf('N'), NOW)).toEqual({
      carId: 1,
      metric: 'gear',
      value: 0,
      ts: NOW,
    });
    expect(messageToEvent('car/1/speed', buf('10'), NOW)).toEqual({
      carId: 1,
      metric: 'speed',
      value: 36,
      ts: NOW,
    });
  });

  it('passes through location and soc, tagging the battery index', () => {
    expect(messageToEvent('car/1/location/latitude', buf('-33.86'), NOW)).toEqual({
      carId: 1,
      metric: 'latitude',
      value: -33.86, // southern hemisphere — negatives are valid
      ts: NOW,
    });
    expect(messageToEvent('car/1/battery/1/soc', buf('72'), NOW)).toEqual({
      carId: 1,
      metric: 'soc',
      batteryIndex: 1,
      value: 72,
      ts: NOW,
    });
  });

  it('reads event time from a JSON payload when present', () => {
    const event = messageToEvent('car/1/speed', buf('{"value":10,"timestamp":1700000005000}'), NOW);
    expect(event).toEqual({ carId: 1, metric: 'speed', value: 36, ts: 1700000005000 });
  });

  it('falls back to receipt time when the payload carries no timestamp', () => {
    const event = messageToEvent('car/1/battery/0/soc', buf('{"value":80}'), NOW);
    expect(event).toEqual({ carId: 1, metric: 'soc', batteryIndex: 0, value: 80, ts: NOW });
  });

  it('drops unknown topics and unparseable values', () => {
    expect(messageToEvent('car/1/unknown', buf('1'), NOW)).toBeNull();
    expect(messageToEvent('car/1/speed', buf('fast'), NOW)).toBeNull();
    expect(messageToEvent('car/1/gear', buf('R'), NOW)).toBeNull();
  });
});
