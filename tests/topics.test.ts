import { describe, it, expect } from 'vitest';
import { parseTopic } from '../src/lib/topics';

describe('parseTopic', () => {
  it('parses location, speed and gear topics', () => {
    expect(parseTopic('car/1/location/latitude')).toEqual({ carId: 1, metric: 'latitude' });
    expect(parseTopic('car/1/location/longitude')).toEqual({ carId: 1, metric: 'longitude' });
    expect(parseTopic('car/1/speed')).toEqual({ carId: 1, metric: 'speed' });
    expect(parseTopic('car/1/gear')).toEqual({ carId: 1, metric: 'gear' });
  });

  it('parses per-battery topics with their index', () => {
    expect(parseTopic('car/1/battery/0/soc')).toEqual({ carId: 1, metric: 'soc', batteryIndex: 0 });
    expect(parseTopic('car/1/battery/1/capacity')).toEqual({
      carId: 1,
      metric: 'capacity',
      batteryIndex: 1,
    });
  });

  it('rejects unknown or malformed topics', () => {
    expect(parseTopic('car/1/unknown')).toBeNull();
    expect(parseTopic('car/x/speed')).toBeNull();
    expect(parseTopic('car/1/battery/x/soc')).toBeNull();
    expect(parseTopic('truck/1/speed')).toBeNull();
    expect(parseTopic('car/1/location')).toBeNull();
  });
});
