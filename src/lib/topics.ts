import type { Metric } from '../types';

export interface ParsedTopic {
  carId: number;
  metric: Metric;
  /** Present only for `car/<id>/battery/<index>/...` topics. */
  batteryIndex?: number;
}

/**
 * Parse an MQTT topic into a structured descriptor, or return `null` for topics
 * we don't recognise or that are malformed.
 *
 *   car/1/location/latitude  -> { carId: 1, metric: 'latitude' }
 *   car/1/speed              -> { carId: 1, metric: 'speed' }
 *   car/1/battery/0/soc      -> { carId: 1, metric: 'soc', batteryIndex: 0 }
 */
export function parseTopic(topic: string): ParsedTopic | null {
  const parts = topic.split('/');
  if (parts[0] !== 'car') return null;

  const carId = Number(parts[1]);
  if (!Number.isInteger(carId)) return null;

  // car/<id>/location/<latitude|longitude>
  if (parts.length === 4 && parts[2] === 'location') {
    if (parts[3] === 'latitude') return { carId, metric: 'latitude' };
    if (parts[3] === 'longitude') return { carId, metric: 'longitude' };
    return null;
  }

  // car/<id>/speed | car/<id>/gear
  if (parts.length === 3) {
    if (parts[2] === 'speed') return { carId, metric: 'speed' };
    if (parts[2] === 'gear') return { carId, metric: 'gear' };
    return null;
  }

  // car/<id>/battery/<index>/<soc|capacity>
  if (parts.length === 5 && parts[2] === 'battery') {
    const batteryIndex = Number(parts[3]);
    if (!Number.isInteger(batteryIndex)) return null;
    if (parts[4] === 'soc') return { carId, metric: 'soc', batteryIndex };
    if (parts[4] === 'capacity') return { carId, metric: 'capacity', batteryIndex };
    return null;
  }

  return null;
}
