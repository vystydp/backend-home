/** The metrics we extract from MQTT and carry through the pipeline. */
export type Metric =
  | 'latitude'
  | 'longitude'
  | 'speed'
  | 'gear'
  | 'soc'
  | 'capacity';

/**
 * One normalized observation derived from a single MQTT message.
 *
 * Values arrive already through the collector's *stateless* transforms:
 * `gear` is an integer (0-6) and `speed` is in km/h. `soc`, `capacity`,
 * `latitude` and `longitude` pass through unchanged. Cross-message work
 * (the weighted SoC and 5s resampling) happens later, in the writer.
 */
export interface TelemetryEvent {
  carId: number;
  metric: Metric;
  /** Present only for per-battery metrics (`soc`, `capacity`). */
  batteryIndex?: number;
  value: number;
  /** Event time in epoch milliseconds (UTC). */
  ts: number;
}

/** One fully-resolved row destined for the `car_state` table. */
export interface CarStateRow {
  carId: number;
  time: Date;
  stateOfCharge: number | null;
  latitude: number | null;
  longitude: number | null;
  gear: number | null;
  speed: number | null;
}
