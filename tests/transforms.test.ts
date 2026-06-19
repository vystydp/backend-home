import { describe, it, expect } from 'vitest';
import { gearToInt, msToKmh, weightedSoc, bucketStart } from '../src/lib/transforms';

describe('gearToInt', () => {
  it('maps N to 0 and digits to themselves', () => {
    expect(gearToInt('N')).toBe(0);
    expect(gearToInt('1')).toBe(1);
    expect(gearToInt('6')).toBe(6);
    expect(gearToInt(' n ')).toBe(0); // tolerant of whitespace/case
  });

  it('rejects out-of-range or garbage', () => {
    expect(gearToInt('7')).toBeNull();
    expect(gearToInt('0')).toBeNull(); // 0 is not a valid gear label; N represents neutral
    expect(gearToInt('R')).toBeNull();
    expect(gearToInt('')).toBeNull();
  });
});

describe('msToKmh', () => {
  it('scales by 3.6', () => {
    expect(msToKmh(0)).toBe(0);
    expect(msToKmh(10)).toBeCloseTo(36);
    expect(msToKmh(27.78)).toBeCloseTo(100.008);
  });
});

describe('weightedSoc', () => {
  const cap = new Map([
    [0, 1000],
    [1, 3000],
  ]);

  it('weights by capacity, not a plain average', () => {
    const soc = new Map([
      [0, 80],
      [1, 60],
    ]);
    // (80*1000 + 60*3000) / 4000 = 65, not the plain mean of 70
    expect(weightedSoc(soc, cap)).toBe(65);
  });

  it('uses a single reporting battery when only one is known', () => {
    expect(weightedSoc(new Map([[1, 50]]), cap)).toBe(50);
  });

  it('returns null when no battery qualifies', () => {
    expect(weightedSoc(new Map(), cap)).toBeNull();
    expect(weightedSoc(new Map([[0, 80]]), new Map([[0, 0]]))).toBeNull();
  });
});

describe('bucketStart', () => {
  it('floors to an epoch-aligned 5s grid', () => {
    expect(bucketStart(0, 5000)).toBe(0);
    expect(bucketStart(4999, 5000)).toBe(0);
    expect(bucketStart(5000, 5000)).toBe(5000);
    expect(bucketStart(7345, 5000)).toBe(5000);
  });
});
