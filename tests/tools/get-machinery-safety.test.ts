import { describe, test, expect, beforeAll, afterAll } from 'vitest';
import { handleGetMachinerySafety } from '../../src/tools/get-machinery-safety.js';
import { createSeededDatabase } from '../helpers/seed-db.js';
import type { Database } from '../../src/db.js';
import { existsSync, unlinkSync } from 'fs';

const TEST_DB = 'tests/test-machinery-safety.db';

describe('get_machinery_safety tool', () => {
  let db: Database;

  beforeAll(() => {
    db = createSeededDatabase(TEST_DB);
  });

  afterAll(() => {
    db.close();
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
  });

  test('returns results for tractor', () => {
    const result = handleGetMachinerySafety(db, { machine_type: 'tractor' });
    expect(result).toHaveProperty('results_count');
    expect((result as { results_count: number }).results_count).toBeGreaterThan(0);
  });

  test('returns results for atv', () => {
    const result = handleGetMachinerySafety(db, { machine_type: 'atv' });
    expect(result).toHaveProperty('results_count');
    expect((result as { results_count: number }).results_count).toBeGreaterThan(0);
  });

  test('returns not_found for unknown machine', () => {
    const result = handleGetMachinerySafety(db, { machine_type: 'submarine' });
    expect(result).toHaveProperty('error', 'not_found');
  });

  test('rejects unsupported jurisdiction', () => {
    const result = handleGetMachinerySafety(db, { machine_type: 'tractor', jurisdiction = 'DK' });
    expect(result).toHaveProperty('error', 'jurisdiction_not_supported');
  });

  test('result includes hazards and control measures', () => {
    const result = handleGetMachinerySafety(db, { machine_type: 'tractor' });
    if ('results' in result) {
      const first = (result as { results: { hazards: string; control_measures: string }[] }).results[0];
      expect(first.hazards).toBeTruthy();
      expect(first.control_measures).toBeTruthy();
    }
  });
});
