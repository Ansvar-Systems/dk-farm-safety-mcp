import { describe, test, expect, beforeAll, afterAll } from 'vitest';
import { handleGetChildrenOnFarmsRules } from '../../src/tools/get-children-on-farms-rules.js';
import { createSeededDatabase } from '../helpers/seed-db.js';
import type { Database } from '../../src/db.js';
import { existsSync, unlinkSync } from 'fs';

const TEST_DB = 'tests/test-children-rules.db';

describe('get_children_on_farms_rules tool', () => {
  let db: Database;

  beforeAll(() => {
    db = createSeededDatabase(TEST_DB);
  });

  afterAll(() => {
    db.close();
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
  });

  test('returns all rules when no filters', () => {
    const result = handleGetChildrenOnFarmsRules(db, {});
    expect(result).toHaveProperty('results_count');
    expect((result as { results_count: number }).results_count).toBe(3);
  });

  test('filters by age group', () => {
    const result = handleGetChildrenOnFarmsRules(db, { age_group: 'under-13' });
    expect((result as { results_count: number }).results_count).toBe(1);
    const first = (result as { results: { age_group: string; permitted: boolean }[] }).results[0];
    expect(first.age_group).toBe('under-13');
    expect(first.permitted).toBe(false);
  });

  test('filters by activity', () => {
    const result = handleGetChildrenOnFarmsRules(db, { activity: 'tractor' });
    expect((result as { results_count: number }).results_count).toBeGreaterThan(0);
  });

  test('rejects unsupported jurisdiction', () => {
    const result = handleGetChildrenOnFarmsRules(db, { jurisdiction: 'US' });
    expect(result).toHaveProperty('error', 'jurisdiction_not_supported');
  });

  test('results have permitted as boolean', () => {
    const result = handleGetChildrenOnFarmsRules(db, {});
    if ('results' in result) {
      for (const r of (result as { results: { permitted: boolean }[] }).results) {
        expect(typeof r.permitted).toBe('boolean');
      }
    }
  });
});
