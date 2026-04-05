import { describe, test, expect, beforeAll, afterAll } from 'vitest';
import { handleSearchSafetyGuidance } from '../../src/tools/search-safety-guidance.js';
import { createSeededDatabase } from '../helpers/seed-db.js';
import type { Database } from '../../src/db.js';
import { existsSync, unlinkSync } from 'fs';

const TEST_DB = 'tests/test-search-safety.db';

describe('search_safety_guidance tool', () => {
  let db: Database;

  beforeAll(() => {
    db = createSeededDatabase(TEST_DB);
  });

  afterAll(() => {
    db.close();
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
  });

  test('returns results for tractor query', () => {
    const result = handleSearchSafetyGuidance(db, { query: 'tractor' });
    expect(result).toHaveProperty('results_count');
    expect((result as { results_count: number }).results_count).toBeGreaterThan(0);
  });

  test('returns results for cattle query', () => {
    const result = handleSearchSafetyGuidance(db, { query: 'cattle' });
    expect(result).toHaveProperty('results_count');
    expect((result as { results_count: number }).results_count).toBeGreaterThan(0);
  });

  test('filters by topic', () => {
    const result = handleSearchSafetyGuidance(db, { query: 'ROPS seatbelt', topic: 'machinery' });
    if ('results' in result) {
      for (const r of (result as { results: { topic: string }[] }).results) {
        expect(r.topic).toBe('machinery');
      }
    }
  });

  test('rejects unsupported jurisdiction', () => {
    const result = handleSearchSafetyGuidance(db, { query: 'tractor', jurisdiction: 'FR' });
    expect(result).toHaveProperty('error', 'jurisdiction_not_supported');
  });

  test('includes _meta with disclaimer', () => {
    const result = handleSearchSafetyGuidance(db, { query: 'COSHH' });
    expect(result).toHaveProperty('_meta');
    if ('_meta' in result) {
      expect((result as { _meta: { disclaimer: string } })._meta.disclaimer).toContain('HSE');
    }
  });
});
