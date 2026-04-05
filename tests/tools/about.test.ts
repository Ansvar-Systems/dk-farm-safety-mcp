import { describe, test, expect } from 'vitest';
import { handleAbout } from '../../src/tools/about.js';

describe('about tool', () => {
  test('returns server metadata', () => {
    const result = handleAbout();
    expect(result.name).toBe('UK Farm Safety MCP');
    expect(result.description).toContain('safety');
    expect(result.jurisdiction).toEqual(['GB']);
    expect(result.tools_count).toBe(10);
    expect(result.links).toHaveProperty('homepage');
    expect(result.links).toHaveProperty('repository');
    expect(result._meta).toHaveProperty('disclaimer');
    expect(result._meta.disclaimer).toContain('HSE');
  });
});
