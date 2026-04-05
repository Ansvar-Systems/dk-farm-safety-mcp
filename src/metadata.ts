export interface Meta {
  disclaimer: string;
  data_age: string;
  source_url: string;
  copyright: string;
  server: string;
  version: string;
}

const DISCLAIMER =
  'Denne server giver generel vejledning baseret paa Arbejdstilsynets publikationer og BFA Groent ' +
  'branchevejledninger. Den udgoeor ikke juridisk raadgivning. Kontakt altid Arbejdstilsynet for ' +
  'aktuelle regler. Ved noedssituationer ring 112.';

export function buildMeta(overrides?: Partial<Meta>): Meta {
  return {
    disclaimer: DISCLAIMER,
    data_age: overrides?.data_age ?? 'unknown',
    source_url: overrides?.source_url ?? 'https://at.dk/regler/at-vejledninger/',
    copyright: 'Data sourced from Arbejdstilsynet and BFA Groent publications. Server: Apache-2.0 Ansvar Systems.',
    server: 'dk-farm-safety-mcp',
    version: '0.1.0',
    ...overrides,
  };
}
