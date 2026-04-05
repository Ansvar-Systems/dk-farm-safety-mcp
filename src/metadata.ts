export interface Meta {
  disclaimer: string;
  data_age: string;
  source_url: string;
  copyright: string;
  server: string;
  version: string;
}

const DISCLAIMER =
  'This server provides general guidance based on HSE publications. It does not constitute ' +
  'legal advice. Always consult current HSE guidance and your own risk assessments. ' +
  'In an emergency, call 999.';

export function buildMeta(overrides?: Partial<Meta>): Meta {
  return {
    disclaimer: DISCLAIMER,
    data_age: overrides?.data_age ?? 'unknown',
    source_url: overrides?.source_url ?? 'https://www.hse.gov.uk/agriculture/',
    copyright: 'Data sourced from HSE publications under Open Government Licence v3.0. Server: Apache-2.0 Ansvar Systems.',
    server: 'dk-farm-safety-mcp',
    version: '0.1.0',
    ...overrides,
  };
}
