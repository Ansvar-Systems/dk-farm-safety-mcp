import { buildMeta } from '../metadata.js';
import { SUPPORTED_JURISDICTIONS } from '../jurisdiction.js';

export function handleAbout() {
  return {
    name: 'Denmark Farm Safety MCP',
    description:
      'UK farm health and safety guidance via MCP. Covers HSE machinery safety, livestock handling, ' +
      'COSHH substance control, children on farms rules, RIDDOR incident reporting, and risk assessment templates.',
    version: '0.1.0',
    jurisdiction: [...SUPPORTED_JURISDICTIONS],
    data_sources: [
      'HSE Agriculture Guidance',
      'RIDDOR (Reporting of Injuries, Diseases and Dangerous Occurrences Regulations)',
      'COSHH (Control of Substances Hazardous to Health)',
      'PUWER (Provision and Use of Work Equipment Regulations)',
      'Farm Safety Foundation',
    ],
    tools_count: 10,
    links: {
      homepage: 'https://ansvar.eu/open-agriculture',
      repository: 'https://github.com/Ansvar-Systems/dk-farm-safety-mcp',
      mcp_network: 'https://ansvar.ai/mcp',
    },
    _meta: buildMeta(),
  };
}
