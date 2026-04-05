import { buildMeta } from '../metadata.js';
import type { Database } from '../db.js';

interface Source {
  name: string;
  authority: string;
  official_url: string;
  retrieval_method: string;
  update_frequency: string;
  license: string;
  coverage: string;
  last_retrieved?: string;
}

export function handleListSources(db: Database): { sources: Source[]; _meta: ReturnType<typeof buildMeta> } {
  const lastIngest = db.get<{ value: string }>('SELECT value FROM db_metadata WHERE key = ?', ['last_ingest']);

  const sources: Source[] = [
    {
      name: 'HSE Agriculture Guidance',
      authority: 'Health and Safety Executive',
      official_url: 'https://www.hse.gov.uk/agriculture/',
      retrieval_method: 'SEED_DATA',
      update_frequency: 'periodic',
      license: 'Open Government Licence v3',
      coverage: 'Machinery safety, livestock handling, working at height, confined spaces, lone working',
      last_retrieved: lastIngest?.value,
    },
    {
      name: 'RIDDOR Regulations',
      authority: 'Health and Safety Executive',
      official_url: 'https://www.hse.gov.uk/riddor/',
      retrieval_method: 'SEED_DATA',
      update_frequency: 'periodic',
      license: 'Open Government Licence v3',
      coverage: 'Incident reporting requirements, deadlines, notification methods, record retention',
      last_retrieved: lastIngest?.value,
    },
    {
      name: 'COSHH in Agriculture',
      authority: 'Health and Safety Executive',
      official_url: 'https://www.hse.gov.uk/agriculture/topics/coshh.htm',
      retrieval_method: 'SEED_DATA',
      update_frequency: 'periodic',
      license: 'Open Government Licence v3',
      coverage: 'Pesticides, sheep dip, veterinary medicines, fuel, ammonia, grain dust, wood preservatives',
      last_retrieved: lastIngest?.value,
    },
    {
      name: 'PUWER (Provision and Use of Work Equipment Regulations)',
      authority: 'Health and Safety Executive',
      official_url: 'https://www.hse.gov.uk/work-equipment-machinery/puwer.htm',
      retrieval_method: 'SEED_DATA',
      update_frequency: 'periodic',
      license: 'Open Government Licence v3',
      coverage: 'Work equipment safety requirements, maintenance, guarding, training',
      last_retrieved: lastIngest?.value,
    },
    {
      name: 'Farm Safety Foundation (Yellow Wellies)',
      authority: 'Farm Safety Foundation',
      official_url: 'https://www.yellowwellies.org/',
      retrieval_method: 'SEED_DATA',
      update_frequency: 'periodic',
      license: 'Public information',
      coverage: 'Farm safety awareness, young farmer guidance, mental health resources',
      last_retrieved: lastIngest?.value,
    },
  ];

  return {
    sources,
    _meta: buildMeta({ source_url: 'https://www.hse.gov.uk/agriculture/' }),
  };
}
