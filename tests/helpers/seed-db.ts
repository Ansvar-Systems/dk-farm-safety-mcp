import { createDatabase, type Database } from '../../src/db.js';

export function createSeededDatabase(dbPath: string): Database {
  const db = createDatabase(dbPath);

  // Machinery safety
  db.run(
    `INSERT INTO safety_guidance (topic, machine_type, species, hazards, control_measures, legal_requirements, ppe_required, regulation_ref, jurisdiction)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      'Tractor rollover protection',
      'tractor',
      null,
      'Overturning on slopes, soft ground, or uneven terrain. Leading cause of fatal injury on UK farms.',
      'Fit ROPS to all tractors. Wear seatbelt at all times. Avoid steep slopes.',
      'PUWER 1998 reg. 25-26 requires ROPS on all tractors.',
      'Seatbelt (mandatory with ROPS fitted)',
      'PUWER 1998 reg. 25-26; HSE AIS25',
      'GB',
    ]
  );

  db.run(
    `INSERT INTO safety_guidance (topic, machine_type, species, hazards, control_measures, legal_requirements, ppe_required, regulation_ref, jurisdiction)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      'ATV/quad bike safety',
      'atv',
      null,
      'Overturning on slopes. Riders thrown off. No ROPS on most ATVs.',
      'Wear helmet at all times. Complete training course. Never carry passengers.',
      'PUWER 1998 requires employer to ensure adequate training.',
      'Helmet, boots, gloves',
      'PUWER 1998; HSE AIS33',
      'GB',
    ]
  );

  // Livestock safety
  db.run(
    `INSERT INTO safety_guidance (topic, machine_type, species, hazards, control_measures, legal_requirements, ppe_required, regulation_ref, jurisdiction)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      'Cattle handling in crush and race',
      null,
      'cattle',
      'Crushing against gates and walls. Kicking. Head butting. Trampling.',
      'Use properly maintained crush and race system. Approach cattle calmly.',
      'MHSW Regulations 1999 require risk assessment for cattle handling.',
      'Steel-toe boots, hard hat when working in crush',
      'MHSW Regulations 1999; HSE AIS35',
      'GB',
    ]
  );

  // Children rules
  db.run(
    `INSERT INTO children_rules (age_group, activity, permitted, conditions, regulation_ref, jurisdiction)
     VALUES (?, ?, ?, ?, ?, ?)`,
    ['under-13', 'Riding or driving tractors', 0, 'Absolutely prohibited.', 'CHAW 1998 reg. 3', 'GB']
  );
  db.run(
    `INSERT INTO children_rules (age_group, activity, permitted, conditions, regulation_ref, jurisdiction)
     VALUES (?, ?, ?, ?, ?, ?)`,
    ['13-15', 'Operating machinery (non-tractor)', 1, 'Permitted only with adequate training and direct supervision.', 'CHAW 1998 reg. 4', 'GB']
  );
  db.run(
    `INSERT INTO children_rules (age_group, activity, permitted, conditions, regulation_ref, jurisdiction)
     VALUES (?, ?, ?, ?, ?, ?)`,
    ['16-17', 'Driving tractors', 1, 'Permitted on-farm with ROPS-fitted tractor after training.', 'CHAW 1998', 'GB']
  );

  // COSHH
  db.run(
    `INSERT INTO coshh_guidance (substance_type, activity, assessment_required, ppe, storage_requirements, disposal_requirements, regulation_ref, jurisdiction)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      'Pesticides',
      'Spraying and application',
      1,
      'Chemical-resistant gloves, coveralls, face shield',
      'Locked store with bunded floor.',
      'Triple-rinse containers and recycle via approved scheme.',
      'COSHH 2002; HSE AIS16',
      'GB',
    ]
  );

  // Reporting
  db.run(
    `INSERT INTO reporting_requirements (incident_type, reportable, deadline, notify, method, record_retention_years, regulation_ref, jurisdiction)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      'Fatal injuries',
      1,
      'Immediately by phone',
      'HSE Incident Contact Centre',
      'Telephone then online form (F2508)',
      3,
      'RIDDOR 2013 reg. 6',
      'GB',
    ]
  );

  // Risk assessment
  db.run(
    `INSERT INTO risk_assessment_templates (activity, hazards, controls, residual_risk, review_frequency, jurisdiction)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [
      'Tractor operation',
      'Overturning; PTO entanglement; falling from cab',
      'ROPS fitted; seatbelt worn; PTO guards in place',
      'Low with controls in place.',
      'Annual',
      'GB',
    ]
  );

  // FTS5 search index
  db.run(
    `INSERT INTO search_index (title, body, topic, jurisdiction) VALUES (?, ?, ?, ?)`,
    ['Tractor rollover protection', 'Overturning on slopes and soft ground. Fit ROPS to all tractors. Wear seatbelt. PUWER 1998.', 'machinery', 'GB']
  );
  db.run(
    `INSERT INTO search_index (title, body, topic, jurisdiction) VALUES (?, ?, ?, ?)`,
    ['Cattle handling safety', 'Crushing against gates. Kicking. Use crush and race system. MHSW Regulations.', 'livestock', 'GB']
  );
  db.run(
    `INSERT INTO search_index (title, body, topic, jurisdiction) VALUES (?, ?, ?, ?)`,
    ['COSHH Pesticides', 'Chemical-resistant gloves. Locked store. COSHH 2002 assessment required.', 'coshh', 'GB']
  );

  // Metadata
  const today = new Date().toISOString().split('T')[0];
  db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('last_ingest', ?)", [today]);
  db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('build_date', ?)", [today]);

  return db;
}
