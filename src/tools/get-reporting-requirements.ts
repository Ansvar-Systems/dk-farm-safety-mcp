import { buildMeta } from '../metadata.js';
import { buildCitation } from '../citation.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface ReportingArgs {
  incident_type: string;
  jurisdiction?: string;
}

export function handleGetReportingRequirements(db: Database, args: ReportingArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  const results = db.all<{
    id: number; incident_type: string; reportable: number;
    deadline: string; notify: string; method: string;
    record_retention_years: number; regulation_ref: string; jurisdiction: string;
  }>(
    `SELECT * FROM reporting_requirements
     WHERE LOWER(incident_type) LIKE LOWER(?) AND jurisdiction = ?
     ORDER BY incident_type`,
    [`%${args.incident_type}%`, jv.jurisdiction]
  );

  if (results.length === 0) {
    return {
      error: 'not_found',
      message: `No reporting requirements found for '${args.incident_type}'. Try 'fatal', 'specified injury', 'dangerous occurrence', or 'occupational disease'.`,
    };
  }

  return {
    incident_type: args.incident_type,
    jurisdiction: jv.jurisdiction,
    results_count: results.length,
    results: results.map(r => ({
      incident_type: r.incident_type,
      reportable: r.reportable === 1,
      deadline: r.deadline,
      notify: r.notify,
      method: r.method,
      record_retention_years: r.record_retention_years,
      regulation_ref: r.regulation_ref,
    })),
    _meta: buildMeta(),
    _citation: buildCitation(
      `DK Reporting Requirements — ${args.incident_type}`,
      `Danish reporting requirements for ${args.incident_type}`,
      'get_reporting_requirements',
      { incident_type: args.incident_type },
    ),
  };
}
