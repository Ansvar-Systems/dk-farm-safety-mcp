import { buildMeta } from '../metadata.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface CoshhArgs {
  substance?: string;
  activity?: string;
  jurisdiction?: string;
}

export function handleGetCoshhRequirements(db: Database, args: CoshhArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  let sql = `SELECT * FROM coshh_guidance WHERE jurisdiction = ?`;
  const params: unknown[] = [jv.jurisdiction];

  if (args.substance) {
    sql += ` AND (LOWER(substance_type) LIKE LOWER(?) OR LOWER(activity) LIKE LOWER(?))`;
    params.push(`%${args.substance}%`, `%${args.substance}%`);
  }

  if (args.activity) {
    sql += ` AND LOWER(activity) LIKE LOWER(?)`;
    params.push(`%${args.activity}%`);
  }

  sql += ' ORDER BY substance_type';

  const results = db.all<{
    id: number; substance_type: string; activity: string;
    assessment_required: number; ppe: string; storage_requirements: string;
    disposal_requirements: string; regulation_ref: string; jurisdiction: string;
  }>(sql, params);

  return {
    jurisdiction: jv.jurisdiction,
    results_count: results.length,
    results: results.map(r => ({
      substance_type: r.substance_type,
      activity: r.activity,
      assessment_required: r.assessment_required === 1,
      ppe: r.ppe,
      storage_requirements: r.storage_requirements,
      disposal_requirements: r.disposal_requirements,
      regulation_ref: r.regulation_ref,
    })),
    _meta: buildMeta(),
  };
}
