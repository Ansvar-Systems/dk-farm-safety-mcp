import { buildMeta } from '../metadata.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface MachineryArgs {
  machine_type: string;
  activity?: string;
  jurisdiction?: string;
}

export function handleGetMachinerySafety(db: Database, args: MachineryArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  let sql = `SELECT * FROM safety_guidance WHERE LOWER(machine_type) LIKE LOWER(?) AND jurisdiction = ?`;
  const params: unknown[] = [`%${args.machine_type}%`, jv.jurisdiction];

  if (args.activity) {
    sql += ` AND (LOWER(topic) LIKE LOWER(?) OR LOWER(hazards) LIKE LOWER(?) OR LOWER(control_measures) LIKE LOWER(?))`;
    params.push(`%${args.activity}%`, `%${args.activity}%`, `%${args.activity}%`);
  }

  sql += ' ORDER BY topic';

  const results = db.all<{
    id: number; topic: string; machine_type: string; species: string;
    hazards: string; control_measures: string; legal_requirements: string;
    ppe_required: string; regulation_ref: string; jurisdiction: string;
  }>(sql, params);

  if (results.length === 0) {
    return {
      error: 'not_found',
      message: `No machinery safety guidance found for '${args.machine_type}'. Try broader terms like 'tractor', 'atv', or 'chainsaw'.`,
    };
  }

  return {
    machine_type: args.machine_type,
    jurisdiction: jv.jurisdiction,
    results_count: results.length,
    results: results.map(r => ({
      topic: r.topic,
      machine_type: r.machine_type,
      hazards: r.hazards,
      control_measures: r.control_measures,
      legal_requirements: r.legal_requirements,
      ppe_required: r.ppe_required,
      regulation_ref: r.regulation_ref,
    })),
    _meta: buildMeta(),
  };
}
