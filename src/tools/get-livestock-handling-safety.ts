import { buildMeta } from '../metadata.js';
import { buildCitation } from '../citation.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface LivestockArgs {
  species: string;
  activity?: string;
  jurisdiction?: string;
}

export function handleGetLivestockHandlingSafety(db: Database, args: LivestockArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  let sql = `SELECT * FROM safety_guidance WHERE LOWER(species) LIKE LOWER(?) AND jurisdiction = ?`;
  const params: unknown[] = [`%${args.species}%`, jv.jurisdiction];

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
      message: `No livestock handling guidance found for '${args.species}'. Try 'cattle', 'sheep', 'pigs', or 'horses'.`,
    };
  }

  return {
    species: args.species,
    jurisdiction: jv.jurisdiction,
    results_count: results.length,
    results: results.map(r => ({
      topic: r.topic,
      species: r.species,
      hazards: r.hazards,
      control_measures: r.control_measures,
      legal_requirements: r.legal_requirements,
      ppe_required: r.ppe_required,
      regulation_ref: r.regulation_ref,
    })),
    _meta: buildMeta(),
    _citation: buildCitation(
      `DK Livestock Handling Safety — ${args.species}`,
      `Danish livestock handling safety for ${args.species}`,
      'get_livestock_handling_safety',
      { species: args.species },
    ),
  };
}
