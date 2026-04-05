import { buildMeta } from '../metadata.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface ChildrenArgs {
  age_group?: string;
  activity?: string;
  jurisdiction?: string;
}

export function handleGetChildrenOnFarmsRules(db: Database, args: ChildrenArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  let sql = `SELECT * FROM children_rules WHERE jurisdiction = ?`;
  const params: unknown[] = [jv.jurisdiction];

  if (args.age_group) {
    sql += ` AND LOWER(age_group) LIKE LOWER(?)`;
    params.push(`%${args.age_group}%`);
  }

  if (args.activity) {
    sql += ` AND LOWER(activity) LIKE LOWER(?)`;
    params.push(`%${args.activity}%`);
  }

  sql += ' ORDER BY age_group, activity';

  const results = db.all<{
    id: number; age_group: string; activity: string;
    permitted: number; conditions: string; regulation_ref: string; jurisdiction: string;
  }>(sql, params);

  return {
    jurisdiction: jv.jurisdiction,
    results_count: results.length,
    results: results.map(r => ({
      age_group: r.age_group,
      activity: r.activity,
      permitted: r.permitted === 1,
      conditions: r.conditions,
      regulation_ref: r.regulation_ref,
    })),
    _meta: buildMeta(),
  };
}
