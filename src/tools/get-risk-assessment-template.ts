import { buildMeta } from '../metadata.js';
import { buildCitation } from '../citation.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface RiskAssessmentArgs {
  activity: string;
  jurisdiction?: string;
}

export function handleGetRiskAssessmentTemplate(db: Database, args: RiskAssessmentArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  const results = db.all<{
    id: number; activity: string; hazards: string; controls: string;
    residual_risk: string; review_frequency: string; jurisdiction: string;
  }>(
    `SELECT * FROM risk_assessment_templates
     WHERE LOWER(activity) LIKE LOWER(?) AND jurisdiction = ?
     ORDER BY activity`,
    [`%${args.activity}%`, jv.jurisdiction]
  );

  if (results.length === 0) {
    return {
      error: 'not_found',
      message: `No risk assessment template found for '${args.activity}'. Try 'tractor', 'cattle', 'confined space', 'chainsaw', or 'pesticide'.`,
    };
  }

  return {
    activity: args.activity,
    jurisdiction: jv.jurisdiction,
    results_count: results.length,
    results: results.map(r => ({
      activity: r.activity,
      hazards: r.hazards,
      controls: r.controls,
      residual_risk: r.residual_risk,
      review_frequency: r.review_frequency,
    })),
    _meta: buildMeta(),
    _citation: buildCitation(
      `DK Risk Assessment — ${args.activity}`,
      `Danish risk assessment template for ${args.activity}`,
      'get_risk_assessment_template',
      { activity: args.activity },
    ),
  };
}
