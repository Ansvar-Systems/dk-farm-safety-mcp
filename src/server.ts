#!/usr/bin/env node

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';
import { createDatabase } from './db.js';
import { handleAbout } from './tools/about.js';
import { handleListSources } from './tools/list-sources.js';
import { handleCheckFreshness } from './tools/check-freshness.js';
import { handleSearchSafetyGuidance } from './tools/search-safety-guidance.js';
import { handleGetMachinerySafety } from './tools/get-machinery-safety.js';
import { handleGetLivestockHandlingSafety } from './tools/get-livestock-handling-safety.js';
import { handleGetCoshhRequirements } from './tools/get-coshh-requirements.js';
import { handleGetChildrenOnFarmsRules } from './tools/get-children-on-farms-rules.js';
import { handleGetReportingRequirements } from './tools/get-reporting-requirements.js';
import { handleGetRiskAssessmentTemplate } from './tools/get-risk-assessment-template.js';

const SERVER_NAME = 'dk-farm-safety-mcp';
const SERVER_VERSION = '0.1.0';

const TOOLS = [
  {
    name: 'about',
    description: 'Get server metadata: name, version, coverage, data sources, and links.',
    inputSchema: { type: 'object' as const, properties: {} },
  },
  {
    name: 'list_sources',
    description: 'List all data sources with authority, URL, license, and freshness info.',
    inputSchema: { type: 'object' as const, properties: {} },
  },
  {
    name: 'check_data_freshness',
    description: 'Check when data was last ingested, staleness status, and how to trigger a refresh.',
    inputSchema: { type: 'object' as const, properties: {} },
  },
  {
    name: 'search_safety_guidance',
    description: 'Full-text search across all farm safety guidance including machinery, livestock, COSHH, and risk assessments.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        query: { type: 'string', description: 'Free-text search query (e.g. "tractor rollover", "slurry gas")' },
        topic: { type: 'string', description: 'Filter by topic (e.g. machinery, livestock, coshh)' },
        jurisdiction: { type: 'string', description: 'ISO 3166-1 alpha-2 code (default: GB)' },
        limit: { type: 'number', description: 'Max results (default: 10, max: 50)' },
      },
      required: ['query'],
    },
  },
  {
    name: 'get_machinery_safety',
    description: 'Get safety guidance for farm machinery: hazards, control measures, PPE, and legal requirements. Covers tractors, ATVs, chainsaws, combines, and more.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        machine_type: { type: 'string', description: 'Type of machinery (e.g. tractor, atv, chainsaw, combine)' },
        activity: { type: 'string', description: 'Specific activity filter (e.g. pto, rollover, maintenance)' },
        jurisdiction: { type: 'string', description: 'ISO 3166-1 alpha-2 code (default: GB)' },
      },
      required: ['machine_type'],
    },
  },
  {
    name: 'get_livestock_handling_safety',
    description: 'Get safety guidance for handling livestock: cattle, sheep, pigs, horses. Includes hazards, control measures, and facility requirements.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        species: { type: 'string', description: 'Animal species (e.g. cattle, sheep, pigs, horses)' },
        activity: { type: 'string', description: 'Specific activity filter (e.g. calving, dipping, handling)' },
        jurisdiction: { type: 'string', description: 'ISO 3166-1 alpha-2 code (default: GB)' },
      },
      required: ['species'],
    },
  },
  {
    name: 'get_coshh_requirements',
    description: 'Get COSHH (Control of Substances Hazardous to Health) requirements for agricultural substances: pesticides, sheep dip, fuel, grain dust, ammonia.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        substance: { type: 'string', description: 'Substance type (e.g. pesticide, sheep dip, diesel, grain dust)' },
        activity: { type: 'string', description: 'Activity involving the substance (e.g. spraying, dipping, storage)' },
        jurisdiction: { type: 'string', description: 'ISO 3166-1 alpha-2 code (default: GB)' },
      },
    },
  },
  {
    name: 'get_children_on_farms_rules',
    description: 'Get rules about children working on or visiting farms. Age-group restrictions, permitted activities, supervision requirements under CHAW Regulations.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        age_group: { type: 'string', description: 'Age group (e.g. under-13, 13-15, 16-17)' },
        activity: { type: 'string', description: 'Activity (e.g. tractor, livestock, machinery)' },
        jurisdiction: { type: 'string', description: 'ISO 3166-1 alpha-2 code (default: GB)' },
      },
    },
  },
  {
    name: 'get_reporting_requirements',
    description: 'Get RIDDOR incident reporting requirements: what to report, deadlines, notification methods, and record retention.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        incident_type: { type: 'string', description: 'Incident type (e.g. fatal, specified injury, dangerous occurrence, occupational disease)' },
        jurisdiction: { type: 'string', description: 'ISO 3166-1 alpha-2 code (default: GB)' },
      },
      required: ['incident_type'],
    },
  },
  {
    name: 'get_risk_assessment_template',
    description: 'Get a risk assessment template for a farm activity: hazards, controls, residual risk, and review frequency.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        activity: { type: 'string', description: 'Farm activity (e.g. tractor operation, cattle handling, chainsaw use, pesticide application)' },
        jurisdiction: { type: 'string', description: 'ISO 3166-1 alpha-2 code (default: GB)' },
      },
      required: ['activity'],
    },
  },
];

const SearchArgsSchema = z.object({
  query: z.string(),
  topic: z.string().optional(),
  jurisdiction: z.string().optional(),
  limit: z.number().optional(),
});

const MachineryArgsSchema = z.object({
  machine_type: z.string(),
  activity: z.string().optional(),
  jurisdiction: z.string().optional(),
});

const LivestockArgsSchema = z.object({
  species: z.string(),
  activity: z.string().optional(),
  jurisdiction: z.string().optional(),
});

const CoshhArgsSchema = z.object({
  substance: z.string().optional(),
  activity: z.string().optional(),
  jurisdiction: z.string().optional(),
});

const ChildrenArgsSchema = z.object({
  age_group: z.string().optional(),
  activity: z.string().optional(),
  jurisdiction: z.string().optional(),
});

const ReportingArgsSchema = z.object({
  incident_type: z.string(),
  jurisdiction: z.string().optional(),
});

const RiskAssessmentArgsSchema = z.object({
  activity: z.string(),
  jurisdiction: z.string().optional(),
});

function textResult(data: unknown) {
  return { content: [{ type: 'text' as const, text: JSON.stringify(data, null, 2) }] };
}

function errorResult(message: string) {
  return { content: [{ type: 'text' as const, text: JSON.stringify({ error: message }) }], isError: true };
}

const db = createDatabase();

const server = new Server(
  { name: SERVER_NAME, version: SERVER_VERSION },
  { capabilities: { tools: {} } }
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: TOOLS }));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;

  try {
    switch (name) {
      case 'about':
        return textResult(handleAbout());
      case 'list_sources':
        return textResult(handleListSources(db));
      case 'check_data_freshness':
        return textResult(handleCheckFreshness(db));
      case 'search_safety_guidance':
        return textResult(handleSearchSafetyGuidance(db, SearchArgsSchema.parse(args)));
      case 'get_machinery_safety':
        return textResult(handleGetMachinerySafety(db, MachineryArgsSchema.parse(args)));
      case 'get_livestock_handling_safety':
        return textResult(handleGetLivestockHandlingSafety(db, LivestockArgsSchema.parse(args)));
      case 'get_coshh_requirements':
        return textResult(handleGetCoshhRequirements(db, CoshhArgsSchema.parse(args)));
      case 'get_children_on_farms_rules':
        return textResult(handleGetChildrenOnFarmsRules(db, ChildrenArgsSchema.parse(args)));
      case 'get_reporting_requirements':
        return textResult(handleGetReportingRequirements(db, ReportingArgsSchema.parse(args)));
      case 'get_risk_assessment_template':
        return textResult(handleGetRiskAssessmentTemplate(db, RiskAssessmentArgsSchema.parse(args)));
      default:
        return errorResult(`Unknown tool: ${name}`);
    }
  } catch (err) {
    return errorResult(err instanceof Error ? err.message : String(err));
  }
});

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((err) => {
  process.stderr.write(`Fatal error: ${err.message}\n`);
  process.exit(1);
});
