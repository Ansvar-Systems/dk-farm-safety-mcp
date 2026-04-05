# Tools Reference

## Meta Tools

### `about`

Get server metadata: name, version, coverage, data sources, and links.

**Parameters:** None

**Returns:** Server name, version, jurisdiction list, data source names, tool count, homepage/repository links.

---

### `list_sources`

List all data sources with authority, URL, license, and freshness info.

**Parameters:** None

**Returns:** Array of data sources, each with `name`, `authority`, `official_url`, `retrieval_method`, `update_frequency`, `license`, `coverage`, `last_retrieved`.

---

### `check_data_freshness`

Check when data was last ingested, staleness status, and how to trigger a refresh.

**Parameters:** None

**Returns:** `status` (fresh/stale/unknown), `last_ingest`, `days_since_ingest`, `staleness_threshold_days`, `refresh_command`.

---

## Domain Tools

### `search_safety_guidance`

Full-text search across all farm safety guidance including machinery, livestock, COSHH, and risk assessments.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | Yes | Free-text search query (e.g. "tractor rollover", "slurry gas") |
| `topic` | string | No | Filter by topic (e.g. machinery, livestock, coshh, riddor, risk-assessment) |
| `jurisdiction` | string | No | ISO 3166-1 alpha-2 code (default: GB) |
| `limit` | number | No | Max results (default: 10, max: 50) |

**Example:** `{ "query": "tractor rollover protection" }`

---

### `get_machinery_safety`

Get safety guidance for farm machinery: hazards, control measures, PPE, and legal requirements.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `machine_type` | string | Yes | Type of machinery (e.g. tractor, atv, chainsaw, combine, baler, telehandler, woodchipper, auger, grain dryer, slurry tanker) |
| `activity` | string | No | Specific activity filter (e.g. pto, rollover, maintenance) |
| `jurisdiction` | string | No | ISO 3166-1 alpha-2 code (default: GB) |

**Returns:** Hazards, control measures, legal requirements (with regulation references), PPE requirements.

**Example:** `{ "machine_type": "tractor", "activity": "pto" }`

---

### `get_livestock_handling_safety`

Get safety guidance for handling livestock: cattle, sheep, pigs, horses.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `species` | string | Yes | Animal species (e.g. cattle, sheep, pigs, horses) |
| `activity` | string | No | Specific activity filter (e.g. calving, dipping, handling) |
| `jurisdiction` | string | No | ISO 3166-1 alpha-2 code (default: GB) |

**Returns:** Hazards, control measures, legal requirements, PPE requirements.

**Example:** `{ "species": "cattle", "activity": "calving" }`

---

### `get_coshh_requirements`

Get COSHH requirements for agricultural substances.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `substance` | string | No | Substance type (e.g. pesticide, sheep dip, diesel, grain dust, ammonia, rodenticide) |
| `activity` | string | No | Activity involving the substance (e.g. spraying, dipping, storage) |
| `jurisdiction` | string | No | ISO 3166-1 alpha-2 code (default: GB) |

**Returns:** Whether COSHH assessment is required, PPE, storage requirements, disposal requirements, regulation references.

**Example:** `{ "substance": "sheep dip" }`

---

### `get_children_on_farms_rules`

Get rules about children working on or visiting farms.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `age_group` | string | No | Age group (e.g. under-13, 13-15, 16-17) |
| `activity` | string | No | Activity (e.g. tractor, livestock, machinery) |
| `jurisdiction` | string | No | ISO 3166-1 alpha-2 code (default: GB) |

**Returns:** Whether activity is permitted, conditions and restrictions, regulation references.

**Example:** `{ "age_group": "under-13" }`

---

### `get_reporting_requirements`

Get RIDDOR incident reporting requirements.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `incident_type` | string | Yes | Incident type (e.g. fatal, specified injury, dangerous occurrence, occupational disease, gas) |
| `jurisdiction` | string | No | ISO 3166-1 alpha-2 code (default: GB) |

**Returns:** Whether reportable, deadline, who to notify, reporting method, record retention period.

**Example:** `{ "incident_type": "fatal" }`

---

### `get_risk_assessment_template`

Get a risk assessment template for a farm activity.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `activity` | string | Yes | Farm activity (e.g. tractor operation, cattle handling, chainsaw use, pesticide application, confined space, grain storage) |
| `jurisdiction` | string | No | ISO 3166-1 alpha-2 code (default: GB) |

**Returns:** Hazards, controls, residual risk level, review frequency.

**Example:** `{ "activity": "cattle handling" }`
