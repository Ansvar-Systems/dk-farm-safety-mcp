# Coverage

## What Is Included

- **Machinery safety** (11 entries): Tractors (rollover, PTO), telehandlers, ATVs/quad bikes, chainsaws, grain dryers, combines, balers, slurry tankers, woodchippers, augers
- **Livestock handling** (7 entries): Cattle (crush/race, bulls, calving), sheep (handling, dipping), pigs (boar management), horses
- **Children on farms** (10 entries): Under-13 prohibited activities, 13-15 restricted with supervision, 16-17 with training, per Children in Agriculture Regulations (CHAW) 1998
- **COSHH substances** (8 entries): Pesticides, sheep dip (organophosphate), veterinary medicines, diesel/fuel, ammonia (slurry), grain dust, wood preservatives, rodenticides
- **RIDDOR reporting** (7 entries): Fatal injuries, specified injuries, over-7-day incapacitation, dangerous occurrences, occupational diseases, gas incidents, public injuries
- **Risk assessment templates** (8 entries): Tractor operation, cattle handling, working at height, confined spaces, lone working, chainsaw use, pesticide application, grain storage

## Jurisdictions

| Code | Country | Status |
|------|---------|--------|
| GB | Great Britain | Supported |

## Key Regulations Referenced

| Regulation | Coverage |
|------------|----------|
| PUWER 1998 | Machinery guarding, ROPS, training requirements |
| COSHH 2002 | Substance control, assessments, exposure limits |
| RIDDOR 2013 | Incident reporting categories and deadlines |
| CHAW 1998 | Children in agriculture age restrictions |
| LOLER 1998 | Lifting operations (telehandlers) |
| MHSW 1999 | Management of Health and Safety at Work |
| Confined Spaces Regulations 1997 | Slurry pit and silo entry |
| DSEAR 2002 | Grain dust explosion risk |
| SSAFO 2010 | Slurry store construction standards |
| Manual Handling Regulations 1992 | Livestock lifting |

## What Is NOT Included

- **Northern Ireland** -- NI follows HSE NI guidance, which may differ in some areas
- **Scotland-specific** -- HSE applies UK-wide but some Scottish regulations differ
- **Workplace stress and mental health** -- Farm Safety Foundation covers this; not in this dataset
- **Asbestos on farms** -- Separate HSE guidance, not farm-specific
- **Construction work on farms** -- CDM Regulations, separate from agriculture guidance
- **Transport on public roads** -- Road traffic law, not HSE agriculture scope
- **Environmental compliance** -- Only included where relevant to COSHH disposal and slurry storage
- **Real-time incident data** -- This is reference guidance, not live statistics

## Known Gaps

1. Data is seed-based (not scraped from live HSE pages) -- accuracy verified against published guidance
2. FTS5 search quality varies with query phrasing -- use specific terms for best results
3. Risk assessment templates are starting points, not complete assessments for specific farms
4. PPE specifications reference British Standards but do not include full product selection guidance

## Data Freshness

Run `check_data_freshness` to see when data was last updated. The ingestion pipeline runs monthly; manual triggers available via `gh workflow run ingest.yml`.
