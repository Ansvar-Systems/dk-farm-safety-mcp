# Denmark Farm Safety MCP

[![CI](https://github.com/Ansvar-Systems/dk-farm-safety-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/Ansvar-Systems/dk-farm-safety-mcp/actions/workflows/ci.yml)
[![GHCR](https://github.com/Ansvar-Systems/dk-farm-safety-mcp/actions/workflows/ghcr-build.yml/badge.svg)](https://github.com/Ansvar-Systems/dk-farm-safety-mcp/actions/workflows/ghcr-build.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

UK farm health and safety guidance via the [Model Context Protocol](https://modelcontextprotocol.io). Query HSE machinery safety, livestock handling rules, COSHH substance requirements, children on farms regulations, RIDDOR reporting, and risk assessment templates -- all from your AI assistant.

Part of [Ansvar Open Agriculture](https://ansvar.eu/open-agriculture).

## Why This Exists

Farming is the most dangerous industry in the UK. HSE publishes safety guidance across dozens of web pages, PDFs, and information sheets. This MCP server puts it all in a queryable database so AI assistants can give accurate, sourced answers about farm safety obligations.

## Quick Start

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "uk-farm-safety": {
      "command": "npx",
      "args": ["-y", "@ansvar/dk-farm-safety-mcp"]
    }
  }
}
```

### Claude Code

```bash
claude mcp add uk-farm-safety npx @ansvar/dk-farm-safety-mcp
```

### Streamable HTTP (remote)

```
https://mcp.ansvar.eu/dk-farm-safety/mcp
```

### Docker (self-hosted)

```bash
docker run -p 3000:3000 ghcr.io/ansvar-systems/dk-farm-safety-mcp:latest
```

### npm (stdio)

```bash
npx @ansvar/dk-farm-safety-mcp
```

## Example Queries

Ask your AI assistant:

- "What safety measures are needed for tractor PTO work?"
- "Can a 14-year-old drive a tractor on a farm?"
- "What PPE is needed for sheep dipping?"
- "What incidents must I report under RIDDOR on a farm?"
- "Give me a risk assessment template for cattle handling"
- "What are the COSHH requirements for pesticide storage?"

## Stats

| Metric | Value |
|--------|-------|
| Tools | 10 (3 meta + 7 domain) |
| Jurisdiction | GB |
| Data sources | HSE Agriculture, RIDDOR, COSHH, PUWER, Farm Safety Foundation |
| License (data) | Open Government Licence v3 |
| License (code) | Apache-2.0 |
| Transport | stdio + Streamable HTTP |

## Tools

| Tool | Description |
|------|-------------|
| `about` | Server metadata and links |
| `list_sources` | Data sources with freshness info |
| `check_data_freshness` | Staleness status and refresh command |
| `search_safety_guidance` | FTS5 search across all safety guidance |
| `get_machinery_safety` | Machinery hazards, controls, PPE, legal requirements |
| `get_livestock_handling_safety` | Livestock handling guidance by species |
| `get_coshh_requirements` | COSHH substance control requirements |
| `get_children_on_farms_rules` | Age-group rules for children on farms |
| `get_reporting_requirements` | RIDDOR incident reporting requirements |
| `get_risk_assessment_template` | Risk assessment templates by activity |

See [TOOLS.md](TOOLS.md) for full parameter documentation.

## Security Scanning

This repository runs security checks on every push:

- **CodeQL** -- static analysis for JavaScript/TypeScript
- **Gitleaks** -- secret detection across full history
- **Dependency review** -- via Dependabot
- **Container scanning** -- via GHCR build pipeline

See [SECURITY.md](SECURITY.md) for reporting policy.

## Disclaimer

This tool provides reference data for informational purposes only. It is not professional health and safety advice. Always consult current HSE guidance and your own risk assessments. In an emergency, call 999. See [DISCLAIMER.md](DISCLAIMER.md).

## Contributing

Issues and pull requests welcome. For security vulnerabilities, email security@ansvar.eu (do not open a public issue).

## License

Apache-2.0. Data sourced under Open Government Licence v3.
