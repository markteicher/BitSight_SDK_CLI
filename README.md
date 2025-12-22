# 🛡️ BitSight SDK + CLI

A Software Developer's Kit and Command Line Interpreter interface for  Bitsight Security Ratings. 

Bitsight, a leading cyber risk intelligence company, provides objective, data-driven security ratings and analytics to help organizations manage digital risks, particularly with third parties. Their platform continuously analyzes external data to score companies' security postures, identify vulnerabilities, and benchmark performance, helping businesses make informed decisions about vendors, cyber insurance, and overall security strategy.

## What it does:
Security Ratings: Assigns objective, data-backed scores (from 250-900) to organizations, reflecting their cybersecurity health.

Third-Party Risk Management (TPRM): Assesses the risk introduced by vendors, clients, and partners, helping companies manage supply chain vulnerabilities.

Attack Surface Management: Maps and monitors an organization's entire digital footprint to find exposures.

Benchmarking: Allows companies to compare their security performance against peers and industry standards.

Risk Quantification: Provides data to help understand the financial impact of cyber risk. 

This application is built to export data via the extensive BitSight API for  writing it into **Microsoft SQL Server (MSSQL)** using a **1:1 physical table representation** of BitSight endpoints.


## ⚠️ Disclaimer

This tool is **not an official BitSight product**.

Use of this software is **not covered** by any license, warranty, or support agreement you may have with BitSight.
All functionality is implemented independently using publicly available Bitsight API Documentation: https://help.bitsighttech.com/hc/en-us/articles/231872628-API-Documentation-Overview

---

## ✅ Help and discovery

Running any of the following prints the **full command tree** (all commands + subcommands):

- `bitsight-cli --help`
- `bitsight-cli -h`
- `bitsight-cli help`

---

## 🚪 Exit / quit

The CLI supports explicit exit commands:

- `bitsight-cli exit`
- `bitsight-cli quit`
- `x`
- `q`

On exit/quit it prints:

> **Thank you for using the BitSight CLI**

---

## ⚙️ Global options

These options apply to all commands (when present on the CLI):

| Option | Description |
|---|---|
| `--verbose` | Enable debug logging |
| `--no-progress` | Disable progress bars |
| `--api-key` | BitSight API token (HTTP Basic Auth username) |
| `--base-url` | BitSight API base URL (e.g., `https://api.bitsighttech.com`) |
| `--proxy-url` | Proxy URL (e.g., `http://proxy:8080`) |
| `--timeout` | HTTP timeout (seconds) |

---

## 🧭 Command taxonomy

The BitSight CLI is organized into two planes:

### A) Control plane (changes state)
- `config` — configuration management
- `db` — database initialization / maintenance
- `ingest` — API → database ingestion

### B) Inspection plane (observes state)
- `show` — query data already in the database (planned/next)
- `stats` — summarize database contents (planned/next)
- `health` — connection + schema + ingestion status checks (planned/next)

> `show/stats/health` are part of the agreed taxonomy and are wired after the control plane is complete.

---

## 🔐 `config` commands

| Command | Purpose |
|---|---|
| `bitsight-cli config init` | Create initial config state |
| `bitsight-cli config show` | Display current config |
| `bitsight-cli config validate` | Validate config + connectivity |
| `bitsight-cli config reset` | Reset config to defaults |
| `bitsight-cli config clear-keys` | Clear stored secrets/keys |
| `bitsight-cli config set ...` | Set config fields |

`config set` flags:

- `--api-key`
- `--base-url`
- `--proxy-url`
- `--proxy-username`
- `--proxy-password`
- `--timeout`

---

## 🗄️ `db` commands (MSSQL only)

### Initialize schema
```bash
bitsight-cli db init --mssql --server <server> --database <db> --username <user> --password <pass> --schema-path db/schema/mssql.sql
```

### Flush data
```bash
# Flush one table
bitsight-cli db flush --mssql --server <server> --database <db> --username <user> --password <pass> --table bitsight_users

# Flush all BitSight tables
bitsight-cli db flush --mssql --server <server> --database <db> --username <user> --password <pass> --all
```

### Status
```bash
bitsight-cli db status
```

---

## 📥 `ingest` commands

Each `ingest` command maps to a BitSight API endpoint and writes results into its corresponding MSSQL table(s).

### Users
- `bitsight-cli ingest users`
- `bitsight-cli ingest user-details --user-guid <guid>`
- `bitsight-cli ingest user-quota`
- `bitsight-cli ingest user-company-views`

### Companies
- `bitsight-cli ingest companies`
- `bitsight-cli ingest company-details --company-guid <guid>`

### Portfolio
- `bitsight-cli ingest portfolio`
- `bitsight-cli ingest portfolio-details --company-guid <guid>`
- `bitsight-cli ingest portfolio-contacts`
- `bitsight-cli ingest portfolio-public-disclosures`

### Ratings
- `bitsight-cli ingest current-ratings`
- `bitsight-cli ingest current-ratings-v2`
- `bitsight-cli ingest ratings-history --company-guid <guid> --since <date> [--backfill]`

### Findings & observations
- `bitsight-clo ingest findings --company-guid <guid> --since <date> [--expand <value>]`
- `bitsight-cli ingest observations --company-guid <guid> --since <date>`

### Threat intelligence / threats
- `bitsight-cli ingest threats`
- bitsight-cli ingest threat-exposures`

### Alerts
- `bitsight-cli ingest alerts --since <date>`

### Credentials
- `bitsight-cli ingest credential-leaks`
- `bitsight-cli ingest exposed-credentials`

---

## 🧩 `ingest-group` commands

Grouped ingestion runs multiple ingestion commands in sequence:

- `bitsight-cli ingest-group core`
- `bitsight-cli ingest-group security`
- `bitsight-cli ingest-group all`

(These groupings are explicitly wired.)

---

## 🗂️ Directory structure

```text
BitSight_SDK_CLI/
├── cli.py
├── core/
│   ├── ingestion.py
│   ├── status_codes.py
│   ├── exit_codes.py
│   ├── config.py
│   ├── db_router.py
│   └── database_interface.py
├── db/
│   ├── init.py
│   ├── mssql.py
│   └── schema/
│       └── mssql.sql 
├── ingest/
│   ├── alerts.py
│   ├── asset_risk_matrix.py
│   ├── asset_summaries.py
│   ├── assets.py
│   ├── base.py
│   ├── client_access_links.py
│   ├── companies.py
│   ├── company_details.py
│   ├── company_findings_summary.py
│   ├── company_infrastructure.py
│   ├── company_overview_report.py
│   ├── company_products.py
│   ├── company_products_post.py
│   ├── company_relationships.py
│   ├── company_requests.py
│   ├── current_ratings.py
│   ├── current_ratings_v2.py
│   ├── findings.py
│   ├── findings_statistics.py
│   ├── observations.py
│   ├── portfolio.py
│   ├── ratings_history.py
│   ├── threats.py
│   ├── threat_statistics.py
│   ├── threats_impact.py
│   ├── threats_evidence.py
│   ├── user_company_views.py
│   ├── user_details.py
│   ├── users.py
│   └── users_quota.py

```

---

## 🧱 Database schema

MSSQL schema file:

- `db/schema/mssql.sql`

Tables store `raw_payload` as `NVARCHAR(MAX)` to preserve the full API response alongside typed columns.

---

## Support
- Bitsight API Documentation: https://help.bitsighttech.com/hc/en-us/articles/231872628-API-Documentation-Overview

## License

#MIT License

#Copyright (c) 2025 Mark Teicher

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

