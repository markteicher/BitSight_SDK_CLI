# 🛡️ BitSight SDK + CLI

A Software Developer's Kit and Command Line Interpreter interfacd for  Bitsight Security Ratings. 

BitSight Security Ratings allows for monitoring security ratings, portfolio companies, alerts, findings, exposed credentials, users, and threat intelligence.

This application is built to export data via the extensive BitSight API for  writing it into **Microsoft SQL Server (MSSQL)** using a **1:1 physical table representation** of BitSight endpoints.

---

## ✅ Help and discovery

Running any of the following prints the **full command tree** (all commands + subcommands):

- `bitsight --help`
- `bitsight -h`
- `bitsight help`

---

## 🚪 Exit / quit

The CLI supports explicit exit commands:

- `bitsight exit`
- `bitsight quit`
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

The CLI is organized into two planes:

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
| `bitsight config init` | Create initial config state |
| `bitsight config show` | Display current config |
| `bitsight config validate` | Validate config + connectivity |
| `bitsight config reset` | Reset config to defaults |
| `bitsight config clear-keys` | Clear stored secrets/keys |
| `bitsight config set ...` | Set config fields |

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
bitsight db init --mssql --server <server> --database <db> --username <user> --password <pass> --schema-path db/schema/mssql.sql
```

### Flush data
```bash
# Flush one table
bitsight db flush --mssql --server <server> --database <db> --username <user> --password <pass> --table bitsight_users

# Flush all BitSight tables
bitsight db flush --mssql --server <server> --database <db> --username <user> --password <pass> --all
```

### Status
```bash
bitsight db status
```

---

## 📥 `ingest` commands

Each `ingest` command maps to a BitSight API endpoint and writes results into its corresponding MSSQL table(s).

### Users
- `bitsight ingest users`
- `bitsight ingest user-details --user-guid <guid>`
- `bitsight ingest user-quota`
- `bitsight ingest user-company-views`

### Companies
- `bitsight ingest companies`
- `bitsight ingest company-details --company-guid <guid>`

### Portfolio
- `bitsight ingest portfolio`
- `bitsight ingest portfolio-details --company-guid <guid>`
- `bitsight ingest portfolio-contacts`
- `bitsight ingest portfolio-public-disclosures`

### Ratings
- `bitsight ingest current-ratings`
- `bitsight ingest ratings-history --company-guid <guid> --since <date> [--backfill]`

### Findings & observations
- `bitsight ingest findings --company-guid <guid> --since <date> [--expand <value>]`
- `bitsight ingest observations --company-guid <guid> --since <date>`

### Threat intelligence / threats
- `bitsight ingest threats`
- `bitsight ingest threat-exposures`

### Alerts
- `bitsight ingest alerts --since <date>`

### Credentials
- `bitsight ingest credential-leaks`
- `bitsight ingest exposed-credentials`

---

## 🧩 `ingest-group` commands

Grouped ingestion runs multiple ingestion commands in sequence:

- `bitsight ingest-group core`
- `bitsight ingest-group security`
- `bitsight ingest-group all`

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
└── ingest/
    ├── users.py
    ├── user_details.py
    ├── users_quota.py
    ├── user_company_views.py
    ├── companies.py
    ├── company_details.py
    ├── portfolio.py
    ├── current_ratings.py
    ├── ratings_history.py
    ├── statistics.py
    ├── findings.py
    ├── findings_statistics.py
    ├── observations.py
    ├── threats.py
    ├── threat_statistics.py
    ├── threats_impact.py
    └── threats_evidence.py
```

---

## 🧱 Database schema

MSSQL schema file:

- `db/schema/mssql.sql`

Tables store `raw_payload` as `NVARCHAR(MAX)` to preserve the full API response alongside typed columns.

---

## Support
- Bitsight API Documentation: https://help.bitsighttech.com/hc/en-us/articles/231872628-API-Documentation-Overview

