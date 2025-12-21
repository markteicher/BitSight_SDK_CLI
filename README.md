# BitSight SDK + CLI

A production-grade BitSight SDK + CLI for collecting BitSight API data and writing it to a database (MSSQL first).

---

## ✨ What this provides

| Capability | What it does |
|---|---|
| 🧠 SDK modules | Python modules for calling BitSight endpoints deterministically |
| 🗄️ Database-first ingestion | Writes API data into MSSQL tables defined in `db/schema/mssql.sql` |
| 🔧 Control plane commands | `config`, `db`, `ingest` (state + data movement) |
| 🔎 Inspection plane commands | `show`, `stats`, `health` (system-of-record visibility) |
| 📦 Deterministic behavior | No stubs. No placeholders. Only real commands/modules |

---

## 📦 Install / Run

If packaged as an executable:

```bash
bitsight --help
```

If running from source:

```bash
python3 cli.py --help
```

---

## 🧭 Help and discovery

Running any of the following prints the **full command tree**:

```bash
bitsight --help
bitsight -h
bitsight help
```

---

## 🚪 Exit / Quit

The CLI supports explicit exit commands:

```bash
bitsight exit
bitsight quit
bitsight x
bitsight q
```

When invoked, the CLI prints:

> Thank for using the BitSight CLI

---

## 🌐 Global options

These flags can be provided before any command:

| Option | Description |
|---|---|
| `--verbose` | Enable debug logging |
| `--no-progress` | Disable progress bars |
| `--api-key <token>` | BitSight API token (Basic Auth username) |
| `--base-url <url>` | BitSight base URL (example: `https://api.bitsighttech.com`) |
| `--proxy-url <url>` | Proxy URL (example: `http://proxy:8080`) |
| `--timeout <seconds>` | HTTP timeout in seconds |

---

## 🧱 Command taxonomy

### A) Control plane

Commands that change state or move data:

- `config` — configuration management
- `db` — database initialization / clearing
- `ingest` — ingest API data into database tables
- `ingest-group` — grouped ingestion sets

### B) Inspection plane

Commands that observe the system-of-record:

- `show` — inspect stored entities (tables)
- `stats` — aggregate counts / rollups
- `health` — connectivity + schema checks

---

## 🧩 Full command tree

> This section lists **every command and subcommand**.

### `bitsight help`

Alias for `bitsight --help`.

---

### `bitsight exit | quit | x | q`

Exit the CLI and print:

> Thank for using the BitSight CLI

---

## 🔧 `config` — configuration management

```bash
bitsight config <subcommand>
```

| Subcommand | Purpose |
|---|---|
| `init` | Create config file with defaults (if missing) |
| `show` | Display current config |
| `validate` | Validate required config fields |
| `set` | Set one or more config values |
| `reset` | Reset config to defaults |
| `clear-keys` | Remove stored API key(s) |

#### `config set` options

```bash
bitsight config set [--api-key ...] [--base-url ...] [--proxy-url ...] [--proxy-username ...] [--proxy-password ...] [--timeout ...]
```

---

## 🗄️ `db` — database management (MSSQL first)

```bash
bitsight db <subcommand>
```

| Subcommand | Purpose |
|---|---|
| `init` | Create BitSight tables in MSSQL using the schema file |
| `status` | Show DB connectivity + schema presence |
| `flush` | Delete rows from one table or all BitSight tables |
| `clear` | Alias for `flush` (same behavior) |
| `migrate` | Reserved for schema migrations (explicit only) |

### `db init`

```bash
bitsight db init --mssql --server <server> --database <db> --username <user> --password <pass> [--schema-path db/schema/mssql.sql]
```

### `db flush` / `db clear`

Flush one table:

```bash
bitsight db flush --mssql --server <server> --database <db> --username <user> --password <pass> --table <table_name>
```

Flush all BitSight tables:

```bash
bitsight db flush --mssql --server <server> --database <db> --username <user> --password <pass> --all
```

---

## 📥 `ingest` — endpoint ingestion (table-by-table)

```bash
bitsight ingest <endpoint> [endpoint options]
```

All ingest commands support:

| Option | Meaning |
|---|---|
| `--flush` | Flush destination before ingest (module-defined behavior) |

### Identity

| Command | Notes |
|---|---|
| `ingest users` | GET users list |
| `ingest user-details --user-guid <guid>` | GET user details |
| `ingest user-quota` | GET user quota |
| `ingest user-company-views` | GET your company views |

### Core entities

| Command | Notes |
|---|---|
| `ingest companies` | GET companies |
| `ingest company-details --company-guid <guid>` | GET company details |

### Portfolio

| Command | Notes |
|---|---|
| `ingest portfolio` | GET portfolio |
| `ingest portfolio-details --company-guid <guid>` | Portfolio details for one company |
| `ingest portfolio-contacts` | Portfolio contact endpoints |
| `ingest portfolio-public-disclosures` | Portfolio public disclosure endpoints |

### Posture

| Command | Notes |
|---|---|
| `ingest current-ratings` | GET current ratings |
| `ingest ratings-history --company-guid <guid> [--since <date>] [--backfill]` | GET ratings history |

### Exposure

| Command | Notes |
|---|---|
| `ingest findings --company-guid <guid> [--since <date>] [--expand <mode>]` | GET findings |
| `ingest observations --company-guid <guid> [--since <date>]` | GET observations |

### Intelligence

| Command | Notes |
|---|---|
| `ingest threats` | GET threats (v2) |
| `ingest threat-exposures` | Threat exposure endpoints |
| `ingest alerts [--since <date>]` | GET alerts |
| `ingest credential-leaks` | Credential leak endpoints |
| `ingest exposed-credentials` | Exposed credentials endpoints |

---

## 📦 `ingest-group` — grouped ingestion

```bash
bitsight ingest-group <group>
```

| Group | Includes |
|---|---|
| `core` | Identity + core entities + portfolio |
| `security` | Posture + exposure + intelligence |
| `all` | Everything |

---

## 🔎 `show` — inspect stored data

```bash
bitsight show <subcommand>
```

| Subcommand | Purpose |
|---|---|
| `tables` | List BitSight tables present |
| `companies` | List companies (from DB) |
| `assets` | List assets (from DB) |

---

## 📊 `stats` — rollups and counts

```bash
bitsight stats <subcommand>
```

| Subcommand | Purpose |
|---|---|
| `tables` | Row counts per BitSight table |
| `assets` | Asset totals |
| `companies` | Company totals |
| `ratings` | Rating record totals |

---

## 💓 `health` — connectivity + schema checks

```bash
bitsight health <subcommand>
```

| Subcommand | Purpose |
|---|---|
| `db` | DB connection + required tables |
| `api` | API connectivity + auth validation |
| `proxy` | Proxy connectivity validation |

---

## ✅ Common examples

Initialize MSSQL schema:

```bash
bitsight db init --mssql --server sql01 --database BitSight --username bitsight --password '***'
```

Ingest users:

```bash
bitsight ingest users
```

Ingest findings for a specific company:

```bash
bitsight ingest findings --company-guid 92105617-5dfe-4fce-8606-acea90f732e2
```

Flush a single table:

```bash
bitsight db flush --mssql --server sql01 --database BitSight --username bitsight --password '***' --table bitsight_users
```

Flush all BitSight tables:

```bash
bitsight db flush --mssql --server sql01 --database BitSight --username bitsight --password '***' --all
```

---

## 📁 Repository structure

```text
BitSight_SDK_CLI/
├── cli.py
├── core/
├── ingest/
├── db/
│   ├── init.py
│   ├── mssql.py
│   └── schema/
│       └── mssql.sql
└── README.md
