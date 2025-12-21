# BitSight_SDK_CLI
Bitsight SDK and CLI

# BitSight SDK + CLI

A production-grade command-line interface and SDK for collecting BitSight data and persisting it into a database backend (currently MSSQL).

This CLI is **non-interactive**. Each invocation executes a single command and exits.

---

## Help and Command Discovery

Running any of the following prints the **complete command tree**, including all commands and subcommands:

- `bitsight --help`
- `bitsight -h`
- `bitsight help`

These commands list **every available operation** supported by the BitSight CLI.

---

## Exit / Quit Commands

The CLI provides explicit commands to terminate execution immediately:

- `bitsight exit`
- `bitsight quit`

These commands exit cleanly and display:

```
Thank you for using the BitSight CLI
```

> Note: Single-key commands such as `x` or `q` are **not supported**.  
> The CLI is not interactive and does not wait for user input.

---

## Command Planes

The CLI is organized into clear functional planes.

### Control Plane
Commands that modify configuration, state, or stored data.

- `config`
- `db`
- `ingest`

### Inspection Plane
Commands that inspect or report on the system of record.

- `show`
- `stats`
- `status`
- `health`

---

## Configuration Commands

Manage BitSight API and runtime configuration.

```
bitsight config init
bitsight config show
bitsight config validate
bitsight config set
bitsight config reset
bitsight config clear-keys
```

---

## Database Commands (MSSQL)

Manage the database backend.

```
bitsight db init --mssql
bitsight db status
bitsight db flush --mssql --table <table>
bitsight db flush --mssql --all
bitsight db clear --mssql
```

---

## Ingestion Commands

Ingest data from BitSight APIs into the configured database.

### Identity
```
bitsight ingest users
bitsight ingest user-details
bitsight ingest user-quota
bitsight ingest user-company-views
```

### Companies & Portfolio
```
bitsight ingest companies
bitsight ingest company-details
bitsight ingest portfolio
```

### Ratings
```
bitsight ingest current-ratings
bitsight ingest ratings-history
```

### Findings & Observations
```
bitsight ingest findings
bitsight ingest observations
```

### Threat Intelligence
```
bitsight ingest threats
bitsight ingest threat-statistics
bitsight ingest threat-impact
bitsight ingest threat-evidence
```

### Exposure & Alerts
```
bitsight ingest exposed-credentials
bitsight ingest alerts
```

---

## Grouped Ingestion

Execute predefined ingestion sets.

```
bitsight ingest-group core
bitsight ingest-group security
bitsight ingest-group all
```

---

## Status and Inspection

Inspect the state of the system and stored data.

```
bitsight status tables
bitsight status last-run
bitsight status health
```

---

## Summary

- The CLI is **explicit, deterministic, and non-interactive**
- All behavior is discoverable via `--help`
- Only documented commands exist
- No implicit shortcuts or hidden behaviors
