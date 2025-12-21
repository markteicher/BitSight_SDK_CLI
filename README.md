# 🛡️ BitSight SDK + CLI

## Overview

The **BitSight SDK + CLI** is a production-grade command-line interface and Python SDK designed to extract, normalize, and persist the complete BitSight API into enterprise databases for operational intelligence and analytics.

---

## ✨ Key Capabilities

### 🧠 Core Platform
| Capability | Description |
|---------|-------------|
| 🔌 Full BitSight API Coverage | Physical ingestion of all BitSight endpoints |
| 🗄️ Database-First Design | Data written directly to enterprise databases |
| 🧱 Enterprise Schema | Strong MSSQL schema with raw payload preservation |
| ⚙️ Deterministic Execution | Explicit commands and predictable behavior |
| 🧩 Modular SDK | Reusable Python ingestion modules |

---

## 📦 Data Domains

- Users, User Details, User Quota
- Companies, Company Details, Portfolio
- Current Ratings, Ratings History
- Findings, Observations, Statistics
- Threat Intelligence (v2)
- Exposed Credentials
- Assets and Infrastructure
- Company Relationships and Requests
- Reports and Compliance (NIST CSF)
- Peer and Risk Analytics
- Lookup and Static Data

---

## 🧭 CLI Command Model

### Control Plane
Commands that change state:

- `config`
- `db`
- `ingest`

### Inspection Plane
Commands that observe state:

- `status`
- `show`
- `stats`

---

## 🚪 Exit & Quit

Supported commands:

- `bitsight exit`
- `bitsight quit`
- `x`
- `q`

Output:
```
Thank you for using the BitSight CLI
```

---

## 🧪 Help & Discovery

The following commands print the full command tree:

- `bitsight --help`
- `bitsight -h`
- `bitsight help`

---

## 🗄️ Database Support

| Database | Status |
|--------|--------|
| MSSQL | Supported |

---

## 📂 Structure

```
bitsight/
├── cli.py
├── core/
├── ingest/
├── db/
└── README.md
```

---

## 📜 License

Apache License 2.0
