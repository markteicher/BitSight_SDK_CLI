# BitSight_SDK_CLI
Bitsight SDK and CLI

Purpose
This package provides a command-line interface and SDK to retrieve BitSight API data and persist it into a configured database backend. The CLI is a control surface only. It does not perform analytics, reporting, or visualization.

Executable
bitsight

Help and Discovery
Running any of the following prints the full command tree:
- bitsight --help
- bitsight -h
- bitsight help

Exit / Quit
The CLI supports explicit exit commands:
- bitsight exit
- bitsight quit
- x
- q

When invoked, the CLI prints:
"Thank you for using the BitSight CLI"

Global Options
These flags are accepted by all commands:
--api-key        BitSight API token
--base-url       BitSight API base URL
--proxy-url      HTTP/HTTPS proxy URL
--timeout        HTTP timeout (seconds)
--verbose        Enable debug logging
--no-progress    Disable progress bars

Command Planes

Control Plane
Commands that change configuration or system state.

config
  init
  show
  set
  validate
  reset
  clear-keys

db
  init        Initialize database schema
  status      Show database connectivity and schema presence
  flush       Delete records (table or all)
  clear       Alias of flush --all (MSSQL only)

ingest
  users
  user-details
  user-quota
  user-company-views
  companies
  company-details
  portfolio
  portfolio-details
  current-ratings
  ratings-history
  findings
  findings-statistics
  findings-details
  observations
  threats
  threat-statistics
  threat-impact
  threat-evidence
  assets
  asset-risk-matrix
  exposed-credentials
  alerts
  insights
  news
  subsidiaries
  subsidiary-statistics
  nist-csf
  ratings-history

Inspection Plane
Commands that observe the system of record.

show
  tables
  counts
  companies
  assets

status
  health
  last-run
  errors

Execution Model
- All commands return explicit exit codes
- All API calls are deterministic
- All database writes are transactional
- All failures surface immediately with exit code and log message

Databases
Currently supported:
- MSSQL

No other database backends are enabled unless explicitly configured.

End of document
