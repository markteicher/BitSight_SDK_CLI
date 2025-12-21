#!/usr/bin/env python3
"""
exit_codes.py

Defines the ONLY process exit codes allowed for BitSight SDK + CLI.

Rules:
- Exactly one exit code is returned per CLI invocation.
- cli.py MUST return one of these codes via sys.exit(int(code)).
- core/ingestion.py MUST map execution outcomes to one of these codes.
- No other module should call sys.exit() directly.
"""

from enum import IntEnum


class ExitCode(IntEnum):
    # ============================================================
    # 0–9: SUCCESS / NON-ERROR TERMINATION
    # ============================================================

    SUCCESS = 0
    """WHEN: command completes and all required operations succeed."""

    SUCCESS_NOOP = 1
    """
    WHEN: command completes successfully but performs no work by design.
    Example: `show` command returns empty set but that is not an error.
    """

    SUCCESS_EMPTY = 2
    """
    WHEN: ingest completes successfully, API reachable, but zero results returned.
    This is not a failure; it is an observed empty dataset.
    """

    SUCCESS_ALREADY_INITIALIZED = 3
    """
    WHEN: requested init/setup action finds system already initialized.
    Example: db schema already present and matches expected objects.
    """

    # ============================================================
    # 10–19: CLI ARGUMENTS / COMMAND DISPATCH
    # ============================================================

    INVALID_ARGUMENTS = 10
    """
    WHEN: argparse rejects the command line (missing required args, bad types,
          mutually-exclusive args, invalid choices).
    STOP: immediately (no network/db work).
    """

    UNKNOWN_COMMAND = 11
    """
    WHEN: top-level command is not recognized by dispatch.
    STOP: immediately.
    """

    UNKNOWN_SUBCOMMAND = 12
    """
    WHEN: subcommand is not recognized by dispatch.
    STOP: immediately.
    """

    COMMAND_NOT_WIRED = 13
    """
    WHEN: command exists in CLI taxonomy but dispatch handler is missing.
    STOP: immediately.
    """

    # ============================================================
    # 20–29: CONFIGURATION STATE / VALIDATION
    # ============================================================

    CONFIG_NOT_FOUND = 20
    """
    WHEN: command requires config but config file does not exist.
    STOP: immediately.
    """

    CONFIG_INVALID = 21
    """
    WHEN: config exists but fails validation (malformed base_url, missing api_key,
          invalid timeout, invalid proxy settings, etc.).
    STOP: immediately.
    """

    CONFIG_PERMISSION_DENIED = 22
    """
    WHEN: config file exists but cannot be read/written due to OS permissions.
    STOP: immediately.
    """

    # ============================================================
    # 30–39: AUTHN / AUTHZ / ENTITLEMENTS
    # ============================================================

    AUTHENTICATION_FAILED = 30
    """
    WHEN: BitSight returns 401 or equivalent auth failure.
    STOP: immediately (retry requires corrected credentials).
    """

    AUTHORIZATION_FAILED = 31
    """
    WHEN: BitSight returns 403 or equivalent permission denial.
    STOP: immediately (retry requires entitlement/role change).
    """

    LICENSE_REQUIRED_OR_EXHAUSTED = 32
    """
    WHEN: BitSight indicates license required/exhausted for the endpoint.
    STOP: immediately (retry requires license/entitlement change).
    """

    # ============================================================
    # 40–49: NETWORK / TRANSPORT
    # ============================================================

    NETWORK_ERROR = 40
    """
    WHEN: DNS/connect/TLS failure prevents API call completion.
    STOP: immediately.
    """

    TIMEOUT = 41
    """
    WHEN: request times out (connect/read timeout).
    STOP: immediately.
    """

    RATE_LIMITED = 42
    """
    WHEN: API returns 429 (rate limited) or equivalent throttling response.
    STOP: immediately (retry after backoff).
    """

    # ============================================================
    # 50–59: API RESPONSE / CONTRACT / DATA SHAPE
    # ============================================================

    API_ERROR = 50
    """
    WHEN: API returns non-2xx that is not auth/rate-limit (e.g., 5xx, 404, 400).
    STOP: immediately unless ingestion controller explicitly tolerates it.
    """

    API_CONTRACT_ERROR = 51
    """
    WHEN: API returns 2xx but payload is missing required keys or has an
          incompatible shape for the expected endpoint.
    STOP: immediately (treat as upstream change or parser bug).
    """

    # ============================================================
    # 60–69: DATABASE CONNECTIVITY / SCHEMA / WRITE
    # ============================================================

    DB_CONNECTION_FAILED = 60
    """
    WHEN: cannot connect/authenticate to DB.
    STOP: immediately.
    """

    DB_SCHEMA_FAILED = 61
    """
    WHEN: schema init/migration fails OR required tables/columns are missing.
    STOP: immediately.
    """

    DB_WRITE_FAILED = 62
    """
    WHEN: insert/update fails in a way that prevents safe continuation
          (constraint violations, deadlock not recovered, transaction failure).
    STOP: immediately unless ingestion controller supports partial continuation.
    """

    # ============================================================
    # 70–79: INGEST EXECUTION OUTCOME (AGGREGATED)
    # ============================================================

    INGEST_PARTIAL_SUCCESS = 70
    """
    WHEN: ingestion ran and produced some successful writes, but also recorded
          one or more record-level or page-level failures.
    STOP: command completes, returns this code.
    """

    INGEST_FAILED = 71
    """
    WHEN: ingestion could not complete required work due to fatal errors.
    STOP: immediately or after controlled shutdown.
    """

    # ============================================================
    # 80–89: INTERNAL FAILURES
    # ============================================================

    INTERNAL_ERROR = 80
    """
    WHEN: unhandled exception escapes to top-level handler.
    STOP: immediately.
    """

    NOT_IMPLEMENTED = 81
    """
    WHEN: code path exists but is intentionally not implemented.
    STOP: immediately.
    """

    # ============================================================
    # 90–99: OPERATOR TERMINATION
    # ============================================================

    OPERATOR_ABORT = 90
    """
    WHEN: user interrupts execution (SIGINT/Ctrl-C).
    STOP: immediately or controlled shutdown.
    """
