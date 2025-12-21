#!/usr/bin/env python3
"""
exit_codes.py

AUTHORITATIVE EXIT CODE CONTRACT FOR BitSight SDK + CLI

This file defines the ONLY exit codes allowed to leave the process boundary.
Every other module (cli.py, core/ingestion.py, db/*, ingest/*) MUST map
internal outcomes to one of these codes.

No module is allowed to call sys.exit(<int>) with an undocumented value.

Exit codes are FINAL SIGNALS to:
- operators
- schedulers
- orchestration systems
- SIEM / SOAR
- automation frameworks

Each exit code below documents:
- EXACT condition that triggers it
- WHICH layer emits it
- WHETHER execution stops immediately
"""

from enum import IntEnum


class ExitCode(IntEnum):
    """
    ExitCode enumeration.

    All exit codes are unique, stable, and non-overlapping.
    Renumbering is forbidden once published.
    """

    # ------------------------------------------------------------------
    # 0 — SUCCESS
    # ------------------------------------------------------------------

    SUCCESS = 0
    """
    WHEN:
        - All requested operations completed successfully
        - No fatal errors occurred
        - All mandatory data was processed
    SET BY:
        - core.ingestion.ExecutionSummary.finalize()
        - cli.py after successful handler completion
    EFFECT:
        - Process exits normally
        - No retries required
    """

    # ------------------------------------------------------------------
    # 10–19 — CLI / OPERATOR ERRORS (INPUT & INVOCATION)
    # ------------------------------------------------------------------

    INVALID_ARGUMENTS = 10
    """
    WHEN:
        - argparse fails validation
        - Required arguments are missing
        - Mutually exclusive options are combined incorrectly
    SET BY:
        - cli.py only
    EFFECT:
        - Execution stops immediately
        - No ingestion or DB work is started
    """

    CONFIG_NOT_INITIALIZED = 11
    """
    WHEN:
        - User runs a command that requires configuration
        - No config file exists or config is incomplete
    SET BY:
        - cli.py
        - config loader
    EFFECT:
        - Execution stops before any network or DB activity
    """

    CONFIG_INVALID = 12
    """
    WHEN:
        - Config file exists but contains invalid values
        - Examples: malformed URL, missing API key, invalid timeout
    SET BY:
        - config validation logic
    EFFECT:
        - Execution stops immediately
    """

    UNSUPPORTED_COMMAND = 13
    """
    WHEN:
        - CLI command is recognized syntactically
        - But no execution handler exists
    SET BY:
        - cli.py dispatch layer
    EFFECT:
        - Execution stops immediately
    """

    # ------------------------------------------------------------------
    # 20–29 — AUTHENTICATION & AUTHORIZATION
    # ------------------------------------------------------------------

    AUTHENTICATION_FAILED = 20
    """
    WHEN:
        - BitSight API returns 401
        - API key is missing, revoked, or invalid
    SET BY:
        - HTTP client layer
        - core.ingestion
    EFFECT:
        - Execution stops
        - No retry without operator intervention
    """

    AUTHORIZATION_FAILED = 21
    """
    WHEN:
        - BitSight API returns 403
        - API key is valid but lacks permissions
    SET BY:
        - HTTP client layer
    EFFECT:
        - Execution stops
        - Operator must adjust entitlements
    """

    LICENSE_EXHAUSTED = 22
    """
    WHEN:
        - BitSight API indicates license quota exceeded
        - Example: current ratings license exhausted
    SET BY:
        - core.ingestion
    EFFECT:
        - Execution stops
        - Retry only after license reset
    """

    # ------------------------------------------------------------------
    # 30–39 — NETWORK & TRANSPORT
    # ------------------------------------------------------------------

    NETWORK_FAILURE = 30
    """
    WHEN:
        - DNS failure
        - TCP connection failure
        - TLS negotiation failure
    SET BY:
        - HTTP client layer
    EFFECT:
        - Execution stops
        - Safe to retry
    """

    TIMEOUT = 31
    """
    WHEN:
        - HTTP request exceeds configured timeout
    SET BY:
        - HTTP client layer
    EFFECT:
        - Execution stops
        - Retry permitted
    """

    RATE_LIMITED = 32
    """
    WHEN:
        - BitSight API returns 429
    SET BY:
        - core.ingestion
    EFFECT:
        - Execution stops
        - Retry after backoff window
    """

    # ------------------------------------------------------------------
    # 40–49 — DATA & API SEMANTICS
    # ------------------------------------------------------------------

    API_CONTRACT_VIOLATION = 40
    """
    WHEN:
        - API response does not match documented schema
        - Required fields are missing or malformed
    SET BY:
        - Response normalization layer
    EFFECT:
        - Execution stops
        - Indicates upstream API change
    """

    UNSUPPORTED_API_VERSION = 41
    """
    WHEN:
        - Endpoint version no longer supported
        - API returns explicit version error
    SET BY:
        - core.ingestion
    EFFECT:
        - Execution stops
    """

    # ------------------------------------------------------------------
    # 50–59 — DATABASE ERRORS
    # ------------------------------------------------------------------

    DATABASE_CONNECTION_FAILED = 50
    """
    WHEN:
        - Cannot connect to database
        - Invalid credentials
        - Network failure to DB
    SET BY:
        - db.mssql
    EFFECT:
        - Execution stops
    """

    DATABASE_SCHEMA_ERROR = 51
    """
    WHEN:
        - Required table does not exist
        - Column mismatch
        - Migration not applied
    SET BY:
        - db layer
    EFFECT:
        - Execution stops
    """

    DATABASE_WRITE_FAILED = 52
    """
    WHEN:
        - INSERT / UPDATE fails
        - Constraint violation
        - Deadlock
    SET BY:
        - db layer
    EFFECT:
        - Execution stops
        - Partial ingestion possible
    """

    # ------------------------------------------------------------------
    # 60–69 — INGESTION EXECUTION STATES
    # ------------------------------------------------------------------

    NO_DATA_RETURNED = 60
    """
    WHEN:
        - API call succeeds
        - But zero records are returned
    SET BY:
        - core.ingestion
    EFFECT:
        - Execution completes normally
        - Used to signal empty result sets
    """

    PARTIAL_SUCCESS = 61
    """
    WHEN:
        - Some records processed successfully
        - Some records failed
    SET BY:
        - core.ingestion.ExecutionSummary
    EFFECT:
        - Execution completes
        - Operator must inspect error counts
    """

    INGESTION_ABORTED = 62
    """
    WHEN:
        - Fatal error occurs mid-ingestion
        - Continuation would corrupt state
    SET BY:
        - core.ingestion
    EFFECT:
        - Execution stops immediately
    """

    # ------------------------------------------------------------------
    # 70–79 — INTERNAL SYSTEM ERRORS
    # ------------------------------------------------------------------

    INTERNAL_ERROR = 70
    """
    WHEN:
        - Unhandled exception escapes ingestion logic
    SET BY:
        - Top-level exception handler
    EFFECT:
        - Execution stops
        - Indicates bug
    """

    NOT_IMPLEMENTED = 71
    """
    WHEN:
        - Code path exists but logic is intentionally missing
    SET BY:
        - Any layer
    EFFECT:
        - Execution stops
    """

    # ------------------------------------------------------------------
    # 90–99 — OPERATOR-INITIATED TERMINATION
    # ------------------------------------------------------------------

    OPERATOR_ABORT = 90
    """
    WHEN:
        - User interrupts execution (Ctrl-C / SIGINT)
    SET BY:
        - Signal handler
    EFFECT:
        - Execution stops cleanly
    """

    CONFIG_RESET = 91
    """
    WHEN:
        - Operator explicitly resets configuration
    SET BY:
        - config command
    EFFECT:
        - Execution ends after reset
    """
