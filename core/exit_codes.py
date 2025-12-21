#!/usr/bin/env python3
"""
BitSight SDK + CLI
Exit Codes

This module defines ALL process exit codes used by the BitSight SDK + CLI.
Exit codes are deterministic, stable, and machine-consumable.

RULES:
- Exit codes NEVER change meaning.
- Exit codes are NOT reused.
- Exit codes are emitted exactly once at process termination.
- Exit codes are the final authoritative signal of execution outcome.
"""

from enum import IntEnum


class ExitCode(IntEnum):

    # ============================================================
    # 0–9 : SUCCESS / NON-FAILURE TERMINATION
    # ============================================================

    SUCCESS = 0
    # Command completed successfully. All requested operations succeeded.

    SUCCESS_NO_CHANGES = 1
    # Command completed successfully. No changes were required.

    SUCCESS_EMPTY_RESULT = 2
    # Command completed successfully. API returned zero records.

    SUCCESS_PARTIAL_SCOPE = 3
    # Command completed successfully for requested scope only.

    SUCCESS_ALREADY_CONFIGURED = 4
    # Requested configuration already present.

    SUCCESS_VALIDATION_OK = 5
    # Validation command completed successfully.

    SUCCESS_DRY_RUN_OK = 6
    # Dry-run execution completed successfully.

    SUCCESS_CACHE_HIT = 7
    # Execution completed using cached data only.

    SUCCESS_SKIPPED = 8
    # Execution intentionally skipped by operator choice.

    SUCCESS_OPERATOR_EXIT = 9
    # Operator requested clean termination.


    # ============================================================
    # 10–19 : CONFIGURATION ERRORS
    # ============================================================

    CONFIG_FILE_MISSING = 10
    # Required config file not found.

    CONFIG_FILE_INVALID = 11
    # Config file exists but is malformed or unreadable.

    CONFIG_VALUE_MISSING = 12
    # Required config value not set.

    CONFIG_VALUE_INVALID = 13
    # Config value set but invalid.

    CONFIG_API_KEY_MISSING = 14
    # API key not configured.

    CONFIG_API_KEY_INVALID = 15
    # API key rejected by BitSight.

    CONFIG_PROXY_INVALID = 16
    # Proxy configuration invalid or unreachable.

    CONFIG_PERMISSION_DENIED = 17
    # Insufficient permissions to read/write config.

    CONFIG_RESET_FAILED = 18
    # Config reset failed.

    CONFIG_UNKNOWN_ERROR = 19
    # Unclassified configuration error.


    # ============================================================
    # 20–29 : CLI / ARGUMENT ERRORS
    # ============================================================

    CLI_INVALID_COMMAND = 20
    # Unknown command or subcommand.

    CLI_INVALID_ARGUMENT = 21
    # Invalid argument value.

    CLI_MISSING_ARGUMENT = 22
    # Required argument not supplied.

    CLI_ARGUMENT_CONFLICT = 23
    # Mutually exclusive arguments supplied.

    CLI_HELP_REQUESTED = 24
    # Help requested and displayed.

    CLI_VERSION_REQUESTED = 25
    # Version requested and displayed.

    CLI_PARSE_ERROR = 26
    # Argument parsing failure.

    CLI_UNSUPPORTED_OPERATION = 27
    # Operation not supported in current context.

    CLI_RUNTIME_ERROR = 28
    # CLI runtime failure not tied to other categories.

    CLI_UNKNOWN_ERROR = 29
    # Unclassified CLI error.


    # ============================================================
    # 30–39 : NETWORK / TRANSPORT ERRORS
    # ============================================================

    NETWORK_UNREACHABLE = 30
    # Network unreachable.

    NETWORK_TIMEOUT = 31
    # Network request timed out.

    NETWORK_DNS_FAILURE = 32
    # DNS resolution failure.

    NETWORK_TLS_FAILURE = 33
    # TLS/SSL handshake failure.

    NETWORK_PROXY_FAILURE = 34
    # Proxy connection failure.

    NETWORK_CONNECTION_REFUSED = 35
    # Connection actively refused.

    NETWORK_INTERRUPTED = 36
    # Network connection interrupted mid-transfer.

    NETWORK_RATE_LIMITED = 37
    # API rate limit exceeded.

    NETWORK_BAD_RESPONSE = 38
    # Non-parseable or malformed response.

    NETWORK_UNKNOWN_ERROR = 39
    # Unclassified network error.


    # ============================================================
    # 40–49 : API ERRORS (BitSight)
    # ============================================================

    API_UNAUTHORIZED = 40
    # HTTP 401 unauthorized.

    API_FORBIDDEN = 41
    # HTTP 403 forbidden.

    API_NOT_FOUND = 42
    # HTTP 404 endpoint or resource not found.

    API_CONFLICT = 43
    # HTTP 409 conflict.

    API_BAD_REQUEST = 44
    # HTTP 400 bad request.

    API_UNPROCESSABLE = 45
    # HTTP 422 unprocessable entity.

    API_SERVER_ERROR = 46
    # HTTP 5xx server error.

    API_SCHEMA_CHANGED = 47
    # API response schema changed unexpectedly.

    API_DEPRECATED_ENDPOINT = 48
    # Endpoint deprecated or removed.

    API_UNKNOWN_ERROR = 49
    # Unclassified API error.


    # ============================================================
    # 50–59 : DATABASE ERRORS
    # ============================================================

    DB_CONNECTION_FAILED = 50
    # Database connection failed.

    DB_AUTH_FAILED = 51
    # Database authentication failed.

    DB_SCHEMA_MISSING = 52
    # Required schema not present.

    DB_SCHEMA_MISMATCH = 53
    # Schema version incompatible.

    DB_WRITE_FAILED = 54
    # Insert/update failed.

    DB_TRANSACTION_FAILED = 55
    # Transaction rolled back.

    DB_CONSTRAINT_VIOLATION = 56
    # Constraint violation.

    DB_TIMEOUT = 57
    # Database operation timed out.

    DB_READ_FAILED = 58
    # Read operation failed.

    DB_UNKNOWN_ERROR = 59
    # Unclassified database error.


    # ============================================================
    # 60–69 : INGESTION ERRORS
    # ============================================================

    INGEST_START_FAILED = 60
    # Ingestion could not start.

    INGEST_PARTIAL_FAILURE = 61
    # Some records failed ingestion.

    INGEST_ZERO_RECORDS = 62
    # Ingestion ran but no records were written.

    INGEST_SCHEMA_MAPPING_FAILED = 63
    # Mapping API payload to schema failed.

    INGEST_DUPLICATE_KEY = 64
    # Duplicate primary key encountered.

    INGEST_DATA_INVALID = 65
    # Data invalid for target schema.

    INGEST_ABORTED = 66
    # Ingestion aborted intentionally.

    INGEST_STATE_CORRUPT = 67
    # Collection state invalid or corrupt.

    INGEST_RETRY_EXHAUSTED = 68
    # Retry attempts exhausted.

    INGEST_UNKNOWN_ERROR = 69
    # Unclassified ingestion error.


    # ============================================================
    # 70–79 : FILESYSTEM / IO ERRORS
    # ============================================================

    FS_NOT_FOUND = 70
    # File or path not found.

    FS_PERMISSION_DENIED = 71
    # Permission denied.

    FS_READ_FAILED = 72
    # Read failed.

    FS_WRITE_FAILED = 73
    # Write failed.

    FS_DISK_FULL = 74
    # Disk full.

    FS_PATH_INVALID = 75
    # Invalid path.

    FS_LOCKED = 76
    # File locked.

    FS_CORRUPT = 77
    # File corrupt.

    FS_IO_ERROR = 78
    # Generic IO error.

    FS_UNKNOWN_ERROR = 79
    # Unclassified filesystem error.


    # ============================================================
    # 80–89 : RUNTIME / SYSTEM ERRORS
    # ============================================================

    RUNTIME_EXCEPTION = 80
    # Unhandled exception.

    RUNTIME_INTERRUPT = 81
    # Keyboard interrupt.

    RUNTIME_SIGNAL_TERMINATED = 82
    # Terminated by OS signal.

    RUNTIME_RESOURCE_EXHAUSTED = 83
    # Memory or resource exhaustion.

    RUNTIME_DEPENDENCY_MISSING = 84
    # Required dependency missing.

    RUNTIME_VERSION_INCOMPATIBLE = 85
    # Incompatible Python or dependency version.

    RUNTIME_THREAD_FAILURE = 86
    # Thread execution failure.

    RUNTIME_DEADLOCK = 87
    # Deadlock detected.

    RUNTIME_ASSERTION_FAILED = 88
    # Internal assertion failed.

    RUNTIME_UNKNOWN_ERROR = 89
    # Unclassified runtime error.


    # ============================================================
    # 90–99 : INTERNAL / LOGIC ERRORS
    # ============================================================

    INTERNAL_STATE_INVALID = 90
    # Internal state invalid.

    INTERNAL_INVARIANT_VIOLATION = 91
    # Invariant violated.

    INTERNAL_DISPATCH_FAILURE = 92
    # Command dispatch failure.

    INTERNAL_HANDLER_MISSING = 93
    # Handler not implemented.

    INTERNAL_UNREACHABLE_CODE = 94
    # Unreachable code path executed.

    INTERNAL_CONFIG_DESYNC = 95
    # Config state desynchronized.

    INTERNAL_DB_DESYNC = 96
    # Database state desynchronized.

    INTERNAL_CACHE_DESYNC = 97
    # Cache state desynchronized.

    INTERNAL_CORRUPTION = 98
    # Internal corruption detected.

    INTERNAL_UNKNOWN_ERROR = 99
    # Unclassified internal error.
