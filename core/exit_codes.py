#!/usr/bin/env python3
"""
File: core/exit_codes.py

Purpose:
    Define the complete, authoritative process exit-code contract for the
    BitSight SDK and command-line interface.

Responsibilities:
    - Provide stable, deterministic, machine-consumable process exit codes.
    - Group execution outcomes into clearly defined operational categories.
    - Prevent exit-code reuse or semantic reassignment.
    - Provide one canonical source for process termination status values.
    - Support shell scripts, schedulers, orchestration platforms, monitoring
      systems, CI/CD pipelines, and automation tooling.

Contract requirements:
    - Exit-code meanings MUST remain stable across releases.
    - Existing numeric values MUST NOT be reassigned.
    - Existing exit-code names MUST NOT be reused for different outcomes.
    - New exit codes MUST use an unused numeric value.
    - Process exit codes MUST be emitted exactly once at process termination.
    - The final process exit code is the authoritative execution result.
    - Internal exceptions must be translated to the most specific applicable
      ExitCode before process termination.
    - Successful and non-failure outcomes occupy values 0 through 9.
    - Failure categories occupy values 10 through 99.

Usage example:
    from core.exit_codes import ExitCode

    return ExitCode.SUCCESS

Process termination example:
    import sys

    sys.exit(ExitCode.CONFIG_API_KEY_MISSING)

Compatibility:
    ExitCode inherits from IntEnum. Members can therefore be passed directly
    to sys.exit(), compared with integers, serialized, and consumed by code
    expecting a standard integer exit status.

Design note:
    Some operating systems and shells reserve or reinterpret selected exit
    values. This module limits application-defined values to the range 0-99
    to maintain broad process-level compatibility.
"""

from enum import IntEnum


class ExitCode(IntEnum):
    """
    Canonical process exit codes for the BitSight SDK and CLI.

    Each member represents one stable process-level execution outcome.

    Implementations must select the most specific applicable code. Generic
    UNKNOWN_ERROR values should be used only when no more precise code can be
    determined reliably.

    Exit codes are grouped by numeric range:

        0-9:
            Successful or intentional non-failure termination.

        10-19:
            Configuration errors.

        20-29:
            CLI command and argument errors.

        30-39:
            Network and transport errors.

        40-49:
            BitSight API errors.

        50-59:
            Database errors.

        60-69:
            Ingestion errors.

        70-79:
            Filesystem and I/O errors.

        80-89:
            Runtime and system errors.

        90-99:
            Internal state and logic errors.
    """

    # =======================================================================
    # 0-9: SUCCESS AND NON-FAILURE TERMINATION
    # =======================================================================

    SUCCESS = 0
    """
    The command completed successfully and all requested operations succeeded.
    """

    SUCCESS_NO_CHANGES = 1
    """
    The command completed successfully, but no state changes were required.
    """

    SUCCESS_EMPTY_RESULT = 2
    """
    The command completed successfully and the API returned zero records.
    """

    SUCCESS_PARTIAL_SCOPE = 3
    """
    The command completed successfully for the explicitly requested scope.
    """

    SUCCESS_ALREADY_CONFIGURED = 4
    """
    The requested configuration was already present and required no update.
    """

    SUCCESS_VALIDATION_OK = 5
    """
    A validation operation completed successfully.
    """

    SUCCESS_DRY_RUN_OK = 6
    """
    A dry-run operation completed successfully without applying changes.
    """

    SUCCESS_CACHE_HIT = 7
    """
    The operation completed successfully using cached data only.
    """

    SUCCESS_SKIPPED = 8
    """
    The operation was intentionally skipped by explicit operator choice.
    """

    SUCCESS_OPERATOR_EXIT = 9
    """
    The operator requested a clean and intentional process termination.
    """

    # =======================================================================
    # 10-19: CONFIGURATION ERRORS
    # =======================================================================

    CONFIG_FILE_MISSING = 10
    """
    A required configuration file could not be found.
    """

    CONFIG_FILE_INVALID = 11
    """
    The configuration file exists but is malformed, unreadable, or otherwise
    unusable.
    """

    CONFIG_VALUE_MISSING = 12
    """
    A required configuration value was not supplied.
    """

    CONFIG_VALUE_INVALID = 13
    """
    A configuration value was supplied but failed validation.
    """

    CONFIG_API_KEY_MISSING = 14
    """
    The BitSight API key is not configured.
    """

    CONFIG_API_KEY_INVALID = 15
    """
    The configured BitSight API key was rejected or determined to be invalid.
    """

    CONFIG_PROXY_INVALID = 16
    """
    The configured proxy is invalid, unsupported, or unreachable.
    """

    CONFIG_PERMISSION_DENIED = 17
    """
    The process lacks permission to read, create, modify, or remove the
    configuration file.
    """

    CONFIG_RESET_FAILED = 18
    """
    The configuration reset operation failed.
    """

    CONFIG_UNKNOWN_ERROR = 19
    """
    An unclassified configuration failure occurred.
    """

    # =======================================================================
    # 20-29: CLI AND ARGUMENT ERRORS
    # =======================================================================

    CLI_INVALID_COMMAND = 20
    """
    An unknown command or subcommand was requested.
    """

    CLI_INVALID_ARGUMENT = 21
    """
    A command-line argument was supplied with an invalid value.
    """

    CLI_MISSING_ARGUMENT = 22
    """
    A required command-line argument was not supplied.
    """

    CLI_ARGUMENT_CONFLICT = 23
    """
    Mutually exclusive or otherwise conflicting arguments were supplied.
    """

    CLI_HELP_REQUESTED = 24
    """
    Command-line help was requested and displayed successfully.
    """

    CLI_VERSION_REQUESTED = 25
    """
    Version information was requested and displayed successfully.
    """

    CLI_PARSE_ERROR = 26
    """
    Command-line argument parsing failed.
    """

    CLI_UNSUPPORTED_OPERATION = 27
    """
    The requested operation is not supported in the current execution context.
    """

    CLI_RUNTIME_ERROR = 28
    """
    A CLI-layer runtime failure occurred that does not belong to a more specific
    category.
    """

    CLI_UNKNOWN_ERROR = 29
    """
    An unclassified CLI failure occurred.
    """

    # =======================================================================
    # 30-39: NETWORK AND TRANSPORT ERRORS
    # =======================================================================

    NETWORK_UNREACHABLE = 30
    """
    The destination network could not be reached.
    """

    NETWORK_TIMEOUT = 31
    """
    A network operation exceeded its configured timeout.
    """

    NETWORK_DNS_FAILURE = 32
    """
    DNS resolution failed.
    """

    NETWORK_TLS_FAILURE = 33
    """
    TLS or SSL negotiation, certificate validation, or handshake processing
    failed.
    """

    NETWORK_PROXY_FAILURE = 34
    """
    A proxy connection, authentication, or forwarding operation failed.
    """

    NETWORK_CONNECTION_REFUSED = 35
    """
    The remote endpoint actively refused the connection.
    """

    NETWORK_INTERRUPTED = 36
    """
    The network connection was interrupted during data transfer.
    """

    NETWORK_RATE_LIMITED = 37
    """
    The remote service rejected or deferred the request because a rate limit
    was exceeded.
    """

    NETWORK_BAD_RESPONSE = 38
    """
    The remote service returned a malformed, incomplete, or non-parseable
    transport response.
    """

    NETWORK_UNKNOWN_ERROR = 39
    """
    An unclassified network or transport failure occurred.
    """

    # =======================================================================
    # 40-49: BITSIGHT API ERRORS
    # =======================================================================

    API_UNAUTHORIZED = 40
    """
    The BitSight API returned HTTP 401 Unauthorized.
    """

    API_FORBIDDEN = 41
    """
    The BitSight API returned HTTP 403 Forbidden.
    """

    API_NOT_FOUND = 42
    """
    The BitSight API returned HTTP 404 for the requested endpoint or resource.
    """

    API_CONFLICT = 43
    """
    The BitSight API returned HTTP 409 Conflict.
    """

    API_BAD_REQUEST = 44
    """
    The BitSight API returned HTTP 400 Bad Request.
    """

    API_UNPROCESSABLE = 45
    """
    The BitSight API returned HTTP 422 Unprocessable Entity.
    """

    API_SERVER_ERROR = 46
    """
    The BitSight API returned an HTTP 5xx server error.
    """

    API_SCHEMA_CHANGED = 47
    """
    The API response structure did not match the expected schema.
    """

    API_DEPRECATED_ENDPOINT = 48
    """
    The requested API endpoint has been deprecated, disabled, or removed.
    """

    API_UNKNOWN_ERROR = 49
    """
    An unclassified BitSight API failure occurred.
    """

    # =======================================================================
    # 50-59: DATABASE ERRORS
    # =======================================================================

    DB_CONNECTION_FAILED = 50
    """
    The application could not establish or maintain a database connection.
    """

    DB_AUTH_FAILED = 51
    """
    Database authentication or authorization failed.
    """

    DB_SCHEMA_MISSING = 52
    """
    A required database schema, table, view, or other object is missing.
    """

    DB_SCHEMA_MISMATCH = 53
    """
    The active database schema is incompatible with the application.
    """

    DB_WRITE_FAILED = 54
    """
    A database insert, update, delete, or merge operation failed.
    """

    DB_TRANSACTION_FAILED = 55
    """
    A database transaction failed and could not be completed successfully.
    """

    DB_CONSTRAINT_VIOLATION = 56
    """
    A database uniqueness, foreign-key, nullability, check, or other constraint
    was violated.
    """

    DB_TIMEOUT = 57
    """
    A database operation exceeded its configured timeout.
    """

    DB_READ_FAILED = 58
    """
    A database query or result-retrieval operation failed.
    """

    DB_UNKNOWN_ERROR = 59
    """
    An unclassified database failure occurred.
    """

    # =======================================================================
    # 60-69: INGESTION ERRORS
    # =======================================================================

    INGEST_START_FAILED = 60
    """
    The ingestion operation could not be initialized.
    """

    INGEST_PARTIAL_FAILURE = 61
    """
    Ingestion completed with one or more record-level failures.
    """

    INGEST_ZERO_RECORDS = 62
    """
    Ingestion ran but wrote zero records to the destination.
    """

    INGEST_SCHEMA_MAPPING_FAILED = 63
    """
    API response data could not be mapped to the target ingestion schema.
    """

    INGEST_DUPLICATE_KEY = 64
    """
    Ingestion encountered a duplicate primary or unique key.
    """

    INGEST_DATA_INVALID = 65
    """
    One or more records failed target-schema validation.
    """

    INGEST_ABORTED = 66
    """
    The ingestion operation was intentionally aborted before completion.
    """

    INGEST_STATE_CORRUPT = 67
    """
    Persisted or in-memory ingestion state is invalid, inconsistent, or corrupt.
    """

    INGEST_RETRY_EXHAUSTED = 68
    """
    All configured ingestion retry attempts were exhausted.
    """

    INGEST_UNKNOWN_ERROR = 69
    """
    An unclassified ingestion failure occurred.
    """

    # =======================================================================
    # 70-79: FILESYSTEM AND I/O ERRORS
    # =======================================================================

    FS_NOT_FOUND = 70
    """
    A required file or filesystem path does not exist.
    """

    FS_PERMISSION_DENIED = 71
    """
    The process lacks permission to access the requested filesystem resource.
    """

    FS_READ_FAILED = 72
    """
    A filesystem read operation failed.
    """

    FS_WRITE_FAILED = 73
    """
    A filesystem write operation failed.
    """

    FS_DISK_FULL = 74
    """
    The target filesystem has insufficient free space.
    """

    FS_PATH_INVALID = 75
    """
    A supplied filesystem path is invalid or unsupported.
    """

    FS_LOCKED = 76
    """
    A required file or filesystem resource is locked.
    """

    FS_CORRUPT = 77
    """
    A file or persisted data structure is corrupt.
    """

    FS_IO_ERROR = 78
    """
    A generic filesystem or input/output failure occurred.
    """

    FS_UNKNOWN_ERROR = 79
    """
    An unclassified filesystem failure occurred.
    """

    # =======================================================================
    # 80-89: RUNTIME AND SYSTEM ERRORS
    # =======================================================================

    RUNTIME_EXCEPTION = 80
    """
    An unhandled runtime exception terminated execution.
    """

    RUNTIME_INTERRUPT = 81
    """
    Execution was interrupted by the operator, typically through
    KeyboardInterrupt.
    """

    RUNTIME_SIGNAL_TERMINATED = 82
    """
    The process was terminated in response to an operating-system signal.
    """

    RUNTIME_RESOURCE_EXHAUSTED = 83
    """
    The process exhausted memory, handles, threads, or another required system
    resource.
    """

    RUNTIME_DEPENDENCY_MISSING = 84
    """
    A required runtime package, library, driver, executable, or service is
    unavailable.
    """

    RUNTIME_VERSION_INCOMPATIBLE = 85
    """
    The active Python, package, driver, or runtime version is incompatible with
    the application.
    """

    RUNTIME_THREAD_FAILURE = 86
    """
    A worker or application thread failed.
    """

    RUNTIME_DEADLOCK = 87
    """
    A runtime or database deadlock was detected.
    """

    RUNTIME_ASSERTION_FAILED = 88
    """
    An internal assertion failed.
    """

    RUNTIME_UNKNOWN_ERROR = 89
    """
    An unclassified runtime or system failure occurred.
    """

    # =======================================================================
    # 90-99: INTERNAL STATE AND LOGIC ERRORS
    # =======================================================================

    INTERNAL_STATE_INVALID = 90
    """
    The application entered an invalid internal state.
    """

    INTERNAL_INVARIANT_VIOLATION = 91
    """
    A required internal invariant was violated.
    """

    INTERNAL_DISPATCH_FAILURE = 92
    """
    Internal command, event, or operation dispatch failed.
    """

    INTERNAL_HANDLER_MISSING = 93
    """
    A required internal handler is missing or not implemented.
    """

    INTERNAL_UNREACHABLE_CODE = 94
    """
    A code path designated as unreachable was executed.
    """

    INTERNAL_CONFIG_DESYNC = 95
    """
    In-memory and persisted configuration state are inconsistent.
    """

    INTERNAL_DB_DESYNC = 96
    """
    Application state and database state are inconsistent.
    """

    INTERNAL_CACHE_DESYNC = 97
    """
    Cached state and authoritative state are inconsistent.
    """

    INTERNAL_CORRUPTION = 98
    """
    Internal application data or state corruption was detected.
    """

    INTERNAL_UNKNOWN_ERROR = 99
    """
    An unclassified internal application failure occurred.
    """
