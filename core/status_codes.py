#!/usr/bin/env python3
"""
File: core/status_codes.py

Purpose:
    Define the complete runtime status-code catalog used by the BitSight SDK
    and command-line interface.

Responsibilities:
    - Represent concrete runtime events with stable integer values.
    - Provide detailed operational status independently of process exit codes.
    - Standardize status reporting across API, ingestion, database, CLI, and
      internal application components.
    - Support structured logging, telemetry, monitoring, testing, and machine
      interpretation.
    - Maintain a fully populated and non-overlapping status range from 0
      through 99.

StatusCode versus ExitCode:
    StatusCode describes the specific runtime event produced by an operation.

    ExitCode describes the final process-level execution outcome emitted when
    the application terminates.

    Multiple StatusCode values may map to the same ExitCode. For example,
    several transport failures may map to one network-related process exit
    category.

Contract requirements:
    - Every status code represents exactly one concrete runtime condition.
    - Existing numeric values must never change meaning.
    - Existing numeric values must never be reused.
    - Existing member names must not be reassigned to different conditions.
    - New status codes must use previously unused values.
    - Callers should select the most specific applicable status.
    - UNKNOWN values should be used only when no specific status can be
      determined reliably.
    - All values from 0 through 99 are intentionally assigned.
    - StatusCode values do not terminate the process.

Usage example:
    from core.status_codes import StatusCode

    status = StatusCode.API_RATE_LIMITED

Logging example:
    logger.warning(
        "BitSight request failed | status=%s value=%d",
        status.name,
        status.value,
    )

Serialization example:
    payload = {
        "status_code": int(status),
        "status_name": status.name,
    }

Compatibility:
    StatusCode inherits from IntEnum. Members therefore behave as integers
    while retaining stable symbolic names.
"""

from enum import IntEnum


class StatusCode(IntEnum):
    """
    Canonical runtime status codes for the BitSight SDK and CLI.

    Numeric ranges:

        0-9:
            Successful and non-error runtime outcomes.

        10-19:
            Configuration events and failures.

        20-29:
            Authentication, authorization, licensing, and entitlement events.

        30-39:
            Network and transport failures.

        40-49:
            BitSight API protocol and response-semantic events.

        50-59:
            Data parsing, validation, and transformation failures.

        60-69:
            Database failures.

        70-79:
            Ingestion-specific events and failures.

        80-89:
            General execution and runtime failures.

        90-99:
            Internal defects, state corruption, and contract violations.
    """

    # =======================================================================
    # 0-9: SUCCESS AND NON-ERROR TERMINATION
    # =======================================================================

    OK = 0
    """
    Execution completed successfully and all requested operations completed.
    """

    OK_NO_DATA = 1
    """
    Execution completed successfully and the source returned no records.

    This status should be used only when an empty result is valid for the
    executed operation.
    """

    OK_NO_CHANGES = 2
    """
    Execution completed successfully, but the fetched data produced no changes
    in the target state.
    """

    OK_ALREADY_CURRENT = 3
    """
    Execution completed successfully because the target already matched the
    required state.
    """

    OK_PARTIAL = 4
    """
    Execution completed successfully for an intentionally limited or explicitly
    defined subset of the total available scope.
    """

    OK_SKIPPED = 5
    """
    Execution was intentionally skipped because a validated runtime condition
    indicated that processing was unnecessary.
    """

    OK_DRY_RUN = 6
    """
    Execution completed successfully in dry-run mode and modified no state.
    """

    OK_NOT_APPLICABLE = 7
    """
    Execution completed successfully because the requested operation did not
    apply to the current context.
    """

    OK_DEPRECATED_ENDPOINT = 8
    """
    Execution completed successfully using an API endpoint that is currently
    deprecated.

    This status should be accompanied by an operational warning so callers can
    migrate before the endpoint is removed.
    """

    OK_EXPERIMENTAL_ENDPOINT = 9
    """
    Execution completed successfully using an experimental, preview, or beta
    API endpoint.
    """

    # =======================================================================
    # 10-19: CONFIGURATION
    # =======================================================================

    CONFIG_MISSING = 10
    """
    A required configuration file or configuration setting was not found.
    """

    CONFIG_INVALID = 11
    """
    Configuration was present but failed structural or semantic validation.
    """

    CONFIG_UNREADABLE = 12
    """
    A configuration file existed but could not be read or decoded.
    """

    CONFIG_PERMISSION_DENIED = 13
    """
    The process was denied access to a required configuration resource.
    """

    CONFIG_INCOMPLETE = 14
    """
    Configuration was present but did not contain all required fields.
    """

    CONFIG_CONFLICT = 15
    """
    Configuration contained mutually exclusive, contradictory, or incompatible
    values.
    """

    CONFIG_UNSUPPORTED = 16
    """
    Configuration selected an unsupported backend, mode, option, or feature.
    """

    CONFIG_ENV_MISSING = 17
    """
    A required environment variable was not defined.
    """

    CONFIG_ENV_INVALID = 18
    """
    An environment variable was defined but contained invalid data.
    """

    CONFIG_RESET_FAILED = 19
    """
    A configuration reset or restoration operation failed.
    """

    # =======================================================================
    # 20-29: AUTHENTICATION AND AUTHORIZATION
    # =======================================================================

    AUTH_API_KEY_MISSING = 20
    """
    A required BitSight API key was not provided.
    """

    AUTH_API_KEY_INVALID = 21
    """
    The supplied BitSight API key was rejected as invalid.
    """

    AUTH_API_KEY_EXPIRED = 22
    """
    The supplied BitSight API key is expired, disabled, revoked, or otherwise
    no longer valid.
    """

    AUTH_FORBIDDEN = 23
    """
    Authentication succeeded, but access to the requested resource or operation
    was denied.
    """

    AUTH_SCOPE_VIOLATION = 24
    """
    The authenticated credential is not authorized for the requested company,
    portfolio, tenant, or operational scope.
    """

    AUTH_LICENSE_MISSING = 25
    """
    The account does not include the license or entitlement required for the
    requested operation.
    """

    AUTH_FEATURE_DISABLED = 26
    """
    The requested feature is disabled for the authenticated account.
    """

    AUTH_ROLE_INSUFFICIENT = 27
    """
    The authenticated identity does not have a role permitting the requested
    operation.
    """

    AUTH_COMPANY_RESTRICTED = 28
    """
    Access to the specified BitSight company or organization is restricted.
    """

    AUTH_UNKNOWN = 29
    """
    Authentication or authorization failed for a reason that could not be
    classified more specifically.
    """

    # =======================================================================
    # 30-39: TRANSPORT AND NETWORK
    # =======================================================================

    TRANSPORT_CONNECTION_FAILED = 30
    """
    A network connection to the BitSight service could not be established.
    """

    TRANSPORT_TIMEOUT = 31
    """
    A network request exceeded the configured timeout.
    """

    TRANSPORT_DNS_FAILURE = 32
    """
    DNS resolution for the remote service failed.
    """

    TRANSPORT_SSL_ERROR = 33
    """
    TLS or SSL negotiation, certificate validation, or secure-channel setup
    failed.
    """

    TRANSPORT_PROXY_ERROR = 34
    """
    Proxy routing or proxy configuration caused the request to fail.
    """

    TRANSPORT_PROXY_AUTH_FAILED = 35
    """
    Authentication with the configured proxy failed.
    """

    TRANSPORT_CONNECTION_RESET = 36
    """
    The remote peer or an intermediate network device reset the connection.
    """

    TRANSPORT_UNREACHABLE = 37
    """
    The remote network or destination host was unreachable.
    """

    TRANSPORT_PARTIAL_RESPONSE = 38
    """
    A response began but was interrupted before the complete payload was
    received.
    """

    TRANSPORT_UNKNOWN = 39
    """
    A transport or network failure occurred that could not be classified more
    specifically.
    """

    # =======================================================================
    # 40-49: BITSIGHT API SEMANTICS
    # =======================================================================

    API_BAD_REQUEST = 40
    """
    The BitSight API rejected the request as malformed or invalid with
    HTTP 400.
    """

    API_UNAUTHORIZED = 41
    """
    The BitSight API rejected authentication with HTTP 401.
    """

    API_FORBIDDEN = 42
    """
    The BitSight API denied access to the authenticated request with HTTP 403.
    """

    API_NOT_FOUND = 43
    """
    The requested BitSight endpoint or resource was not found with HTTP 404.
    """

    API_METHOD_NOT_ALLOWED = 44
    """
    The requested HTTP method is not supported by the endpoint, corresponding
    to HTTP 405.
    """

    API_CONFLICT = 45
    """
    The request conflicted with the current remote state, corresponding to
    HTTP 409.
    """

    API_RATE_LIMITED = 46
    """
    The BitSight API rate limit was exceeded, corresponding to HTTP 429.
    """

    API_SERVER_ERROR = 47
    """
    The BitSight API returned a server-side HTTP 5xx response.
    """

    API_SCHEMA_CHANGED = 48
    """
    The API response structure differed from the expected application contract.
    """

    API_UNEXPECTED_RESPONSE = 49
    """
    The API response was received but could not be interpreted as a valid
    BitSight payload.
    """

    # =======================================================================
    # 50-59: DATA PARSING AND VALIDATION
    # =======================================================================

    DATA_PARSE_ERROR = 50
    """
    The application failed to parse a response or input payload.
    """

    DATA_VALIDATION_ERROR = 51
    """
    Parsed data failed one or more application validation rules.
    """

    DATA_SCHEMA_MISMATCH = 52
    """
    Parsed data did not conform to the expected data schema.
    """

    DATA_TRUNCATION = 53
    """
    Data was truncated because of an application, database, protocol, or field
    size limit.
    """

    DATA_DUPLICATE = 54
    """
    A duplicate record was detected during processing.
    """

    DATA_MISSING_FIELD = 55
    """
    A required data field was absent.
    """

    DATA_TYPE_MISMATCH = 56
    """
    A field value did not match the expected data type.
    """

    DATA_ENCODING_ERROR = 57
    """
    Data decoding, encoding, or character conversion failed.
    """

    DATA_SANITIZATION_FAILED = 58
    """
    Incoming data could not be sanitized according to the required rules.
    """

    DATA_UNKNOWN = 59
    """
    A data-processing failure occurred that could not be classified more
    specifically.
    """

    # =======================================================================
    # 60-69: DATABASE
    # =======================================================================

    DB_CONNECTION_FAILED = 60
    """
    The application failed to establish or maintain a database connection.
    """

    DB_AUTH_FAILED = 61
    """
    Database authentication or authorization failed.
    """

    DB_SCHEMA_MISSING = 62
    """
    A required database schema, table, view, procedure, or other object was
    missing.
    """

    DB_SCHEMA_MISMATCH = 63
    """
    The active database structure did not match the expected application
    schema.
    """

    DB_CONSTRAINT_VIOLATION = 64
    """
    A uniqueness, foreign-key, nullability, check, or other database constraint
    was violated.
    """

    DB_INSERT_FAILED = 65
    """
    A database insert operation failed.
    """

    DB_UPDATE_FAILED = 66
    """
    A database update operation failed.
    """

    DB_TRANSACTION_FAILED = 67
    """
    A database transaction failed or could not be committed successfully.
    """

    DB_TIMEOUT = 68
    """
    A database operation exceeded its configured timeout.
    """

    DB_UNKNOWN = 69
    """
    A database failure occurred that could not be classified more specifically.
    """

    # =======================================================================
    # 70-79: INGESTION LOGIC
    # =======================================================================

    INGESTION_START_FAILED = 70
    """
    The ingestion workload could not be initialized.
    """

    INGESTION_FETCH_FAILED = 71
    """
    The ingestion workload failed while fetching source data.
    """

    INGESTION_ZERO_RECORDS = 72
    """
    The ingestion workload fetched fewer records than the configured minimum
    expectation.

    This status is distinct from OK_NO_DATA. OK_NO_DATA represents an accepted
    empty result, while this status represents an operationally unexpected
    record count.
    """

    INGESTION_WRITE_FAILED = 73
    """
    The ingestion workload failed to write records to the destination.
    """

    INGESTION_PARTIAL_WRITE = 74
    """
    Some ingestion records were processed successfully while others failed.
    """

    INGESTION_FLUSH_FAILED = 75
    """
    A buffered ingestion flush operation failed.
    """

    INGESTION_BACKFILL_FAILED = 76
    """
    An ingestion backfill operation failed.
    """

    INGESTION_INTERRUPTED = 77
    """
    The ingestion workload was interrupted by an external signal or operator
    action.
    """

    INGESTION_STATE_UPDATE_FAILED = 78
    """
    The ingestion workload completed record processing but failed to update
    ingestion state or reconciliation metadata.
    """

    INGESTION_UNKNOWN = 79
    """
    An ingestion failure occurred that could not be classified more
    specifically.
    """

    # =======================================================================
    # 80-89: EXECUTION AND RUNTIME
    # =======================================================================

    EXECUTION_INTERRUPTED = 80
    """
    General execution was interrupted by the operator or operating system.
    """

    EXECUTION_UNHANDLED_EXCEPTION = 81
    """
    An exception escaped the expected application error-handling path.
    """

    EXECUTION_TIMEOUT = 82
    """
    Overall execution exceeded the permitted runtime.
    """

    EXECUTION_RESOURCE_EXHAUSTED = 83
    """
    Execution failed because memory, storage, threads, handles, or another
    required resource was exhausted.
    """

    EXECUTION_DEPENDENCY_FAILED = 84
    """
    A required package, driver, service, process, or other dependency failed.
    """

    EXECUTION_INVALID_STATE = 85
    """
    Execution entered a state that does not permit the requested operation.
    """

    EXECUTION_NOT_IMPLEMENTED = 86
    """
    The requested operation is recognized but has not been implemented.
    """

    EXECUTION_ABORTED = 87
    """
    Execution was explicitly aborted before normal completion.
    """

    EXECUTION_RETRY_EXHAUSTED = 88
    """
    All configured retry attempts were exhausted without success.
    """

    EXECUTION_UNKNOWN = 89
    """
    A runtime execution failure occurred that could not be classified more
    specifically.
    """

    # =======================================================================
    # 90-99: INTERNAL AND INVARIANT VIOLATIONS
    # =======================================================================

    INTERNAL_ASSERTION_FAILED = 90
    """
    An internal assertion failed.
    """

    INTERNAL_INVARIANT_VIOLATION = 91
    """
    A required application invariant was violated.
    """

    INTERNAL_STATE_CORRUPTION = 92
    """
    Internal state was inconsistent, invalid, or corrupted.
    """

    INTERNAL_UNEXPECTED_NULL = 93
    """
    A required internal value was unexpectedly None.
    """

    INTERNAL_UNREACHABLE_CODE = 94
    """
    Execution reached a code path designated as unreachable.
    """

    INTERNAL_CONFIGURATION_DRIFT = 95
    """
    Runtime configuration differed from the previously validated or persisted
    configuration state.
    """

    INTERNAL_SCHEMA_REGRESSION = 96
    """
    An observed API or database schema regressed from a previously supported
    structure.
    """

    INTERNAL_CONTRACT_BREACH = 97
    """
    An internal component violated a documented interface or behavioral
    contract.
    """

    INTERNAL_VERSION_MISMATCH = 98
    """
    Application components, dependencies, schemas, or runtime versions were
    incompatible.
    """

    INTERNAL_UNKNOWN = 99
    """
    An internal failure occurred that could not be classified more specifically
    and likely indicates an application defect.
    """
