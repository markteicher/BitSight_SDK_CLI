#!/usr/bin/env python3
"""
core/status_codes.py

Mechanical status code catalog for BitSight SDK + CLI.

Rules:
- Each status code represents ONE concrete runtime event.
- No code is reused for multiple failure modes.
- All ranges are fully populated.
- Descriptions are factual and operational.
"""

from enum import IntEnum


class StatusCode(IntEnum):

    # ============================================================
    # 0–9 : SUCCESS / NON-ERROR TERMINATION
    # ============================================================

    OK = 0
    """Execution completed successfully. All operations completed as requested."""

    OK_NO_DATA = 1
    """Execution completed successfully. Endpoint returned zero records."""

    OK_NO_CHANGES = 2
    """Execution completed successfully. Data fetched but no state changes occurred."""

    OK_ALREADY_CURRENT = 3
    """Execution completed successfully. Target state already matched source state."""

    OK_PARTIAL = 4
    """Execution completed with partial success. Some records succeeded, others skipped."""

    OK_SKIPPED = 5
    """Execution intentionally skipped due to configuration or runtime condition."""

    OK_DRY_RUN = 6
    """Execution completed successfully in dry-run mode. No state was modified."""

    OK_NOT_APPLICABLE = 7
    """Execution completed successfully. Operation not applicable in this context."""

    OK_DEPRECATED_ENDPOINT = 8
    """Execution completed successfully using a deprecated API endpoint."""

    OK_EXPERIMENTAL_ENDPOINT = 9
    """Execution completed successfully using an experimental or beta endpoint."""

    # ============================================================
    # 10–19 : CONFIGURATION
    # ============================================================

    CONFIG_MISSING = 10
    """Required configuration file or setting was not found."""

    CONFIG_INVALID = 11
    """Configuration exists but failed validation."""

    CONFIG_UNREADABLE = 12
    """Configuration file exists but could not be read."""

    CONFIG_PERMISSION_DENIED = 13
    """Access to configuration file was denied."""

    CONFIG_INCOMPLETE = 14
    """Configuration is missing required fields."""

    CONFIG_CONFLICT = 15
    """Configuration contains mutually exclusive or conflicting values."""

    CONFIG_UNSUPPORTED = 16
    """Configuration specifies an unsupported option or mode."""

    CONFIG_ENV_MISSING = 17
    """Required environment variable is missing."""

    CONFIG_ENV_INVALID = 18
    """Environment variable exists but contains invalid data."""

    CONFIG_RESET_FAILED = 19
    """Configuration reset operation failed."""

    # ============================================================
    # 20–29 : AUTHENTICATION / AUTHORIZATION
    # ============================================================

    AUTH_API_KEY_MISSING = 20
    """API key was not provided."""

    AUTH_API_KEY_INVALID = 21
    """API key was rejected by the BitSight API."""

    AUTH_API_KEY_EXPIRED = 22
    """API key was valid but has expired."""

    AUTH_FORBIDDEN = 23
    """Authenticated successfully, but access to the requested resource is forbidden."""

    AUTH_SCOPE_VIOLATION = 24
    """API key is not authorized for the requested company or scope."""

    AUTH_LICENSE_MISSING = 25
    """Required BitSight license or entitlement is not present."""

    AUTH_FEATURE_DISABLED = 26
    """Requested feature is disabled for this account."""

    AUTH_ROLE_INSUFFICIENT = 27
    """User role does not permit the requested action."""

    AUTH_COMPANY_RESTRICTED = 28
    """Access to the specified company is restricted."""

    AUTH_UNKNOWN = 29
    """Authentication or authorization failed for an unknown reason."""

    # ============================================================
    # 30–39 : TRANSPORT / NETWORK
    # ============================================================

    TRANSPORT_CONNECTION_FAILED = 30
    """Failed to establish network connection to the BitSight API."""

    TRANSPORT_TIMEOUT = 31
    """Network request timed out."""

    TRANSPORT_DNS_FAILURE = 32
    """DNS resolution failed."""

    TRANSPORT_SSL_ERROR = 33
    """TLS/SSL negotiation failed."""

    TRANSPORT_PROXY_ERROR = 34
    """Proxy configuration caused request failure."""

    TRANSPORT_PROXY_AUTH_FAILED = 35
    """Proxy authentication failed."""

    TRANSPORT_CONNECTION_RESET = 36
    """Connection was reset by the remote peer."""

    TRANSPORT_UNREACHABLE = 37
    """Network destination is unreachable."""

    TRANSPORT_PARTIAL_RESPONSE = 38
    """Partial response received due to connection interruption."""

    TRANSPORT_UNKNOWN = 39
    """Unknown transport or network error."""

    # ============================================================
    # 40–49 : API SEMANTICS
    # ============================================================

    API_BAD_REQUEST = 40
    """Request was malformed or contained invalid parameters (HTTP 400)."""

    API_UNAUTHORIZED = 41
    """Authentication failed (HTTP 401)."""

    API_FORBIDDEN = 42
    """Authenticated successfully, but API access is forbidden (HTTP 403)."""

    API_NOT_FOUND = 43
    """Requested endpoint or resource does not exist (HTTP 404)."""

    API_METHOD_NOT_ALLOWED = 44
    """HTTP method is not supported by the endpoint (HTTP 405)."""

    API_CONFLICT = 45
    """Request conflicts with current state (HTTP 409)."""

    API_RATE_LIMITED = 46
    """API rate limit exceeded (HTTP 429)."""

    API_SERVER_ERROR = 47
    """BitSight API returned a server-side error (HTTP 5xx)."""

    API_SCHEMA_CHANGED = 48
    """API response schema differs from expected contract."""

    API_UNEXPECTED_RESPONSE = 49
    """API response could not be interpreted."""

    # ============================================================
    # 50–59 : DATA PARSING / VALIDATION
    # ============================================================

    DATA_PARSE_ERROR = 50
    """Failed to parse API response payload."""

    DATA_VALIDATION_ERROR = 51
    """Parsed data failed validation rules."""

    DATA_SCHEMA_MISMATCH = 52
    """Parsed data does not match expected schema."""

    DATA_TRUNCATION = 53
    """Data was truncated due to size constraints."""

    DATA_DUPLICATE = 54
    """Duplicate record detected."""

    DATA_MISSING_FIELD = 55
    """Required data field is missing."""

    DATA_TYPE_MISMATCH = 56
    """Field type does not match expected type."""

    DATA_ENCODING_ERROR = 57
    """Data encoding error encountered."""

    DATA_SANITIZATION_FAILED = 58
    """Failed to sanitize incoming data."""

    DATA_UNKNOWN = 59
    """Unknown data handling error."""

    # ============================================================
    # 60–69 : DATABASE
    # ============================================================

    DB_CONNECTION_FAILED = 60
    """Failed to connect to the database."""

    DB_AUTH_FAILED = 61
    """Database authentication failed."""

    DB_SCHEMA_MISSING = 62
    """Required database schema or table is missing."""

    DB_SCHEMA_MISMATCH = 63
    """Database schema does not match expected structure."""

    DB_CONSTRAINT_VIOLATION = 64
    """Database constraint violation occurred."""

    DB_INSERT_FAILED = 65
    """Failed to insert record into database."""

    DB_UPDATE_FAILED = 66
    """Failed to update record in database."""

    DB_TRANSACTION_FAILED = 67
    """Database transaction failed and was rolled back."""

    DB_TIMEOUT = 68
    """Database operation timed out."""

    DB_UNKNOWN = 69
    """Unknown database error."""

    # ============================================================
    # 70–79 : INGESTION LOGIC
    # ============================================================

    INGESTION_START_FAILED = 70
    """Ingestion process failed to initialize."""

    INGESTION_FETCH_FAILED = 71
    """Failed to fetch data from API during ingestion."""

    INGESTION_ZERO_RECORDS = 72
    """Ingestion returned zero records when records were expected."""

    INGESTION_WRITE_FAILED = 73
    """Failed to write ingested data to database."""

    INGESTION_PARTIAL_WRITE = 74
    """Only a subset of records were written successfully."""

    INGESTION_FLUSH_FAILED = 75
    """Flush operation failed."""

    INGESTION_BACKFILL_FAILED = 76
    """Backfill operation failed."""

    INGESTION_INTERRUPTED = 77
    """Ingestion interrupted by external signal."""

    INGESTION_STATE_UPDATE_FAILED = 78
    """Failed to update ingestion state metadata."""

    INGESTION_UNKNOWN = 79
    """Unknown ingestion error."""

    # ============================================================
    # 80–89 : EXECUTION / RUNTIME
    # ============================================================

    EXECUTION_INTERRUPTED = 80
    """Execution interrupted by user or system."""

    EXECUTION_UNHANDLED_EXCEPTION = 81
    """Unhandled exception occurred during execution."""

    EXECUTION_TIMEOUT = 82
    """Execution exceeded allowed runtime."""

    EXECUTION_RESOURCE_EXHAUSTED = 83
    """Execution failed due to resource exhaustion."""

    EXECUTION_DEPENDENCY_FAILED = 84
    """Required dependency failed."""

    EXECUTION_INVALID_STATE = 85
    """Execution entered an invalid state."""

    EXECUTION_NOT_IMPLEMENTED = 86
    """Requested operation is not implemented."""

    EXECUTION_ABORTED = 87
    """Execution was explicitly aborted."""

    EXECUTION_RETRY_EXHAUSTED = 88
    """All retry attempts were exhausted."""

    EXECUTION_UNKNOWN = 89
    """Unknown execution error."""

    # ============================================================
    # 90–99 : INTERNAL / INVARIANT VIOLATIONS
    # ============================================================

    INTERNAL_ASSERTION_FAILED = 90
    """Required internal assertion failed."""

    INTERNAL_INVARIANT_VIOLATION = 91
    """A core system invariant was violated."""

    INTERNAL_STATE_CORRUPTION = 92
    """Internal state was detected to be inconsistent or corrupted."""

    INTERNAL_UNEXPECTED_NULL = 93
    """A required internal value was unexpectedly None."""

    INTERNAL_UNREACHABLE_CODE = 94
    """Execution reached a code path marked as unreachable."""

    INTERNAL_CONFIGURATION_DRIFT = 95
    """Runtime configuration differs from validated configuration."""

    INTERNAL_SCHEMA_REGRESSION = 96
    """Observed schema regressed from previously known structure."""

    INTERNAL_CONTRACT_BREACH = 97
    """Internal interface contract between components was violated."""

    INTERNAL_VERSION_MISMATCH = 98
    """Component versions are incompatible at runtime."""

    INTERNAL_UNKNOWN = 99
    """Unknown internal error indicating a system defect."""
