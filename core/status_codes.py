# core/status_codes.py
from enum import Enum

class StatusCode(str, Enum):
    # -------------------------
    # SUCCESS
    # -------------------------
    OK = "OK"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"

    # -------------------------
    # CONFIGURATION
    # -------------------------
    CONFIG_MISSING = "CONFIG_MISSING"
    CONFIG_INVALID = "CONFIG_INVALID"
    CONFIG_UNREADABLE = "CONFIG_UNREADABLE"

    # -------------------------
    # AUTH / ACCESS
    # -------------------------
    AUTH_FAILED = "AUTH_FAILED"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    INVALID_API_KEY = "INVALID_API_KEY"
    LICENSE_NOT_ENTITLED = "LICENSE_NOT_ENTITLED"

    # -------------------------
    # API TRANSPORT
    # -------------------------
    API_TIMEOUT = "API_TIMEOUT"
    API_CONNECTION_FAILED = "API_CONNECTION_FAILED"
    API_DNS_FAILURE = "API_DNS_FAILURE"
    API_SSL_ERROR = "API_SSL_ERROR"

    # -------------------------
    # API SEMANTICS
    # -------------------------
    API_BAD_REQUEST = "API_BAD_REQUEST"
    API_RATE_LIMITED = "API_RATE_LIMITED"
    API_NOT_FOUND = "API_NOT_FOUND"
    API_SERVER_ERROR = "API_SERVER_ERROR"
    API_UNEXPECTED_RESPONSE = "API_UNEXPECTED_RESPONSE"
    API_SCHEMA_CHANGED = "API_SCHEMA_CHANGED"

    # -------------------------
    # DATA HANDLING
    # -------------------------
    PAYLOAD_PARSE_ERROR = "PAYLOAD_PARSE_ERROR"
    PAYLOAD_VALIDATION_ERROR = "PAYLOAD_VALIDATION_ERROR"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    DATA_TRUNCATION = "DATA_TRUNCATION"
    DUPLICATE_RECORD = "DUPLICATE_RECORD"

    # -------------------------
    # DATABASE
    # -------------------------
    DB_CONNECTION_FAILED = "DB_CONNECTION_FAILED"
    DB_AUTH_FAILED = "DB_AUTH_FAILED"
    DB_TRANSACTION_FAILED = "DB_TRANSACTION_FAILED"
    DB_CONSTRAINT_VIOLATION = "DB_CONSTRAINT_VIOLATION"
    DB_INSERT_FAILED = "DB_INSERT_FAILED"
    DB_TIMEOUT = "DB_TIMEOUT"
    DB_SCHEMA_MISSING = "DB_SCHEMA_MISSING"
    DB_SCHEMA_MISMATCH = "DB_SCHEMA_MISMATCH"

    # -------------------------
    # INGESTION LOGIC
    # -------------------------
    NO_RECORDS_RETURNED = "NO_RECORDS_RETURNED"
    RECORD_WRITE_FAILED = "RECORD_WRITE_FAILED"
    FLUSH_FAILED = "FLUSH_FAILED"
    BACKFILL_FAILED = "BACKFILL_FAILED"

    # -------------------------
    # EXECUTION
    # -------------------------
    INTERRUPTED = "INTERRUPTED"
    UNHANDLED_EXCEPTION = "UNHANDLED_EXCEPTION"
