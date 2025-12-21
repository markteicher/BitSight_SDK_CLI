#!/usr/bin/env python3

from enum import Enum


class StatusCode(str, Enum):
    """
    Status codes are factual execution events recorded during runtime.
    They DO NOT determine program exit codes.
    They DO NOT imply success or failure.
    They only describe what occurred.
    """

    # -------------------------------------------------
    # EXECUTION LIFECYCLE EVENTS
    # -------------------------------------------------
    EXECUTION_STARTED = "EXECUTION_STARTED"
    EXECUTION_COMPLETED = "EXECUTION_COMPLETED"
    EXECUTION_INTERRUPTED = "EXECUTION_INTERRUPTED"
    EXECUTION_EXCEPTION_RAISED = "EXECUTION_EXCEPTION_RAISED"

    # -------------------------------------------------
    # CONFIGURATION EVENTS
    # -------------------------------------------------
    CONFIG_LOADED = "CONFIG_LOADED"
    CONFIG_MISSING = "CONFIG_MISSING"
    CONFIG_INVALID = "CONFIG_INVALID"
    CONFIG_RESET = "CONFIG_RESET"

    # -------------------------------------------------
    # AUTHENTICATION / AUTHORIZATION EVENTS
    # -------------------------------------------------
    API_KEY_PRESENT = "API_KEY_PRESENT"
    API_KEY_MISSING = "API_KEY_MISSING"
    API_KEY_REJECTED = "API_KEY_REJECTED"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    LICENSE_NOT_ENTITLED = "LICENSE_NOT_ENTITLED"

    # -------------------------------------------------
    # API TRANSPORT EVENTS
    # -------------------------------------------------
    API_REQUEST_SENT = "API_REQUEST_SENT"
    API_RESPONSE_RECEIVED = "API_RESPONSE_RECEIVED"
    API_TIMEOUT_OCCURRED = "API_TIMEOUT_OCCURRED"
    API_CONNECTION_FAILED = "API_CONNECTION_FAILED"
    API_SSL_ERROR = "API_SSL_ERROR"
    API_DNS_FAILURE = "API_DNS_FAILURE"

    # -------------------------------------------------
    # API RESPONSE SEMANTICS
    # -------------------------------------------------
    API_RESPONSE_2XX = "API_RESPONSE_2XX"
    API_RESPONSE_4XX = "API_RESPONSE_4XX"
    API_RESPONSE_5XX = "API_RESPONSE_5XX"
    API_RESPONSE_SCHEMA_CHANGED = "API_RESPONSE_SCHEMA_CHANGED"
    API_RESPONSE_UNEXPECTED = "API_RESPONSE_UNEXPECTED"

    # -------------------------------------------------
    # DATA EXTRACTION EVENTS
    # -------------------------------------------------
    PAYLOAD_RECEIVED = "PAYLOAD_RECEIVED"
    PAYLOAD_PARSE_STARTED = "PAYLOAD_PARSE_STARTED"
    PAYLOAD_PARSE_COMPLETED = "PAYLOAD_PARSE_COMPLETED"
    PAYLOAD_PARSE_FAILED = "PAYLOAD_PARSE_FAILED"

    # -------------------------------------------------
    # RECORD-LEVEL EVENTS
    # -------------------------------------------------
    RECORD_DISCOVERED = "RECORD_DISCOVERED"
    RECORD_NORMALIZED = "RECORD_NORMALIZED"
    RECORD_SKIPPED = "RECORD_SKIPPED"
    RECORD_DUPLICATE_DETECTED = "RECORD_DUPLICATE_DETECTED"
    RECORD_VALIDATION_FAILED = "RECORD_VALIDATION_FAILED"

    # -------------------------------------------------
    # DATABASE EVENTS
    # -------------------------------------------------
    DB_CONNECTION_OPENED = "DB_CONNECTION_OPENED"
    DB_CONNECTION_FAILED = "DB_CONNECTION_FAILED"
    DB_TRANSACTION_STARTED = "DB_TRANSACTION_STARTED"
    DB_TRANSACTION_COMMITTED = "DB_TRANSACTION_COMMITTED"
    DB_TRANSACTION_ROLLED_BACK = "DB_TRANSACTION_ROLLED_BACK"
    DB_WRITE_ATTEMPTED = "DB_WRITE_ATTEMPTED"
    DB_WRITE_SUCCEEDED = "DB_WRITE_SUCCEEDED"
    DB_WRITE_FAILED = "DB_WRITE_FAILED"
    DB_SCHEMA_MISSING = "DB_SCHEMA_MISSING"
    DB_CONSTRAINT_VIOLATION = "DB_CONSTRAINT_VIOLATION"

    # -------------------------------------------------
    # INGESTION CONTROL EVENTS
    # -------------------------------------------------
    INGESTION_STARTED = "INGESTION_STARTED"
    INGESTION_COMPLETED = "INGESTION_COMPLETED"
    INGESTION_FLUSH_REQUESTED = "INGESTION_FLUSH_REQUESTED"
    INGESTION_FLUSH_COMPLETED = "INGESTION_FLUSH_COMPLETED"
    INGESTION_BACKFILL_STARTED = "INGESTION_BACKFILL_STARTED"
    INGESTION_BACKFILL_COMPLETED = "INGESTION_BACKFILL_COMPLETED"

    # -------------------------------------------------
    # OBSERVABILITY EVENTS
    # -------------------------------------------------
    NO_RECORDS_RETURNED = "NO_RECORDS_RETURNED"
    PARTIAL_DATA_RETURNED = "PARTIAL_DATA_RETURNED"
