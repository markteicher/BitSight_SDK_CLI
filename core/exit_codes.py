#!/usr/bin/env python3
"""
BitSight SDK + CLI
Process Exit Codes

Exit codes represent the final outcome of a CLI execution.
Exactly one exit code is returned per run.
Detailed failure reasons are tracked separately in execution status.
"""

from enum import IntEnum


class ExitCode(IntEnum):
    """
    Process-level exit codes.

    These codes indicate overall execution outcome only.
    They do not encode detailed failure diagnostics.
    """

    # Successful execution
    SUCCESS = 0

    # Completed with partial failures (some records or endpoints failed)
    PARTIAL_SUCCESS = 10

    # Configuration or argument error
    CONFIG_ERROR = 20

    # Authentication or authorization failure
    AUTH_ERROR = 30

    # BitSight API failure (transport, rate limit, server error, etc.)
    API_ERROR = 40

    # Database failure (connection, transaction, schema, constraint)
    DB_ERROR = 50

    # Execution interrupted by operator (e.g., SIGINT)
    INTERRUPTED = 130

    # Unhandled or unexpected exception
    UNKNOWN_ERROR = 255
