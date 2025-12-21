#!/usr/bin/env python3

import logging
import time
from datetime import datetime
from typing import Iterable, Dict, Any, Optional

from core.status_codes import StatusCode
from core.exit_codes import ExitCode


class IngestionExecution:
    """
    Execution wrapper for a single ingestion operation.

    Responsibilities:
    - Track execution start and end time
    - Track record counts (retrieved, processed, written)
    - Track execution status and error conditions
    - Provide a single execution result for CLI and automation layers

    This class does not:
    - Fetch data from APIs
    - Transform payloads
    - Write directly to databases
    """

    def __init__(self, name: str):
        """
        Initialize execution state for one ingestion run.

        :param name: Logical name of the ingestion operation
        """
        self.name = name
        self.started_at = datetime.utcnow()
        self.finished_at: Optional[datetime] = None

        self.records_retrieved = 0
        self.records_processed = 0
        self.records_written = 0
        self.errors = 0

        self.status_code: Optional[StatusCode] = None
        self.exit_code: Optional[ExitCode] = None
        self.error_detail: Optional[str] = None

    def mark_retrieved(self, count: int) -> None:
        """
        Record how many records were retrieved from the source API.
        """
        self.records_retrieved += count

    def mark_processed(self, count: int) -> None:
        """
        Record how many records were processed after retrieval.
        """
        self.records_processed += count

    def mark_written(self, count: int) -> None:
        """
        Record how many records were successfully written to the database.
        """
        self.records_written += count

    def mark_error(self, status_code: StatusCode, detail: str) -> None:
        """
        Record an error condition for this execution.

        This method:
        - Increments the error counter
        - Stores the last status code
        - Stores a human-readable error detail

        It does not terminate execution.
        """
        self.errors += 1
        self.status_code = status_code
        self.error_detail = detail

    def finalize_success(self) -> None:
        """
        Mark execution as successful.

        Sets:
        - finished_at timestamp
        - status_code to OK
        - exit_code to SUCCESS
        """
        self.finished_at = datetime.utcnow()
        self.status_code = StatusCode.OK
        self.exit_code = ExitCode.SUCCESS

    def finalize_failure(self, exit_code: ExitCode) -> None:
        """
        Mark execution as failed.

        Sets:
        - finished_at timestamp
        - exit_code to the provided value

        status_code must already be set prior to calling this method.
        """
        self.finished_at = datetime.utcnow()
        self.exit_code = exit_code

    def duration_seconds(self) -> float:
        """
        Return execution duration in seconds.
        """
        end_time = self.finished_at or datetime.utcnow()
        return (end_time - self.started_at).total_seconds()

    def as_dict(self) -> Dict[str, Any]:
        """
        Return a structured execution summary.

        This structure is suitable for:
        - CLI output
        - Logging
        - Status reporting
        """
        return {
            "name": self.name,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_seconds": self.duration_seconds(),
            "records_retrieved": self.records_retrieved,
            "records_processed": self.records_processed,
            "records_written": self.records_written,
            "errors": self.errors,
            "status_code": self.status_code.name if self.status_code else None,
            "exit_code": self.exit_code.value if self.exit_code else None,
            "error_detail": self.error_detail,
        }
