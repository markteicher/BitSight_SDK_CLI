#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import List

from core.status_codes import StatusCode


class IngestionExecution:
    """
    Records the factual execution of a single ingestion run.

    DESIGN GUARANTEES
    -----------------
    - This class NEVER:
        * exits the program
        * logs success/failure conclusions
        * formats output
        * swallows exceptions
    - This class ONLY:
        * records timestamps
        * records counters
        * records ordered execution events
    """

    # -------------------------------------------------
    # construction
    # -------------------------------------------------
    def __init__(self, source: str):
        if not source:
            raise ValueError("source must be provided")

        self.source = source

        # lifecycle timestamps
        self.started_at: datetime = datetime.utcnow()
        self.completed_at: datetime | None = None

        # execution state flags (explicit, not inferred)
        self.started: bool = True
        self.completed: bool = False
        self.interrupted: bool = False
        self.exception_raised: bool = False

        # ordered execution events
        self.status_events: List[StatusCode] = []

        # counters (never negative, monotonic)
        self.records_discovered: int = 0
        self.records_written: int = 0
        self.records_skipped: int = 0
        self.records_failed: int = 0

        # initial events
        self._record(StatusCode.EXECUTION_STARTED)
        self._record(StatusCode.INGESTION_STARTED)

    # -------------------------------------------------
    # internal invariants
    # -------------------------------------------------
    def _assert_active(self) -> None:
        if self.completed:
            raise RuntimeError("IngestionExecution already completed")

    def _record(self, event: StatusCode) -> None:
        logging.debug("INGEST_EVENT [%s]: %s", self.source, event.value)
        self.status_events.append(event)

    # -------------------------------------------------
    # record-level accounting
    # -------------------------------------------------
    def record_discovered(self) -> None:
        self._assert_active()
        self.records_discovered += 1
        self._record(StatusCode.RECORD_DISCOVERED)

    def record_written(self) -> None:
        self._assert_active()
        self.records_written += 1
        self._record(StatusCode.DB_WRITE_SUCCEEDED)

    def record_skipped(self) -> None:
        self._assert_active()
        self.records_skipped += 1
        self._record(StatusCode.RECORD_SKIPPED)

    def record_failed(self) -> None:
        self._assert_active()
        self.records_failed += 1
        self._record(StatusCode.DB_WRITE_FAILED)

    # -------------------------------------------------
    # lifecycle transitions
    # -------------------------------------------------
    def mark_completed(self) -> None:
        """
        Normal termination.
        """
        self._assert_active()

        self.completed_at = datetime.utcnow()
        self.completed = True

        self._record(StatusCode.INGESTION_COMPLETED)
        self._record(StatusCode.EXECUTION_COMPLETED)

    def mark_interrupted(self) -> None:
        """
        External interruption (SIGINT, operator stop, etc).
        """
        self._assert_active()

        self.completed_at = datetime.utcnow()
        self.completed = True
        self.interrupted = True

        self._record(StatusCode.EXECUTION_INTERRUPTED)

    def mark_exception(self) -> None:
        """
        Exception occurred upstream. This method DOES NOT swallow it.
        Caller MUST still raise.
        """
        if self.completed:
            # do not double-record
            return

        self.completed_at = datetime.utcnow()
        self.completed = True
        self.exception_raised = True

        self._record(StatusCode.EXECUTION_EXCEPTION_RAISED)
