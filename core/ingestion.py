#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import List, Dict, Any

from core.status_codes import StatusCode


class IngestionExecution:
    """
    Tracks ingestion execution events and metrics.
    This class records WHAT happened, not whether it was good or bad.
    """

    def __init__(self, source: str):
        self.source = source
        self.started_at = datetime.utcnow()
        self.completed_at = None

        self.status_events: List[StatusCode] = []
        self.metrics: Dict[str, int] = {
            "records_discovered": 0,
            "records_written": 0,
            "records_skipped": 0,
            "records_failed": 0,
        }

        self.record_event(StatusCode.EXECUTION_STARTED)
        self.record_event(StatusCode.INGESTION_STARTED)

    # -------------------------------------------------
    # EVENT RECORDING
    # -------------------------------------------------
    def record_event(self, status: StatusCode) -> None:
        logging.debug("STATUS_EVENT: %s", status.value)
        self.status_events.append(status)

    # -------------------------------------------------
    # RECORD-LEVEL METRICS
    # -------------------------------------------------
    def record_discovered(self) -> None:
        self.metrics["records_discovered"] += 1
        self.record_event(StatusCode.RECORD_DISCOVERED)

    def record_written(self) -> None:
        self.metrics["records_written"] += 1
        self.record_event(StatusCode.DB_WRITE_SUCCEEDED)

    def record_skipped(self) -> None:
        self.metrics["records_skipped"] += 1
        self.record_event(StatusCode.RECORD_SKIPPED)

    def record_failed(self) -> None:
        self.metrics["records_failed"] += 1
        self.record_event(StatusCode.DB_WRITE_FAILED)

    # -------------------------------------------------
    # LIFECYCLE COMPLETION
    # -------------------------------------------------
    def complete(self) -> None:
        self.completed_at = datetime.utcnow()
        self.record_event(StatusCode.INGESTION_COMPLETED)
        self.record_event(StatusCode.EXECUTION_COMPLETED)

    def interrupt(self) -> None:
        self.completed_at = datetime.utcnow()
        self.record_event(StatusCode.EXECUTION_INTERRUPTED)

    def exception(self) -> None:
        self.completed_at = datetime.utcnow()
        self.record_event(StatusCode.EXECUTION_EXCEPTION_RAISED)

    # -------------------------------------------------
    # SNAPSHOT
    # -------------------------------------------------
    def summary(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "metrics": self.metrics,
            "status_events": [s.value for s in self.status_events],
        }
