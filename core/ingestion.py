#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import List, Optional

from tqdm import tqdm

from core.status_codes import StatusCode


class IngestionExecution:
    __slots__ = (
        "source",
        "started_at",
        "completed_at",
        "completed",
        "interrupted",
        "exception_raised",
        "status_events",
        "records_discovered",
        "records_written",
        "records_skipped",
        "records_failed",
        "_progress",
    )

    def __init__(self, source: str, total: Optional[int] = None, disable_progress: bool = False):
        if not isinstance(source, str) or not source.strip():
            raise ValueError("source must be a non-empty string")

        self.source = source.strip()

        self.started_at = datetime.utcnow()
        self.completed_at = None

        self.completed = False
        self.interrupted = False
        self.exception_raised = False

        self.status_events: List[StatusCode] = []

        self.records_discovered = 0
        self.records_written = 0
        self.records_skipped = 0
        self.records_failed = 0

        self._progress = tqdm(
            total=total,
            desc=f"ingest:{self.source}",
            unit="records",
            disable=disable_progress,
            leave=True,
        )

        self._event(StatusCode.EXECUTION_STARTED)
        self._event(StatusCode.INGESTION_STARTED)

    # -------------------------------------------------
    # internal enforcement
    # -------------------------------------------------
    def _require_active(self) -> None:
        if self.completed:
            raise RuntimeError(f"ingestion '{self.source}' already completed")

    def _event(self, code: StatusCode) -> None:
        if not isinstance(code, StatusCode):
            raise TypeError("status event must be StatusCode")
        logging.debug("INGEST [%s] %s", self.source, code.value)
        self.status_events.append(code)

    # -------------------------------------------------
    # record accounting
    # -------------------------------------------------
    def record_discovered(self) -> None:
        self._require_active()
        self.records_discovered += 1
        self._progress.update(1)
        self._event(StatusCode.RECORD_DISCOVERED)

    def record_written(self) -> None:
        self._require_active()
        self.records_written += 1
        self._event(StatusCode.DB_WRITE_SUCCEEDED)

    def record_skipped(self) -> None:
        self._require_active()
        self.records_skipped += 1
        self._event(StatusCode.RECORD_SKIPPED)

    def record_failed(self) -> None:
        self._require_active()
        self.records_failed += 1
        self._event(StatusCode.DB_WRITE_FAILED)

    # -------------------------------------------------
    # terminal transitions
    # -------------------------------------------------
    def mark_completed(self) -> None:
        self._require_active()

        self.completed = True
        self.completed_at = datetime.utcnow()

        self._progress.close()

        self._event(StatusCode.INGESTION_COMPLETED)
        self._event(StatusCode.EXECUTION_COMPLETED)

    def mark_interrupted(self) -> None:
        self._require_active()

        self.completed = True
        self.interrupted = True
        self.completed_at = datetime.utcnow()

        self._progress.close()

        self._event(StatusCode.EXECUTION_INTERRUPTED)

    def mark_exception(self) -> None:
        if self.completed:
            raise RuntimeError(f"exception already recorded for '{self.source}'")

        self.completed = True
        self.exception_raised = True
        self.completed_at = datetime.utcnow()

        self._progress.close()

        self._event(StatusCode.EXECUTION_EXCEPTION_RAISED)
