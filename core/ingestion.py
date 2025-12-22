#!/usr/bin/env python3
"""
core/ingestion.py

Canonical ingestion execution framework for BitSight SDK + CLI.

Design intent (combat / resilient):
- Deterministic execution lifecycle
- Explicit state transitions
- Clear separation of fetch / write concerns
- Accurate StatusCode + ExitCode emission
- No silent failures
- No implicit behavior
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, Tuple

from tqdm import tqdm

from core.status_codes import StatusCode
from core.exit_codes import ExitCode


# ============================================================
# Result object
# ============================================================

@dataclass(frozen=True)
class IngestionResult:
    status_code: StatusCode
    exit_code: ExitCode

    records_fetched: int
    records_written: int
    records_failed: int

    duration_seconds: float
    message: Optional[str] = None


# ============================================================
# Executor
# ============================================================

class IngestionExecutor:
    """
    Executes a single ingestion workload.

    Assumptions:
    - Transport already validated
    - Database already connected
    - Caller owns process exit
    """

    def __init__(
        self,
        *,
        fetcher: Callable[[], Iterable],
        writer: Callable[[object], None],
        expected_min_records: int = 0,
        show_progress: bool = True,
    ):
        self.fetcher = fetcher
        self.writer = writer
        self.expected_min_records = expected_min_records
        self.show_progress = show_progress

    # --------------------------------------------------------
    # Public API
    # --------------------------------------------------------
    def run(self) -> IngestionResult:
        start_time = time.time()

        try:
            records = self._fetch_records()
            fetched = len(records)

            if fetched == 0:
                return self._finish(
                    status=StatusCode.OK_NO_DATA,
                    exit_code=ExitCode.SUCCESS_EMPTY_RESULT,
                    fetched=0,
                    written=0,
                    failed=0,
                    start_time=start_time,
                )

            written, failed = self._write_records(records)

            # All writes failed
            if written == 0 and failed > 0:
                return self._finish(
                    status=StatusCode.INGESTION_WRITE_FAILED,
                    exit_code=ExitCode.DB_WRITE_FAILED,
                    fetched=fetched,
                    written=0,
                    failed=failed,
                    start_time=start_time,
                )

            # Partial failure
            if failed > 0:
                return self._finish(
                    status=StatusCode.INGESTION_PARTIAL_WRITE,
                    exit_code=ExitCode.INGEST_PARTIAL_FAILURE,
                    fetched=fetched,
                    written=written,
                    failed=failed,
                    start_time=start_time,
                )

            # Full success
            return self._finish(
                status=StatusCode.OK,
                exit_code=ExitCode.SUCCESS,
                fetched=fetched,
                written=written,
                failed=0,
                start_time=start_time,
            )

        except KeyboardInterrupt:
            logging.warning("Ingestion interrupted by operator")
            return self._finish(
                status=StatusCode.EXECUTION_INTERRUPTED,
                exit_code=ExitCode.RUNTIME_INTERRUPT,
                fetched=0,
                written=0,
                failed=0,
                start_time=start_time,
            )

        except Exception as exc:
            logging.exception("Unhandled ingestion exception")
            return self._finish(
                status=StatusCode.EXECUTION_UNHANDLED_EXCEPTION,
                exit_code=ExitCode.RUNTIME_EXCEPTION,
                fetched=0,
                written=0,
                failed=0,
                start_time=start_time,
                message=str(exc),
            )

    # --------------------------------------------------------
    # Internal helpers
    # --------------------------------------------------------
    def _fetch_records(self) -> list:
        try:
            records = self.fetcher()
        except Exception as e:
            logging.exception("Fetcher failed")
            raise RuntimeError(StatusCode.INGESTION_FETCH_FAILED) from e

        if isinstance(records, (str, bytes, dict)) or not isinstance(records, Iterable):
            raise TypeError("Fetcher must return an iterable of records")

        records = list(records)

        if self.expected_min_records and len(records) < self.expected_min_records:
            logging.warning(
                "Fetched %d records, expected at least %d",
                len(records),
                self.expected_min_records,
            )

        logging.info("Fetched %d records", len(records))
        return records

    def _write_records(self, records: list) -> Tuple[int, int]:
        written = 0
        failed = 0

        iterator = records
        if self.show_progress:
            iterator = tqdm(records, desc="Ingesting", unit="record")

        for record in iterator:
            try:
                self.writer(record)
                written += 1
            except Exception:
                failed += 1
                logging.exception("Record write failed")

        logging.info(
            "Write complete | written=%d failed=%d",
            written,
            failed,
        )
        return written, failed

    def _finish(
        self,
        *,
        status: StatusCode,
        exit_code: ExitCode,
        fetched: int,
        written: int,
        failed: int,
        start_time: float,
        message: Optional[str] = None,
    ) -> IngestionResult:
        duration = time.time() - start_time

        logging.info(
            "Ingestion result | status=%s exit=%s fetched=%d written=%d failed=%d duration=%.2fs",
            status.name,
            exit_code.name,
            fetched,
            written,
            failed,
            duration,
        )

        return IngestionResult(
            status_code=status,
            exit_code=exit_code,
            records_fetched=fetched,
            records_written=written,
            records_failed=failed,
            duration_seconds=duration,
            message=message,
        )
