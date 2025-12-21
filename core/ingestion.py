#!/usr/bin/env python3
"""
core/ingestion.py

Canonical ingestion execution framework for BitSight SDK + CLI.

Responsibilities:
- Execute one ingestion unit deterministically
- Track counts and outcomes
- Emit StatusCode for runtime events
- Emit ExitCode exactly once at termination
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Iterable, Optional

from tqdm import tqdm

from core.status_codes import StatusCode
from core.exit_codes import ExitCode


# ============================================================
# Data structures
# ============================================================

@dataclass
class IngestionResult:
    status_code: StatusCode
    exit_code: ExitCode
    records_fetched: int = 0
    records_written: int = 0
    records_failed: int = 0
    duration_seconds: float = 0.0
    message: Optional[str] = None


# ============================================================
# Ingestion executor
# ============================================================

class IngestionExecutor:
    """
    Executes a single ingestion workload.

    This class does not:
    - parse CLI arguments
    - perform API authentication
    - manage database connections

    It assumes all dependencies are already validated.
    """

    def __init__(
        self,
        *,
        fetcher: callable,
        writer: callable,
        expected_min_records: int = 0,
        show_progress: bool = True,
    ):
        self.fetcher = fetcher
        self.writer = writer
        self.expected_min_records = expected_min_records
        self.show_progress = show_progress

    # --------------------------------------------------------
    # Public
    # --------------------------------------------------------
    def run(self) -> IngestionResult:
        start_time = time.time()

        try:
            records = self._fetch()
            fetched_count = len(records)

            if fetched_count == 0:
                return self._finish(
                    StatusCode.OK_NO_DATA,
                    ExitCode.SUCCESS_EMPTY_RESULT,
                    fetched=0,
                    written=0,
                    failed=0,
                    start_time=start_time,
                )

            written, failed = self._write(records)

            if written == 0 and failed > 0:
                return self._finish(
                    StatusCode.INGESTION_WRITE_FAILED,
                    ExitCode.INGEST_WRITE_FAILED,
                    fetched=fetched_count,
                    written=0,
                    failed=failed,
                    start_time=start_time,
                )

            if failed > 0:
                return self._finish(
                    StatusCode.INGESTION_PARTIAL_WRITE,
                    ExitCode.INGEST_PARTIAL_FAILURE,
                    fetched=fetched_count,
                    written=written,
                    failed=failed,
                    start_time=start_time,
                )

            return self._finish(
                StatusCode.OK,
                ExitCode.SUCCESS,
                fetched=fetched_count,
                written=written,
                failed=0,
                start_time=start_time,
            )

        except KeyboardInterrupt:
            return self._finish(
                StatusCode.OK_SKIPPED,
                ExitCode.SUCCESS_OPERATOR_EXIT,
                start_time=start_time,
            )

        except Exception as exc:
            logging.exception("Unhandled ingestion exception")
            return self._finish(
                StatusCode.EXECUTION_UNHANDLED_EXCEPTION,
                ExitCode.RUNTIME_EXCEPTION,
                start_time=start_time,
                message=str(exc),
            )

    # --------------------------------------------------------
    # Internal
    # --------------------------------------------------------
    def _fetch(self) -> list:
        records = self.fetcher()

        if isinstance(records, (str, bytes, dict)) or not isinstance(records, Iterable):
            raise TypeError("Fetcher must return an iterable of records")

        records = list(records)

        if self.expected_min_records > 0 and len(records) < self.expected_min_records:
            logging.warning(
                "Fetched %d records, expected at least %d",
                len(records),
                self.expected_min_records,
            )

        return records

    def _write(self, records: list) -> tuple[int, int]:
        written = 0
        failed = 0

        iterator = records
        if self.show_progress:
            iterator = tqdm(records, desc="Ingesting records", unit="record")

        for record in iterator:
            try:
                self.writer(record)
                written += 1
            except Exception:
                failed += 1
                logging.exception("Record write failed")

        return written, failed

    def _finish(
        self,
        status: StatusCode,
        exit_code: ExitCode,
        *,
        fetched: int = 0,
        written: int = 0,
        failed: int = 0,
        start_time: float,
        message: Optional[str] = None,
    ) -> IngestionResult:
        duration = time.time() - start_time

        logging.info(
            "Ingestion completed | status=%s exit=%s fetched=%d written=%d failed=%d duration=%.2fs",
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
