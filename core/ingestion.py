#!/usr/bin/env python3
"""
core/ingestion.py

Canonical ingestion execution framework for BitSight SDK + CLI.

Responsibilities:
- Execute exactly ONE ingestion workload
- Track deterministic counts
- Surface partial vs total failure
- Emit StatusCode for runtime semantics
- Emit ExitCode exactly once at termination
- Enforce dry-run at the writer boundary
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, Any

from tqdm import tqdm

from core.status_codes import StatusCode
from core.exit_codes import ExitCode


# ============================================================
# Result structure
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

    Contract:
    - fetcher() MUST return an iterable
    - writer(record) MUST raise on failure
    - executor owns counting, timing, semantics
    """

    def __init__(
        self,
        *,
        fetcher: Callable[[], Iterable[Any]],
        writer: Callable[[Any], None],
        expected_min_records: int = 0,
        show_progress: bool = True,
        dry_run: bool = False,
    ):
        if not callable(fetcher):
            raise TypeError("fetcher must be callable")

        if not callable(writer):
            raise TypeError("writer must be callable")

        self.fetcher = fetcher
        self.writer = writer
        self.expected_min_records = expected_min_records
        self.show_progress = show_progress
        self.dry_run = dry_run

    # --------------------------------------------------------
    # Public
    # --------------------------------------------------------
    def run(self) -> IngestionResult:
        start_time = time.monotonic()

        fetched = 0
        written = 0
        failed = 0

        try:
            # -----------------------------
            # FETCH
            # -----------------------------
            try:
                records = self._fetch()
            except Exception as e:
                logging.exception("Ingestion fetch failed")
                return self._finish(
                    StatusCode.INGESTION_FETCH_FAILED,
                    ExitCode.INGEST_START_FAILED,
                    fetched=0,
                    written=0,
                    failed=0,
                    start_time=start_time,
                    message=str(e),
                )

            fetched = len(records)

            if fetched == 0:
                return self._finish(
                    StatusCode.OK_NO_DATA,
                    ExitCode.SUCCESS_EMPTY_RESULT,
                    fetched=0,
                    written=0,
                    failed=0,
                    start_time=start_time,
                )

            if self.expected_min_records and fetched < self.expected_min_records:
                logging.warning(
                    "Fetched %d records; expected minimum %d",
                    fetched,
                    self.expected_min_records,
                )

            # -----------------------------
            # WRITE (or DRY-RUN)
            # -----------------------------
            written, failed = self._write(records)

            if self.dry_run:
                return self._finish(
                    StatusCode.OK_DRY_RUN,
                    ExitCode.SUCCESS_DRY_RUN_OK,
                    fetched=fetched,
                    written=written,
                    failed=0,
                    start_time=start_time,
                )

            if written == 0 and failed > 0:
                return self._finish(
                    StatusCode.INGESTION_WRITE_FAILED,
                    ExitCode.DB_WRITE_FAILED,
                    fetched=fetched,
                    written=0,
                    failed=failed,
                    start_time=start_time,
                )

            if failed > 0:
                return self._finish(
                    StatusCode.INGESTION_PARTIAL_WRITE,
                    ExitCode.INGEST_PARTIAL_FAILURE,
                    fetched=fetched,
                    written=written,
                    failed=failed,
                    start_time=start_time,
                )

            return self._finish(
                StatusCode.OK,
                ExitCode.SUCCESS,
                fetched=fetched,
                written=written,
                failed=0,
                start_time=start_time,
            )

        except KeyboardInterrupt:
            logging.warning("Ingestion interrupted by operator")
            return self._finish(
                StatusCode.EXECUTION_INTERRUPTED,
                ExitCode.SUCCESS_OPERATOR_EXIT,
                fetched=fetched,
                written=written,
                failed=failed,
                start_time=start_time,
            )

        except Exception as e:
            logging.exception("Unhandled ingestion exception")
            return self._finish(
                StatusCode.EXECUTION_UNHANDLED_EXCEPTION,
                ExitCode.RUNTIME_EXCEPTION,
                fetched=fetched,
                written=written,
                failed=failed,
                start_time=start_time,
                message=str(e),
            )

    # --------------------------------------------------------
    # Internal
    # --------------------------------------------------------
    def _fetch(self) -> list[Any]:
        records = self.fetcher()

        if records is None:
            raise TypeError("Fetcher returned None")

        if isinstance(records, (str, bytes, dict)) or not isinstance(records, Iterable):
            raise TypeError("Fetcher must return an iterable of records")

        return list(records)

    def _write(self, records: list[Any]) -> tuple[int, int]:
        written = 0
        failed = 0

        iterator = records
        if self.show_progress:
            iterator = tqdm(records, desc="Ingesting records", unit="record")

        for record in iterator:
            try:
                if self.dry_run:
                    logging.debug("DRY-RUN: write suppressed")
                    written += 1
                else:
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
        fetched: int,
        written: int,
        failed: int,
        start_time: float,
        message: Optional[str] = None,
    ) -> IngestionResult:
        duration = time.monotonic() - start_time

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
