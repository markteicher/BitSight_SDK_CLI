#!/usr/bin/env python3
"""
core/ingestion.py

Canonical ingestion execution framework for BitSight SDK + CLI.

Responsibilities:
- Execute one ingestion unit deterministically
- Track counts and outcomes
- Emit StatusCode for runtime events
- Return an ExitCode decision (CLI/process emits exactly once)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, Any, Tuple
from collections.abc import Mapping

from tqdm import tqdm

from core.status_codes import StatusCode
from core.exit_codes import ExitCode


# ============================================================
# Data structures
# ============================================================

@dataclass(frozen=True)
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

    It assumes all dependencies are already validated upstream.
    """

    def __init__(
        self,
        *,
        fetcher: Callable[[], Iterable[Any]],
        writer: Callable[[Any], None],
        expected_min_records: int = 0,
        show_progress: bool = True,
        fail_fast: bool = False,
        max_failures: Optional[int] = None,
        record_label: Optional[Callable[[Any], str]] = None,
        log_every_n_failures: int = 1,
    ):
        self.fetcher = fetcher
        self.writer = writer
        self.expected_min_records = int(expected_min_records or 0)
        self.show_progress = bool(show_progress)
        self.fail_fast = bool(fail_fast)
        self.max_failures = max_failures if max_failures is None else int(max_failures)
        self.record_label = record_label
        self.log_every_n_failures = max(1, int(log_every_n_failures or 1))

        if not callable(self.fetcher):
            raise TypeError("fetcher must be callable")
        if not callable(self.writer):
            raise TypeError("writer must be callable")
        if self.max_failures is not None and self.max_failures < 1:
            raise ValueError("max_failures must be >= 1 when provided")

    # --------------------------------------------------------
    # Public
    # --------------------------------------------------------
    def run(self) -> IngestionResult:
        start_time = time.monotonic()

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
                    message="No records returned by fetcher",
                )

            written, failed, aborted = self._write(records)

            if aborted:
                return self._finish(
                    StatusCode.OK_SKIPPED,
                    ExitCode.INGEST_ABORTED,
                    fetched=fetched_count,
                    written=written,
                    failed=failed,
                    start_time=start_time,
                    message="Ingestion aborted due to fail_fast/max_failures",
                )

            if written == 0 and failed > 0:
                # Correction: DO NOT use a non-existent ExitCode (e.g. INGEST_WRITE_FAILED).
                # Your enum defines DB_WRITE_FAILED for write failures.
                return self._finish(
                    StatusCode.INGESTION_WRITE_FAILED,
                    ExitCode.DB_WRITE_FAILED,
                    fetched=fetched_count,
                    written=0,
                    failed=failed,
                    start_time=start_time,
                    message="All writes failed",
                )

            if failed > 0:
                return self._finish(
                    StatusCode.INGESTION_PARTIAL_WRITE,
                    ExitCode.INGEST_PARTIAL_FAILURE,
                    fetched=fetched_count,
                    written=written,
                    failed=failed,
                    start_time=start_time,
                    message="Partial write failure",
                )

            return self._finish(
                StatusCode.OK,
                ExitCode.SUCCESS,
                fetched=fetched_count,
                written=written,
                failed=0,
                start_time=start_time,
                message="Ingestion succeeded",
            )

        except KeyboardInterrupt:
            return self._finish(
                StatusCode.OK_SKIPPED,
                ExitCode.SUCCESS_OPERATOR_EXIT,
                start_time=start_time,
                message="Operator interrupted execution",
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

        # Reject common “wrong” return types deterministically.
        if records is None:
            raise TypeError("Fetcher returned None; must return an iterable of records")
        if isinstance(records, (str, bytes)):
            raise TypeError("Fetcher returned str/bytes; must return iterable of records")
        if isinstance(records, Mapping):
            raise TypeError("Fetcher returned a mapping/dict; must return iterable of records")
        if not isinstance(records, Iterable):
            raise TypeError("Fetcher must return an iterable of records")

        records_list = list(records)

        if self.expected_min_records > 0 and len(records_list) < self.expected_min_records:
            logging.warning(
                "Fetched %d records, expected at least %d",
                len(records_list),
                self.expected_min_records,
            )

        return records_list

    def _write(self, records: list) -> Tuple[int, int, bool]:
        written = 0
        failed = 0
        aborted = False

        iterator = records
        if self.show_progress:
            iterator = tqdm(records, desc="Ingesting records", unit="record")

        for record in iterator:
            try:
                self.writer(record)
                written += 1
            except Exception as exc:
                failed += 1

                # Operator-visible and deterministic failure logging
                if failed % self.log_every_n_failures == 0:
                    label = self._record_label(record)
                    logging.error(
                        "Record write failed (%d failures) %s err=%s",
                        failed,
                        f"record={label}" if label else "",
                        exc,
                    )
                    logging.debug("Record write exception detail", exc_info=True)

                # Combat controls: stop if configured.
                if self.fail_fast:
                    aborted = True
                    break
                if self.max_failures is not None and failed >= self.max_failures:
                    aborted = True
                    break

        return written, failed, aborted

    def _record_label(self, record: Any) -> Optional[str]:
        if not self.record_label:
            return None
        try:
            v = self.record_label(record)
            v = (v or "").strip()
            return v or None
        except Exception:
            return None

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
        duration = time.monotonic() - start_time

        logging.info(
            "Ingestion completed | status=%s exit=%s fetched=%d written=%d failed=%d duration=%.2fs",
            getattr(status, "name", str(status)),
            getattr(exit_code, "name", str(exit_code)),
            fetched,
            written,
            failed,
            duration,
        )

        if message:
            logging.info("Ingestion message | %s", message)

        return IngestionResult(
            status_code=status,
            exit_code=exit_code,
            records_fetched=fetched,
            records_written=written,
            records_failed=failed,
            duration_seconds=duration,
            message=message,
        )
