#!/usr/bin/env python3
"""
core/ingestion.py

Canonical ingestion execution framework for BitSight SDK + CLI.

Responsibilities:
- Execute exactly ONE ingestion workload
- Track deterministic counts
- Reconcile net-new / updated / unchanged / removed records
- Surface partial vs total failure
- Emit StatusCode for runtime semantics
- Emit ExitCode exactly once at termination
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, Any, Set

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

    records_new: int
    records_updated: int
    records_unchanged: int
    records_removed: int

    duration_seconds: float
    message: Optional[str] = None


# ============================================================
# Executor
# ============================================================

class IngestionExecutor:
    """
    Executes a single ingestion workload.

    Contract:
    - fetcher() -> iterable of records
    - writer(record) -> returns one of: "new", "updated", "unchanged"
    - key_fn(record) -> returns stable primary key
    - finalize(remaining_keys) -> handles removals (optional)

    Writer owns reconciliation logic.
    Executor owns counting, timing, and semantics.
    """

    def __init__(
        self,
        *,
        fetcher: Callable[[], Iterable[Any]],
        writer: Callable[[Any], str],
        key_fn: Callable[[Any], Any],
        finalize: Optional[Callable[[Set[Any]], int]] = None,
        expected_min_records: int = 0,
        show_progress: bool = True,
    ):
        if not callable(fetcher):
            raise TypeError("fetcher must be callable")

        if not callable(writer):
            raise TypeError("writer must be callable")

        if not callable(key_fn):
            raise TypeError("key_fn must be callable")

        if finalize is not None and not callable(finalize):
            raise TypeError("finalize must be callable if provided")

        self.fetcher = fetcher
        self.writer = writer
        self.key_fn = key_fn
        self.finalize = finalize

        self.expected_min_records = expected_min_records
        self.show_progress = show_progress

    # --------------------------------------------------------
    # Public
    # --------------------------------------------------------
    def run(self) -> IngestionResult:
        start_time = time.monotonic()

        fetched = 0
        written = 0
        failed = 0

        new = 0
        updated = 0
        unchanged = 0
        removed = 0

        seen_keys: Set[Any] = set()

        try:
            # -----------------------------
            # FETCH
            # -----------------------------
            try:
                records = list(self.fetcher())
            except Exception as e:
                logging.exception("Ingestion fetch failed")
                return self._finish(
                    StatusCode.INGESTION_FETCH_FAILED,
                    ExitCode.INGEST_START_FAILED,
                    start_time,
                    message=str(e),
                )

            fetched = len(records)

            if fetched == 0:
                return self._finish(
                    StatusCode.OK_NO_DATA,
                    ExitCode.SUCCESS_EMPTY_RESULT,
                    start_time,
                )

            if self.expected_min_records and fetched < self.expected_min_records:
                logging.warning(
                    "Fetched %d records; expected minimum %d",
                    fetched,
                    self.expected_min_records,
                )

            # -----------------------------
            # WRITE / RECONCILE
            # -----------------------------
            iterator = records
            if self.show_progress:
                iterator = tqdm(records, desc="Ingesting records", unit="record")

            for record in iterator:
                try:
                    key = self.key_fn(record)
                    seen_keys.add(key)

                    outcome = self.writer(record)

                    written += 1

                    if outcome == "new":
                        new += 1
                    elif outcome == "updated":
                        updated += 1
                    elif outcome == "unchanged":
                        unchanged += 1
                    else:
                        logging.error("Invalid writer outcome: %s", outcome)
                        failed += 1

                except Exception:
                    failed += 1
                    logging.exception("Record write failed")

            # -----------------------------
            # FINALIZE (REMOVALS)
            # -----------------------------
            if self.finalize:
                try:
                    removed = int(self.finalize(seen_keys))
                except Exception:
                    logging.exception("Finalize (removal reconciliation) failed")
                    return self._finish(
                        StatusCode.INGESTION_STATE_UPDATE_FAILED,
                        ExitCode.INGEST_PARTIAL_FAILURE,
                        start_time,
                        fetched=fetched,
                        written=written,
                        failed=failed,
                        new=new,
                        updated=updated,
                        unchanged=unchanged,
                        removed=removed,
                    )

            # -----------------------------
            # SEMANTICS
            # -----------------------------
            if written == 0 and failed > 0:
                return self._finish(
                    StatusCode.INGESTION_WRITE_FAILED,
                    ExitCode.DB_WRITE_FAILED,
                    start_time,
                    fetched,
                    written,
                    failed,
                    new,
                    updated,
                    unchanged,
                    removed,
                )

            if failed > 0 or removed > 0:
                return self._finish(
                    StatusCode.INGESTION_PARTIAL_WRITE,
                    ExitCode.INGEST_PARTIAL_FAILURE,
                    start_time,
                    fetched,
                    written,
                    failed,
                    new,
                    updated,
                    unchanged,
                    removed,
                )

            return self._finish(
                StatusCode.OK,
                ExitCode.SUCCESS,
                start_time,
                fetched,
                written,
                failed,
                new,
                updated,
                unchanged,
                removed,
            )

        except KeyboardInterrupt:
            logging.warning("Ingestion interrupted by operator")
            return self._finish(
                StatusCode.EXECUTION_INTERRUPTED,
                ExitCode.SUCCESS_OPERATOR_EXIT,
                start_time,
                fetched,
                written,
                failed,
                new,
                updated,
                unchanged,
                removed,
            )

        except Exception as e:
            logging.exception("Unhandled ingestion exception")
            return self._finish(
                StatusCode.EXECUTION_UNHANDLED_EXCEPTION,
                ExitCode.RUNTIME_EXCEPTION,
                start_time,
                fetched,
                written,
                failed,
                new,
                updated,
                unchanged,
                removed,
                message=str(e),
            )

    # --------------------------------------------------------
    # Internal
    # --------------------------------------------------------
    def _finish(
        self,
        status: StatusCode,
        exit_code: ExitCode,
        start_time: float,
        fetched: int = 0,
        written: int = 0,
        failed: int = 0,
        new: int = 0,
        updated: int = 0,
        unchanged: int = 0,
        removed: int = 0,
        message: Optional[str] = None,
    ) -> IngestionResult:
        duration = time.monotonic() - start_time

        logging.info(
            "Ingestion completed | status=%s exit=%s fetched=%d written=%d "
            "new=%d updated=%d unchanged=%d removed=%d failed=%d duration=%.2fs",
            status.name,
            exit_code.name,
            fetched,
            written,
            new,
            updated,
            unchanged,
            removed,
            failed,
            duration,
        )

        return IngestionResult(
            status_code=status,
            exit_code=exit_code,
            records_fetched=fetched,
            records_written=written,
            records_failed=failed,
            records_new=new,
            records_updated=updated,
            records_unchanged=unchanged,
            records_removed=removed,
            duration_seconds=duration,
            message=message,
        )
