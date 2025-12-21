#!/usr/bin/env python3
"""
core/ingestion.py

Central ingestion execution contract.

This module:
- Defines the execution record (counts, timings, status-code tallies)
- Enforces deterministic run ordering (fetch -> persist -> commit)
- Converts failures into explicit exit codes (no silent success)
"""

from __future__ import annotations

import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple

# Required: core/exit_codes.py must define ExitCodes (Enum or constants container).
# Expected names are referenced explicitly below.
from core.exit_codes import ExitCodes


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class IngestionMetrics:
    endpoint_name: str
    table_name: str

    started_at_utc: datetime
    finished_at_utc: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    records_retrieved: int = 0
    records_processed: int = 0
    records_written: int = 0

    http_status_counts: Dict[int, int] = field(default_factory=dict)

    errors_total: int = 0
    error_messages: List[str] = field(default_factory=list)

    exit_code: int = int(ExitCodes.OK)


class DatabaseWriter(Protocol):
    """
    DB abstraction expected by ingestion modules.

    Required methods are intentionally narrow:
    - begin/commit/rollback must be real (transactional)
    - write_rows must raise on failure (no silent drops)
    """

    def begin(self) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...

    def write_rows(self, table_name: str, rows: Iterable[Dict[str, Any]]) -> int: ...
    def flush_table(self, table_name: str) -> None: ...


class Fetcher(Protocol):
    """
    Fetch abstraction expected by ingestion modules.

    Implementations must:
    - return iterable of dict records (already normalized or raw)
    - raise on HTTP or parsing failures
    - optionally return status codes counted via observe_http_status()
    """

    def fetch(self) -> List[Dict[str, Any]]: ...


class IngestionJob(Protocol):
    """
    Job contract for any ingest/* module.

    A job is responsible for:
    - naming (endpoint + table)
    - fetch()
    - persist(rows)
    """

    endpoint_name: str
    table_name: str

    def fetch(self) -> List[Dict[str, Any]]: ...
    def persist(self, db: DatabaseWriter, rows: List[Dict[str, Any]]) -> int: ...


class IngestionExecution:
    """
    Orchestrates one ingestion job and returns a deterministic exit code.

    This runner:
    - does not invent schema
    - does not mutate payloads
    - does not swallow exceptions
    """

    def __init__(
        self,
        job: IngestionJob,
        db: DatabaseWriter,
        *,
        flush: bool = False,
    ):
        self.job = job
        self.db = db
        self.flush = flush

        self.metrics = IngestionMetrics(
            endpoint_name=job.endpoint_name,
            table_name=job.table_name,
            started_at_utc=utc_now(),
        )

    def observe_http_status(self, status_code: int) -> None:
        self.metrics.http_status_counts[status_code] = (
            self.metrics.http_status_counts.get(status_code, 0) + 1
        )

    def _finalize(self, exit_code: int) -> int:
        finished = utc_now()
        duration = (finished - self.metrics.started_at_utc).total_seconds()

        object.__setattr__(self.metrics, "finished_at_utc", finished)
        object.__setattr__(self.metrics, "duration_seconds", duration)
        object.__setattr__(self.metrics, "exit_code", int(exit_code))

        logging.info(
            "INGEST DONE endpoint=%s table=%s exit_code=%s duration_s=%.3f retrieved=%d processed=%d written=%d errors=%d http_status=%s",
            self.metrics.endpoint_name,
            self.metrics.table_name,
            int(exit_code),
            duration,
            self.metrics.records_retrieved,
            self.metrics.records_processed,
            self.metrics.records_written,
            self.metrics.errors_total,
            dict(self.metrics.http_status_counts),
        )
        return int(exit_code)

    def run(self) -> int:
        """
        Returns process exit code (int). Caller decides whether to sys.exit().

        Exit code mapping rules are mechanical:
        - OK: all steps succeeded
        - DB_*: any DB failure (begin/flush/write/commit)
        - API_*: fetch failure (HTTP / decode / unexpected response)
        - UNHANDLED_EXCEPTION: anything not mapped explicitly
        """
        logging.info(
            "INGEST START endpoint=%s table=%s flush=%s started_at_utc=%s",
            self.job.endpoint_name,
            self.job.table_name,
            self.flush,
            self.metrics.started_at_utc.isoformat(),
        )

        # -----------------------
        # TRANSACTION BEGIN
        # -----------------------
        try:
            self.db.begin()
        except Exception as e:
            self._record_error(f"DB begin failed: {e!r}")
            return self._finalize(ExitCodes.DB_CONNECT_OR_BEGIN_FAILED)

        # -----------------------
        # OPTIONAL FLUSH
        # -----------------------
        if self.flush:
            try:
                self.db.flush_table(self.job.table_name)
            except Exception as e:
                self._record_error(f"DB flush failed table={self.job.table_name}: {e!r}")
                try:
                    self.db.rollback()
                except Exception as rb:
                    self._record_error(f"DB rollback failed after flush failure: {rb!r}")
                    return self._finalize(ExitCodes.DB_ROLLBACK_FAILED)
                return self._finalize(ExitCodes.DB_FLUSH_FAILED)

        # -----------------------
        # FETCH
        # -----------------------
        try:
            rows = self.job.fetch()
            object.__setattr__(self.metrics, "records_retrieved", len(rows))
        except Exception as e:
            self._record_error(f"Fetch failed endpoint={self.job.endpoint_name}: {e!r}")
            try:
                self.db.rollback()
            except Exception as rb:
                self._record_error(f"DB rollback failed after fetch failure: {rb!r}")
                return self._finalize(ExitCodes.DB_ROLLBACK_FAILED)
            return self._finalize(ExitCodes.API_FETCH_FAILED)

        # -----------------------
        # PERSIST
        # -----------------------
        try:
            object.__setattr__(self.metrics, "records_processed", len(rows))
            written = self.job.persist(self.db, rows)
            object.__setattr__(self.metrics, "records_written", int(written))
        except Exception as e:
            self._record_error(
                f"Persist failed table={self.job.table_name} endpoint={self.job.endpoint_name}: {e!r}"
            )
            try:
                self.db.rollback()
            except Exception as rb:
                self._record_error(f"DB rollback failed after persist failure: {rb!r}")
                return self._finalize(ExitCodes.DB_ROLLBACK_FAILED)
            return self._finalize(ExitCodes.DB_WRITE_FAILED)

        # -----------------------
        # COMMIT
        # -----------------------
        try:
            self.db.commit()
        except Exception as e:
            self._record_error(f"DB commit failed: {e!r}")
            try:
                self.db.rollback()
            except Exception as rb:
                self._record_error(f"DB rollback failed after commit failure: {rb!r}")
                return self._finalize(ExitCodes.DB_ROLLBACK_FAILED)
            return self._finalize(ExitCodes.DB_COMMIT_FAILED)

        return self._finalize(ExitCodes.OK)

    def _record_error(self, msg: str) -> None:
        logging.error(msg)
        object.__setattr__(self.metrics, "errors_total", self.metrics.errors_total + 1)
        # store bounded detail so logs stay usable
        if len(self.metrics.error_messages) < 50:
            self.metrics.error_messages.append(msg)


def exit_with(code: int) -> None:
    """
    Single exit path for CLI.

    CLI should call:
        exit_with(execution.run())
    """
    sys.exit(int(code))
