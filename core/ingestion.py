#!/usr/bin/env python3
"""
File: core/ingestion.py

Purpose:
    Provide the canonical synchronous ingestion execution framework used by the
    BitSight SDK and command-line interface.

Responsibilities:
    - Execute exactly one ingestion workload per run() invocation.
    - Fetch source records through a caller-supplied fetch function.
    - Write and reconcile each fetched record through a caller-supplied writer.
    - Track deterministic ingestion counters.
    - Track stable source-record keys for removal reconciliation.
    - Execute optional post-write removal reconciliation.
    - Distinguish successful, partial-failure, total-failure, interrupted, and
      unhandled-exception outcomes.
    - Return one immutable IngestionResult containing StatusCode and ExitCode
      values for the caller.
    - Record total execution duration using a monotonic clock.

Execution contract:
    fetcher() -> Iterable[Any]
        Returns the source records for one ingestion workload.

    writer(record) -> str
        Processes one source record and returns exactly one supported outcome:

            "new"
            "updated"
            "unchanged"

    key_fn(record) -> Any
        Returns the stable, hashable primary key used to identify the source
        record during reconciliation.

    finalize(seen_keys) -> int
        Optionally reconciles records that were not present in the current
        source dataset and returns the number of records removed.

Ownership:
    - The writer owns record-level persistence and reconciliation decisions.
    - The finalize callback owns removal reconciliation.
    - The executor owns orchestration, counting, timing, logging, and runtime
      outcome semantics.
    - The application entry point owns process termination and is responsible
      for passing result.exit_code to sys.exit() exactly once.

Important:
    This module does not terminate the process. Returning an ExitCode in
    IngestionResult is not the same as emitting a process exit code.

Process termination example:
    result = executor.run()
    sys.exit(result.exit_code)

Safety:
    - Removal reconciliation is not treated as a failure when finalize()
      succeeds and reports removed records.
    - Failed records remain represented in seen_keys after successful key
      extraction. This prevents a transient write failure from causing an
      existing destination record to be incorrectly treated as absent.
    - Invalid writer outcomes are counted as failed records and are not counted
      as successfully written records.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Literal, Optional, Set, Union

from tqdm import tqdm

from core.exit_codes import ExitCode
from core.status_codes import StatusCode


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public ingestion type definitions
# ---------------------------------------------------------------------------

WriterOutcome = Literal["new", "updated", "unchanged"]

RecordFetcher = Callable[[], Iterable[Any]]
RecordWriter = Callable[[Any], WriterOutcome]
RecordKeyFunction = Callable[[Any], Any]
Finalizer = Callable[[Set[Any]], int]


# ---------------------------------------------------------------------------
# Immutable ingestion result
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IngestionResult:
    """
    Immutable result returned by one ingestion execution.

    Attributes:
        status_code:
            Runtime semantic status describing the ingestion outcome.

        exit_code:
            Stable process-level outcome selected for the application entry
            point. This module does not call sys.exit().

        records_fetched:
            Number of source records materialized from the fetcher.

        records_written:
            Number of records for which the writer returned a valid successful
            outcome.

        records_failed:
            Number of records that failed key extraction, writer execution, or
            writer-outcome validation.

        records_new:
            Number of successfully written records classified as new.

        records_updated:
            Number of successfully written records classified as updated.

        records_unchanged:
            Number of successfully processed records that required no update.

        records_removed:
            Number of destination records successfully removed by finalize().

        duration_seconds:
            Total elapsed execution time measured with time.monotonic().

        message:
            Optional diagnostic message for the terminal outcome.

    Counter invariants:
        records_written ==
            records_new + records_updated + records_unchanged

        records_fetched ==
            records_written + records_failed

    Notes:
        The second invariant assumes every fetched record is processed exactly
        once and that no execution-level failure stops processing before the
        record loop completes.
    """

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


# ---------------------------------------------------------------------------
# Canonical ingestion executor
# ---------------------------------------------------------------------------

class IngestionExecutor:
    """
    Execute exactly one synchronous ingestion workload.

    The executor is reusable, but each call to run() represents a separate
    ingestion execution with new counters, timing, and reconciliation state.

    The executor does not retain record-level state between run() calls.

    Contract:
        - fetcher must be callable.
        - writer must be callable.
        - key_fn must be callable.
        - finalize must be callable when supplied.
        - expected_min_records must be a non-negative integer.
        - show_progress must be a boolean.
        - key_fn must return a hashable value.
        - writer must return "new", "updated", or "unchanged".
        - finalize must return a non-negative integer.
    """

    _VALID_WRITER_OUTCOMES = frozenset(
        {
            "new",
            "updated",
            "unchanged",
        }
    )

    def __init__(
        self,
        *,
        fetcher: RecordFetcher,
        writer: RecordWriter,
        key_fn: RecordKeyFunction,
        finalize: Optional[Finalizer] = None,
        expected_min_records: int = 0,
        show_progress: bool = True,
    ) -> None:
        """
        Initialize one ingestion executor.

        Args:
            fetcher:
                Callable returning an iterable of source records.

            writer:
                Callable that processes one record and returns one supported
                writer outcome.

            key_fn:
                Callable returning the stable, hashable key for one record.

            finalize:
                Optional callback receiving all source keys observed during the
                run. It returns the number of destination records removed.

            expected_min_records:
                Optional lower-bound expectation for fetched records. A result
                below this threshold generates a warning but does not
                automatically fail the ingestion.

            show_progress:
                When True, display a tqdm progress bar during record processing.

        Raises:
            TypeError:
                If a callback is not callable or an argument has an invalid
                type.

            ValueError:
                If expected_min_records is negative.
        """
        if not callable(fetcher):
            raise TypeError("fetcher must be callable")

        if not callable(writer):
            raise TypeError("writer must be callable")

        if not callable(key_fn):
            raise TypeError("key_fn must be callable")

        if finalize is not None and not callable(finalize):
            raise TypeError("finalize must be callable when provided")

        if isinstance(expected_min_records, bool) or not isinstance(
            expected_min_records,
            int,
        ):
            raise TypeError("expected_min_records must be an integer")

        if expected_min_records < 0:
            raise ValueError(
                "expected_min_records must be greater than or equal to 0"
            )

        if not isinstance(show_progress, bool):
            raise TypeError("show_progress must be a boolean")

        self.fetcher = fetcher
        self.writer = writer
        self.key_fn = key_fn
        self.finalize = finalize
        self.expected_min_records = expected_min_records
        self.show_progress = show_progress

    # -----------------------------------------------------------------------
    # Public execution
    # -----------------------------------------------------------------------

    def run(self) -> IngestionResult:
        """
        Execute one complete ingestion workload.

        Execution stages:
            1. Fetch and materialize source records.
            2. Validate the fetched-record count.
            3. Extract each record key.
            4. Execute the writer for each record.
            5. Validate and count each writer outcome.
            6. Execute optional removal reconciliation.
            7. Select final StatusCode and ExitCode values.
            8. Return one immutable IngestionResult.

        Returns:
            IngestionResult containing counters, duration, status, exit code,
            and an optional diagnostic message.

        Exception handling:
            - Fetch failures return INGEST_START_FAILED.
            - Record failures are counted and processing continues.
            - Finalization failures return a partial-failure result.
            - KeyboardInterrupt returns an interrupted runtime result.
            - Unhandled execution-level exceptions return RUNTIME_EXCEPTION.
        """
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
            # ---------------------------------------------------------------
            # Stage 1: Fetch and materialize source records
            # ---------------------------------------------------------------

            try:
                fetched_records = self.fetcher()

                if fetched_records is None:
                    raise TypeError(
                        "fetcher returned None; an iterable was required"
                    )

                records = list(fetched_records)

            except KeyboardInterrupt:
                raise

            except Exception as exc:
                logger.exception("Ingestion fetch failed")

                return self._finish(
                    status=StatusCode.INGESTION_FETCH_FAILED,
                    exit_code=ExitCode.INGEST_START_FAILED,
                    start_time=start_time,
                    message=str(exc),
                )

            fetched = len(records)

            # An empty source result is a valid non-failure outcome.
            #
            # finalize() is intentionally not invoked for an empty source
            # result. This prevents an unexpected empty API response from
            # triggering destructive removal of all destination records.
            if fetched == 0:
                return self._finish(
                    status=StatusCode.OK_NO_DATA,
                    exit_code=ExitCode.SUCCESS_EMPTY_RESULT,
                    start_time=start_time,
                    fetched=fetched,
                )

            if (
                self.expected_min_records > 0
                and fetched < self.expected_min_records
            ):
                logger.warning(
                    "Fetched record count is below the configured expectation "
                    "| fetched=%d expected_minimum=%d",
                    fetched,
                    self.expected_min_records,
                )

            # ---------------------------------------------------------------
            # Stage 2: Write and reconcile source records
            # ---------------------------------------------------------------

            record_iterator: Iterable[Any]

            if self.show_progress:
                record_iterator = tqdm(
                    records,
                    desc="Ingesting records",
                    unit="record",
                    total=fetched,
                )
            else:
                record_iterator = records

            for record_index, record in enumerate(record_iterator, start=1):
                try:
                    # Extract and store the key before invoking the writer.
                    #
                    # When writing later fails, the record remains represented
                    # in seen_keys and is therefore not incorrectly considered
                    # absent during removal reconciliation.
                    key = self.key_fn(record)

                    try:
                        seen_keys.add(key)
                    except TypeError as exc:
                        raise TypeError(
                            "key_fn must return a hashable value"
                        ) from exc

                    outcome = self.writer(record)

                    if outcome not in self._VALID_WRITER_OUTCOMES:
                        raise ValueError(
                            "writer returned an unsupported outcome: "
                            f"{outcome!r}"
                        )

                    # Count the record as written only after the writer has
                    # completed and returned a valid outcome.
                    written += 1

                    if outcome == "new":
                        new += 1
                    elif outcome == "updated":
                        updated += 1
                    else:
                        unchanged += 1

                except KeyboardInterrupt:
                    raise

                except Exception:
                    failed += 1

                    logger.exception(
                        "Record ingestion failed | record_index=%d",
                        record_index,
                    )

            # ---------------------------------------------------------------
            # Stage 3: Finalize removal reconciliation
            # ---------------------------------------------------------------

            if self.finalize is not None:
                try:
                    finalize_result = self.finalize(seen_keys)

                    if isinstance(finalize_result, bool):
                        raise TypeError(
                            "finalize must return a non-negative integer"
                        )

                    removed = int(finalize_result)

                    if removed < 0:
                        raise ValueError(
                            "finalize returned a negative removal count"
                        )

                except KeyboardInterrupt:
                    raise

                except Exception as exc:
                    logger.exception(
                        "Removal reconciliation failed"
                    )

                    return self._finish(
                        status=StatusCode.INGESTION_STATE_UPDATE_FAILED,
                        exit_code=ExitCode.INGEST_PARTIAL_FAILURE,
                        start_time=start_time,
                        fetched=fetched,
                        written=written,
                        failed=failed,
                        new=new,
                        updated=updated,
                        unchanged=unchanged,
                        removed=0,
                        message=str(exc),
                    )

            # ---------------------------------------------------------------
            # Stage 4: Determine final execution semantics
            # ---------------------------------------------------------------

            # Every record failed. No valid writer outcome was produced.
            if written == 0 and failed > 0:
                return self._finish(
                    status=StatusCode.INGESTION_WRITE_FAILED,
                    exit_code=ExitCode.DB_WRITE_FAILED,
                    start_time=start_time,
                    fetched=fetched,
                    written=written,
                    failed=failed,
                    new=new,
                    updated=updated,
                    unchanged=unchanged,
                    removed=removed,
                    message="All fetched records failed ingestion",
                )

            # At least one record succeeded and at least one record failed.
            if failed > 0:
                return self._finish(
                    status=StatusCode.INGESTION_PARTIAL_WRITE,
                    exit_code=ExitCode.INGEST_PARTIAL_FAILURE,
                    start_time=start_time,
                    fetched=fetched,
                    written=written,
                    failed=failed,
                    new=new,
                    updated=updated,
                    unchanged=unchanged,
                    removed=removed,
                    message="One or more records failed ingestion",
                )

            # Successful removals are valid reconciliation changes and do not
            # represent a partial failure.
            if new == 0 and updated == 0 and removed == 0:
                return self._finish(
                    status=StatusCode.OK,
                    exit_code=ExitCode.SUCCESS_NO_CHANGES,
                    start_time=start_time,
                    fetched=fetched,
                    written=written,
                    failed=failed,
                    new=new,
                    updated=updated,
                    unchanged=unchanged,
                    removed=removed,
                )

            return self._finish(
                status=StatusCode.OK,
                exit_code=ExitCode.SUCCESS,
                start_time=start_time,
                fetched=fetched,
                written=written,
                failed=failed,
                new=new,
                updated=updated,
                unchanged=unchanged,
                removed=removed,
            )

        except KeyboardInterrupt:
            logger.warning("Ingestion interrupted by operator")

            return self._finish(
                status=StatusCode.EXECUTION_INTERRUPTED,
                exit_code=ExitCode.RUNTIME_INTERRUPT,
                start_time=start_time,
                fetched=fetched,
                written=written,
                failed=failed,
                new=new,
                updated=updated,
                unchanged=unchanged,
                removed=removed,
                message="Ingestion interrupted by operator",
            )

        except Exception as exc:
            logger.exception("Unhandled ingestion exception")

            return self._finish(
                status=StatusCode.EXECUTION_UNHANDLED_EXCEPTION,
                exit_code=ExitCode.RUNTIME_EXCEPTION,
                start_time=start_time,
                fetched=fetched,
                written=written,
                failed=failed,
                new=new,
                updated=updated,
                unchanged=unchanged,
                removed=removed,
                message=str(exc),
            )

    # -----------------------------------------------------------------------
    # Internal result construction
    # -----------------------------------------------------------------------

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
        """
        Construct and log the terminal result for one run() invocation.

        Args:
            status:
                Runtime StatusCode describing the execution outcome.

            exit_code:
                Stable ExitCode for the application entry point.

            start_time:
                Monotonic timestamp captured at the beginning of run().

            fetched:
                Number of fetched source records.

            written:
                Number of records with valid writer outcomes.

            failed:
                Number of records that failed processing.

            new:
                Number of newly created records.

            updated:
                Number of updated records.

            unchanged:
                Number of unchanged records.

            removed:
                Number of successfully removed records.

            message:
                Optional diagnostic message.

        Returns:
            Immutable IngestionResult.

        Raises:
            ValueError:
                If counters violate required ingestion invariants.
        """
        duration = max(0.0, time.monotonic() - start_time)

        self._validate_counters(
            fetched=fetched,
            written=written,
            failed=failed,
            new=new,
            updated=updated,
            unchanged=unchanged,
            removed=removed,
        )

        logger.info(
            "Ingestion completed | status=%s exit=%s "
            "fetched=%d written=%d new=%d updated=%d unchanged=%d "
            "removed=%d failed=%d duration=%.3fs",
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

    @staticmethod
    def _validate_counters(
        *,
        fetched: int,
        written: int,
        failed: int,
        new: int,
        updated: int,
        unchanged: int,
        removed: int,
    ) -> None:
        """
        Validate internal ingestion counter invariants.

        Raises:
            ValueError:
                If any counter is negative, successful outcome totals do not
                equal records_written, or processed records exceed fetched
                records.
        """
        counters = {
            "fetched": fetched,
            "written": written,
            "failed": failed,
            "new": new,
            "updated": updated,
            "unchanged": unchanged,
            "removed": removed,
        }

        for counter_name, counter_value in counters.items():
            if counter_value < 0:
                raise ValueError(
                    f"Ingestion counter cannot be negative: "
                    f"{counter_name}={counter_value}"
                )

        classified_written = new + updated + unchanged

        if written != classified_written:
            raise ValueError(
                "records_written must equal records_new + records_updated + "
                "records_unchanged"
            )

        if written + failed > fetched:
            raise ValueError(
                "records_written + records_failed cannot exceed "
                "records_fetched"
            )
