#!/usr/bin/env python3

import logging
import time
from dataclasses import dataclass
from typing import Optional, Any, Iterable

from tqdm import tqdm

from core.status_codes import StatusCode


@dataclass
class IngestionResult:
    status: StatusCode
    records_received: int
    records_written: int
    records_failed: int
    start_time: float
    end_time: float
    error: Optional[Exception] = None


class IngestionExecution:
    """
    Orchestrates a single ingestion execution.
    This class does not fetch data and does not write data.
    It coordinates those steps and records execution outcomes.
    """

    def __init__(
        self,
        name: str,
        progress_enabled: bool = True,
    ):
        self.name = name
        self.progress_enabled = progress_enabled

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------
    def run(
        self,
        fetch_iterable: Iterable[Any],
        write_fn,
    ) -> IngestionResult:
        """
        Executes ingestion using:
          - fetch_iterable: iterable of records from API layer
          - write_fn(record): writes one record to destination

        Returns an IngestionResult summarizing execution.
        """
        start = time.time()

        received = 0
        written = 0
        failed = 0

        try:
            iterator = (
                tqdm(fetch_iterable, desc=self.name)
                if self.progress_enabled
                else fetch_iterable
            )

            for record in iterator:
                received += 1
                try:
                    write_fn(record)
                    written += 1
                except Exception as exc:
                    failed += 1
                    logging.exception(
                        "Record write failed during ingestion '%s'", self.name
                    )

            status = (
                StatusCode.OK
                if failed == 0
                else StatusCode.PARTIAL_SUCCESS
            )

            return IngestionResult(
                status=status,
                records_received=received,
                records_written=written,
                records_failed=failed,
                start_time=start,
                end_time=time.time(),
            )

        except Exception as exc:
            logging.exception("Ingestion execution failed: %s", self.name)
            return IngestionResult(
                status=StatusCode.UNHANDLED_EXCEPTION,
                records_received=received,
                records_written=written,
                records_failed=failed,
                start_time=start,
                end_time=time.time(),
                error=exc,
            )
