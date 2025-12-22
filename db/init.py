#!/usr/bin/env python3

import logging
from pathlib import Path
from typing import List

from db.mssql import MSSQLDatabase


class MSSQLInitializer:
    """
    Executes a full BitSight MSSQL schema in a combat-grade, deterministic manner.

    Guarantees:
      - GO-delimited batch execution
      - Explicit transaction handling
      - Loud, operator-visible failure
      - Zero silent partial deployment
    """

    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        schema_path: str,
    ):
        self.schema_path = Path(schema_path)

        if not self.schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")

        self.db = MSSQLDatabase(
            server=server,
            database=database,
            username=username,
            password=password,
        )

    # ------------------------------------------------------------
    # Public
    # ------------------------------------------------------------
    def run(self) -> None:
        logging.info("Initializing BitSight MSSQL schema")
        batches = self._load_schema_batches()

        try:
            for idx, batch in enumerate(batches, start=1):
                batch = batch.strip()
                if not batch:
                    continue

                logging.info("Executing schema batch %d/%d", idx, len(batches))
                logging.debug("Batch preview:\n%s", batch[:500])

                self.db.execute(batch)

            self.db.commit()
            logging.info("MSSQL schema initialized successfully")

        except Exception:
            self.db.rollback()
            logging.exception(
                "MSSQL schema initialization failed — transaction rolled back"
            )
            raise

        finally:
            self.db.close()

    # ------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------
    def _load_schema_batches(self) -> List[str]:
        """
        Split schema file into MSSQL execution batches using GO delimiters.

        Semicolons are NOT treated as batch boundaries.
        """

        raw_sql = self.schema_path.read_text(encoding="utf-8")

        # Normalize BOMs and line endings
        raw_sql = raw_sql.replace("\ufeff", "")

        batches: List[str] = []
        buffer: List[str] = []

        for line in raw_sql.splitlines():
            if line.strip().upper() == "GO":
                batch = "\n".join(buffer).strip()
                if batch:
                    batches.append(batch)
                buffer.clear()
            else:
                buffer.append(line)

        # Final batch (no trailing GO)
        final_batch = "\n".join(buffer).strip()
        if final_batch:
            batches.append(final_batch)

        logging.info("Loaded %d MSSQL schema batches", len(batches))
        return batches
