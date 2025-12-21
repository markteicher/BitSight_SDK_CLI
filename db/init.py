#!/usr/bin/env python3

import logging
from pathlib import Path
from typing import List

from db.mssql import MSSQLDatabase


class MSSQLInitializer:
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
        statements = self._load_schema_statements()

        try:
            for stmt in statements:
                stmt = stmt.strip()
                if not stmt:
                    continue

                logging.debug("Executing schema statement")
                self.db.execute(stmt)

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
    def _load_schema_statements(self) -> List[str]:
        """
        Split schema file into executable MSSQL statements.
        Uses semicolon delimiters safely.
        """
        raw_sql = self.schema_path.read_text(encoding="utf-8")

        # Normalize line endings and strip BOMs
        raw_sql = raw_sql.replace("\ufeff", "").strip()

        statements = []
        buffer: List[str] = []

        for line in raw_sql.splitlines():
            stripped = line.strip()

            # Preserve comments
            if stripped.startswith("--") or stripped.startswith("/*"):
                buffer.append(line)
                continue

            buffer.append(line)

            if stripped.endswith(";"):
                statement = "\n".join(buffer).strip().rstrip(";")
                statements.append(statement)
                buffer.clear()

        # Catch any trailing statement without semicolon
        if buffer:
            statements.append("\n".join(buffer).strip())

        logging.info("Loaded %d schema statements", len(statements))
        return statements
