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
        Load and split schema into executable MSSQL statements.

        Contract:
          - Statements are semicolon-delimited
          - 'GO' is ignored defensively if present
          - Block and line comments are preserved but never executed alone
        """
        raw_sql = self.schema_path.read_text(encoding="utf-8")
        raw_sql = raw_sql.replace("\ufeff", "").replace("\r\n", "\n").strip()

        statements: List[str] = []
        buffer: List[str] = []

        in_block_comment = False

        def flush_buffer() -> None:
            stmt = "\n".join(buffer).strip()
            buffer.clear()

            if not stmt:
                return

            # Drop comment-only statements
            lines = [ln.strip() for ln in stmt.splitlines() if ln.strip()]
            if lines and all(ln.startswith("--") for ln in lines):
                return

            statements.append(stmt.rstrip(";").strip())

        for line in raw_sql.splitlines():
            stripped = line.strip()

            # Ignore batch separators defensively
            if not in_block_comment and stripped.upper() == "GO":
                continue

            # Handle block comments
            if not in_block_comment and "/*" in stripped:
                in_block_comment = True

            if in_block_comment:
                buffer.append(line)
                if "*/" in stripped:
                    in_block_comment = False
                continue

            # Line comments / blanks
            if stripped.startswith("--") or not stripped:
                buffer.append(line)
                continue

            buffer.append(line)

            if stripped.endswith(";"):
                flush_buffer()

        if buffer:
            flush_buffer()

        logging.info("Loaded %d schema statements", len(statements))
        return statements
