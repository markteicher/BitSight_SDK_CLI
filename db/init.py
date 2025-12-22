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
        logging.info("Initializing BitSight MSSQL schema | file=%s", self.schema_path)
        statements = self._load_schema_statements()

        try:
            for i, stmt in enumerate(statements, start=1):
                stmt = stmt.strip()
                if not stmt:
                    continue

                preview = stmt.replace("\n", " ")[:180]
                logging.info("Schema apply | stmt=%d/%d | preview=%s", i, len(statements), preview)

                self.db.execute(stmt)

            self.db.commit()
            logging.info("MSSQL schema initialized successfully | statements=%d", len(statements))

        except Exception:
            self.db.rollback()
            logging.exception("MSSQL schema initialization failed — transaction rolled back")
            raise

        finally:
            self.db.close()

    # ------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------
    def _load_schema_statements(self) -> List[str]:
        """
        Split schema file into executable MSSQL statements.

        Contract:
        - Schema file must terminate executable units with semicolons.
        - Comments (single-line and block) are preserved but do not control splitting.
        """
        raw_sql = self.schema_path.read_text(encoding="utf-8")

        # Normalize line endings and strip BOMs
        raw_sql = raw_sql.replace("\ufeff", "")
        raw_sql = raw_sql.replace("\r\n", "\n").replace("\r", "\n")

        statements: List[str] = []
        buffer: List[str] = []

        in_block_comment = False

        for line in raw_sql.splitlines():
            raw_line = line

            # Track block comments so we don't accidentally treat internal lines as code boundaries
            stripped = raw_line.strip()

            if in_block_comment:
                buffer.append(raw_line)
                if "*/" in stripped:
                    in_block_comment = False
                continue

            if stripped.startswith("/*"):
                buffer.append(raw_line)
                if "*/" not in stripped:
                    in_block_comment = True
                continue

            # Single-line comment: keep it with surrounding context for operator readability
            if stripped.startswith("--"):
                buffer.append(raw_line)
                continue

            buffer.append(raw_line)

            # Statement boundary: line ends with semicolon (common in guarded DDL blocks: END;)
            if stripped.endswith(";"):
                statement = "\n".join(buffer).strip()

                # Drop trailing semicolon for pyodbc execute() safety/consistency
                if statement.endswith(";"):
                    statement = statement[:-1].rstrip()

                if statement:
                    statements.append(statement)

                buffer.clear()

        # Trailing fragment: only add if it contains something meaningful
        trailing = "\n".join(buffer).strip()
        if trailing:
            statements.append(trailing)

        logging.info("Loaded schema statements | count=%d", len(statements))
        return statements
