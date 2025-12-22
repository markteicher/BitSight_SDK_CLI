#!/usr/bin/env python3

import logging
from pathlib import Path
from typing import List

from db.mssql import MSSQLDatabase


class MSSQLInitializer:
    """
    Resilient schema initializer.

    Design goals:
      - Re-runnable: schema file is deployment-guarded (IF OBJECT_ID... IS NULL)
      - Transaction-safe: all-or-nothing apply, rollback on failure
      - Robust statement splitting: supports IF/BEGIN/END blocks and index guards
      - Operator-visible logging: shows which statement failed (without dumping secrets)
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
        logging.info("DB_INIT Starting MSSQL schema initialization")
        statements = self._load_schema_statements()
        logging.info("DB_INIT Loaded %d executable statements", len(statements))

        try:
            for i, stmt in enumerate(statements, start=1):
                preview = self._preview(stmt)
                logging.info("DB_INIT Executing [%d/%d] %s", i, len(statements), preview)
                self.db.execute(stmt)

            self.db.commit()
            logging.info("DB_INIT MSSQL schema initialized successfully")

        except Exception as e:
            # Always rollback for combat-grade behavior
            try:
                self.db.rollback()
            except Exception:
                logging.exception("DB_INIT Rollback failed")
                raise

            logging.exception("DB_INIT Schema initialization failed: %s", e)
            raise

        finally:
            self.db.close()

    # ------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------
    def _load_schema_statements(self) -> List[str]:
        """
        Split schema file into executable SQL statements.

        IMPORTANT:
        - We cannot split purely on semicolons because guarded blocks
          (IF ... BEGIN ... END) contain internal semicolons.
        - We split on semicolons ONLY at depth==0 (outside BEGIN/END blocks).
        """
        raw_sql = self.schema_path.read_text(encoding="utf-8")

        # Normalize and strip BOM
        raw_sql = raw_sql.replace("\ufeff", "").replace("\r\n", "\n").strip()

        statements: List[str] = []
        buffer: List[str] = []
        depth = 0  # BEGIN/END nesting

        for line in raw_sql.split("\n"):
            stripped = line.strip()

            # Always keep comments and blank lines (useful for readability/debug)
            buffer.append(line)

            # Track BEGIN/END nesting conservatively (common in guarded DDL)
            # We only treat standalone BEGIN/END (or BEGIN/END as first token) as control flow.
            # This avoids accidental matches inside column names, etc.
            first_token = stripped.split(None, 1)[0].upper() if stripped else ""

            if first_token == "BEGIN":
                depth += 1
            elif first_token == "END":
                # END may appear as "END;" on same line
                if depth > 0:
                    depth -= 1

            # Split only when we hit a statement terminator at top-level
            if depth == 0 and stripped.endswith(";"):
                stmt = "\n".join(buffer).strip()
                # remove the final semicolon (pyodbc is fine either way, but keep deterministic)
                if stmt.endswith(";"):
                    stmt = stmt[:-1].rstrip()
                if stmt:
                    statements.append(stmt)
                buffer.clear()

        # Trailing statement without semicolon (still execute it)
        trailing = "\n".join(buffer).strip()
        if trailing:
            statements.append(trailing)

        return statements

    @staticmethod
    def _preview(sql: str, max_len: int = 140) -> str:
        """
        Compact single-line preview for logs.
        """
        one_line = " ".join(sql.split())
        if len(one_line) <= max_len:
            return one_line
        return one_line[: max_len - 3] + "..."


__all__ = ["MSSQLInitializer"]
