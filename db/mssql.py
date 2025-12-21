#!/usr/bin/env python3

import logging
import pyodbc
import json
from typing import Iterable, Tuple, Any


class MSSQLDatabase:
    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        driver: str = "ODBC Driver 18 for SQL Server",
        encrypt: bool = True,
        trust_cert: bool = False,
        timeout: int = 30,
    ):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.encrypt = encrypt
        self.trust_cert = trust_cert
        self.timeout = timeout

        self.connection = self._connect()

    # ------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------
    def _connect(self) -> pyodbc.Connection:
        conn_str = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"Encrypt={'yes' if self.encrypt else 'no'};"
            f"TrustServerCertificate={'yes' if self.trust_cert else 'no'};"
            f"Connection Timeout={self.timeout};"
        )

        logging.info(
            "Connecting to MSSQL server=%s database=%s",
            self.server,
            self.database,
        )
        return pyodbc.connect(conn_str, autocommit=False)

    # ------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------
    def _normalize_param(self, value: Any) -> Any:
        """
        Ensure payloads are safe for MSSQL insertion.
        Dicts/lists are always stored as JSON strings.
        """
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return value

    def table_exists(self, table_name: str) -> bool:
        """
        Validate required schema exists before ingestion.
        """
        sql = """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'dbo'
          AND TABLE_NAME = ?
        """
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (table_name,))
            return cursor.fetchone() is not None

    # ------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------
    def execute(self, sql: str, params: Tuple[Any, ...] = ()) -> None:
        normalized = tuple(self._normalize_param(p) for p in params)
        with self.connection.cursor() as cursor:
            cursor.execute(sql, normalized)

    def executemany(self, sql: str, rows: Iterable[Tuple[Any, ...]]) -> None:
        with self.connection.cursor() as cursor:
            cursor.fast_executemany = True
            normalized_rows = [
                tuple(self._normalize_param(p) for p in row)
                for row in rows
            ]
            cursor.executemany(sql, normalized_rows)

    # ------------------------------------------------------------
    # Transaction Control
    # ------------------------------------------------------------
    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    # ------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------
    def close(self) -> None:
        try:
            self.connection.close()
        except Exception:
            pass

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
