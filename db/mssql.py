#!/usr/bin/env python3

from __future__ import annotations

import logging
from typing import Iterable, Sequence, Any

import pyodbc

from core.database_interface import DatabaseInterface


class MSSQLDatabase(DatabaseInterface):
    """
    Concrete MSSQL implementation of DatabaseInterface.
    """

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

        self.connection: pyodbc.Connection = self._connect()

    # ------------------------------------------------------------------
    # connection
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # interface implementation
    # ------------------------------------------------------------------

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        cursor = self.connection.cursor()
        cursor.execute(sql, params)

    def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        cursor = self.connection.cursor()
        cursor.fast_executemany = True
        cursor.executemany(sql, rows)

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def close(self) -> None:
        self.connection.close()

    # ------------------------------------------------------------------
    # health / validation
    # ------------------------------------------------------------------

    def ping(self) -> None:
        """
        Validate database connectivity.
        Raises on failure.
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT 1")
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("MSSQL ping failed")

    def scalar(self, sql: str, params: Sequence[Any] = ()) -> Any:
        """
        Execute a query expected to return exactly one scalar value.
        Raises if no row is returned.
        """
        cursor = self.connection.cursor()
        cursor.execute(sql, params)
        row = cursor.fetchone()

        if row is None:
            raise RuntimeError("scalar query returned no rows")

        return row[0]

    def table_exists(self, schema: str, table: str) -> bool:
        """
        Check whether a table exists in the database.
        """
        sql = """
        SELECT 1
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ?
          AND TABLE_NAME = ?
        """

        cursor = self.connection.cursor()
        cursor.execute(sql, (schema, table))
        return cursor.fetchone() is not None
