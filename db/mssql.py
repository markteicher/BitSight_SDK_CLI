#!/usr/bin/env python3

import logging
import pyodbc
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

        logging.info("Connecting to MSSQL server %s database %s", self.server, self.database)
        return pyodbc.connect(conn_str, autocommit=False)

    def execute(self, sql: str, params: Tuple[Any, ...] = ()) -> None:
        cursor = self.connection.cursor()
        cursor.execute(sql, params)

    def executemany(self, sql: str, rows: Iterable[Tuple[Any, ...]]) -> None:
        cursor = self.connection.cursor()
        cursor.fast_executemany = True
        cursor.executemany(sql, rows)

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def close(self) -> None:
        self.connection.close()
