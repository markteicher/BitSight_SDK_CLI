#!/usr/bin/env python3

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from core.database_interface import DatabaseInterface
from db.mssql import MSSQLDatabase


@dataclass(frozen=True)
class MSSQLOptions:
    server: str
    database: str
    username: str
    password: str
    driver: str = "ODBC Driver 18 for SQL Server"
    encrypt: bool = True
    trust_cert: bool = False
    timeout: int = 30


class DatabaseRouter:
    """
    Creates a concrete database client based on configuration.
    """

    def __init__(self, db_type: str, mssql: Optional[MSSQLOptions] = None):
        self.db_type = (db_type or "").strip().lower()
        self.mssql = mssql

    def connect(self) -> DatabaseInterface:
        if self.db_type == "mssql":
            if not self.mssql:
                raise ValueError("mssql options missing")

            return MSSQLDatabase(
                server=self.mssql.server,
                database=self.mssql.database,
                username=self.mssql.username,
                password=self.mssql.password,
                driver=self.mssql.driver,
                encrypt=self.mssql.encrypt,
                trust_cert=self.mssql.trust_cert,
                timeout=self.mssql.timeout,
            )

        raise ValueError(f"unsupported db_type: {self.db_type}")
