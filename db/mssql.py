#!/usr/bin/env python3

import logging
import pyodbc
from typing import Iterable, Tuple, Any, Optional

from core.status_codes import StatusCode
from core.exit_codes import ExitCode


class DatabaseInterface:
    def connect(self) -> None: ...
    def ping(self) -> None: ...
    def execute(self, sql: str, params: Tuple[Any, ...] = ()) -> None: ...
    def executemany(self, sql: str, rows: Iterable[Tuple[Any, ...]]) -> None: ...
    def scalar(self, sql: str, params: Tuple[Any, ...] = ()) -> Any: ...
    def table_exists(self, table: str, schema: str = "dbo") -> bool: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


class MSSQLDatabase(DatabaseInterface):
    """
    Combat-grade MSSQL database adapter.

    Guarantees:
      - Deterministic connection behavior
      - Explicit failure signaling
      - Transaction integrity
      - Schema inspection
      - Operator-visible failure causes
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

        self.connection: Optional[pyodbc.Connection] = None
        self.connect()

    # ------------------------------------------------------------
    # CONNECTION / HEALTH
    # ------------------------------------------------------------
    def connect(self) -> None:
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

        try:
            logging.info("DB_CONNECT server=%s database=%s", self.server, self.database)
            self.connection = pyodbc.connect(conn_str, autocommit=False)
        except pyodbc.InterfaceError as e:
            logging.error("DB_INTERFACE_ERROR %s", e)
            raise RuntimeError(StatusCode.DB_CONNECTION_FAILED) from e
        except pyodbc.Error as e:
            logging.error("DB_AUTH_OR_CONNECT_ERROR %s", e)
            raise RuntimeError(StatusCode.DB_AUTH_FAILED) from e

    def ping(self) -> None:
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
        except Exception as e:
            logging.error("DB_PING_FAILED %s", e)
            raise RuntimeError(StatusCode.DB_CONNECTION_FAILED) from e

    # ------------------------------------------------------------
    # EXECUTION
    # ------------------------------------------------------------
    def execute(self, sql: str, params: Tuple[Any, ...] = ()) -> None:
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql, params)
        except pyodbc.IntegrityError as e:
            logging.error("DB_CONSTRAINT_VIOLATION %s", e)
            raise RuntimeError(StatusCode.DB_CONSTRAINT_VIOLATION) from e
        except pyodbc.ProgrammingError as e:
            logging.error("DB_SCHEMA_MISMATCH %s", e)
            raise RuntimeError(StatusCode.DB_SCHEMA_MISMATCH) from e
        except pyodbc.Error as e:
            logging.error("DB_EXECUTION_FAILED %s", e)
            raise RuntimeError(StatusCode.DB_INSERT_FAILED) from e

    def executemany(self, sql: str, rows: Iterable[Tuple[Any, ...]]) -> None:
        try:
            cursor = self.connection.cursor()
            cursor.fast_executemany = True
            cursor.executemany(sql, rows)
        except Exception as e:
            logging.error("DB_BATCH_INSERT_FAILED %s", e)
            raise RuntimeError(StatusCode.DB_TRANSACTION_FAILED) from e

    def scalar(self, sql: str, params: Tuple[Any, ...] = ()) -> Any:
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql, params)
            row = cursor.fetchone()
            return row[0] if row else None
        except Exception as e:
            logging.error("DB_SCALAR_FAILED %s", e)
            raise RuntimeError(StatusCode.DB_QUERY_FAILED) from e

    # ------------------------------------------------------------
    # SCHEMA INSPECTION
    # ------------------------------------------------------------
    def table_exists(self, table: str, schema: str = "dbo") -> bool:
        sql = """
        SELECT COUNT(1)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ?
          AND TABLE_NAME = ?
        """
        result = self.scalar(sql, (schema, table))
        return bool(result)

    # ------------------------------------------------------------
    # TRANSACTIONS
    # ------------------------------------------------------------
    def commit(self) -> None:
        try:
            self.connection.commit()
        except Exception as e:
            logging.error("DB_COMMIT_FAILED %s", e)
            raise RuntimeError(StatusCode.DB_TRANSACTION_FAILED) from e

    def rollback(self) -> None:
        try:
            self.connection.rollback()
        except Exception as e:
            logging.critical("DB_ROLLBACK_FAILED %s", e)
            raise RuntimeError(StatusCode.DB_TRANSACTION_FAILED) from e

    # ------------------------------------------------------------
    # SHUTDOWN
    # ------------------------------------------------------------
    def close(self) -> None:
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            logging.error("DB_CLOSE_FAILED %s", e)
