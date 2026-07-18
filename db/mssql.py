#!/usr/bin/env python3
"""
File: db/mssql.py

Purpose:
    Provide the canonical Microsoft SQL Server database adapter for the
    BitSight SDK and command-line interface.

Responsibilities:
    - Establish encrypted pyodbc database connections.
    - Validate database connection configuration.
    - Execute parameterized SQL statements.
    - Execute optimized batch operations.
    - Retrieve scalar query results.
    - Inspect database table existence.
    - Provide explicit transaction control.
    - Map database failures to deterministic StatusCode values.
    - Prevent database credentials from appearing in logs.

Transaction contract:
    - Connections operate with autocommit disabled.
    - execute(), executemany(), and scalar() do not commit automatically.
    - Callers must explicitly invoke commit() after successful writes.
    - executemany() rolls back its active transaction when batch execution
      fails.
    - commit() and rollback() fail explicitly when no active connection exists.
    - close() is idempotent.

Security:
    - Passwords are never logged.
    - Connection-string values are escaped before interpolation.
    - TLS encryption is enabled by default.
    - Server-certificate trust is disabled by default.
    - SQL values should be supplied through parameter placeholders rather than
      interpolated into SQL strings.

Failure contract:
    - Database failures raise MSSQLDatabaseError.
    - MSSQLDatabaseError inherits from RuntimeError for compatibility.
    - Every MSSQLDatabaseError contains a deterministic StatusCode.
    - The original pyodbc exception is preserved through exception chaining.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable, Optional, Tuple

import pyodbc

from core.database_interface import DatabaseInterface
from core.status_codes import StatusCode


logger = logging.getLogger(__name__)


DEFAULT_ODBC_DRIVER = "ODBC Driver 18 for SQL Server"
DEFAULT_CONNECTION_TIMEOUT_SECONDS = 30
DEFAULT_SCHEMA = "dbo"

ParameterTuple = Tuple[Any, ...]
RowIterable = Iterable[Tuple[Any, ...]]


class MSSQLDatabaseError(RuntimeError):
    """
    Canonical Microsoft SQL Server adapter exception.

    Attributes:
        status_code:
            StatusCode describing the database failure.

        sqlstate:
            Optional ODBC SQLSTATE extracted from the underlying pyodbc
            exception.

    Compatibility:
        This exception inherits from RuntimeError so existing callers that
        catch RuntimeError continue to function.
    """

    def __init__(
        self,
        message: str,
        status_code: StatusCode,
        sqlstate: Optional[str] = None,
    ) -> None:
        """
        Initialize a database exception.

        Args:
            message:
                Operational failure description that contains no credentials.

            status_code:
                Deterministic database status code.

            sqlstate:
                Optional five-character ODBC SQLSTATE.

        Raises:
            TypeError:
                If status_code is not a StatusCode.
        """
        if not isinstance(status_code, StatusCode):
            raise TypeError("status_code must be a StatusCode")

        super().__init__(message)

        self.status_code = status_code
        self.sqlstate = sqlstate

    def __str__(self) -> str:
        """
        Return the human-readable error description.

        Returns:
            Exception message without exposing credentials.
        """
        return super().__str__()


class MSSQLDatabase(DatabaseInterface):
    """
    Synchronous Microsoft SQL Server database adapter.

    Guarantees:
        - Deterministic connection establishment.
        - Explicit transaction ownership.
        - Parameterized execution support.
        - Optimized batch insertion through fast_executemany.
        - Idempotent connection shutdown.
        - Deterministic StatusCode mapping.
        - Credential-safe operational logging.

    Lifecycle:
        The constructor validates configuration and immediately establishes the
        database connection. The caller owns the resulting connection and must
        invoke close() when finished.

    Thread safety:
        Instances are not thread-safe. A connection must not be shared across
        concurrent threads without external synchronization.
    """

    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        driver: str = DEFAULT_ODBC_DRIVER,
        encrypt: bool = True,
        trust_cert: bool = False,
        timeout: int = DEFAULT_CONNECTION_TIMEOUT_SECONDS,
    ) -> None:
        """
        Configure and connect the MSSQL adapter.

        Args:
            server:
                SQL Server hostname, address, instance name, or listener.

            database:
                Target database name.

            username:
                SQL Server login username.

            password:
                SQL Server login password.

            driver:
                Installed pyodbc SQL Server driver name.

            encrypt:
                Whether the ODBC connection must use TLS encryption.

            trust_cert:
                Whether the driver may trust the server certificate without
                validating its certificate chain.

            timeout:
                Connection timeout in seconds.

        Raises:
            TypeError:
                If configuration values have invalid types.

            ValueError:
                If required string values are empty or timeout is not positive.

            MSSQLDatabaseError:
                If the connection cannot be established.
        """
        self._validate_non_empty_string(server, "server")
        self._validate_non_empty_string(database, "database")
        self._validate_non_empty_string(username, "username")
        self._validate_non_empty_string(driver, "driver")

        if not isinstance(password, str):
            raise TypeError("password must be a string")

        if not isinstance(encrypt, bool):
            raise TypeError("encrypt must be a boolean")

        if not isinstance(trust_cert, bool):
            raise TypeError("trust_cert must be a boolean")

        if isinstance(timeout, bool) or not isinstance(timeout, int):
            raise TypeError("timeout must be an integer")

        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")

        self.server = server.strip()
        self.database = database.strip()
        self.username = username.strip()
        self.password = password
        self.driver = driver.strip()
        self.encrypt = encrypt
        self.trust_cert = trust_cert
        self.timeout = timeout

        self.connection: Optional[pyodbc.Connection] = None

        self.connect()

    # -----------------------------------------------------------------------
    # Connection management
    # -----------------------------------------------------------------------

    def connect(self) -> None:
        """
        Establish the configured SQL Server connection.

        If a connection is already active, this method returns without creating
        another connection.

        Returns:
            None.

        Raises:
            MSSQLDatabaseError:
                If driver initialization, authentication, or connection
                establishment fails.
        """
        if self.connection is not None:
            return

        connection_string = self._build_connection_string()

        logger.info(
            "Connecting to MSSQL | server=%s database=%s",
            self.server,
            self.database,
        )

        try:
            self.connection = pyodbc.connect(
                connection_string,
                autocommit=False,
                timeout=self.timeout,
            )

        except pyodbc.InterfaceError as exc:
            sqlstate = self._extract_sqlstate(exc)

            logger.exception(
                "MSSQL driver or interface initialization failed | "
                "server=%s database=%s sqlstate=%s",
                self.server,
                self.database,
                sqlstate,
            )

            raise MSSQLDatabaseError(
                "MSSQL driver or interface initialization failed",
                StatusCode.DB_CONNECTION_FAILED,
                sqlstate,
            ) from exc

        except pyodbc.Error as exc:
            sqlstate = self._extract_sqlstate(exc)
            status_code = self._classify_connection_error(sqlstate)

            logger.exception(
                "MSSQL connection failed | server=%s database=%s "
                "sqlstate=%s status=%s",
                self.server,
                self.database,
                sqlstate,
                status_code.name,
            )

            raise MSSQLDatabaseError(
                "MSSQL connection failed",
                status_code,
                sqlstate,
            ) from exc

    def ping(self) -> None:
        """
        Verify that the active database connection can execute a query.

        Returns:
            None.

        Raises:
            MSSQLDatabaseError:
                If no connection is active or the health query fails.
        """
        connection = self._require_connection()

        try:
            cursor = connection.cursor()

            try:
                cursor.execute("SELECT 1")
                row = cursor.fetchone()

                if row is None or row[0] != 1:
                    raise MSSQLDatabaseError(
                        "MSSQL health check returned an unexpected result",
                        StatusCode.DB_CONNECTION_FAILED,
                    )
            finally:
                cursor.close()

        except MSSQLDatabaseError:
            raise

        except pyodbc.Error as exc:
            sqlstate = self._extract_sqlstate(exc)

            logger.exception(
                "MSSQL health check failed | server=%s database=%s "
                "sqlstate=%s",
                self.server,
                self.database,
                sqlstate,
            )

            raise MSSQLDatabaseError(
                "MSSQL health check failed",
                StatusCode.DB_CONNECTION_FAILED,
                sqlstate,
            ) from exc

    def close(self) -> None:
        """
        Close the active database connection.

        The operation is idempotent. Calling close() when no connection exists
        has no effect.

        Returns:
            None.

        Raises:
            MSSQLDatabaseError:
                If the active connection cannot be closed.
        """
        connection = self.connection

        if connection is None:
            return

        try:
            connection.close()

        except pyodbc.Error as exc:
            sqlstate = self._extract_sqlstate(exc)

            logger.exception(
                "Failed to close MSSQL connection | server=%s database=%s "
                "sqlstate=%s",
                self.server,
                self.database,
                sqlstate,
            )

            raise MSSQLDatabaseError(
                "Failed to close MSSQL connection",
                StatusCode.DB_UNKNOWN,
                sqlstate,
            ) from exc

        finally:
            self.connection = None

    # -----------------------------------------------------------------------
    # SQL execution
    # -----------------------------------------------------------------------

    def execute(
        self,
        sql: str,
        params: ParameterTuple = (),
    ) -> None:
        """
        Execute one SQL statement without committing the transaction.

        Args:
            sql:
                SQL statement containing pyodbc parameter placeholders.

            params:
                Positional parameter values corresponding to placeholders.

        Returns:
            None.

        Raises:
            TypeError:
                If sql or params has an invalid type.

            ValueError:
                If sql is empty.

            MSSQLDatabaseError:
                If execution fails.
        """
        self._validate_sql(sql)
        self._validate_params(params)

        connection = self._require_connection()

        try:
            cursor = connection.cursor()

            try:
                cursor.execute(sql, params)
            finally:
                cursor.close()

        except pyodbc.IntegrityError as exc:
            self._raise_operation_error(
                message="MSSQL constraint violation",
                status_code=StatusCode.DB_CONSTRAINT_VIOLATION,
                exception=exc,
            )

        except pyodbc.ProgrammingError as exc:
            self._raise_operation_error(
                message="MSSQL statement or schema mismatch",
                status_code=StatusCode.DB_SCHEMA_MISMATCH,
                exception=exc,
            )

        except pyodbc.Error as exc:
            self._raise_operation_error(
                message="MSSQL statement execution failed",
                status_code=StatusCode.DB_UNKNOWN,
                exception=exc,
            )

    def executemany(
        self,
        sql: str,
        rows: RowIterable,
    ) -> None:
        """
        Execute one parameterized SQL statement for multiple rows.

        The cursor enables pyodbc fast_executemany to reduce round trips and
        improve batch-write throughput.

        Args:
            sql:
                Parameterized SQL statement.

            rows:
                Iterable of parameter tuples.

        Returns:
            None.

        Raises:
            TypeError:
                If sql is invalid or rows is not iterable.

            ValueError:
                If sql is empty.

            MSSQLDatabaseError:
                If batch execution or rollback fails.

        Transaction behavior:
            A failed batch is rolled back immediately to prevent callers from
            continuing with a partially applied transaction.
        """
        self._validate_sql(sql)

        try:
            iterator = iter(rows)
        except TypeError as exc:
            raise TypeError("rows must be iterable") from exc

        connection = self._require_connection()

        try:
            cursor = connection.cursor()

            try:
                cursor.fast_executemany = True
                cursor.executemany(sql, iterator)
            finally:
                cursor.close()

        except pyodbc.IntegrityError as exc:
            self._rollback_after_batch_failure(connection, exc)

            self._raise_operation_error(
                message="MSSQL batch constraint violation",
                status_code=StatusCode.DB_CONSTRAINT_VIOLATION,
                exception=exc,
            )

        except pyodbc.ProgrammingError as exc:
            self._rollback_after_batch_failure(connection, exc)

            self._raise_operation_error(
                message="MSSQL batch statement or schema mismatch",
                status_code=StatusCode.DB_SCHEMA_MISMATCH,
                exception=exc,
            )

        except pyodbc.Error as exc:
            self._rollback_after_batch_failure(connection, exc)

            self._raise_operation_error(
                message="MSSQL batch execution failed",
                status_code=StatusCode.DB_TRANSACTION_FAILED,
                exception=exc,
            )

    def scalar(
        self,
        sql: str,
        params: ParameterTuple = (),
    ) -> Any:
        """
        Execute a query and return the first column of the first row.

        Args:
            sql:
                SQL query containing optional pyodbc placeholders.

            params:
                Positional parameter values corresponding to placeholders.

        Returns:
            The first column from the first returned row, or None when the query
            returns no rows.

        Raises:
            TypeError:
                If sql or params has an invalid type.

            ValueError:
                If sql is empty.

            MSSQLDatabaseError:
                If query execution fails.
        """
        self._validate_sql(sql)
        self._validate_params(params)

        connection = self._require_connection()

        try:
            cursor = connection.cursor()

            try:
                cursor.execute(sql, params)
                row = cursor.fetchone()
                return row[0] if row is not None else None
            finally:
                cursor.close()

        except pyodbc.ProgrammingError as exc:
            self._raise_operation_error(
                message="MSSQL scalar query schema mismatch",
                status_code=StatusCode.DB_SCHEMA_MISMATCH,
                exception=exc,
            )

        except pyodbc.Error as exc:
            self._raise_operation_error(
                message="MSSQL scalar query failed",
                status_code=StatusCode.DB_UNKNOWN,
                exception=exc,
            )

    # -----------------------------------------------------------------------
    # Schema inspection
    # -----------------------------------------------------------------------

    def table_exists(
        self,
        table: str,
        schema: str = DEFAULT_SCHEMA,
    ) -> bool:
        """
        Determine whether a base table exists.

        Args:
            table:
                Table name.

            schema:
                Database schema name.

        Returns:
            True when the table exists; otherwise False.

        Raises:
            TypeError:
                If table or schema is not a string.

            ValueError:
                If table or schema is empty.

            MSSQLDatabaseError:
                If the metadata query fails.
        """
        self._validate_non_empty_string(table, "table")
        self._validate_non_empty_string(schema, "schema")

        sql = """
            SELECT COUNT_BIG(1)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME = ?
              AND TABLE_TYPE = 'BASE TABLE'
        """

        result = self.scalar(
            sql,
            (
                schema.strip(),
                table.strip(),
            ),
        )

        return bool(result)

    # -----------------------------------------------------------------------
    # Transaction control
    # -----------------------------------------------------------------------

    def commit(self) -> None:
        """
        Commit the active database transaction.

        Returns:
            None.

        Raises:
            MSSQLDatabaseError:
                If no connection is active or the commit fails.
        """
        connection = self._require_connection()

        try:
            connection.commit()

        except pyodbc.Error as exc:
            sqlstate = self._extract_sqlstate(exc)

            logger.exception(
                "MSSQL commit failed | server=%s database=%s sqlstate=%s",
                self.server,
                self.database,
                sqlstate,
            )

            raise MSSQLDatabaseError(
                "MSSQL transaction commit failed",
                StatusCode.DB_TRANSACTION_FAILED,
                sqlstate,
            ) from exc

    def rollback(self) -> None:
        """
        Roll back the active database transaction.

        Returns:
            None.

        Raises:
            MSSQLDatabaseError:
                If no connection is active or rollback fails.
        """
        connection = self._require_connection()

        try:
            connection.rollback()

        except pyodbc.Error as exc:
            sqlstate = self._extract_sqlstate(exc)

            logger.critical(
                "MSSQL rollback failed | server=%s database=%s sqlstate=%s",
                self.server,
                self.database,
                sqlstate,
                exc_info=True,
            )

            raise MSSQLDatabaseError(
                "MSSQL transaction rollback failed",
                StatusCode.DB_TRANSACTION_FAILED,
                sqlstate,
            ) from exc

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _require_connection(self) -> pyodbc.Connection:
        """
        Return the active connection or fail deterministically.

        Returns:
            Active pyodbc connection.

        Raises:
            MSSQLDatabaseError:
                If no active connection exists.
        """
        if self.connection is None:
            logger.error(
                "MSSQL connection is not active | server=%s database=%s",
                self.server,
                self.database,
            )

            raise MSSQLDatabaseError(
                "MSSQL connection is not active",
                StatusCode.DB_CONNECTION_FAILED,
            )

        return self.connection

    def _build_connection_string(self) -> str:
        """
        Build the escaped ODBC connection string.

        Returns:
            Credential-bearing connection string suitable for pyodbc.connect.

        Security:
            The returned value must never be logged.
        """
        return (
            f"DRIVER={self._escape_odbc_value(self.driver)};"
            f"SERVER={self._escape_odbc_value(self.server)};"
            f"DATABASE={self._escape_odbc_value(self.database)};"
            f"UID={self._escape_odbc_value(self.username)};"
            f"PWD={self._escape_odbc_value(self.password)};"
            f"Encrypt={'yes' if self.encrypt else 'no'};"
            f"TrustServerCertificate={'yes' if self.trust_cert else 'no'};"
            f"Connection Timeout={self.timeout};"
        )

    def _rollback_after_batch_failure(
        self,
        connection: pyodbc.Connection,
        original_exception: BaseException,
    ) -> None:
        """
        Roll back a failed batch transaction.

        Args:
            connection:
                Active database connection.

            original_exception:
                Exception that caused batch execution to fail.

        Raises:
            MSSQLDatabaseError:
                If rollback also fails.
        """
        try:
            connection.rollback()

        except pyodbc.Error as rollback_exception:
            sqlstate = self._extract_sqlstate(rollback_exception)

            logger.critical(
                "MSSQL rollback after batch failure failed | "
                "server=%s database=%s sqlstate=%s",
                self.server,
                self.database,
                sqlstate,
                exc_info=True,
            )

            raise MSSQLDatabaseError(
                "MSSQL batch failed and transaction rollback also failed",
                StatusCode.DB_TRANSACTION_FAILED,
                sqlstate,
            ) from original_exception

    def _raise_operation_error(
        self,
        message: str,
        status_code: StatusCode,
        exception: pyodbc.Error,
    ) -> None:
        """
        Log and raise a deterministic database operation failure.

        Args:
            message:
                Credential-safe failure description.

            status_code:
                Status code assigned to the failure.

            exception:
                Original pyodbc exception.

        Raises:
            MSSQLDatabaseError:
                Always.
        """
        sqlstate = self._extract_sqlstate(exception)

        logger.exception(
            "%s | server=%s database=%s sqlstate=%s status=%s",
            message,
            self.server,
            self.database,
            sqlstate,
            status_code.name,
        )

        raise MSSQLDatabaseError(
            message,
            status_code,
            sqlstate,
        ) from exception

    @staticmethod
    def _classify_connection_error(
        sqlstate: Optional[str],
    ) -> StatusCode:
        """
        Map a connection SQLSTATE to a deterministic status code.

        Args:
            sqlstate:
                ODBC SQLSTATE or None.

        Returns:
            DB_AUTH_FAILED for authentication or authorization SQLSTATE values;
            otherwise DB_CONNECTION_FAILED.
        """
        if sqlstate in {
            "28000",  # Invalid authorization specification
            "42000",  # Syntax/access violation; frequently returned for login
        }:
            return StatusCode.DB_AUTH_FAILED

        return StatusCode.DB_CONNECTION_FAILED

    @staticmethod
    def _extract_sqlstate(
        exception: BaseException,
    ) -> Optional[str]:
        """
        Extract an ODBC SQLSTATE from a pyodbc exception.

        Args:
            exception:
                Exception returned by pyodbc.

        Returns:
            SQLSTATE string when available; otherwise None.
        """
        if not exception.args:
            return None

        first_argument = exception.args[0]

        if isinstance(first_argument, str) and len(first_argument) >= 5:
            candidate = first_argument[:5]

            if candidate.isalnum():
                return candidate

        return None

    @staticmethod
    def _escape_odbc_value(value: str) -> str:
        """
        Escape one ODBC connection-string value.

        Args:
            value:
                Raw connection-string value.

        Returns:
            Brace-delimited value with closing braces escaped according to ODBC
            connection-string rules.
        """
        return "{" + value.replace("}", "}}") + "}"

    @staticmethod
    def _validate_non_empty_string(
        value: str,
        field_name: str,
    ) -> None:
        """
        Validate a required string.

        Args:
            value:
                Value to validate.

            field_name:
                Argument name used in validation errors.

        Raises:
            TypeError:
                If value is not a string.

            ValueError:
                If value is empty or contains only whitespace.
        """
        if not isinstance(value, str):
            raise TypeError(f"{field_name} must be a string")

        if not value.strip():
            raise ValueError(f"{field_name} must not be empty")

    @staticmethod
    def _validate_sql(sql: str) -> None:
        """
        Validate a SQL statement.

        Args:
            sql:
                SQL statement to validate.

        Raises:
            TypeError:
                If sql is not a string.

            ValueError:
                If sql is empty.
        """
        if not isinstance(sql, str):
            raise TypeError("sql must be a string")

        if not sql.strip():
            raise ValueError("sql must not be empty")

    @staticmethod
    def _validate_params(params: ParameterTuple) -> None:
        """
        Validate SQL parameter values.

        Args:
            params:
                Parameter tuple to validate.

        Raises:
            TypeError:
                If params is not a tuple.
        """
        if not isinstance(params, tuple):
            raise TypeError("params must be a tuple")
