#!/usr/bin/env python3
"""
File: core/database_interface.py

Purpose:
    Define the canonical synchronous database contract used by the BitSight
    SDK, ingestion engine, and command-line interface.

Responsibilities:
    - Standardize database connection lifecycle behavior.
    - Define connectivity and capability checks.
    - Define single-statement and batch execution operations.
    - Define scalar query behavior.
    - Define explicit transaction control.
    - Prevent backend implementations from exposing incompatible behavior.

Contract requirements:
    - Every database backend MUST implement this interface.
    - Implementations MUST preserve the declared method signatures.
    - Implementations MUST NOT narrow declared return types.
    - Implementations MUST NOT introduce hidden optional behavior.
    - Implementations MUST raise exceptions when operations fail.
    - All methods are synchronous.
    - Operations must behave deterministically for the same backend state and
      supplied arguments.

Implementation guidance:
    Backend-specific exceptions may be raised directly or wrapped by the
    implementation layer. This interface does not define a shared database
    exception hierarchy.

    Connection state management is the responsibility of each implementation.
    Methods that require an active connection should fail clearly when no
    usable connection exists.

Security:
    SQL statements and parameters may contain sensitive information.
    Implementations must not log credentials, connection strings, tokens,
    secrets, or unredacted parameter values without an explicit secure logging
    policy.
"""

from abc import ABC, abstractmethod
from typing import Any, Iterable, Optional, Tuple


class DatabaseInterface(ABC):
    """
    Canonical database interface for all supported persistence backends.

    The BitSight ingestion engine and CLI depend exclusively on this contract
    rather than backend-specific connection objects or database drivers.

    Implementations must provide concrete behavior for every abstract method.
    Additional backend-specific methods may be added, but core application
    logic must not require them.
    """

    # -----------------------------------------------------------------------
    # Connection lifecycle
    # -----------------------------------------------------------------------

    @abstractmethod
    def connect(self) -> None:
        """
        Establish the backend database connection.

        Implementations should initialize the underlying driver connection and
        leave the backend ready to execute statements.

        Returns:
            None.

        Raises:
            Exception:
                A backend-specific connection or authentication exception when
                the connection cannot be established.

        Contract:
            - Must not silently suppress connection failures.
            - Must not return a boolean success indicator.
            - Repeated calls must have documented backend behavior.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """
        Close the active database connection and release associated resources.

        Returns:
            None.

        Raises:
            Exception:
                A backend-specific exception when resource cleanup fails.

        Contract:
            - Must be safe to call more than once.
            - Must tolerate an already-closed or never-opened connection.
            - Must release cursors, transactions, and driver resources owned by
              the implementation where applicable.
        """
        raise NotImplementedError

    # -----------------------------------------------------------------------
    # Health and capability checks
    # -----------------------------------------------------------------------

    @abstractmethod
    def ping(self) -> None:
        """
        Verify that the backend connection is usable.

        The implementation should perform the smallest reliable operation
        supported by the backend, such as executing a lightweight validation
        query or invoking a driver-native health check.

        Returns:
            None.

        Raises:
            Exception:
                A backend-specific exception when connectivity validation fails.

        Contract:
            - Must raise on failure.
            - Must not return a boolean.
            - Must verify actual backend usability rather than only checking
              whether a connection object exists.
        """
        raise NotImplementedError

    @abstractmethod
    def table_exists(self, table: str, schema: str = "dbo") -> bool:
        """
        Determine whether a table exists in the specified schema.

        Args:
            table:
                Unqualified table name.

            schema:
                Database schema containing the table. Defaults to "dbo".

        Returns:
            True when the specified table exists; otherwise False.

        Raises:
            Exception:
                A backend-specific exception when metadata inspection fails.

        Contract:
            - Schema handling must be explicitly supported.
            - Implementations must not ignore the supplied schema.
            - Identifier comparison behavior should match the backend's
              documented case-sensitivity and collation rules.
            - Callers must not pass user-controlled identifiers without
              appropriate validation.
        """
        raise NotImplementedError

    # -----------------------------------------------------------------------
    # Statement execution
    # -----------------------------------------------------------------------

    @abstractmethod
    def execute(
        self,
        sql: str,
        params: Tuple[Any, ...] = (),
    ) -> None:
        """
        Execute one SQL statement with optional positional parameters.

        Args:
            sql:
                SQL statement using the placeholder syntax required by the
                backend driver.

            params:
                Positional parameter values bound by the backend driver.

        Returns:
            None.

        Raises:
            Exception:
                A backend-specific exception when statement preparation,
                parameter binding, or execution fails.

        Contract:
            - Must execute exactly one statement operation.
            - Must use driver-supported parameter binding.
            - Must not construct SQL by interpolating parameter values.
            - Must not commit implicitly unless the backend implementation
              explicitly documents and consistently enforces autocommit.
            - Must raise on failure.
        """
        raise NotImplementedError

    @abstractmethod
    def executemany(
        self,
        sql: str,
        rows: Iterable[Tuple[Any, ...]],
    ) -> None:
        """
        Execute one parameterized SQL statement for multiple parameter rows.

        Args:
            sql:
                Parameterized SQL statement using the placeholder syntax
                required by the backend driver.

            rows:
                Iterable containing one positional parameter tuple per
                execution.

        Returns:
            None.

        Raises:
            Exception:
                A backend-specific exception when batch preparation, parameter
                binding, or execution fails.

        Contract:
            - Must preserve input row ordering where the backend supports it.
            - Must not silently discard invalid rows.
            - Must not interpolate values directly into SQL.
            - Must not commit implicitly unless autocommit behavior is an
              explicit property of the implementation.
            - Must raise on failure.
        """
        raise NotImplementedError

    @abstractmethod
    def scalar(
        self,
        sql: str,
        params: Tuple[Any, ...] = (),
    ) -> Optional[Any]:
        """
        Execute a query and return the first column of the first result row.

        Args:
            sql:
                SQL query using the placeholder syntax required by the backend
                driver.

            params:
                Positional parameter values bound by the backend driver.

        Returns:
            The first column value from the first returned row.

            None when the query produces no rows.

        Raises:
            Exception:
                A backend-specific exception when query preparation, parameter
                binding, execution, or result retrieval fails.

        Contract:
            - Must not return an entire row object.
            - Must not return a collection.
            - Must preserve a database NULL value as None.
            - Must also return None when no rows are produced.
            - Callers requiring distinction between no rows and SQL NULL must
              use a different query or backend-specific operation.
        """
        raise NotImplementedError

    # -----------------------------------------------------------------------
    # Transaction control
    # -----------------------------------------------------------------------

    @abstractmethod
    def commit(self) -> None:
        """
        Commit the current transaction.

        Returns:
            None.

        Raises:
            Exception:
                A backend-specific exception when the transaction cannot be
                committed.

        Contract:
            - Must raise on commit failure.
            - Must not silently roll back after a failed commit.
            - Must not start a new transaction unless that is normal documented
              behavior of the underlying driver.
        """
        raise NotImplementedError

    @abstractmethod
    def rollback(self) -> None:
        """
        Roll back the current transaction.

        Returns:
            None.

        Raises:
            Exception:
                A backend-specific exception when rollback fails.

        Contract:
            - Must raise on rollback failure.
            - Must leave the connection in the most recoverable state supported
              by the backend.
            - Must not suppress the original transaction failure when rollback
              is invoked during exception handling.
        """
        raise NotImplementedError
