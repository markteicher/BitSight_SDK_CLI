#!/usr/bin/env python3
"""
core/database_interface.py

Database contract for BitSight SDK + CLI.

Rules:
- Interface MUST reflect real backend behavior.
- No narrowing return types in implementations.
- No hidden optional behavior.
- All methods are synchronous and deterministic.
"""

from abc import ABC, abstractmethod
from typing import Iterable, Tuple, Any, Optional


class DatabaseInterface(ABC):
    """
    Canonical database interface.

    All database backends MUST implement this contract.
    The ingestion engine and CLI rely exclusively on it.
    """

    # ------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------
    @abstractmethod
    def connect(self) -> None:
        """
        Establish a database connection.

        Must raise a backend-specific exception on failure.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """
        Close the database connection.

        Must be safe to call multiple times.
        """
        raise NotImplementedError

    # ------------------------------------------------------------
    # Health and capability
    # ------------------------------------------------------------
    @abstractmethod
    def ping(self) -> None:
        """
        Verify database connectivity.

        Must raise on failure.
        Must NOT return a boolean.
        """
        raise NotImplementedError

    @abstractmethod
    def table_exists(self, table: str, schema: str = "dbo") -> bool:
        """
        Return True if the specified table exists.

        Schema must be explicitly supported.
        """
        raise NotImplementedError

    # ------------------------------------------------------------
    # Statement execution
    # ------------------------------------------------------------
    @abstractmethod
    def execute(self, sql: str, params: Tuple[Any, ...] = ()) -> None:
        """
        Execute a single SQL statement.

        Must raise on failure.
        """
        raise NotImplementedError

    @abstractmethod
    def executemany(self, sql: str, rows: Iterable[Tuple[Any, ...]]) -> None:
        """
        Execute a parameterized SQL statement for multiple rows.

        Must raise on failure.
        """
        raise NotImplementedError

    @abstractmethod
    def scalar(self, sql: str, params: Tuple[Any, ...] = ()) -> Optional[Any]:
        """
        Execute a query and return a single scalar value.

        Returns None if no rows are produced.
        """
        raise NotImplementedError

    # ------------------------------------------------------------
    # Transaction control
    # ------------------------------------------------------------
    @abstractmethod
    def commit(self) -> None:
        """
        Commit the current transaction.

        Must raise on failure.
        """
        raise NotImplementedError

    @abstractmethod
    def rollback(self) -> None:
        """
        Roll back the current transaction.

        Must raise on failure.
        """
        raise NotImplementedError
