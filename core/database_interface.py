#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Iterable, Tuple, Any, Optional


class DatabaseInterface(ABC):
    """
    Database contract for BitSight SDK + CLI.

    Any supported database backend MUST implement this interface.
    The CLI, ingestion engine, and db tooling rely exclusively on
    these methods.
    """

    # ------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------
    @abstractmethod
    def connect(self) -> None:
        """
        Establish a database connection.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """
        Close the database connection.
        """
        raise NotImplementedError

    # ------------------------------------------------------------
    # Health and capability
    # ------------------------------------------------------------
    @abstractmethod
    def ping(self) -> bool:
        """
        Return True if the database is reachable.
        """
        raise NotImplementedError

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """
        Return True if the specified table exists.
        """
        raise NotImplementedError

    # ------------------------------------------------------------
    # Statement execution
    # ------------------------------------------------------------
    @abstractmethod
    def execute(self, sql: str, params: Tuple[Any, ...] = ()) -> None:
        """
        Execute a single SQL statement.
        """
        raise NotImplementedError

    @abstractmethod
    def executemany(self, sql: str, rows: Iterable[Tuple[Any, ...]]) -> None:
        """
        Execute a parameterized SQL statement for multiple rows.
        """
        raise NotImplementedError

    @abstractmethod
    def scalar(self, sql: str, params: Tuple[Any, ...] = ()) -> Optional[Any]:
        """
        Execute a query and return a single scalar value.
        """
        raise NotImplementedError

    # ------------------------------------------------------------
    # Transaction control
    # ------------------------------------------------------------
    @abstractmethod
    def commit(self) -> None:
        """
        Commit the current transaction.
        """
        raise NotImplementedError

    @abstractmethod
    def rollback(self) -> None:
        """
        Roll back the current transaction.
        """
        raise NotImplementedError
