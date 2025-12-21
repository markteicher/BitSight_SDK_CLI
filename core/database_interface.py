#!/usr/bin/env python3

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, Optional, Sequence


class DatabaseInterface(ABC):
    """
    Database contract used by ingestion modules and CLI.
    """

    @abstractmethod
    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        raise NotImplementedError

    @abstractmethod
    def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        raise NotImplementedError

    @abstractmethod
    def commit(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def rollback(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def ping(self) -> None:
        """
        Must raise on failure.
        """
        raise NotImplementedError

    @abstractmethod
    def scalar(self, sql: str, params: Sequence[Any] = ()) -> Any:
        """
        Execute a query expected to return a single value.
        Must raise if no row is returned.
        """
        raise NotImplementedError

    @abstractmethod
    def table_exists(self, schema: str, table: str) -> bool:
        raise NotImplementedError
