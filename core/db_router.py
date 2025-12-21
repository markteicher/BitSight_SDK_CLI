#!/usr/bin/env python3

from typing import Optional

from core.database_interface import DatabaseInterface


class DatabaseRouter:
    """
    Database router for BitSight SDK + CLI.

    Resolves the active database backend and returns a concrete
    DatabaseInterface implementation.
    """

    @staticmethod
    def get_database(
        *,
        backend: str,
        server: Optional[str] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs,
    ) -> DatabaseInterface:
        """
        Return a database implementation for the requested backend.
        """

        backend = backend.lower().strip()

        if backend == "mssql":
            from db.mssql import MSSQLDatabase

            return MSSQLDatabase(
                server=server,
                database=database,
                username=username,
                password=password,
                **kwargs,
            )

        raise ValueError(f"Unsupported database backend: {backend}")
