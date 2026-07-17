#!/usr/bin/env python3
"""
File: core/database_router.py

Purpose:
    Resolve the configured database backend and return a concrete
    DatabaseInterface implementation for the BitSight SDK and CLI.

Responsibilities:
    - Normalize the requested backend name.
    - Validate required backend configuration.
    - Import backend implementations only when selected.
    - Instantiate and return a concrete DatabaseInterface implementation.
    - Reject unsupported or empty backend names with clear exceptions.

Supported backends:
    - mssql

Design:
    Backend imports are intentionally deferred until runtime. This prevents
    optional database driver dependencies from being imported when their
    backend is not selected.

Security:
    Credentials passed into this router must not be logged, printed, or exposed
    in exception messages. Backend implementations are responsible for secure
    connection handling and secret protection.
"""

from typing import Any, Optional

from core.database_interface import DatabaseInterface


class DatabaseRouter:
    """
    Factory-style router for supported database backends.

    The router converts a backend identifier and connection settings into a
    concrete implementation of DatabaseInterface.

    The router does not open the database connection. Callers remain
    responsible for invoking connect() on the returned implementation.
    """

    @staticmethod
    def get_database(
        *,
        backend: str,
        server: Optional[str] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs: Any,
    ) -> DatabaseInterface:
        """
        Return a database implementation for the requested backend.

        Args:
            backend:
                Backend identifier. Currently supported value: "mssql".

            server:
                Database server hostname, address, or instance name.

            database:
                Target database name.

            username:
                Optional database username.

            password:
                Optional database password.

            **kwargs:
                Additional backend-specific constructor arguments.

        Returns:
            Concrete DatabaseInterface implementation.

        Raises:
            TypeError:
                If backend is not a string.

            ValueError:
                If backend is empty, unsupported, or required backend
                configuration is missing.

            ImportError:
                If the selected backend implementation or required driver
                dependency cannot be imported.

        Contract:
            - Must return an object implementing DatabaseInterface.
            - Must not establish the database connection.
            - Must not log or expose credentials.
            - Must reject unsupported backend names.
            - Must preserve backend-specific keyword arguments.
        """
        if not isinstance(backend, str):
            raise TypeError("backend must be a string")

        normalized_backend = backend.strip().lower()

        if not normalized_backend:
            raise ValueError("backend must not be empty")

        if normalized_backend == "mssql":
            if not server or not server.strip():
                raise ValueError(
                    "server is required for the mssql backend"
                )

            if not database or not database.strip():
                raise ValueError(
                    "database is required for the mssql backend"
                )

            # Deferred import keeps optional MSSQL dependencies isolated from
            # deployments that do not select the MSSQL backend.
            from db.mssql import MSSQLDatabase

            implementation = MSSQLDatabase(
                server=server.strip(),
                database=database.strip(),
                username=username,
                password=password,
                **kwargs,
            )

            if not isinstance(implementation, DatabaseInterface):
                raise TypeError(
                    "MSSQLDatabase must implement DatabaseInterface"
                )

            return implementation

        raise ValueError(
            f"Unsupported database backend: {normalized_backend}"
        )
