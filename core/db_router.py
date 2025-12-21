#!/usr/bin/env python3

import logging
from typing import Optional

from core.status_codes import StatusCode
from core.exit_codes import ExitCode
from core.database_interface import DatabaseInterface

from db.mssql import MSSQLDatabase


class DatabaseRouter:
    """
    Database router responsible for:
      - Selecting the correct database backend
      - Enforcing single active backend
      - Performing connection preflight
      - Returning a concrete DatabaseInterface
    """

    def __init__(
        self,
        backend: str,
        server: Optional[str] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs,
    ):
        self.backend = backend.lower() if backend else None
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.kwargs = kwargs

    # ------------------------------------------------------------
    # Public
    # ------------------------------------------------------------
    def connect(self) -> DatabaseInterface:
        """
        Resolve backend and return a connected DatabaseInterface.
        """
        if not self.backend:
            logging.error("DB_BACKEND_NOT_SPECIFIED")
            raise RuntimeError(StatusCode.DB_BACKEND_NOT_SPECIFIED)

        if self.backend == "mssql":
            return self._connect_mssql()

        logging.error("DB_BACKEND_UNSUPPORTED backend=%s", self.backend)
        raise RuntimeError(StatusCode.DB_BACKEND_UNSUPPORTED)

    # ------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------
    def _connect_mssql(self) -> DatabaseInterface:
        """
        Create and validate MSSQL database connection.
        """

        self._require(self.server, "server")
        self._require(self.database, "database")
        self._require(self.username, "username")
        self._require(self.password, "password")

        logging.info(
            "DB_ROUTER_SELECT backend=mssql server=%s database=%s",
            self.server,
            self.database,
        )

        try:
            db = MSSQLDatabase(
                server=self.server,
                database=self.database,
                username=self.username,
                password=self.password,
                **self.kwargs,
            )
        except Exception as e:
            logging.error("DB_CONNECT_FAILED backend=mssql error=%s", e)
            raise

        # Mandatory preflight checks
        self._preflight(db)

        return db

    # ------------------------------------------------------------
    # Preflight
    # ------------------------------------------------------------
    def _preflight(self, db: DatabaseInterface) -> None:
        """
        Perform mandatory database preflight checks.
        """

        try:
            db.ping()
        except Exception as e:
            logging.error("DB_PREFLIGHT_PING_FAILED %s", e)
            raise RuntimeError(StatusCode.DB_CONNECTION_FAILED) from e

        # Required core tables (non-negotiable)
        required_tables = [
            "bitsight_users",
            "bitsight_companies",
            "bitsight_current_ratings",
            "bitsight_ratings_history",
            "bitsight_findings",
        ]

        for table in required_tables:
            if not db.table_exists(table):
                logging.error("DB_SCHEMA_MISSING table=%s", table)
                raise RuntimeError(StatusCode.DB_SCHEMA_MISSING)

        logging.info("DB_PREFLIGHT_OK")

    # ------------------------------------------------------------
    # Validation Helpers
    # ------------------------------------------------------------
    @staticmethod
    def _require(value: Optional[str], name: str) -> None:
        if not value:
            logging.error("DB_PARAM_MISSING %s", name)
            raise RuntimeError(StatusCode.DB_CONFIGURATION_INVALID)
