#!/usr/bin/env python3

import logging
import os
from pathlib import Path

from db.mssql import MSSQLDatabase


class MSSQLInitializer:
    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        schema_path: str,
    ):
        self.schema_path = Path(schema_path)

        if not self.schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")

        self.db = MSSQLDatabase(
            server=server,
            database=database,
            username=username,
            password=password,
        )

    def run(self) -> None:
        logging.info("Initializing MSSQL BitSight schema")

        sql = self.schema_path.read_text(encoding="utf-8")

        try:
            self.db.execute(sql)
            self.db.commit()
            logging.info("MSSQL schema initialized successfully")

        except Exception:
            self.db.rollback()
            logging.exception("MSSQL schema initialization failed — rolled back")
            raise

        finally:
            self.db.close()
