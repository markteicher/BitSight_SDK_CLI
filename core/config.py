#!/usr/bin/env python3

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from core.status_codes import StatusCode


class Config:
    """
    Deterministic configuration manager for BitSight SDK + CLI.

    Responsibilities:
      - Load configuration from disk
      - Validate required keys
      - Expose typed accessors
      - Persist updates atomically
      - Never silently mutate state
    """

    DEFAULT_PATH = Path.home() / ".bitsight" / "config.json"

    # ------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------
    def __init__(self, path: Optional[Path] = None):
        self.path = path or self.DEFAULT_PATH
        self._data: Dict[str, Any] = {}

    # ------------------------------------------------------------
    # Load / Save
    # ------------------------------------------------------------
    def load(self) -> None:
        """
        Load configuration from disk.
        """
        if not self.path.exists():
            logging.error("CONFIG_NOT_FOUND path=%s", self.path)
            raise RuntimeError(StatusCode.CONFIG_MISSING)

        try:
            raw = self.path.read_text(encoding="utf-8")
            self._data = json.loads(raw)
        except json.JSONDecodeError as e:
            logging.error("CONFIG_PARSE_ERROR %s", e)
            raise RuntimeError(StatusCode.CONFIG_INVALID) from e
        except Exception as e:
            logging.error("CONFIG_READ_FAILED %s", e)
            raise RuntimeError(StatusCode.CONFIG_UNREADABLE) from e

        logging.info("CONFIG_LOADED path=%s", self.path)

    def save(self) -> None:
        """
        Persist configuration to disk atomically.
        """
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.path.with_suffix(".tmp")

            tmp_path.write_text(
                json.dumps(self._data, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            tmp_path.replace(self.path)

        except Exception as e:
            logging.error("CONFIG_WRITE_FAILED %s", e)
            raise RuntimeError(StatusCode.CONFIG_UNWRITABLE) from e

        logging.info("CONFIG_SAVED path=%s", self.path)

    # ------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------
    def validate(self) -> None:
        """
        Validate presence and structure of required configuration.
        """
        self._require("api")
        self._require("database")

        api = self._data["api"]
        self._require_key(api, "api_key")
        self._require_key(api, "base_url")

        db = self._data["database"]
        self._require_key(db, "backend")

        if db["backend"] == "mssql":
            for k in ("server", "database", "username", "password"):
                self._require_key(db, k)

        logging.info("CONFIG_VALIDATED")

    # ------------------------------------------------------------
    # Mutation (explicit only)
    # ------------------------------------------------------------
    def set(self, section: str, key: str, value: Any) -> None:
        """
        Explicitly set a configuration value.
        """
        if section not in self._data:
            self._data[section] = {}

        self._data[section][key] = value
        logging.debug("CONFIG_SET %s.%s", section, key)

    def reset(self) -> None:
        """
        Clear configuration in memory.
        """
        self._data.clear()
        logging.info("CONFIG_RESET")

    # ------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------
    def api_key(self) -> str:
        return self._get("api", "api_key")

    def base_url(self) -> str:
        return self._get("api", "base_url")

    def proxy(self) -> Optional[Dict[str, str]]:
        return self._data.get("api", {}).get("proxy")

    def database(self) -> Dict[str, Any]:
        return self._data["database"]

    # ------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------
    def _get(self, section: str, key: str) -> Any:
        try:
            return self._data[section][key]
        except KeyError:
            logging.error("CONFIG_KEY_MISSING %s.%s", section, key)
            raise RuntimeError(StatusCode.CONFIG_INVALID)

    def _require(self, section: str) -> None:
        if section not in self._data:
            logging.error("CONFIG_SECTION_MISSING %s", section)
            raise RuntimeError(StatusCode.CONFIG_INVALID)

    @staticmethod
    def _require_key(obj: Dict[str, Any], key: str) -> None:
        if key not in obj or obj[key] in (None, ""):
            logging.error("CONFIG_VALUE_MISSING %s", key)
            raise RuntimeError(StatusCode.CONFIG_INVALID)
