#!/usr/bin/env python3

import json
import os
from dataclasses import dataclass, asdict, replace
from pathlib import Path
from typing import Optional, Dict, Any


DEFAULT_BASE_URL = "https://api.bitsighttech.com"
DEFAULT_TIMEOUT = 60
DEFAULT_CONFIG_DIRNAME = ".bitsight"
DEFAULT_CONFIG_FILENAME = "config.json"
ENV_CONFIG_PATH = "BITSIGHT_CONFIG_PATH"


class ConfigError(Exception):
    """Raised when configuration cannot be loaded, saved, or validated."""


def default_config_path() -> Path:
    env = os.environ.get(ENV_CONFIG_PATH)
    if env:
        return Path(env).expanduser()
    return Path.home() / DEFAULT_CONFIG_DIRNAME / DEFAULT_CONFIG_FILENAME


def _normalize_base_url(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return DEFAULT_BASE_URL
    # Accept both https://api.bitsighttech.com and https://api.bitsighttech.com/
    v = v.rstrip("/")
    return v


@dataclass(frozen=True)
class Config:
    api_key: Optional[str] = None
    base_url: str = DEFAULT_BASE_URL

    proxy_url: Optional[str] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None

    timeout: int = DEFAULT_TIMEOUT

    mssql_server: Optional[str] = None
    mssql_database: Optional[str] = None
    mssql_username: Optional[str] = None
    mssql_password: Optional[str] = None
    mssql_driver: str = "ODBC Driver 18 for SQL Server"
    mssql_encrypt: bool = True
    mssql_trust_cert: bool = False
    mssql_timeout: int = 30

    def to_dict(self, include_secrets: bool = True) -> Dict[str, Any]:
        d = asdict(self)
        if not include_secrets:
            for k in ("api_key", "proxy_password", "mssql_password"):
                if d.get(k):
                    d[k] = "***"
        return d

    def proxies(self) -> Optional[Dict[str, str]]:
        if not self.proxy_url:
            return None
        url = self.proxy_url.strip()
        if not url:
            return None
        return {"http": url, "https": url}

    def validate(self, require_api_key: bool = False) -> None:
        if require_api_key and not (self.api_key and self.api_key.strip()):
            raise ConfigError("Missing api_key")

        if self.timeout <= 0:
            raise ConfigError("timeout must be > 0")

        base = _normalize_base_url(self.base_url)
        if not (base.startswith("http://") or base.startswith("https://")):
            raise ConfigError("base_url must start with http:// or https://")

        if self.proxy_url is not None:
            p = self.proxy_url.strip()
            if p and not (p.startswith("http://") or p.startswith("https://")):
                raise ConfigError("proxy_url must start with http:// or https://")

        # DB config is validated at point-of-use.


class ConfigStore:
    def __init__(self, path: Optional[str] = None):
        self.path = Path(path).expanduser() if path else default_config_path()

    def exists(self) -> bool:
        return self.path.exists()

    def load(self) -> Config:
        if not self.path.exists():
            return Config()

        try:
            raw = self.path.read_text(encoding="utf-8")
        except Exception as e:
            raise ConfigError(f"Unable to read config: {self.path} ({e})") from e

        try:
            data = json.loads(raw) if raw.strip() else {}
        except Exception as e:
            raise ConfigError(f"Invalid JSON in config: {self.path} ({e})") from e

        if not isinstance(data, dict):
            raise ConfigError("Config root must be a JSON object")

        # Backward-compatible: ignore unknown keys.
        kwargs: Dict[str, Any] = {}
        for field in Config.__dataclass_fields__.keys():
            if field in data:
                kwargs[field] = data[field]

        # Normalize.
        if "base_url" in kwargs:
            kwargs["base_url"] = _normalize_base_url(str(kwargs["base_url"]))

        cfg = Config(**kwargs)
        cfg.validate(require_api_key=False)
        return cfg

    def save(self, cfg: Config) -> None:
        cfg = replace(cfg, base_url=_normalize_base_url(cfg.base_url))
        cfg.validate(require_api_key=False)

        self.path.parent.mkdir(parents=True, exist_ok=True)

        payload = json.dumps(cfg.to_dict(include_secrets=True), indent=2, sort_keys=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")

        try:
            tmp.write_text(payload + "\n", encoding="utf-8")
            os.replace(tmp, self.path)
        except Exception as e:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            raise ConfigError(f"Unable to write config: {self.path} ({e})") from e

    def reset(self) -> Config:
        cfg = Config()
        self.save(cfg)
        return cfg

    def clear_keys(self) -> Config:
        cfg = self.load()
        cfg = replace(
            cfg,
            api_key=None,
            proxy_password=None,
            mssql_password=None,
        )
        self.save(cfg)
        return cfg

    def set_fields(self, **updates: Any) -> Config:
        cfg = self.load()
        allowed = set(Config.__dataclass_fields__.keys())
        for k in updates.keys():
            if k not in allowed:
                raise ConfigError(f"Unknown config field: {k}")

        if "base_url" in updates and updates["base_url"] is not None:
            updates["base_url"] = _normalize_base_url(str(updates["base_url"]))

        if "timeout" in updates and updates["timeout"] is not None:
            updates["timeout"] = int(updates["timeout"])

        cfg = replace(cfg, **{k: v for k, v in updates.items() if v is not None})
        self.save(cfg)
        return cfg
