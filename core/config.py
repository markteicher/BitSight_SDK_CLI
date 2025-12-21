#!/usr/bin/env python3

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional, Dict, Any


@dataclass
class ProxyConfig:
    url: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None


@dataclass
class MSSQLConfig:
    server: Optional[str] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    driver: str = "ODBC Driver 18 for SQL Server"
    encrypt: bool = True
    trust_cert: bool = False
    timeout: int = 30


@dataclass
class AppConfig:
    api_key: Optional[str] = None
    base_url: str = "https://api.bitsighttech.com"
    timeout: int = 60
    proxy: ProxyConfig = ProxyConfig()
    db_type: str = "mssql"
    mssql: MSSQLConfig = MSSQLConfig()


class ConfigStore:
    """
    Reads and writes config as JSON.
    """

    def __init__(self, path: Optional[str] = None):
        default_path = Path.home() / ".bitsight" / "config.json"
        self.path = Path(path).expanduser() if path else default_path

    def exists(self) -> bool:
        return self.path.exists()

    def load(self) -> AppConfig:
        if not self.path.exists():
            return AppConfig()

        data = json.loads(self.path.read_text(encoding="utf-8"))
        return self._from_dict(data)

    def save(self, cfg: AppConfig) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = asdict(cfg)
        self.path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def reset(self) -> None:
        if self.path.exists():
            self.path.unlink()

    def clear_keys(self) -> None:
        cfg = self.load()
        cfg.api_key = None
        if cfg.proxy:
            cfg.proxy.password = None
        if cfg.mssql:
            cfg.mssql.password = None
        self.save(cfg)

    @staticmethod
    def validate(cfg: AppConfig) -> None:
        if not cfg.base_url or not cfg.base_url.startswith(("http://", "https://")):
            raise ValueError("base_url invalid")

        if cfg.timeout is not None and (not isinstance(cfg.timeout, int) or cfg.timeout <= 0):
            raise ValueError("timeout invalid")

        if cfg.proxy and cfg.proxy.url:
            if not cfg.proxy.url.startswith(("http://", "https://")):
                raise ValueError("proxy.url invalid")

        if (cfg.db_type or "").strip().lower() == "mssql":
            m = cfg.mssql
            if not (m.server and m.database and m.username and m.password):
                raise ValueError("mssql config missing required fields")

    @staticmethod
    def proxies_dict(cfg: AppConfig) -> Optional[Dict[str, str]]:
        if not cfg.proxy or not cfg.proxy.url:
            return None
        return {"http": cfg.proxy.url, "https": cfg.proxy.url}

    @staticmethod
    def _from_dict(data: Dict[str, Any]) -> AppConfig:
        proxy_in = data.get("proxy") or {}
        mssql_in = data.get("mssql") or {}

        cfg = AppConfig(
            api_key=data.get("api_key"),
            base_url=data.get("base_url") or "https://api.bitsighttech.com",
            timeout=int(data.get("timeout") or 60),
            proxy=ProxyConfig(
                url=proxy_in.get("url"),
                username=proxy_in.get("username"),
                password=proxy_in.get("password"),
            ),
            db_type=data.get("db_type") or "mssql",
            mssql=MSSQLConfig(
                server=mssql_in.get("server"),
                database=mssql_in.get("database"),
                username=mssql_in.get("username"),
                password=mssql_in.get("password"),
                driver=mssql_in.get("driver") or "ODBC Driver 18 for SQL Server",
                encrypt=bool(mssql_in.get("encrypt", True)),
                trust_cert=bool(mssql_in.get("trust_cert", False)),
                timeout=int(mssql_in.get("timeout") or 30),
            ),
        )
        return cfg
