#!/usr/bin/env python3

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from tqdm import tqdm

from core.config import Config


# ----------------------------------------------------------------------
# logging
# ----------------------------------------------------------------------
def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ----------------------------------------------------------------------
# config helpers
# ----------------------------------------------------------------------
def _config_init(cfg: Config) -> None:
    """
    Create a deterministic config skeleton on disk.
    Overwrites existing config only if file does not exist.
    """
    if cfg.path.exists():
        logging.info("CONFIG_ALREADY_EXISTS path=%s", cfg.path)
        return

    cfg.reset()
    cfg.set("api", "api_key", "")
    cfg.set("api", "base_url", "https://api.bitsighttech.com")
    cfg.set("api", "proxy", None)
    cfg.set("api", "timeout", 60)

    cfg.set("database", "backend", "mssql")
    cfg.set("database", "server", "")
    cfg.set("database", "database", "")
    cfg.set("database", "username", "")
    cfg.set("database", "password", "")
    cfg.set("database", "driver", "ODBC Driver 18 for SQL Server")
    cfg.set("database", "encrypt", True)
    cfg.set("database", "trust_cert", False)
    cfg.set("database", "timeout", 30)

    cfg.save()
    logging.info("CONFIG_INIT_OK path=%s", cfg.path)


def _config_show(cfg: Config) -> None:
    cfg.load()
    # Redact secrets deterministically
    data = json.loads(json.dumps(cfg._data))  # deep copy without imports
    if isinstance(data.get("api", {}), dict):
        if data["api"].get("api_key"):
            data["api"]["api_key"] = "***REDACTED***"
        proxy = data["api"].get("proxy")
        if isinstance(proxy, dict) and proxy.get("password"):
            proxy["password"] = "***REDACTED***"

    if isinstance(data.get("database", {}), dict) and data["database"].get("password"):
        data["database"]["password"] = "***REDACTED***"

    sys.stdout.write(json.dumps(data, indent=2, sort_keys=True) + "\n")
    logging.info("CONFIG_SHOW_OK path=%s", cfg.path)


def _config_validate(cfg: Config) -> None:
    cfg.load()
    cfg.validate()
    logging.info("CONFIG_VALIDATE_OK")


def _config_reset(cfg: Config) -> None:
    """
    Remove config file from disk (full reset).
    """
    if cfg.path.exists():
        cfg.path.unlink()
        logging.info("CONFIG_RESET_OK path=%s", cfg.path)
    else:
        logging.info("CONFIG_RESET_NOOP path=%s", cfg.path)


def _config_clear_keys(cfg: Config) -> None:
    """
    Clear secret material only (API key, proxy password, DB password).
    """
    cfg.load()

    api = cfg._data.get("api", {})
    if isinstance(api, dict):
        api["api_key"] = ""
        proxy = api.get("proxy")
        if isinstance(proxy, dict):
            proxy["password"] = ""

    db = cfg._data.get("database", {})
    if isinstance(db, dict):
        db["password"] = ""

    cfg.save()
    logging.info("CONFIG_CLEAR_KEYS_OK")


def _config_set(cfg: Config, args: argparse.Namespace) -> None:
    cfg.load()

    if args.api_key is not None:
        cfg.set("api", "api_key", args.api_key)
    if args.base_url is not None:
        cfg.set("api", "base_url", args.base_url)
    if args.timeout is not None:
        cfg.set("api", "timeout", int(args.timeout))

    # proxy: only set if any proxy fields provided
    if (
        args.proxy_url is not None
        or args.proxy_username is not None
        or args.proxy_password is not None
    ):
        proxy: Dict[str, Any] = cfg._data.get("api", {}).get("proxy") or {}
        if args.proxy_url is not None:
            proxy["url"] = args.proxy_url
        if args.proxy_username is not None:
            proxy["username"] = args.proxy_username
        if args.proxy_password is not None:
            proxy["password"] = args.proxy_password
        cfg.set("api", "proxy", proxy)

    cfg.save()
    logging.info("CONFIG_SET_OK")


# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(prog="bitsight", description="BitSight SDK + CLI")

    # global flags
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--no-progress", action="store_true", help="Disable progress bars (CI-safe)"
    )
    parser.add_argument(
        "--config-path",
        default=None,
        help="Override config path (default: ~/.bitsight/config.json)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # ------------------------------------------------------------------
    # config
    # ------------------------------------------------------------------
    config = subparsers.add_parser("config", help="Configuration management")
    config_sub = config.add_subparsers(dest="subcommand", required=True)

    config_sub.add_parser("init")
    config_sub.add_parser("show")
    config_sub.add_parser("validate")
    config_sub.add_parser("reset")
    config_sub.add_parser("clear-keys")

    config_set = config_sub.add_parser("set")
    config_set.add_argument("--api-key")
    config_set.add_argument("--base-url")
    config_set.add_argument("--proxy-url")
    config_set.add_argument("--proxy-username")
    config_set.add_argument("--proxy-password")
    config_set.add_argument("--timeout", type=int)

    # ------------------------------------------------------------------
    # db (unchanged wiring here; will be routed later via DatabaseRouter)
    # ------------------------------------------------------------------
    db = subparsers.add_parser("db", help="Database management")
    db_sub = db.add_subparsers(dest="subcommand", required=True)

    db_init = db_sub.add_parser("init")
    db_init.add_argument("--sqlite")
    db_init.add_argument("--mssql", action="store_true")
    db_init.add_argument("--postgres", action="store_true")
    db_init.add_argument("--server")
    db_init.add_argument("--database")
    db_init.add_argument("--username")
    db_init.add_argument("--password")

    db_sub.add_parser("status")

    db_flush = db_sub.add_parser("flush")
    db_flush.add_argument("--table")
    db_flush.add_argument("--all", action="store_true")

    db_sub.add_parser("migrate")

    # ------------------------------------------------------------------
    # ingest (unchanged dispatch for now)
    # ------------------------------------------------------------------
    ingest = subparsers.add_parser("ingest", help="Data ingestion")
    ingest_sub = ingest.add_subparsers(dest="subcommand", required=True)

    def ingest_cmd(name, extra_args=None):
        p = ingest_sub.add_parser(name)
        if extra_args:
            for arg in extra_args:
                p.add_argument(*arg[0], **arg[1])
        p.add_argument("--flush", action="store_true")
        p.set_defaults(handler=name.replace("-", "_"))
        return p

    ingest_cmd("users")
    ingest_cmd("user-details", [(["--user-guid"], {})])
    ingest_cmd("user-quota")
    ingest_cmd("user-company-views")

    ingest_cmd("companies")
    ingest_cmd("company-details", [(["--company-guid"], {})])

    ingest_cmd("portfolio")
    ingest_cmd("current-ratings")
    ingest_cmd("ratings-history", [(["--company-guid"], {}), (["--since"], {})])
    ingest_cmd("findings", [(["--company-guid"], {}), (["--since"], {})])
    ingest_cmd("observations", [(["--company-guid"], {}), (["--since"], {})])

    ingest_cmd("threats")
    ingest_cmd("alerts", [(["--since"], {})])
    ingest_cmd("exposed-credentials")

    # ------------------------------------------------------------------
    # parse + runtime
    # ------------------------------------------------------------------
    args = parser.parse_args()
    setup_logging(args.verbose)
    logging.debug("CLI arguments: %s", vars(args))

    cfg_path = Path(args.config_path) if args.config_path else None
    cfg = Config(path=cfg_path)

    # ------------------------------------------------------------------
    # dispatch: config (wired to core/config.py)
    # ------------------------------------------------------------------
    if args.command == "config":
        if args.subcommand == "init":
            _config_init(cfg)
            return 0
        if args.subcommand == "show":
            _config_show(cfg)
            return 0
        if args.subcommand == "validate":
            _config_validate(cfg)
            return 0
        if args.subcommand == "reset":
            _config_reset(cfg)
            return 0
        if args.subcommand == "clear-keys":
            _config_clear_keys(cfg)
            return 0
        if args.subcommand == "set":
            _config_set(cfg, args)
            return 0

        logging.error("CONFIG_UNHANDLED subcommand=%s", args.subcommand)
        return 1

    # Other command groups remain to be wired next.
    logging.info("COMMAND_ACCEPTED command=%s subcommand=%s", args.command, getattr(args, "subcommand", None))
    return 0


if __name__ == "__main__":
    sys.exit(main())
