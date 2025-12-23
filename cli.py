#!/usr/bin/env python3
"""
cli.py

BitSight SDK + CLI entrypoint (bitsight-cli)

Responsibilities:
- Argument parsing
- Logging setup (text / JSON)
- Config load + override precedence
- Transport preflight validation
- Ingest dispatch
- DB init dispatch
- Single authoritative ExitCode emission
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys
from typing import Any, Dict, Optional

from core.config import ConfigStore, Config, ConfigError
from core.exit_codes import ExitCode
from core.status_codes import StatusCode
from core.transport import (
    TransportConfig,
    TransportError,
    build_session,
    validate_bitsight_api,
)

# ============================================================
# Logging
# ============================================================

class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "timestamp": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
                "level": record.levelname,
                "message": record.getMessage(),
                "logger": record.name,
            }
        )


def setup_logging(verbose: bool, json_logs: bool) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        JsonLogFormatter()
        if json_logs
        else logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG if verbose else logging.INFO)


# ============================================================
# Helpers
# ============================================================

def _exit(code: ExitCode) -> None:
    sys.exit(int(code))


def _merge_config(file_cfg: Config, args: argparse.Namespace) -> Config:
    """
    CLI args override config file values.
    """
    updates: Dict[str, Any] = {}

    for field in (
        "api_key",
        "base_url",
        "proxy_url",
        "proxy_username",
        "proxy_password",
        "timeout",
        "mssql_server",
        "mssql_database",
        "mssql_username",
        "mssql_password",
        "mssql_driver",
        "mssql_encrypt",
        "mssql_trust_cert",
        "mssql_timeout",
    ):
        if hasattr(args, field):
            value = getattr(args, field)
            if value is not None:
                updates[field] = value

    return Config(**{**file_cfg.__dict__, **updates}) if updates else file_cfg


def _transport_cfg(cfg: Config) -> TransportConfig:
    return TransportConfig(
        base_url=cfg.base_url,
        api_key=cfg.api_key or "",
        timeout=cfg.timeout,
        proxy_url=cfg.proxy_url,
        proxy_username=cfg.proxy_username,
        proxy_password=cfg.proxy_password,
        verify_ssl=True,
    )


def _ingest_module_name(name: str) -> str:
    return name.replace("-", "_")


# ============================================================
# DB args
# ============================================================

def _add_db_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--server", dest="mssql_server")
    p.add_argument("--database", dest="mssql_database")
    p.add_argument("--username", dest="mssql_username")
    p.add_argument("--password", dest="mssql_password")
    p.add_argument("--driver", dest="mssql_driver")
    p.add_argument("--encrypt", dest="mssql_encrypt", action="store_true")
    p.add_argument("--no-encrypt", dest="mssql_encrypt", action="store_false")
    p.set_defaults(mssql_encrypt=None)
    p.add_argument("--trust-cert", dest="mssql_trust_cert", action="store_true")
    p.add_argument("--no-trust-cert", dest="mssql_trust_cert", action="store_false")
    p.set_defaults(mssql_trust_cert=None)
    p.add_argument("--db-timeout", dest="mssql_timeout", type=int)


# ============================================================
# Ingest dispatch
# ============================================================

def dispatch_ingest(subcommand: str, args: argparse.Namespace) -> ExitCode:
    module_name = _ingest_module_name(subcommand)
    module_path = f"ingest.{module_name}"

    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError:
        logging.error("Missing ingest module: %s.py", module_name)
        return ExitCode.INTERNAL_HANDLER_MISSING

    # function entrypoints
    for name in ("main", "run", "cli"):
        fn = getattr(module, name, None)
        if callable(fn):
            result = fn(args)
            return getattr(result, "exit_code", ExitCode.SUCCESS)

    # class entrypoints
    for attr in dir(module):
        if attr.endswith("Ingest"):
            cls = getattr(module, attr)
            if isinstance(cls, type) and hasattr(cls, "run"):
                result = cls().run()
                return getattr(result, "exit_code", ExitCode.SUCCESS)

    logging.error("No valid entrypoint found for ingest module %s", module_name)
    return ExitCode.INTERNAL_HANDLER_MISSING


# ============================================================
# Main
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="bitsight-cli",
        description="BitSight SDK + CLI",
    )

    # global flags
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--json-logs", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument("--api-key")
    parser.add_argument("--base-url")
    parser.add_argument("--proxy-url")
    parser.add_argument("--proxy-username")
    parser.add_argument("--proxy-password")
    parser.add_argument("--timeout", type=int)

    subparsers = parser.add_subparsers(dest="command")

    # exit aliases
    for name in ("exit", "quit", "q", "x"):
        subparsers.add_parser(name)

    # --------------------------------------------------------
    # config
    # --------------------------------------------------------
    cfg_p = subparsers.add_parser("config")
    cfg_sub = cfg_p.add_subparsers(dest="subcommand", required=True)

    for name in ("init", "show", "validate", "reset", "clear-keys"):
        cfg_sub.add_parser(name)

    cfg_set = cfg_sub.add_parser("set")
    cfg_set.add_argument("--api-key")
    cfg_set.add_argument("--base-url")
    cfg_set.add_argument("--proxy-url")
    cfg_set.add_argument("--proxy-username")
    cfg_set.add_argument("--proxy-password")
    cfg_set.add_argument("--timeout", type=int)

    # --------------------------------------------------------
    # db
    # --------------------------------------------------------
    db_p = subparsers.add_parser("db")
    db_sub = db_p.add_subparsers(dest="subcommand", required=True)

    db_init = db_sub.add_parser("init")
    _add_db_args(db_init)
    db_init.add_argument("--schema-path", default="db/schema/mssql.sql")
    db_init.add_argument("--stamp-schema", action="store_true")

    db_sub.add_parser("status")

    # --------------------------------------------------------
    # ingest
    # --------------------------------------------------------
    ingest_p = subparsers.add_parser("ingest")
    ingest_sub = ingest_p.add_subparsers(dest="subcommand", required=True)

    def ingest_cmd(name: str, extra=None):
        p = ingest_sub.add_parser(name)
        if extra:
            for spec in extra:
                p.add_argument(*spec[0], **spec[1])
        p.add_argument("--flush", action="store_true")
        return p

    ingest_cmd("users")
    ingest_cmd("user-details", [(["--user-guid"], {"required": True})])
    ingest_cmd("user-quota")
    ingest_cmd("user-company-views")

    ingest_cmd("companies")
    ingest_cmd("company-details", [(["--company-guid"], {"required": True})])
    ingest_cmd("portfolio")
    ingest_cmd("current-ratings")
    ingest_cmd("current-ratings-v2")
    ingest_cmd("ratings-history", [(["--company-guid"], {"required": True})])
    ingest_cmd("findings", [(["--company-guid"], {"required": True})])
    ingest_cmd("observations", [(["--company-guid"], {"required": True})])

    ingest_cmd("threats")
    ingest_cmd("threat-statistics")
    ingest_cmd("threats-impact", [(["--threat-guid"], {"required": True})])
    ingest_cmd(
        "threats-evidence",
        [
            (["--threat-guid"], {"required": True}),
            (["--entity-guid"], {"required": True}),
        ],
    )

    # ========================================================
    # Execute
    # ========================================================
    args = parser.parse_args()
    setup_logging(args.verbose, args.json_logs)

    if args.command in ("exit", "quit", "q", "x"):
        logging.info("Operator exit")
        _exit(ExitCode.SUCCESS_OPERATOR_EXIT)

    if args.command is None:
        parser.print_help()
        _exit(ExitCode.CLI_HELP_REQUESTED)

    store = ConfigStore()
    try:
        file_cfg = store.load()
        cfg = _merge_config(file_cfg, args)
    except ConfigError as e:
        logging.error(str(e))
        _exit(ExitCode.CONFIG_FILE_INVALID)

    try:
        # ---------------- config ----------------
        if args.command == "config":
            if args.subcommand == "init":
                store.reset()
                _exit(ExitCode.SUCCESS)

            if args.subcommand == "show":
                logging.info(json.dumps(cfg.to_dict(include_secrets=False), indent=2))
                _exit(ExitCode.SUCCESS)

            if args.subcommand == "validate":
                cfg.validate()
                _exit(ExitCode.SUCCESS_VALIDATION_OK)

            if args.subcommand == "reset":
                store.reset()
                _exit(ExitCode.SUCCESS)

            if args.subcommand == "clear-keys":
                store.clear_keys()
                _exit(ExitCode.SUCCESS)

            if args.subcommand == "set":
                store.set_fields(
                    api_key=args.api_key,
                    base_url=args.base_url,
                    proxy_url=args.proxy_url,
                    proxy_username=args.proxy_username,
                    proxy_password=args.proxy_password,
                    timeout=args.timeout,
                )
                _exit(ExitCode.SUCCESS)

        # ---------------- db ----------------
        if args.command == "db":
            if args.subcommand == "init":
                from db.init import MSSQLInitializer

                cfg.validate(require_api_key=False)

                if not all(
                    [
                        cfg.mssql_server,
                        cfg.mssql_database,
                        cfg.mssql_username,
                        cfg.mssql_password,
                    ]
                ):
                    logging.error("Missing MSSQL connection parameters")
                    _exit(ExitCode.CONFIG_VALUE_MISSING)

                init = MSSQLInitializer(
                    server=cfg.mssql_server,
                    database=cfg.mssql_database,
                    username=cfg.mssql_username,
                    password=cfg.mssql_password,
                    schema_path=args.schema_path,
                )

                if hasattr(init, "stamp_schema"):
                    init.stamp_schema = bool(args.stamp_schema)

                init.run()
                _exit(ExitCode.SUCCESS)

            _exit(ExitCode.EXECUTION_NOT_IMPLEMENTED)

        # ---------------- ingest ----------------
        if args.command == "ingest":
            cfg.validate(require_api_key=True)
            tcfg = _transport_cfg(cfg)
            session, proxies = build_session(tcfg)
            validate_bitsight_api(session, tcfg, proxies)

            exit_code = dispatch_ingest(args.subcommand, args)
            _exit(exit_code)

        _exit(ExitCode.CLI_INVALID_COMMAND)

    except TransportError as e:
        logging.error(str(e))
        _exit(ExitCode.API_UNKNOWN_ERROR)

    except KeyboardInterrupt:
        logging.info("Interrupted")
        _exit(ExitCode.SUCCESS_OPERATOR_EXIT)

    except Exception:
        logging.exception("Unhandled exception")
        _exit(ExitCode.RUNTIME_EXCEPTION)


if __name__ == "__main__":
    main()
