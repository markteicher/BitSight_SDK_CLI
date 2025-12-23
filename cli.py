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
from typing import Any, Dict

from core.config import ConfigStore, ConfigError
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
# Exit handling
# ============================================================

def _exit(code: ExitCode) -> None:
    sys.exit(int(code))


def _map_transport_error(e: TransportError) -> ExitCode:
    mapping = {
        StatusCode.AUTH_API_KEY_MISSING: ExitCode.CONFIG_API_KEY_MISSING,
        StatusCode.API_UNAUTHORIZED: ExitCode.API_UNAUTHORIZED,
        StatusCode.API_FORBIDDEN: ExitCode.API_FORBIDDEN,
        StatusCode.API_NOT_FOUND: ExitCode.API_NOT_FOUND,
        StatusCode.API_RATE_LIMITED: ExitCode.NETWORK_RATE_LIMITED,
        StatusCode.TRANSPORT_TIMEOUT: ExitCode.NETWORK_TIMEOUT,
        StatusCode.TRANSPORT_PROXY_ERROR: ExitCode.NETWORK_PROXY_FAILURE,
        StatusCode.TRANSPORT_SSL_ERROR: ExitCode.NETWORK_TLS_FAILURE,
        StatusCode.TRANSPORT_DNS_FAILURE: ExitCode.NETWORK_DNS_FAILURE,
        StatusCode.TRANSPORT_CONNECTION_FAILED: ExitCode.NETWORK_UNREACHABLE,
    }
    return mapping.get(e.status_code, ExitCode.API_UNKNOWN_ERROR)


# ============================================================
# Helpers
# ============================================================

def _merge_config(file_cfg, args: argparse.Namespace):
    updates: Dict[str, Any] = {}
    for field in vars(file_cfg).keys():
        if hasattr(args, field):
            val = getattr(args, field)
            if val is not None:
                updates[field] = val
    return file_cfg.__class__(**{**file_cfg.__dict__, **updates})


def _transport_cfg(cfg):
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

    for fn_name in ("main", "run", "cli"):
        fn = getattr(module, fn_name, None)
        if callable(fn):
            result = fn(
                args,
                dry_run=args.dry_run,
                show_progress=not args.no_progress,
            )
            return getattr(result, "exit_code", ExitCode.SUCCESS)

    for attr in dir(module):
        cls = getattr(module, attr)
        if isinstance(cls, type) and hasattr(cls, "run"):
            result = cls(
                dry_run=args.dry_run,
                show_progress=not args.no_progress,
            ).run()
            return getattr(result, "exit_code", ExitCode.SUCCESS)

    logging.error("No valid entrypoint for ingest module %s", module_name)
    return ExitCode.INTERNAL_HANDLER_MISSING


# ============================================================
# Main
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="bitsight-cli",
        description="BitSight SDK + CLI",
    )

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

    for name in ("exit", "quit", "q", "x"):
        subparsers.add_parser(name)

    cfg_p = subparsers.add_parser("config")
    cfg_sub = cfg_p.add_subparsers(dest="subcommand", required=True)
    for name in ("init", "show", "validate", "reset", "clear-keys"):
        cfg_sub.add_parser(name)

    db_p = subparsers.add_parser("db")
    db_sub = db_p.add_subparsers(dest="subcommand", required=True)
    db_init = db_sub.add_parser("init")
    _add_db_args(db_init)
    db_init.add_argument("--schema-path", default="db/schema/mssql.sql")
    db_init.add_argument("--stamp-schema", action="store_true")

    ingest_p = subparsers.add_parser("ingest")
    ingest_sub = ingest_p.add_subparsers(dest="subcommand", required=True)

    def ingest_cmd(name, extra=None):
        p = ingest_sub.add_parser(name)
        if extra:
            for spec in extra:
                p.add_argument(*spec[0], **spec[1])
        p.add_argument("--flush", action="store_true")
        return p

    ingest_cmd("users")
    ingest_cmd("user-details", [(["--user-guid"], {"required": True})])
    ingest_cmd("user-quota")

    args = parser.parse_args()
    setup_logging(args.verbose, args.json_logs)

    if args.command in ("exit", "quit", "q", "x"):
        _exit(ExitCode.SUCCESS_OPERATOR_EXIT)

    store = ConfigStore()
    try:
        cfg = _merge_config(store.load(), args)
    except Exception as e:
        logging.error(str(e))
        _exit(ExitCode.CONFIG_FILE_INVALID)

    try:
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
        _exit(_map_transport_error(e))

    except KeyboardInterrupt:
        _exit(ExitCode.SUCCESS_OPERATOR_EXIT)

    except Exception:
        logging.exception("Unhandled exception")
        _exit(ExitCode.RUNTIME_EXCEPTION)


if __name__ == "__main__":
    main()
