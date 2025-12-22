#!/usr/bin/env python3

import argparse
import importlib
import logging
import sys
import json
from typing import Optional, Dict, Callable

from tqdm import tqdm


# ----------------------------------------------------------------------
# logging
# ----------------------------------------------------------------------
class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        return json.dumps(payload)


def setup_logging(verbose: bool, json_logs: bool) -> None:
    handler = logging.StreamHandler(sys.stdout)

    if json_logs:
        handler.setFormatter(JsonLogFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s",
                "%Y-%m-%d %H:%M:%S",
            )
        )

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG if verbose else logging.INFO)


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def build_proxies(args: argparse.Namespace) -> Optional[Dict[str, str]]:
    if not args.proxy_url:
        return None
    return {
        "http": args.proxy_url,
        "https": args.proxy_url,
    }


def exit_cli() -> None:
    print("Thank you for using the BitSight CLI")
    sys.exit(0)


def require(condition: bool, message: str) -> None:
    if not condition:
        logging.error(message)
        sys.exit(1)


def ingest_module_name(subcommand: str) -> str:
    return subcommand.replace("-", "_")


def wrap_writer(writer: Callable, dry_run: bool) -> Callable:
    """
    Dry-run wrapper: preserves ingestion flow, blocks side effects.
    """
    if not dry_run:
        return writer

    def _noop_writer(record):
        logging.debug("DRY-RUN: record suppressed")
        return None

    return _noop_writer


def dispatch_ingest(subcommand: str, args: argparse.Namespace) -> None:
    module_name = ingest_module_name(subcommand)
    full_module = f"ingest.{module_name}"

    try:
        module = importlib.import_module(full_module)
    except ModuleNotFoundError:
        logging.error("Missing ingest module: %s.py", module_name)
        sys.exit(1)

    # function-style entrypoints
    for entry in ("main", "run", "cli"):
        fn = getattr(module, entry, None)
        if callable(fn):
            fn(args)
            return

    # class-style entrypoints
    for attr in dir(module):
        if attr.endswith("Ingest"):
            cls = getattr(module, attr)
            if isinstance(cls, type):
                obj = cls()
                if hasattr(obj, "run"):
                    obj.run()
                    return

    logging.error("No valid entrypoint found in ingest module: %s", module_name)
    sys.exit(1)


def _add_db_connection_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--mssql", action="store_true")
    p.add_argument("--server")
    p.add_argument("--database")
    p.add_argument("--username")
    p.add_argument("--password")


# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        prog="bitsight",
        description="BitSight SDK + CLI",
        add_help=True,
    )

    # global options
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--json-logs", action="store_true")   # NEW
    parser.add_argument("--dry-run", action="store_true")     # NEW
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument("--api-key")
    parser.add_argument("--base-url")
    parser.add_argument("--proxy-url")
    parser.add_argument("--timeout", type=int)

    subparsers = parser.add_subparsers(dest="command")

    # ------------------------------------------------------------------
    # exit aliases
    # ------------------------------------------------------------------
    for name in ("exit", "quit", "x", "q"):
        subparsers.add_parser(name)

    # ------------------------------------------------------------------
    # config
    # ------------------------------------------------------------------
    config = subparsers.add_parser("config")
    config_sub = config.add_subparsers(dest="subcommand", required=True)

    for name in ("init", "show", "validate", "reset", "clear-keys"):
        config_sub.add_parser(name)

    config_set = config_sub.add_parser("set")
    config_set.add_argument("--api-key")
    config_set.add_argument("--base-url")
    config_set.add_argument("--proxy-url")
    config_set.add_argument("--proxy-username")
    config_set.add_argument("--proxy-password")
    config_set.add_argument("--timeout", type=int)

    # ------------------------------------------------------------------
    # db
    # ------------------------------------------------------------------
    db = subparsers.add_parser("db")
    db_sub = db.add_subparsers(dest="subcommand", required=True)

    db_init = db_sub.add_parser("init")
    _add_db_connection_args(db_init)
    db_init.add_argument("--schema-path", default="db/schema/mssql.sql")
    db_init.add_argument("--stamp-schema", action="store_true")  # NEW

    for name in ("flush", "clear"):
        p = db_sub.add_parser(name)
        _add_db_connection_args(p)
        p.add_argument("--table")
        p.add_argument("--all", action="store_true")

    db_sub.add_parser("status")

    # ------------------------------------------------------------------
    # ingest
    # ------------------------------------------------------------------
    ingest = subparsers.add_parser("ingest")
    ingest_sub = ingest.add_subparsers(dest="subcommand", required=True)

    def ingest_cmd(name: str, extra=None):
        p = ingest_sub.add_parser(name)
        if extra:
            for argspec in extra:
                p.add_argument(*argspec[0], **argspec[1])
        p.add_argument("--flush", action="store_true")
        return p

    # identity / users
    ingest_cmd("users")
    ingest_cmd("user-details", [(["--user-guid"], {"required": True})])
    ingest_cmd("user-quota")
    ingest_cmd("user-company-views")

    # core data
    ingest_cmd("companies")
    ingest_cmd("company-details", [(["--company-guid"], {"required": True})])
    ingest_cmd("portfolio")
    ingest_cmd("current-ratings")
    ingest_cmd("current-ratings-v2")
    ingest_cmd("ratings-history", [(["--company-guid"], {"required": True})])
    ingest_cmd("findings", [(["--company-guid"], {"required": True})])
    ingest_cmd("observations", [(["--company-guid"], {"required": True})])

    # threats
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

    # ------------------------------------------------------------------
    # parse + dispatch
    # ------------------------------------------------------------------
    args = parser.parse_args()
    setup_logging(args.verbose, args.json_logs)

    if args.command in ("exit", "quit", "x", "q"):
        exit_cli()

    if args.command in (None, "help"):
        parser.print_help()
        return

    if args.command == "ingest":
        dispatch_ingest(args.subcommand, args)
        return

    if args.command == "db":
        logging.info("db command selected: %s", args.subcommand)
        return

    if args.command == "config":
        logging.info("config command selected: %s", args.subcommand)
        return

    logging.error("Unhandled command")
    sys.exit(1)


if __name__ == "__main__":
    main()
