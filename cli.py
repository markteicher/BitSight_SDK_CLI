#!/usr/bin/env python3

import argparse
import importlib
import json
import logging
import sys
from typing import Optional, Dict


# ----------------------------------------------------------------------
# logging
# ----------------------------------------------------------------------
class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Attach exception info if present (combat debugging)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


def setup_logging(verbose: bool, json_logs: bool) -> None:
    handler = logging.StreamHandler(sys.stdout)

    if json_logs:
        handler.setFormatter(JsonLogFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG if verbose else logging.INFO)


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def ingest_module_name(subcommand: str) -> str:
    return subcommand.replace("-", "_")


def dispatch_ingest(subcommand: str, args: argparse.Namespace) -> int:
    module_name = ingest_module_name(subcommand)
    full_module = f"ingest.{module_name}"

    try:
        module = importlib.import_module(full_module)
    except ModuleNotFoundError:
        logging.error("Missing ingest module: %s.py", module_name)
        return 1

    # function-style entrypoints
    for entry in ("main", "run", "cli"):
        fn = getattr(module, entry, None)
        if callable(fn):
            fn(args)
            return 0

    # class-style entrypoints
    for attr in dir(module):
        if attr.endswith("Ingest"):
            cls = getattr(module, attr)
            if isinstance(cls, type):
                obj = cls()
                run = getattr(obj, "run", None)
                if callable(run):
                    run()
                    return 0

    logging.error("No valid entrypoint found in ingest module: %s", module_name)
    return 1


def _add_db_connection_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--mssql", action="store_true")
    p.add_argument("--server")
    p.add_argument("--database")
    p.add_argument("--username")
    p.add_argument("--password")


# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        prog="bitsight-cli",
        description="BitSight SDK + CLI",
        add_help=True,
    )

    # global options
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--json-logs", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument("--api-key")
    parser.add_argument("--base-url")
    parser.add_argument("--proxy-url")
    parser.add_argument("--timeout", type=int)

    subparsers = parser.add_subparsers(dest="command")

    # ------------------------------------------------------------------
    # help + exit aliases
    # ------------------------------------------------------------------
    subparsers.add_parser("help")
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
    db_init.add_argument("--stamp-schema", action="store_true")

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

    if args.command in (None, "help"):
        parser.print_help()
        return 0

    if args.command in ("exit", "quit", "x", "q"):
        print("Thank you for using the BitSight CLI")
        return 0

    if args.command == "ingest":
        return dispatch_ingest(args.subcommand, args)

    if args.command == "db":
        logging.info("db command selected: %s", args.subcommand)
        return 0

    if args.command == "config":
        logging.info("config command selected: %s", args.subcommand)
        return 0

    logging.error("Unhandled command")
    return 1


if __name__ == "__main__":
    sys.exit(main())
