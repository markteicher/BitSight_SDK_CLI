#!/usr/bin/env python3

import argparse
import importlib
import logging
import sys
from typing import Optional, Dict

from tqdm import tqdm


# ----------------------------------------------------------------------
# logging
# ----------------------------------------------------------------------
def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


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

    logging.error(
        "No valid entrypoint found in ingest module: %s", module_name
    )
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
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument("--api-key")
    parser.add_argument("--base-url")
    parser.add_argument("--proxy-url")
    parser.add_argument("--timeout", type=int)

    subparsers = parser.add_subparsers(dest="command")

    # ------------------------------------------------------------------
    # exit aliases
    # ------------------------------------------------------------------
    subparsers.add_parser("exit")
    subparsers.add_parser("quit")
    subparsers.add_parser("x")
    subparsers.add_parser("q")

    # ------------------------------------------------------------------
    # config
    # ------------------------------------------------------------------
    config = subparsers.add_parser("config")
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
    # db
    # ------------------------------------------------------------------
    db = subparsers.add_parser("db")
    db_sub = db.add_subparsers(dest="subcommand", required=True)

    db_init = db_sub.add_parser("init")
    _add_db_connection_args(db_init)
    db_init.add_argument("--schema-path", default="db/schema/mssql.sql")

    db_flush = db_sub.add_parser("flush")
    _add_db_connection_args(db_flush)
    db_flush.add_argument("--table")
    db_flush.add_argument("--all", action="store_true")

    db_clear = db_sub.add_parser("clear")
    _add_db_connection_args(db_clear)
    db_clear.add_argument("--table")
    db_clear.add_argument("--all", action="store_true")

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

    # users
    ingest_cmd("users")
    ingest_cmd("user-details", [
        (["--user-guid"], {"required": True})
    ])

    # existing ingest commands (unchanged)
    ingest_cmd("user-quota")
    ingest_cmd("user-company-views")

    ingest_cmd("companies")
    ingest_cmd("company-details", [(["--company-guid"], {"required": True})])

    ingest_cmd("portfolio")
    ingest_cmd("current-ratings")
    ingest_cmd("current-ratings-v2")

    ingest_cmd("ratings-history", [
        (["--company-guid"], {"required": True}),
    ])

    ingest_cmd("findings", [
        (["--company-guid"], {"required": True}),
    ])

    ingest_cmd("observations", [
        (["--company-guid"], {"required": True}),
    ])

    ingest_cmd("threats")
    ingest_cmd("threat-statistics")
    ingest_cmd("threats-impact", [
        (["--threat-guid"], {"required": True}),
    ])
    ingest_cmd("threats-evidence", [
        (["--threat-guid"], {"required": True}),
        (["--entity-guid"], {"required": True}),
    ])

    # ------------------------------------------------------------------
    # help
    # ------------------------------------------------------------------
    subparsers.add_parser("help")

    # ------------------------------------------------------------------
    # parse + dispatch
    # ------------------------------------------------------------------
    args = parser.parse_args()
    setup_logging(args.verbose)

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
