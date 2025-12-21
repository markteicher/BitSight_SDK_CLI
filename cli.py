#!/usr/bin/env python3

import argparse
import importlib
import logging
import sys
from typing import Optional, Dict

from tqdm import tqdm  # progress enabled unless --no-progress


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
    return {"http": args.proxy_url, "https": args.proxy_url}


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

    for entry in ("main", "run", "cli"):
        fn = getattr(module, entry, None)
        if callable(fn):
            fn(args)
            return

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
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bars")
    parser.add_argument("--api-key", help="BitSight API token")
    parser.add_argument("--base-url", help="BitSight API base URL")
    parser.add_argument("--proxy-url", help="Proxy URL")
    parser.add_argument("--timeout", type=int, help="HTTP timeout seconds")

    subparsers = parser.add_subparsers(dest="command")

    # ------------------------------------------------------------------
    # exit / quit (explicit)
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
    # db (MSSQL only)
    # ------------------------------------------------------------------
    db = subparsers.add_parser("db")
    db_sub = db.add_subparsers(dest="subcommand", required=True)

    db_init = db_sub.add_parser("init")
    db_init.add_argument("--mssql", action="store_true")
    db_init.add_argument("--server")
    db_init.add_argument("--database")
    db_init.add_argument("--username")
    db_init.add_argument("--password")
    db_init.add_argument("--schema-path", default="db/schema/mssql.sql")

    db_flush = db_sub.add_parser("flush")
    db_flush.add_argument("--mssql", action="store_true")
    db_flush.add_argument("--server")
    db_flush.add_argument("--database")
    db_flush.add_argument("--username")
    db_flush.add_argument("--password")
    db_flush.add_argument("--table")
    db_flush.add_argument("--all", action="store_true")

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

    ingest_cmd("users")
    ingest_cmd("user-details", [(["--user-guid"], {})])
    ingest_cmd("user-quota")
    ingest_cmd("user-company-views")

    ingest_cmd("companies")
    ingest_cmd("company-details", [(["--company-guid"], {})])

    ingest_cmd("portfolio")
    ingest_cmd("portfolio-details", [(["--company-guid"], {})])
    ingest_cmd("portfolio-contacts")
    ingest_cmd("portfolio-public-disclosures")

    ingest_cmd("current-ratings")
    ingest_cmd("ratings-history", [
        (["--company-guid"], {}),
        (["--since"], {}),
        (["--backfill"], {"action": "store_true"}),
    ])

    ingest_cmd("findings", [
        (["--company-guid"], {}),
        (["--since"], {}),
        (["--expand"], {}),
    ])

    ingest_cmd("observations", [
        (["--company-guid"], {}),
        (["--since"], {}),
    ])

    ingest_cmd("threats")
    ingest_cmd("threat-exposures")

    ingest_cmd("alerts", [(["--since"], {})])
    ingest_cmd("credential-leaks")
    ingest_cmd("exposed-credentials")

    # ------------------------------------------------------------------
    # ingest-group
    # ------------------------------------------------------------------
    ingest_group = subparsers.add_parser("ingest-group")
    ingest_group_sub = ingest_group.add_subparsers(dest="subcommand", required=True)

    ingest_group_sub.add_parser("core")
    ingest_group_sub.add_parser("security")
    ingest_group_sub.add_parser("all")

    # ------------------------------------------------------------------
    # help alias
    # ------------------------------------------------------------------
    subparsers.add_parser("help")

    # ------------------------------------------------------------------
    # parse
    # ------------------------------------------------------------------
    args = parser.parse_args()
    setup_logging(args.verbose)

    # shorthand exit aliases
    if args.command in ("exit", "quit", "x", "q"):
        exit_cli()

    if args.command in (None, "help"):
        parser.print_help()
        return

    # ------------------------------------------------------------------
    # dispatch
    # ------------------------------------------------------------------
    if args.command == "ingest":
        dispatch_ingest(args.subcommand, args)
        return

    if args.command == "db":
        logging.info("db command selected: %s", args.subcommand)
        return

    if args.command == "config":
        logging.info("config command selected: %s", args.subcommand)
        return

    if args.command == "ingest-group":
        logging.info("ingest-group selected: %s", args.subcommand)
        return

    logging.error("Unhandled command")
    sys.exit(1)


if __name__ == "__main__":
    main()
