#!/usr/bin/env python3

import argparse
import importlib
import logging
import sys
from typing import Optional, Dict

from tqdm import tqdm  # noqa: F401


CLI_VERSION = "0.1.0"


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _require(condition: bool, message: str) -> None:
    if not condition:
        logging.error(message)
        sys.exit(1)


def _ingest_module_name(subcommand: str) -> str:
    return subcommand.replace("-", "_")


def _call_ingest_entrypoint(module, args: argparse.Namespace) -> None:
    for fn_name in ("main", "run", "cli"):
        fn = getattr(module, fn_name, None)
        if callable(fn):
            fn(args)
            return

    ingest_cls = None
    for attr in dir(module):
        if attr.endswith("Ingest"):
            candidate = getattr(module, attr, None)
            if isinstance(candidate, type):
                ingest_cls = candidate
                break

    if ingest_cls is not None:
        obj = ingest_cls()
        run = getattr(obj, "run", None)
        if callable(run):
            run()
            return

    logging.error(
        "Ingest module '%s' has no supported entrypoint "
        "(main(args), run(args), cli(args), or *Ingest().run()).",
        module.__name__,
    )
    sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bitsight", description="BitSight SDK + CLI")

    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bars")
    parser.add_argument("--version", action="store_true", help="Print CLI version and exit")

    parser.add_argument("--api-key", help="BitSight API token (Basic Auth username)")
    parser.add_argument("--base-url", help="BitSight API base URL (e.g., https://api.bitsighttech.com)")
    parser.add_argument("--proxy-url", help="Proxy URL (e.g., http://proxy:8080)")
    parser.add_argument("--timeout", type=int, help="HTTP timeout seconds")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # help / exit / quit
    subparsers.add_parser("help", help="Show help and exit")
    subparsers.add_parser("exit", help="Exit and print goodbye message")
    subparsers.add_parser("quit", help="Exit and print goodbye message")

    # config
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

    # db
    db = subparsers.add_parser("db", help="Database management")
    db_sub = db.add_subparsers(dest="subcommand", required=True)

    db_init = db_sub.add_parser("init", help="Initialize database schema")
    db_init.add_argument("--mssql", action="store_true", help="Initialize MSSQL schema")
    db_init.add_argument("--server")
    db_init.add_argument("--database")
    db_init.add_argument("--username")
    db_init.add_argument("--password")
    db_init.add_argument(
        "--schema-path",
        default="db/schema/mssql.sql",
        help="Path to MSSQL schema file",
    )

    db_sub.add_parser("status", help="Show database status")

    db_flush = db_sub.add_parser("flush", help="Flush data from database tables")
    db_flush.add_argument("--mssql", action="store_true", help="Use MSSQL backend")
    db_flush.add_argument("--server")
    db_flush.add_argument("--database")
    db_flush.add_argument("--username")
    db_flush.add_argument("--password")
    db_flush.add_argument("--table", help="Single dbo.<table> name (e.g., bitsight_users)")
    db_flush.add_argument("--all", action="store_true", help="Flush all BitSight tables")

    db_clear = db_sub.add_parser("clear", help="Clear BitSight data from database")
    db_clear.add_argument("--mssql", action="store_true", help="Use MSSQL backend")
    db_clear.add_argument("--server")
    db_clear.add_argument("--database")
    db_clear.add_argument("--username")
    db_clear.add_argument("--password")
    db_clear.add_argument("--table", help="Clear a single dbo.<table>")
    db_clear.add_argument("--all", action="store_true", help="Clear all BitSight tables")
    db_clear.add_argument("--yes", action="store_true", help="Required confirmation flag")
    db_clear.add_argument("--dry-run", action="store_true", help="Show actions without executing")

    # ingest
    ingest = subparsers.add_parser("ingest", help="Data ingestion")
    ingest_sub = ingest.add_subparsers(dest="subcommand", required=True)

    def ingest_cmd(name: str, extra_args=None):
        p = ingest_sub.add_parser(name)
        if extra_args:
            for arg in extra_args:
                p.add_argument(*arg[0], **arg[1])
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

    ingest_cmd(
        "ratings-history",
        [
            (["--company-guid"], {}),
            (["--since"], {}),
            (["--backfill"], {"action": "store_true"}),
        ],
    )

    ingest_cmd(
        "findings",
        [
            (["--company-guid"], {}),
            (["--since"], {}),
            (["--expand"], {}),
        ],
    )

    ingest_cmd(
        "observations",
        [
            (["--company-guid"], {}),
            (["--since"], {}),
        ],
    )

    ingest_cmd("threats")
    ingest_cmd("threat-exposures")
    ingest_cmd("alerts", [(["--since"], {})])
    ingest_cmd("credential-leaks")
    ingest_cmd("exposed-credentials")

    # ingest-group
    ingest_group = subparsers.add_parser("ingest-group", help="Grouped ingestion")
    ingest_group_sub = ingest_group.add_subparsers(dest="subcommand", required=True)
    ingest_group_sub.add_parser("core")
    ingest_group_sub.add_parser("security")
    ingest_group_sub.add_parser("all")

    # status
    status = subparsers.add_parser("status", help="System status")
    status_sub = status.add_subparsers(dest="subcommand", required=True)
    status_sub.add_parser("tables")
    status_sub.add_parser("last-run")
    status_sub.add_parser("health")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if getattr(args, "version", False):
        print(CLI_VERSION)
        sys.exit(0)

    setup_logging(getattr(args, "verbose", False))
    logging.debug("CLI arguments: %s", vars(args))

    if args.command == "help":
        parser.print_help()
        sys.exit(0)

    if args.command in ("exit", "quit"):
        print("Thank you for using the BitSight CLI")
        sys.exit(0)

    if args.command == "db":
        if args.subcommand == "init":
            _require(args.mssql, "db init currently supports MSSQL only (--mssql)")
            _require(
                args.server and args.database and args.username and args.password,
                "db init --mssql requires --server/--database/--username/--password",
            )

            from db.init import MSSQLInitializer

            init = MSSQLInitializer(
                server=args.server,
                database=args.database,
                username=args.username,
                password=args.password,
                schema_path=args.schema_path,
            )
            init.run()
            return

        if args.subcommand in ("flush", "clear"):
            _require(getattr(args, "mssql", False), f"db {args.subcommand} currently supports MSSQL only (--mssql)")
            _require(
                args.server and args.database and args.username and args.password,
                f"db {args.subcommand} --mssql requires --server/--database/--username/--password",
            )
            _require(args.all or args.table, f"db {args.subcommand} requires either --all or --table")

            if args.subcommand == "clear":
                _require(args.yes or args.dry_run, "db clear requires --yes or --dry-run")

            from db.mssql import MSSQLDatabase

            dbh = MSSQLDatabase(
                server=args.server,
                database=args.database,
                username=args.username,
                password=args.password,
            )

            try:
                action = "Flushing" if args.subcommand == "flush" else "Clearing"

                if args.table:
                    table = args.table.strip()
                    _require(dbh.table_exists(table), f"Table does not exist: dbo.{table}")

                    if getattr(args, "dry_run", False):
                        logging.info("DRY-RUN: would %s dbo.%s", action.lower(), table)
                    else:
                        logging.info("%s dbo.%s", action, table)
                        dbh.execute(f"DELETE FROM dbo.{table}")
                        dbh.commit()
                        logging.info("%s complete: dbo.%s", action, table)

                else:
                    tables = dbh.list_bitsight_tables()
                    _require(len(tables) > 0, "No BitSight tables found")

                    for t in tables:
                        if getattr(args, "dry_run", False):
                            logging.info("DRY-RUN: would %s dbo.%s", action.lower(), t)
                        else:
                            logging.info("%s dbo.%s", action, t)
                            dbh.execute(f"DELETE FROM dbo.{t}")

                    if not getattr(args, "dry_run", False):
                        dbh.commit()
                        logging.info("%s complete: %d tables", action, len(tables))

            except Exception:
                dbh.rollback()
                logging.exception("db %s failed — rolled back", args.subcommand)
                sys.exit(1)
            finally:
                dbh.close()

            return

        if args.subcommand == "status":
            logging.error("db status is not wired yet")
            sys.exit(1)

        logging.error("Unhandled db subcommand: %s", args.subcommand)
        sys.exit(1)

    if args.command == "ingest":
        module_name = _ingest_module_name(args.subcommand)
        full_module = f"ingest.{module_name}"

        try:
            module = importlib.import_module(full_module)
        except ModuleNotFoundError:
            logging.error("Missing ingest module: %s.py (expected import %s)", module_name, full_module)
            sys.exit(1)

        _call_ingest_entrypoint(module, args)
        return

    if args.command == "ingest-group":
        logging.error("ingest-group is not wired yet")
        sys.exit(1)

    if args.command == "config":
        logging.error("config is not wired yet")
        sys.exit(1)

    if args.command == "status":
        logging.error("status is not wired yet")
        sys.exit(1)

    logging.error("Unhandled command: %s", args.command)
    sys.exit(1)


if __name__ == "__main__":
    main()
