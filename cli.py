#!/usr/bin/env python3

import argparse
import importlib
import logging
import sys
from typing import Optional, Dict, Any, Callable

# tqdm is optional at runtime via --no-progress
from tqdm import tqdm  # noqa: F401


# ----------------------------------------------------------------------
# logging
# ----------------------------------------------------------------------
def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def _progress_enabled(args: argparse.Namespace) -> bool:
    return not getattr(args, "no_progress", False)


def _build_proxies(args: argparse.Namespace) -> Optional[Dict[str, str]]:
    proxy_url = getattr(args, "proxy_url", None)
    if not proxy_url:
        return None
    return {"http": proxy_url, "https": proxy_url}


def _ingest_module_name(subcommand: str) -> str:
    # map CLI command -> python module
    # e.g., "current-ratings" -> "current_ratings"
    return subcommand.replace("-", "_")


def _call_ingest_entrypoint(module, args: argparse.Namespace) -> None:
    """
    No stubs: we only call real functions/classes if they exist.
    Supported patterns (first match wins):
      - module.main(args)
      - module.run(args)
      - module.cli(args)
      - module.<SomethingIngest>().run()
    """
    for fn_name in ("main", "run", "cli"):
        fn = getattr(module, fn_name, None)
        if callable(fn):
            logging.debug("Dispatching to %s.%s(args)", module.__name__, fn_name)
            fn(args)
            return

    # try an *Ingest class
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
            logging.debug("Dispatching to %s.%s().run()", module.__name__, ingest_cls.__name__)
            run()
            return

    # If we got here: module exists, but has no callable entrypoint
    public_callables = [
        name for name in dir(module)
        if not name.startswith("_") and callable(getattr(module, name, None))
    ]
    logging.error(
        "Ingest module '%s' has no supported entrypoint. "
        "Expected one of: main(args), run(args), cli(args), or *Ingest().run(). "
        "Found callables: %s",
        module.__name__,
        public_callables,
    )
    sys.exit(1)


def _require(condition: bool, message: str) -> None:
    if not condition:
        logging.error(message)
        sys.exit(1)


# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(prog="bitsight", description="BitSight SDK + CLI")

    # global flags
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bars (CI-safe)",
    )

    # common connection flags (usable now, and by ingest modules that choose to read args)
    parser.add_argument("--api-key", help="BitSight API token (Basic Auth username)")
    parser.add_argument("--base-url", help="BitSight API base URL (e.g., https://api.bitsighttech.com)")
    parser.add_argument("--proxy-url", help="Proxy URL (e.g., http://proxy:8080)")
    parser.add_argument("--timeout", type=int, help="HTTP timeout seconds")

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
    # db
    # ------------------------------------------------------------------
    db = subparsers.add_parser("db", help="Database management")
    db_sub = db.add_subparsers(dest="subcommand", required=True)

    db_init = db_sub.add_parser("init")
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

    db_sub.add_parser("status")

    db_flush = db_sub.add_parser("flush")
    db_flush.add_argument("--mssql", action="store_true", help="Flush data in MSSQL")
    db_flush.add_argument("--server")
    db_flush.add_argument("--database")
    db_flush.add_argument("--username")
    db_flush.add_argument("--password")
    db_flush.add_argument("--table", help="Single dbo.<table> name (e.g., bitsight_users)")
    db_flush.add_argument("--all", action="store_true", help="Flush all BitSight tables")

    # ------------------------------------------------------------------
    # ingest
    # ------------------------------------------------------------------
    ingest = subparsers.add_parser("ingest", help="Data ingestion")
    ingest_sub = ingest.add_subparsers(dest="subcommand", required=True)

    def ingest_cmd(name: str, extra_args=None):
        p = ingest_sub.add_parser(name)
        if extra_args:
            for arg in extra_args:
                p.add_argument(*arg[0], **arg[1])
        p.add_argument("--flush", action="store_true", help="Flush destination before ingest (module-defined)")
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
    ingest_group = subparsers.add_parser("ingest-group", help="Grouped ingestion")
    ingest_group_sub = ingest_group.add_subparsers(dest="subcommand", required=True)
    ingest_group_sub.add_parser("core")
    ingest_group_sub.add_parser("security")
    ingest_group_sub.add_parser("all")

    # ------------------------------------------------------------------
    # status
    # ------------------------------------------------------------------
    status = subparsers.add_parser("status", help="System status")
    status_sub = status.add_subparsers(dest="subcommand", required=True)
    status_sub.add_parser("tables")
    status_sub.add_parser("last-run")
    status_sub.add_parser("health")

    # ------------------------------------------------------------------
    # parse + runtime
    # ------------------------------------------------------------------
    args = parser.parse_args()
    setup_logging(args.verbose)
    logging.debug("CLI arguments: %s", vars(args))

    # ------------------------------------------------------------------
    # dispatch
    # ------------------------------------------------------------------
    if args.command == "db":
        if args.subcommand == "init":
            _require(args.mssql, "db init currently requires --mssql")
            _require(args.server and args.database and args.username and args.password, "db init --mssql requires --server/--database/--username/--password")

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

        if args.subcommand == "flush":
            _require(args.mssql, "db flush currently requires --mssql")
            _require(args.server and args.database and args.username and args.password, "db flush --mssql requires --server/--database/--username/--password")
            _require(args.all or args.table, "db flush requires either --all or --table")

            from db.mssql import MSSQLDatabase

            dbh = MSSQLDatabase(
                server=args.server,
                database=args.database,
                username=args.username,
                password=args.password,
            )
            try:
                if args.table:
                    table = args.table.strip()
                    _require(dbh.table_exists(table), f"Table does not exist: dbo.{table}")
                    logging.info("Flushing table dbo.%s", table)
                    dbh.execute(f"DELETE FROM dbo.{table}")
                    dbh.commit()
                    logging.info("Flush complete: dbo.%s", table)
                else:
                    # Explicit BitSight tables flush list belongs in schema + ingest taxonomy;
                    # we only flush what exists to stay deterministic across versions.
                    tables = [
                        "bitsight_collection_state",
                        "bitsight_users",
                        "bitsight_user_details",
                        "bitsight_user_quota",
                        "bitsight_user_company_views",
                        "bitsight_companies",
                        "bitsight_company_details",
                        "bitsight_portfolio",
                        "bitsight_current_ratings",
                        "bitsight_ratings_history",
                        "bitsight_findings",
                        "bitsight_findings_statistics",
                        "bitsight_observations",
                        "bitsight_threats",
                        "bitsight_alerts",
                        "bitsight_exposed_credentials",
                        "bitsight_news",
                        "bitsight_insights",
                        "bitsight_subsidiaries",
                        "bitsight_subsidiary_statistics",
                        "bitsight_nist_csf_reports",
                        "bitsight_reports",
                        "bitsight_statistics",
                        "bitsight_industries",
                        "bitsight_tiers",
                        "bitsight_lifecycle_states",
                    ]
                    existing = [t for t in tables if dbh.table_exists(t)]
                    _require(len(existing) > 0, "No known BitSight tables found to flush")
                    for t in existing:
                        logging.info("Flushing table dbo.%s", t)
                        dbh.execute(f"DELETE FROM dbo.{t}")
                    dbh.commit()
                    logging.info("Flush complete: %d tables", len(existing))
            except Exception:
                dbh.rollback()
                logging.exception("Flush failed — rolled back")
                raise
            finally:
                dbh.close()
            return

        if args.subcommand == "status":
            logging.info("db status: not implemented in this CLI file (use ingest/state tables when available)")
            return

        logging.error("Unhandled db command: %s", args.subcommand)
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
        logging.info("Running ingest group: %s", args.subcommand)
        logging.error("ingest-group wiring must be explicit (no implicit stubs).")
        sys.exit(1)

    if args.command == "config":
        logging.error("config command wiring is not implemented in this cli.py yet.")
        sys.exit(1)

    if args.command == "status":
        logging.error("status command wiring is not implemented in this cli.py yet.")
        sys.exit(1)

    logging.error("Unhandled command: %s", args.command)
    sys.exit(1)


if __name__ == "__main__":
    main()
