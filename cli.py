#!/usr/bin/env python3

import argparse
import logging
import sys
from tqdm import tqdm

# ----------------------------------------------------------------------
# logging
# ----------------------------------------------------------------------

def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)]
    )


# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        prog="bitsight",
        description="BitSight SDK + CLI"
    )

    # global flags
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging"
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bars (CI-safe)"
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
    # db
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
    # ingest
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
    ingest_cmd("user-details", [
        (["--user-guid"], {})
    ])
    ingest_cmd("user-quota")
    ingest_cmd("user-company-views")

    ingest_cmd("companies")
    ingest_cmd("company-details", [
        (["--company-guid"], {})
    ])

    ingest_cmd("portfolio")
    ingest_cmd("portfolio-details", [
        (["--company-guid"], {})
    ])
    ingest_cmd("portfolio-contacts")
    ingest_cmd("portfolio-public-disclosures")

    ingest_cmd("current-ratings")

    ingest_cmd("ratings-history", [
        (["--company-guid"], {}),
        (["--since"], {}),
        (["--backfill"], {"action": "store_true"})
    ])

    ingest_cmd("findings", [
        (["--company-guid"], {}),
        (["--since"], {}),
        (["--expand"], {})
    ])

    ingest_cmd("observations", [
        (["--company-guid"], {}),
        (["--since"], {})
    ])

    ingest_cmd("threats")
    ingest_cmd("threat-exposures")

    ingest_cmd("alerts", [
        (["--since"], {})
    ])

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
    # dispatch (thin wiring only)
    # ------------------------------------------------------------------
    if args.command == "ingest":
        if args.subcommand == "users":
            from ingest.users import UsersIngest
            UsersIngest().run()

        elif args.subcommand == "companies":
            from ingest.companies import CompaniesIngest
            CompaniesIngest().run()

        elif args.subcommand == "current-ratings":
            from ingest.current_ratings import CurrentRatingsIngest
            CurrentRatingsIngest().run()

        elif args.subcommand == "alerts":
            from ingest.alerts import AlertsIngest
            AlertsIngest(since=args.since).run()

        else:
            logging.error("Unhandled ingest command: %s", args.subcommand)
            sys.exit(1)

    elif args.command == "ingest-group":
        logging.info("Running ingest group: %s", args.subcommand)

    else:
        logging.info("Command executed: %s %s", args.command, args.subcommand)


if __name__ == "__main__":
    main()
