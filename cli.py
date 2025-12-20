import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        prog="bitsight",
        description="BitSight SDK + CLI"
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
        return p

    ingest_cmd("users", [
        (["--limit"], {"type": int})
    ])

    ingest_cmd("user-details", [
        (["--user-guid"], {})
    ])

    ingest_cmd("user-quota")
    ingest_cmd("user-company-views")

    ingest_cmd("companies", [
        (["--limit"], {"type": int})
    ])

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
    # ingest-group (orchestration)
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
    # dispatch
    # ------------------------------------------------------------------
    args = parser.parse_args()

    # Placeholder dispatch
    print("COMMAND:", args.command)
    print("SUBCOMMAND:", args.subcommand)
    print("ARGS:", vars(args))


if __name__ == "__main__":
    main()
