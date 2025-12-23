#!/usr/bin/env python3
"""
ingest/company_details.py

Net-new + update ingestion for BitSight Company Details (single company).

CLI:
  bitsight-cli ingest company-details --company-guid <GUID> [--dry-run]

Table:
  dbo.bitsight_company_details (PK: company_guid)

Behavior:
- net-new: INSERT
- update:  UPDATE when payload_hash changes
- removal: not applicable (single keyed fetch). If the API 404s, we surface the API error.
- dry-run: fetch + diff, no DB mutations
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from core.config import ConfigStore, Config, ConfigError
from core.db_router import DatabaseRouter
from core.exit_codes import ExitCode
from core.ingestion import IngestionExecutor, IngestionResult
from core.transport import TransportConfig, TransportError, build_session
from ingest.base import (
    TableSpec,
    DeltaState,
    fetch_one,
    make_delta_writer,
    log_delta,
)

# BitSight company details endpoint (v1 is typical; adjust if your repo differs)
# Example: /ratings/v1/companies/{company_guid}
BITSIGHT_COMPANY_DETAILS_ENDPOINT_TMPL = "/ratings/v1/companies/{company_guid}"


def _merge_cfg(file_cfg: Config, args) -> Config:
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
            v = getattr(args, field)
            if v is not None:
                updates[field] = v
    return Config(**{**file_cfg.__dict__, **updates}) if updates else file_cfg


def run(args) -> IngestionResult:
    """
    Entrypoint for cli.py dispatch_ingest().
    """
    company_guid = getattr(args, "company_guid", None)
    if not company_guid:
        msg = "Missing required --company-guid"
        logging.error(msg)
        return IngestionResult(
            status_code=None,  # type: ignore[arg-type]
            exit_code=ExitCode.CLI_MISSING_ARGUMENT,
            records_fetched=0,
            records_written=0,
            records_failed=0,
            duration_seconds=0.0,
            message=msg,
        )

    store = ConfigStore()
    try:
        cfg = _merge_cfg(store.load(), args)
    except ConfigError as e:
        logging.error(str(e))
        return IngestionResult(
            status_code=None,  # type: ignore[arg-type]
            exit_code=ExitCode.CONFIG_FILE_INVALID,
            records_fetched=0,
            records_written=0,
            records_failed=0,
            duration_seconds=0.0,
            message=str(e),
        )

    cfg.validate(require_api_key=True)

    if not all([cfg.mssql_server, cfg.mssql_database, cfg.mssql_username, cfg.mssql_password]):
        msg = "Missing MSSQL connection parameters (server/database/username/password)"
        logging.error(msg)
        return IngestionResult(
            status_code=None,  # type: ignore[arg-type]
            exit_code=ExitCode.CONFIG_VALUE_MISSING,
            records_fetched=0,
            records_written=0,
            records_failed=0,
            duration_seconds=0.0,
            message=msg,
        )

    dry_run = bool(getattr(args, "dry_run", False))
    show_progress = not bool(getattr(args, "no_progress", False))

    # Transport/session
    tcfg = TransportConfig(
        base_url=cfg.base_url,
        api_key=cfg.api_key or "",
        timeout=cfg.timeout,
        proxy_url=cfg.proxy_url,
        proxy_username=cfg.proxy_username,
        proxy_password=cfg.proxy_password,
        verify_ssl=True,
    )
    session, proxies = build_session(tcfg)

    # DB connect
    db = DatabaseRouter.get_database(
        backend="mssql",
        server=cfg.mssql_server,
        database=cfg.mssql_database,
        username=cfg.mssql_username,
        password=cfg.mssql_password,
        driver=cfg.mssql_driver,
        encrypt=cfg.mssql_encrypt,
        trust_cert=cfg.mssql_trust_cert,
        timeout=cfg.mssql_timeout,
    )
    db.connect()

    spec = TableSpec(table="dbo.bitsight_company_details", pk_col="company_guid")
    delta = DeltaState()

    endpoint = BITSIGHT_COMPANY_DETAILS_ENDPOINT_TMPL.format(company_guid=str(company_guid).strip())

    def fetcher() -> List[Dict[str, Any]]:
        rec = fetch_one(
            session=session,
            base_url=cfg.base_url,
            api_key=cfg.api_key or "",
            endpoint=endpoint,
            timeout=cfg.timeout,
            proxies=proxies,
        )

        # Ensure the key is present in the record we write.
        if isinstance(rec, dict) and "company_guid" not in rec:
            rec["company_guid"] = str(company_guid).strip()

        return [rec]

    def key_fn(rec: Dict[str, Any]) -> str:
        guid = rec.get("company_guid") or rec.get("guid")
        if guid is None:
            raise ValueError("Company details payload missing company_guid/guid")
        return str(guid)

    writer = make_delta_writer(
        db=db,
        spec=spec,
        key_fn=key_fn,
        delta=delta,
        dry_run=dry_run,
    )

    executor = IngestionExecutor(
        fetcher=fetcher,
        writer=writer,
        expected_min_records=1,
        show_progress=show_progress,
        dry_run=dry_run,  # requires corrected core/ingestion.py to accept this
    )

    try:
        result = executor.run()
        log_delta(label="COMPANY_DETAILS", delta=delta, dry_run=dry_run)

        if dry_run:
            db.rollback()
        else:
            db.commit()

        return result

    except TransportError as e:
        db.rollback()
        logging.error(str(e))
        return IngestionResult(
            status_code=None,  # type: ignore[arg-type]
            exit_code=ExitCode.API_UNKNOWN_ERROR,
            records_fetched=0,
            records_written=0,
            records_failed=0,
            duration_seconds=0.0,
            message=str(e),
        )
    except Exception as e:
        db.rollback()
        logging.exception("company-details ingestion failed")
        return IngestionResult(
            status_code=None,  # type: ignore[arg-type]
            exit_code=ExitCode.RUNTIME_EXCEPTION,
            records_fetched=0,
            records_written=0,
            records_failed=0,
            duration_seconds=0.0,
            message=str(e),
        )
    finally:
        db.close()


def main(args) -> IngestionResult:
    return run(args)
