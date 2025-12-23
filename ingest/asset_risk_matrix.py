#!/usr/bin/env python3
"""
ingest/asset_risk_matrix.py

BitSight SDK + CLI
Ingest: Asset Risk Matrix

Implements:
- NET_NEW (insert)
- UPDATED (update)
- UNCHANGED (no-op)
- REMOVED (delete OR soft-delete when supported)

Uses:
- core.transport (session/proxy/auth)
- core.ingestion.IngestionExecutor (counts/timing/semantics)
- MSSQL backend via db.mssql.MSSQLDatabase

Assumptions / Contracts:
- Target table: dbo.bitsight_asset_risk_matrix
  Minimum columns expected:
    - company_guid UNIQUEIDENTIFIER
    - ingested_at  DATETIME2
    - raw_payload  NVARCHAR(MAX)
  Optional columns (if present, will be used):
    - row_hash     NVARCHAR(64)   (sha256 hex)
    - is_deleted   BIT
    - deleted_at   DATETIME2
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from core.config import ConfigStore, Config
from core.exit_codes import ExitCode
from core.ingestion import IngestionExecutor, IngestionResult
from core.status_codes import StatusCode
from core.transport import (
    TransportConfig,
    TransportError,
    build_session,
    validate_bitsight_api,
)

from db.mssql import MSSQLDatabase


# ---------------------------------------------------------------------
# Endpoint wiring (adjust ONLY if your BitSight endpoint differs)
# ---------------------------------------------------------------------
# If your actual endpoint is different, update ONLY this.
ASSET_RISK_MATRIX_PATH_TEMPLATE = "/ratings/v1/companies/{company_guid}/assets/risk-matrix"


# ---------------------------------------------------------------------
# Delta counters (extra beyond IngestionResult)
# ---------------------------------------------------------------------
@dataclass
class DeltaCounts:
    net_new: int = 0
    updated: int = 0
    unchanged: int = 0
    removed: int = 0


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _canonical_json(obj: Any) -> str:
    # Deterministic stable hashing
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _require_company_guid(args) -> str:
    cg = getattr(args, "company_guid", None)
    if not cg:
        raise ValueError("--company-guid is required for asset_risk_matrix ingestion")
    return str(cg).strip()


def _table_name() -> str:
    return "dbo.bitsight_asset_risk_matrix"


def _column_exists(db: MSSQLDatabase, column: str) -> bool:
    sql = """
    SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
      AND TABLE_NAME = 'bitsight_asset_risk_matrix'
      AND COLUMN_NAME = ?
    """
    return db.scalar(sql, (column,)) is not None


def _get_existing_row(db: MSSQLDatabase, company_guid: str) -> Optional[Dict[str, Any]]:
    cols = ["company_guid", "ingested_at", "raw_payload"]
    if _column_exists(db, "row_hash"):
        cols.append("row_hash")
    if _column_exists(db, "is_deleted"):
        cols.append("is_deleted")
    if _column_exists(db, "deleted_at"):
        cols.append("deleted_at")

    sql = f"SELECT {', '.join(cols)} FROM {_table_name()} WHERE company_guid = ?"
    row = db.fetchone_dict(sql, (company_guid,))
    return row


def _upsert_row(
    db: MSSQLDatabase,
    *,
    company_guid: str,
    ingested_at: datetime,
    raw_payload: str,
    row_hash: Optional[str],
    clear_deleted: bool,
) -> None:
    has_row_hash = _column_exists(db, "row_hash")
    has_is_deleted = _column_exists(db, "is_deleted")
    has_deleted_at = _column_exists(db, "deleted_at")

    # Build MERGE dynamically to avoid schema mismatch.
    set_clauses = ["target.ingested_at = source.ingested_at", "target.raw_payload = source.raw_payload"]
    insert_cols = ["company_guid", "ingested_at", "raw_payload"]
    insert_vals = ["source.company_guid", "source.ingested_at", "source.raw_payload"]

    if has_row_hash:
        set_clauses.append("target.row_hash = source.row_hash")
        insert_cols.append("row_hash")
        insert_vals.append("source.row_hash")

    if clear_deleted and has_is_deleted:
        set_clauses.append("target.is_deleted = 0")
        if has_deleted_at:
            set_clauses.append("target.deleted_at = NULL")
        if "is_deleted" not in insert_cols:
            insert_cols.append("is_deleted")
            insert_vals.append("0")
        if has_deleted_at and "deleted_at" not in insert_cols:
            insert_cols.append("deleted_at")
            insert_vals.append("NULL")

    sql = f"""
    MERGE {_table_name()} AS target
    USING (SELECT ? AS company_guid, ? AS ingested_at, ? AS raw_payload{", ? AS row_hash" if has_row_hash else ""}) AS source
        ON target.company_guid = source.company_guid
    WHEN MATCHED THEN
        UPDATE SET {", ".join(set_clauses)}
    WHEN NOT MATCHED THEN
        INSERT ({", ".join(insert_cols)})
        VALUES ({", ".join(insert_vals)});
    """

    params: Tuple[Any, ...]
    if has_row_hash:
        params = (company_guid, ingested_at, raw_payload, row_hash or "")
    else:
        params = (company_guid, ingested_at, raw_payload)

    db.execute(sql, params)


def _soft_delete_or_delete(db: MSSQLDatabase, company_guid: str) -> None:
    has_is_deleted = _column_exists(db, "is_deleted")
    has_deleted_at = _column_exists(db, "deleted_at")

    if has_is_deleted:
        if has_deleted_at:
            db.execute(
                f"UPDATE {_table_name()} SET is_deleted = 1, deleted_at = ? WHERE company_guid = ?",
                (_utc_now(), company_guid),
            )
        else:
            db.execute(
                f"UPDATE {_table_name()} SET is_deleted = 1 WHERE company_guid = ?",
                (company_guid,),
            )
        return

    # Hard delete fallback
    db.execute(f"DELETE FROM {_table_name()} WHERE company_guid = ?", (company_guid,))


# ---------------------------------------------------------------------
# Fetch from API
# ---------------------------------------------------------------------
def _fetch_asset_risk_matrix(
    *,
    session,
    tcfg: TransportConfig,
    proxies: Optional[Dict[str, str]],
    company_guid: str,
) -> Any:
    base = tcfg.base_url.rstrip("/")
    path = ASSET_RISK_MATRIX_PATH_TEMPLATE.format(company_guid=company_guid)
    url = f"{base}{path}"

    logging.info("Fetching asset risk matrix: %s", url)

    resp = session.get(
        url,
        auth=(tcfg.api_key, ""),
        timeout=tcfg.timeout,
        proxies=proxies,
        verify=tcfg.verify_ssl,
        headers={"Accept": "application/json"},
    )

    if resp.status_code == 200:
        try:
            return resp.json()
        except Exception as e:
            raise TransportError(f"JSON parse failure: {e}", StatusCode.DATA_PARSE_ERROR, resp.status_code) from e

    if resp.status_code == 401:
        raise TransportError("Unauthorized", StatusCode.API_UNAUTHORIZED, resp.status_code)
    if resp.status_code == 403:
        raise TransportError("Forbidden", StatusCode.API_FORBIDDEN, resp.status_code)
    if resp.status_code == 404:
        raise TransportError("Not found", StatusCode.API_NOT_FOUND, resp.status_code)
    if resp.status_code == 429:
        raise TransportError("Rate limited", StatusCode.API_RATE_LIMITED, resp.status_code)
    if 500 <= resp.status_code <= 599:
        raise TransportError("Server error", StatusCode.API_SERVER_ERROR, resp.status_code)

    raise TransportError(
        f"Unexpected HTTP status {resp.status_code}",
        StatusCode.API_UNEXPECTED_RESPONSE,
        resp.status_code,
    )


# ---------------------------------------------------------------------
# Ingest runner
# ---------------------------------------------------------------------
def run(args) -> IngestionResult:
    """
    Entry point called by cli.py dispatch.
    Returns IngestionResult (with deterministic ExitCode/StatusCode).
    """
    store = ConfigStore()
    cfg: Config = store.load()
    cfg = cfg  # CLI already merges; safe default for direct calls.

    # CLI should have validated, but keep defensive.
    cfg.validate(require_api_key=True)

    company_guid = _require_company_guid(args)

    # MSSQL requirements
    if not (cfg.mssql_server and cfg.mssql_database and cfg.mssql_username and cfg.mssql_password):
        logging.error("Missing MSSQL configuration (server/database/username/password)")
        return IngestionResult(
            status_code=StatusCode.CONFIG_INCOMPLETE,
            exit_code=ExitCode.CONFIG_VALUE_MISSING,
            records_fetched=0,
            records_written=0,
            records_failed=0,
            duration_seconds=0.0,
            message="Missing MSSQL connection parameters",
        )

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

    # Transport preflight (consistent gate)
    validate_bitsight_api(session, tcfg, proxies)

    # DB
    db = MSSQLDatabase(
        server=cfg.mssql_server,
        database=cfg.mssql_database,
        username=cfg.mssql_username,
        password=cfg.mssql_password,
        driver=cfg.mssql_driver,
        encrypt=cfg.mssql_encrypt if cfg.mssql_encrypt is not None else True,
        trust_cert=cfg.mssql_trust_cert if cfg.mssql_trust_cert is not None else False,
        timeout=cfg.mssql_timeout or 30,
    )

    db.connect()

    # Delta counters (logged at end)
    deltas = DeltaCounts()
    dry_run = bool(getattr(args, "dry_run", False))
    show_progress = not bool(getattr(args, "no_progress", False))

    # -------------------------------------------------------------
    # Fetcher returns a 1-record iterable: the entire matrix snapshot
    # (Your table is company-scoped. Delta is snapshot-level by company.)
    # -------------------------------------------------------------
    def fetcher() -> Iterable[Any]:
        payload = _fetch_asset_risk_matrix(
            session=session,
            tcfg=tcfg,
            proxies=proxies,
            company_guid=company_guid,
        )
        return [payload]

    # -------------------------------------------------------------
    # Writer implements NET_NEW / UPDATED / UNCHANGED
    # Removals are applied only if --prune is provided and data is empty.
    # -------------------------------------------------------------
    def writer(record: Any) -> None:
        nonlocal deltas

        ingested_at = _utc_now()
        canonical = _canonical_json(record)
        row_hash = _sha256_hex(canonical)

        existing = _get_existing_row(db, company_guid)

        # If API returned an explicit empty result and operator wants prune,
        # treat as REMOVED.
        prune = bool(getattr(args, "prune", False))
        if prune and (record is None or record == {} or record == []):
            if existing:
                if dry_run:
                    logging.info("DRY-RUN: would remove asset_risk_matrix row for company_guid=%s", company_guid)
                else:
                    _soft_delete_or_delete(db, company_guid)
                deltas.removed += 1
            else:
                deltas.unchanged += 1
            return

        if not existing:
            # NET_NEW
            if dry_run:
                logging.info("DRY-RUN: would insert asset_risk_matrix for company_guid=%s", company_guid)
            else:
                _upsert_row(
                    db,
                    company_guid=company_guid,
                    ingested_at=ingested_at,
                    raw_payload=canonical,
                    row_hash=row_hash,
                    clear_deleted=True,
                )
            deltas.net_new += 1
            return

        # Compare hashes if available; else compare payload
        existing_hash = existing.get("row_hash") if isinstance(existing, dict) else None
        existing_payload = existing.get("raw_payload") if isinstance(existing, dict) else None

        changed = False
        if existing_hash:
            changed = (existing_hash != row_hash)
        else:
            changed = (str(existing_payload or "") != canonical)

        if not changed:
            deltas.unchanged += 1
            logging.info("UNCHANGED asset_risk_matrix | company_guid=%s", company_guid)
            return

        # UPDATED
        if dry_run:
            logging.info("DRY-RUN: would update asset_risk_matrix for company_guid=%s", company_guid)
        else:
            _upsert_row(
                db,
                company_guid=company_guid,
                ingested_at=ingested_at,
                raw_payload=canonical,
                row_hash=row_hash,
                clear_deleted=True,
            )
        deltas.updated += 1

    executor = IngestionExecutor(
        fetcher=fetcher,
        writer=writer,
        expected_min_records=0,
        show_progress=show_progress,
    )

    try:
        result = executor.run()

        # Commit/rollback boundaries are owned here (side effects)
        if result.exit_code in (ExitCode.SUCCESS, ExitCode.SUCCESS_EMPTY_RESULT, ExitCode.SUCCESS_PARTIAL_SCOPE):
            if not dry_run:
                db.commit()
            else:
                db.rollback()
        else:
            db.rollback()

        logging.info(
            "DELTA SUMMARY | endpoint=asset_risk_matrix company_guid=%s net_new=%d updated=%d unchanged=%d removed=%d",
            company_guid,
            deltas.net_new,
            deltas.updated,
            deltas.unchanged,
            deltas.removed,
        )

        return result

    finally:
        try:
            db.close()
        except Exception:
            pass


# ---------------------------------------------------------------------
# CLI-compatible entrypoints
# ---------------------------------------------------------------------
def main(args) -> IngestionResult:
    return run(args)


def cli(args) -> IngestionResult:
    return run(args)
