#!/usr/bin/env python3
"""
ingest/company_infrastructure.py

BitSight SDK + CLI — Company Infrastructure ingestion

Implements:
- net-new inserts
- updates (change-detected)
- removals (records no longer returned by API)

Target table (per your schema):
  dbo.bitsight_company_infrastructure
    PK: (company_guid, temporary_id)

CLI:
  bitsight-cli ingest company-infrastructure --company-guid <GUID> [--dry-run] [--no-progress]
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from core.config import ConfigStore, Config, ConfigError
from core.db_router import DatabaseRouter
from core.exit_codes import ExitCode
from core.ingestion import IngestionExecutor, IngestionResult
from core.transport import TransportConfig, TransportError, build_session

# NOTE: If your repo uses a different path, change it here.
# Common BitSight pattern is company-scoped infrastructure.
BITSIGHT_COMPANY_INFRA_ENDPOINT_TMPL = "/ratings/v1/companies/{company_guid}/infrastructure"
DEFAULT_PAGE_LIMIT = 100


# ============================================================
# Delta accounting
# ============================================================

@dataclass
class DeltaCounts:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    removed: int = 0

    def log(self, *, label: str, dry_run: bool) -> None:
        prefix = "DRY-RUN " if dry_run else ""
        logging.info(
            "%s%s delta | inserted=%d updated=%d unchanged=%d removed=%d",
            prefix,
            label,
            self.inserted,
            self.updated,
            self.unchanged,
            self.removed,
        )


# ============================================================
# Helpers
# ============================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _payload_hash(payload: Any) -> str:
    # Deterministic hash for change detection; avoids DB collation weirdness.
    import hashlib

    b = _stable_json(payload).encode("utf-8", errors="replace")
    return hashlib.sha256(b).hexdigest()


def _parse_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("true", "t", "yes", "y", "1"):
        return True
    if s in ("false", "f", "no", "n", "0"):
        return False
    return None


def _as_date(v: Any) -> Optional[str]:
    # Keep as ISO date string; driver will usually coerce. If not, store NULL.
    if not v:
        return None
    s = str(v).strip()
    return s if s else None


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


def _endpoint(company_guid: str) -> str:
    return BITSIGHT_COMPANY_INFRA_ENDPOINT_TMPL.format(company_guid=company_guid)


def _fetch_paged(
    *,
    session: requests.Session,
    base_url: str,
    api_key: str,
    endpoint: str,
    timeout: int,
    proxies: Optional[Dict[str, str]],
    limit: int = DEFAULT_PAGE_LIMIT,
) -> List[Dict[str, Any]]:
    base = base_url.rstrip("/")
    url = f"{base}{endpoint}"

    out: List[Dict[str, Any]] = []
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        logging.debug("GET %s params=%s", url, params)

        r = session.get(
            url,
            params=params,
            auth=(api_key, ""),
            timeout=timeout,
            proxies=proxies,
            headers={"Accept": "application/json"},
        )
        r.raise_for_status()
        data = r.json()

        # Supports either {"results": [...], "count": n} or a raw list.
        if isinstance(data, dict) and isinstance(data.get("results"), list):
            batch = data["results"]
            out.extend(batch)

            count = data.get("count")
            if isinstance(count, int):
                offset += len(batch)
                if offset >= count or len(batch) == 0:
                    break
                continue

            # Fallback if count missing: stop when less than limit.
            if len(batch) < limit:
                break
            offset += len(batch)
            continue

        if isinstance(data, list):
            out = data
            break

        raise ValueError("Unexpected infrastructure response shape")

    return out


def _map_record(company_guid: str, rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map API record into dbo.bitsight_company_infrastructure columns.
    Anything unknown stays NULL; full payload stored in raw_payload.
    """
    # temporary_id is required by schema PK; try common keys.
    temporary_id = (
        rec.get("temporary_id")
        or rec.get("temporaryId")
        or rec.get("id")
        or rec.get("guid")
        or rec.get("value")  # last-resort
    )
    if not temporary_id:
        raise ValueError("Infrastructure record missing temporary_id/id")

    mapped = {
        "company_guid": company_guid,
        "temporary_id": str(temporary_id),

        "value": rec.get("value"),
        "asset_type": rec.get("asset_type") or rec.get("type"),
        "source": rec.get("source"),
        "country": rec.get("country"),

        "start_date": _as_date(rec.get("start_date") or rec.get("startDate")),
        "end_date": _as_date(rec.get("end_date") or rec.get("endDate")),

        "is_active": _parse_bool(rec.get("is_active") or rec.get("active")),
        "attributed_guid": rec.get("attributed_guid") or rec.get("attributedCompanyGuid"),
        "attributed_name": rec.get("attributed_name") or rec.get("attributedCompanyName"),
        "ip_count": rec.get("ip_count") or rec.get("ipCount"),
        "is_suppressed": _parse_bool(rec.get("is_suppressed") or rec.get("suppressed")),
        "asn": rec.get("asn"),
        "tags": _stable_json(rec.get("tags")) if rec.get("tags") is not None else None,

        "ingested_at": _utcnow().isoformat(),
        "raw_payload": _stable_json(rec),
        "payload_hash": _payload_hash(rec),
    }
    return mapped


# ============================================================
# DB operations
# ============================================================

def _load_existing_hashes(db, company_guid: str) -> Dict[str, str]:
    sql = """
        SELECT temporary_id,
               CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', raw_payload), 2) AS payload_hash
        FROM dbo.bitsight_company_infrastructure
        WHERE company_guid = ?
    """
    rows = db.query(sql, (company_guid,))  # requires MSSQLDatabase.query(); if not present, see NOTE below
    return {str(tid): str(h) for (tid, h) in rows}


def _upsert_one(db, row: Dict[str, Any], *, existed: bool) -> None:
    # We intentionally do UPDATE-by-PK; INSERT if not existed.
    if not existed:
        sql = """
        INSERT INTO dbo.bitsight_company_infrastructure
        (company_guid, temporary_id, value, asset_type, source, country, start_date, end_date,
         is_active, attributed_guid, attributed_name, ip_count, is_suppressed, asn, tags,
         ingested_at, raw_payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            row["company_guid"], row["temporary_id"], row["value"], row["asset_type"], row["source"],
            row["country"], row["start_date"], row["end_date"], row["is_active"],
            row["attributed_guid"], row["attributed_name"], row["ip_count"], row["is_suppressed"],
            row["asn"], row["tags"], row["ingested_at"], row["raw_payload"],
        )
        db.execute(sql, params)
        return

    sql = """
    UPDATE dbo.bitsight_company_infrastructure
    SET value = ?,
        asset_type = ?,
        source = ?,
        country = ?,
        start_date = ?,
        end_date = ?,
        is_active = ?,
        attributed_guid = ?,
        attributed_name = ?,
        ip_count = ?,
        is_suppressed = ?,
        asn = ?,
        tags = ?,
        ingested_at = ?,
        raw_payload = ?
    WHERE company_guid = ? AND temporary_id = ?
    """
    params = (
        row["value"], row["asset_type"], row["source"], row["country"],
        row["start_date"], row["end_date"], row["is_active"], row["attributed_guid"],
        row["attributed_name"], row["ip_count"], row["is_suppressed"], row["asn"],
        row["tags"], row["ingested_at"], row["raw_payload"],
        row["company_guid"], row["temporary_id"],
    )
    db.execute(sql, params)


def _delete_removed(db, company_guid: str, keep_ids: Iterable[str]) -> int:
    keep = list({str(x) for x in keep_ids})
    if not keep:
        # If API returns empty list, we treat as "remove all for company"
        sql = "DELETE FROM dbo.bitsight_company_infrastructure WHERE company_guid = ?"
        db.execute(sql, (company_guid,))
        return -1  # caller can interpret as full purge

    # Chunk IN lists to avoid parameter limits.
    removed = 0
    chunk_size = 500

    for i in range(0, len(keep), chunk_size):
        chunk = keep[i : i + chunk_size]
        placeholders = ",".join(["?"] * len(chunk))
        sql = f"""
            DELETE FROM dbo.bitsight_company_infrastructure
            WHERE company_guid = ?
              AND temporary_id NOT IN ({placeholders})
        """
        db.execute(sql, (company_guid, *chunk))
        # we don’t have reliable rowcount via interface, so we count later via status query if needed
    return removed


# ============================================================
# Entrypoint
# ============================================================

def run(args) -> IngestionResult:
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

    company_guid = getattr(args, "company_guid", None)
    if not company_guid:
        msg = "--company-guid is required"
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

    delta = DeltaCounts()
    endpoint = _endpoint(company_guid)

    # --- preload current hashes for change detection
    # NOTE: This requires MSSQLDatabase to expose query(sql, params)->rows.
    # If your MSSQLDatabase only supports scalar/execute/executemany, add query().
    existing_hashes: Dict[str, str] = {}
    try:
        existing_hashes = _load_existing_hashes(db, company_guid)
    except AttributeError:
        logging.error(
            "MSSQLDatabase is missing .query(); add it or adapt _load_existing_hashes() to your DB class."
        )
        db.close()
        return IngestionResult(
            status_code=None,  # type: ignore[arg-type]
            exit_code=ExitCode.INTERNAL_CONTRACT_BREACH,
            records_fetched=0,
            records_written=0,
            records_failed=0,
            duration_seconds=0.0,
            message="DB contract missing: query()",
        )

    seen_ids: List[str] = []

    def fetcher() -> List[Dict[str, Any]]:
        raw = _fetch_paged(
            session=session,
            base_url=cfg.base_url,
            api_key=cfg.api_key or "",
            endpoint=endpoint,
            timeout=cfg.timeout,
            proxies=proxies,
        )
        mapped: List[Dict[str, Any]] = []
        for rec in raw:
            mapped.append(_map_record(company_guid, rec))
        return mapped

    def writer(row: Dict[str, Any]) -> None:
        tid = row["temporary_id"]
        seen_ids.append(tid)

        # Compare hashes: DB hash is derived from raw_payload in SQL, row hash from payload.
        # Either is acceptable as change detector; if mismatch, update.
        old_hash = existing_hashes.get(tid)
        new_hash = row["payload_hash"]

        existed = tid in existing_hashes

        if existed and old_hash and old_hash.lower() == new_hash.lower():
            delta.unchanged += 1
            return

        if dry_run:
            if existed:
                delta.updated += 1
            else:
                delta.inserted += 1
            return

        _upsert_one(db, row, existed=existed)

        if existed:
            delta.updated += 1
        else:
            delta.inserted += 1

    executor = IngestionExecutor(
        fetcher=fetcher,
        writer=writer,
        expected_min_records=0,
        show_progress=show_progress,
        dry_run=dry_run,  # requires corrected core/ingestion.py signature
    )

    try:
        result = executor.run()

        # Removals (delete rows not present anymore)
        current_set = set(seen_ids)
        removed_ids = [tid for tid in existing_hashes.keys() if tid not in current_set]

        if removed_ids:
            delta.removed = len(removed_ids)
            if not dry_run:
                # Delete “NOT IN (current)” in chunks (more efficient than per-row)
                _delete_removed(db, company_guid, current_set)

        delta.log(label="COMPANY_INFRASTRUCTURE", dry_run=dry_run)

        if dry_run:
            db.rollback()
        else:
            db.commit()

        return result

    except requests.HTTPError as e:
        db.rollback()
        logging.error("HTTP error: %s", str(e))
        return IngestionResult(
            status_code=None,  # type: ignore[arg-type]
            exit_code=ExitCode.NETWORK_BAD_RESPONSE,
            records_fetched=0,
            records_written=0,
            records_failed=0,
            duration_seconds=0.0,
            message=str(e),
        )

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
        logging.exception("company-infrastructure ingestion failed")
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
