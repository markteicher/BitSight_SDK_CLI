#!/usr/bin/env python3
"""
ingest/assets.py

Net-new + update + removal ingestion for BitSight "assets" style list endpoints.

Design:
- Fetch full current set from API (paged)
- Upsert by stable primary key
- Mark removals when a previously-seen key no longer appears
- Works with IngestionExecutor
- Respects --dry-run (no DB writes, still computes counts)

Expected table (MSSQL) shape (minimum required columns):
  dbo.bitsight_assets (
      asset_key    NVARCHAR(255) NOT NULL PRIMARY KEY,
      ingested_at  DATETIME2(7)   NOT NULL,
      is_active    BIT           NOT NULL,
      payload_hash CHAR(64)      NOT NULL,
      raw_payload  NVARCHAR(MAX) NOT NULL
  )

If your schema uses a different table/key, adjust:
- TABLE_NAME
- _asset_key(record)
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

import requests

from core.ingestion import IngestionExecutor, IngestionResult
from core.status_codes import StatusCode
from core.exit_codes import ExitCode

TABLE_NAME = "dbo.bitsight_assets"
ENDPOINT = "/ratings/v1/assets"   # adjust if your repo uses a different assets endpoint
PAGE_SIZE = 100


# ============================================================
# Key + hashing
# ============================================================

def _asset_key(rec: Dict[str, Any]) -> str:
    """
    Stable primary key resolver.

    Tries common BitSight patterns first. Falls back to a deterministic composite key.
    """
    for k in ("guid", "asset_guid", "id", "uuid", "temporary_id"):
        v = rec.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()

    # Composite fallback (still deterministic, but only as good as the fields present)
    parts = [
        str(rec.get("company_guid", "")).strip(),
        str(rec.get("asset_type", "")).strip(),
        str(rec.get("value", "")).strip(),
        str(rec.get("ip_address", "")).strip(),
        str(rec.get("domain", "")).strip(),
    ]
    key = "|".join(p for p in parts if p)
    if not key:
        # last resort: hash the record itself
        key = hashlib.sha256(json.dumps(rec, sort_keys=True).encode()).hexdigest()
    return key


def _payload_hash(rec: Dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(rec, sort_keys=True).encode()).hexdigest()


# ============================================================
# Fetcher (paged)
# ============================================================

def fetch_assets(
    *,
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int,
    proxies: Optional[Dict[str, str]],
    page_size: int = PAGE_SIZE,
) -> Iterable[Dict[str, Any]]:
    """
    Fetch all assets.

    Supports both:
      - {"results":[...], "links":{"next":...}} style
      - plain list payloads (rare, but handled)
    """
    base_url = base_url.rstrip("/")
    url = f"{base_url}{ENDPOINT}"
    offset = 0

    out: List[Dict[str, Any]] = []

    while True:
        params = {"limit": page_size, "offset": offset}
        logging.info("Fetching assets page | limit=%d offset=%d", page_size, offset)

        resp = session.get(
            url,
            params=params,
            auth=(api_key, ""),
            headers={"Accept": "application/json"},
            timeout=timeout,
            proxies=proxies,
        )
        resp.raise_for_status()

        payload = resp.json()

        if isinstance(payload, list):
            out.extend(payload)
            break

        results = payload.get("results")
        if not isinstance(results, list):
            raise ValueError("Unexpected assets payload: missing 'results' list")

        out.extend(results)

        links = payload.get("links") or {}
        nxt = links.get("next")
        if not nxt:
            break

        # If API is offset-based, we can just increment offset safely.
        # If API is URL-based, we still follow offset increments deterministically.
        offset += page_size

    return out


# ============================================================
# Writer factory
# ============================================================

def make_writer(
    *,
    db,
    dry_run: bool,
    state: Dict[str, Any],
) -> Callable[[Dict[str, Any]], None]:
    """
    writer() performs:
      - detect net-new vs update vs unchanged
      - upsert active records
      - tracks seen keys in state for removal pass
    """

    def writer(rec: Dict[str, Any]) -> None:
        key = _asset_key(rec)
        h = _payload_hash(rec)
        now = datetime.now(timezone.utc)

        state["seen_keys"].add(key)

        existing = db.scalar(
            f"SELECT payload_hash FROM {TABLE_NAME} WHERE asset_key = ?",
            (key,),
        )

        if existing is None:
            state["net_new"] += 1
            if dry_run:
                return

            db.execute(
                f"""
                INSERT INTO {TABLE_NAME} (asset_key, ingested_at, is_active, payload_hash, raw_payload)
                VALUES (?, ?, 1, ?, ?)
                """,
                (key, now, h, json.dumps(rec)),
            )
            return

        if str(existing) == h:
            state["unchanged"] += 1
            if dry_run:
                return

            # still ensure it remains active (in case it was previously deactivated)
            db.execute(
                f"""
                UPDATE {TABLE_NAME}
                   SET is_active = 1,
                       ingested_at = ?
                 WHERE asset_key = ?
                """,
                (now, key),
            )
            return

        state["updated"] += 1
        if dry_run:
            return

        db.execute(
            f"""
            UPDATE {TABLE_NAME}
               SET ingested_at = ?,
                   is_active = 1,
                   payload_hash = ?,
                   raw_payload = ?
             WHERE asset_key = ?
            """,
            (now, h, json.dumps(rec), key),
        )

    return writer


def _apply_removals(*, db, dry_run: bool, seen_keys: Set[str], state: Dict[str, Any]) -> None:
    """
    Any previously active asset_key not seen in current run becomes inactive.
    """
    rows = db.scalar(
        f"SELECT COUNT(1) FROM {TABLE_NAME} WHERE is_active = 1",
        (),
    )
    _ = rows  # count used only for sanity if needed

    # Fetch active keys (streaming would be nicer, but keep interface minimal)
    active_keys = db.scalar(
        f"""
        SELECT STRING_AGG(asset_key, '||')
        FROM {TABLE_NAME}
        WHERE is_active = 1
        """,
        (),
    )

    if not active_keys:
        return

    active_list = str(active_keys).split("||")
    to_deactivate = [k for k in active_list if k and k not in seen_keys]

    state["removed"] = len(to_deactivate)
    if dry_run or not to_deactivate:
        return

    now = datetime.now(timezone.utc)
    for k in to_deactivate:
        db.execute(
            f"""
            UPDATE {TABLE_NAME}
               SET is_active = 0,
                   ingested_at = ?
             WHERE asset_key = ?
            """,
            (now, k),
        )


# ============================================================
# Entry point
# ============================================================

def run(args) -> IngestionResult:
    """
    Expected args:
      args.session (requests.Session)
      args.base_url (str)
      args.api_key (str)
      args.timeout (int)
      args.proxies (dict|None)
      args.db (DatabaseInterface)
      args.dry_run (bool)
      args.no_progress (bool)
    """

    state = {
        "seen_keys": set(),
        "net_new": 0,
        "updated": 0,
        "removed": 0,
        "unchanged": 0,
    }

    executor = IngestionExecutor(
        fetcher=lambda: fetch_assets(
            session=args.session,
            base_url=args.base_url,
            api_key=args.api_key,
            timeout=args.timeout,
            proxies=getattr(args, "proxies", None),
        ),
        writer=make_writer(
            db=args.db,
            dry_run=bool(getattr(args, "dry_run", False)),
            state=state,
        ),
        expected_min_records=0,
        show_progress=not bool(getattr(args, "no_progress", False)),
    )

    result = executor.run()

    # Removal pass (post-fetch, post-write)
    try:
        _apply_removals(
            db=args.db,
            dry_run=bool(getattr(args, "dry_run", False)),
            seen_keys=state["seen_keys"],
            state=state,
        )
    except Exception:
        logging.exception("Removal pass failed")
        return IngestionResult(
            status_code=StatusCode.INGESTION_STATE_UPDATE_FAILED,
            exit_code=ExitCode.INGEST_STATE_CORRUPT,
            records_fetched=result.records_fetched,
            records_written=result.records_written,
            records_failed=result.records_failed,
            duration_seconds=result.duration_seconds,
            message="Removal pass failed",
        )

    logging.info(
        "ASSETS DELTA | net_new=%d updated=%d removed=%d unchanged=%d dry_run=%s",
        state["net_new"],
        state["updated"],
        state["removed"],
        state["unchanged"],
        bool(getattr(args, "dry_run", False)),
    )

    # Keep executor’s success semantics, but if *everything* was removed, still OK.
    return result
