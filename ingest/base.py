#!/usr/bin/env python3
"""
ingest/base.py

Common ingestion foundation for BitSight SDK + CLI ingest modules.

Goals:
- One place for: paging, key+hash helpers, delta tracking, dry-run behavior
- Supports: net-new + update + removal (soft delete via is_active)
- Works cleanly with core.ingestion.IngestionExecutor

This module is intentionally backend-agnostic: it expects an already-connected
DatabaseInterface-compatible object with .execute/.scalar/.commit/.rollback.

Required table conventions (minimum):
- Primary key column (string-ish): <pk_col>
- is_active BIT (soft delete)
- payload_hash CHAR(64) (change detection)
- raw_payload NVARCHAR(MAX) (full record)
- ingested_at DATETIME2(7)

If a table differs, pass the correct column names via TableSpec.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests


# ============================================================
# Table spec
# ============================================================

@dataclass(frozen=True)
class TableSpec:
    table: str
    pk_col: str
    is_active_col: str = "is_active"
    ingested_at_col: str = "ingested_at"
    payload_hash_col: str = "payload_hash"
    raw_payload_col: str = "raw_payload"


# ============================================================
# Delta counters
# ============================================================

@dataclass
class DeltaState:
    seen_keys: Set[str]
    net_new: int = 0
    updated: int = 0
    removed: int = 0
    unchanged: int = 0

    def __init__(self) -> None:
        self.seen_keys = set()


# ============================================================
# Hashing + JSON helpers
# ============================================================

def stable_json(obj: Any) -> str:
    """
    Stable JSON encoding for hashing + storage.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def payload_hash(obj: Any) -> str:
    return hashlib.sha256(stable_json(obj).encode("utf-8")).hexdigest()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ============================================================
# Paging
# ============================================================

def fetch_paged_results(
    *,
    session: requests.Session,
    base_url: str,
    api_key: str,
    endpoint: str,
    timeout: int,
    proxies: Optional[Dict[str, str]],
    page_size: int = 100,
    params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch ALL records from a BitSight endpoint that uses:
      {"count": N, "links": {"next": ...}, "results": [...]}

    Also tolerates a plain list JSON payload.

    Deterministic: uses offset increments, not the next URL.
    """
    base_url = (base_url or "").strip().rstrip("/")
    if not base_url:
        raise ValueError("base_url is required")

    endpoint = (endpoint or "").strip()
    if not endpoint.startswith("/"):
        raise ValueError("endpoint must start with '/'")

    url = f"{base_url}{endpoint}"
    offset = 0
    out: List[Dict[str, Any]] = []

    p = dict(params or {})
    while True:
        p.update({"limit": page_size, "offset": offset})
        logging.info("Fetching page | endpoint=%s limit=%d offset=%d", endpoint, page_size, offset)

        resp = session.get(
            url,
            params=p,
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
            raise ValueError(f"Unexpected payload from {endpoint}: missing 'results' list")

        out.extend(results)

        links = payload.get("links") or {}
        nxt = links.get("next")
        if not nxt:
            break

        offset += page_size

    return out


# ============================================================
# DB helpers (generic)
# ============================================================

def select_payload_hash(db, spec: TableSpec, key: str) -> Optional[str]:
    sql = f"SELECT {spec.payload_hash_col} FROM {spec.table} WHERE {spec.pk_col} = ?"
    v = db.scalar(sql, (key,))
    return None if v is None else str(v)


def upsert_active_record(
    *,
    db,
    spec: TableSpec,
    key: str,
    now: datetime,
    h: str,
    raw_json: str,
    exists: bool,
    changed: bool,
    dry_run: bool,
) -> None:
    """
    Upsert semantics:
      - if not exists: INSERT active
      - if exists and changed: UPDATE active + payload_hash + raw_payload
      - if exists and unchanged: ensure active + touch ingested_at
    """
    if dry_run:
        return

    if not exists:
        sql = f"""
        INSERT INTO {spec.table}
            ({spec.pk_col}, {spec.ingested_at_col}, {spec.is_active_col}, {spec.payload_hash_col}, {spec.raw_payload_col})
        VALUES (?, ?, 1, ?, ?)
        """
        db.execute(sql, (key, now, h, raw_json))
        return

    if changed:
        sql = f"""
        UPDATE {spec.table}
           SET {spec.ingested_at_col} = ?,
               {spec.is_active_col} = 1,
               {spec.payload_hash_col} = ?,
               {spec.raw_payload_col} = ?
         WHERE {spec.pk_col} = ?
        """
        db.execute(sql, (now, h, raw_json, key))
        return

    # unchanged: still mark active and touch ingested_at for “last seen”
    sql = f"""
    UPDATE {spec.table}
       SET {spec.ingested_at_col} = ?,
           {spec.is_active_col} = 1
     WHERE {spec.pk_col} = ?
    """
    db.execute(sql, (now, key))


def deactivate_missing_records(
    *,
    db,
    spec: TableSpec,
    now: datetime,
    seen_keys: Set[str],
    dry_run: bool,
) -> int:
    """
    Soft-delete anything currently active that is NOT in seen_keys.

    Returns: number of records deactivated.
    """
    # Pull active keys; keep this generic (works everywhere)
    rows = db.scalar(
        f"SELECT STRING_AGG(CAST({spec.pk_col} AS NVARCHAR(4000)), '||') "
        f"FROM {spec.table} WHERE {spec.is_active_col} = 1",
        (),
    )

    if not rows:
        return 0

    active_keys = [k for k in str(rows).split("||") if k]
    to_deactivate = [k for k in active_keys if k not in seen_keys]

    if dry_run or not to_deactivate:
        return len(to_deactivate)

    for k in to_deactivate:
        sql = f"""
        UPDATE {spec.table}
           SET {spec.is_active_col} = 0,
               {spec.ingested_at_col} = ?
         WHERE {spec.pk_col} = ?
        """
        db.execute(sql, (now, k))

    return len(to_deactivate)


# ============================================================
# Writer factory (net-new + update + unchanged)
# ============================================================

def make_delta_writer(
    *,
    db,
    spec: TableSpec,
    key_fn: Callable[[Dict[str, Any]], str],
    delta: DeltaState,
    dry_run: bool,
) -> Callable[[Dict[str, Any]], None]:
    """
    Returns a writer(record) suitable for IngestionExecutor.
    """
    if not callable(key_fn):
        raise TypeError("key_fn must be callable")

    def writer(rec: Dict[str, Any]) -> None:
        key = str(key_fn(rec)).strip()
        if not key:
            raise ValueError("Record key resolved to empty string")

        delta.seen_keys.add(key)

        h = payload_hash(rec)
        raw = stable_json(rec)
        now = utc_now()

        existing = select_payload_hash(db, spec, key)
        if existing is None:
            delta.net_new += 1
            upsert_active_record(
                db=db,
                spec=spec,
                key=key,
                now=now,
                h=h,
                raw_json=raw,
                exists=False,
                changed=True,
                dry_run=dry_run,
            )
            return

        if existing == h:
            delta.unchanged += 1
            upsert_active_record(
                db=db,
                spec=spec,
                key=key,
                now=now,
                h=h,
                raw_json=raw,
                exists=True,
                changed=False,
                dry_run=dry_run,
            )
            return

        delta.updated += 1
        upsert_active_record(
            db=db,
            spec=spec,
            key=key,
            now=now,
            h=h,
            raw_json=raw,
            exists=True,
            changed=True,
            dry_run=dry_run,
        )

    return writer


# ============================================================
# Reporting
# ============================================================

def log_delta(
    *,
    label: str,
    delta: DeltaState,
    dry_run: bool,
) -> None:
    logging.info(
        "%s DELTA | net_new=%d updated=%d removed=%d unchanged=%d dry_run=%s",
        label,
        delta.net_new,
        delta.updated,
        delta.removed,
        delta.unchanged,
        dry_run,
    )
