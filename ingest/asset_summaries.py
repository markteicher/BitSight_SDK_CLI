#!/usr/bin/env python3
"""
ingest/asset_summaries.py

Asset summaries ingestion (singleton snapshot endpoint).

Semantics:
- Single snapshot per run
- Net-new on first ingest
- Update on payload change
- No removal concept (API does not support it)
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable

import requests

from core.ingestion import IngestionExecutor, IngestionResult
from core.status_codes import StatusCode
from core.exit_codes import ExitCode
from core.database_interface import DatabaseInterface

BITSIGHT_ASSET_SUMMARIES_ENDPOINT = "/ratings/v1/assets/summaries"


# ============================================================
# Fetcher
# ============================================================

def fetch_asset_summaries(
    *,
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int,
    proxies: Dict[str, str] | None,
) -> Iterable[Dict[str, Any]]:
    base_url = base_url.rstrip("/")
    url = f"{base_url}{BITSIGHT_ASSET_SUMMARIES_ENDPOINT}"

    logging.info("Fetching asset summaries: %s", url)

    resp = session.get(
        url,
        auth=(api_key, ""),
        headers={"Accept": "application/json"},
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    payload = resp.json()

    return [
        {
            "snapshot_key": "global",
            "payload": payload,
        }
    ]


# ============================================================
# Writer
# ============================================================

def make_writer(
    *,
    db: DatabaseInterface,
    dry_run: bool,
):
    def writer(record: Dict[str, Any]) -> None:
        snapshot_key = record["snapshot_key"]
        payload = record["payload"]

        payload_json = json.dumps(payload, sort_keys=True)
        payload_hash = hashlib.sha256(payload_json.encode()).hexdigest()
        ingested_at = datetime.now(timezone.utc)

        existing_hash = db.scalar(
            """
            SELECT payload_hash
            FROM dbo.bitsight_asset_summaries
            WHERE snapshot_key = ?
            """,
            (snapshot_key,),
        )

        if existing_hash == payload_hash:
            logging.debug("Asset summaries unchanged — skipping update")
            return

        if dry_run:
            logging.info("DRY-RUN: asset summaries would be written")
            return

        db.execute(
            """
            MERGE dbo.bitsight_asset_summaries AS target
            USING (SELECT ? AS snapshot_key) AS src
            ON target.snapshot_key = src.snapshot_key
            WHEN MATCHED THEN
              UPDATE SET
                ingested_at = ?,
                payload_hash = ?,
                raw_payload = ?
            WHEN NOT MATCHED THEN
              INSERT (snapshot_key, ingested_at, payload_hash, raw_payload)
              VALUES (?, ?, ?, ?);
            """,
            (
                snapshot_key,
                ingested_at,
                payload_hash,
                payload_json,
                snapshot_key,
                ingested_at,
                payload_hash,
                payload_json,
            ),
        )

    return writer


# ============================================================
# Entry point
# ============================================================

def run(args) -> IngestionResult:
    executor = IngestionExecutor(
        fetcher=lambda: fetch_asset_summaries(
            session=args.session,
            base_url=args.base_url,
            api_key=args.api_key,
            timeout=args.timeout,
            proxies=args.proxies,
        ),
        writer=make_writer(
            db=args.db,
            dry_run=args.dry_run,
        ),
        expected_min_records=1,
        show_progress=not args.no_progress,
    )

    return executor.run()
