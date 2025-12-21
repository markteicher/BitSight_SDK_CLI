#!/usr/bin/env python3

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from core.ingestion import IngestionExecutor
from core.status_codes import StatusCode
from core.transport import TransportConfig, build_session, validate_bitsight_api, TransportError
from db.mssql import MSSQLDatabase

# BitSight Alerts endpoint
BITSIGHT_ALERTS_ENDPOINT = "/ratings/v1/alerts"


# ----------------------------------------------------------------------
# ingest entrypoint(s)
# ----------------------------------------------------------------------
def main(args) -> None:
    AlertsIngest(since=getattr(args, "since", None), no_progress=getattr(args, "no_progress", False)).run(args)


def run(args) -> None:
    main(args)


# ----------------------------------------------------------------------
# ingestion implementation
# ----------------------------------------------------------------------
class AlertsIngest:
    def __init__(self, since: Optional[str] = None, no_progress: bool = False):
        self.since = since
        self.no_progress = no_progress

    def run(self, args) -> None:
        api_key = getattr(args, "api_key", None)
        base_url = getattr(args, "base_url", None) or "https://api.bitsighttech.com"
        timeout = int(getattr(args, "timeout", None) or 60)

        # DB args (MSSQL only for now)
        server = getattr(args, "server", None)
        database = getattr(args, "database", None)
        username = getattr(args, "username", None)
        password = getattr(args, "password", None)

        if not api_key:
            raise SystemExit("Missing required --api-key")
        if not (server and database and username and password):
            raise SystemExit("Missing MSSQL connection args: --server --database --username --password")

        ingested_at = datetime.now(timezone.utc)

        # Transport/session
        cfg = TransportConfig(
            base_url=base_url,
            api_key=api_key,
            timeout=timeout,
            proxy_url=getattr(args, "proxy_url", None),
            verify_ssl=True,
        )
        session, proxies = build_session(cfg)

        # Validate API path + auth
        try:
            validate_bitsight_api(session, cfg, proxies)
        except TransportError as te:
            logging.error("API validation failed | status=%s http=%s message=%s", te.status_code.name, te.http_status, str(te))
            raise SystemExit(1)

        # DB handle
        db = MSSQLDatabase(
            server=server,
            database=database,
            username=username,
            password=password,
        )

        try:
            executor = IngestionExecutor(
                fetcher=lambda: fetch_alerts(
                    session=session,
                    base_url=cfg.base_url,
                    api_key=cfg.api_key,
                    timeout=cfg.timeout,
                    proxies=proxies,
                    since=self.since,
                    ingested_at=ingested_at,
                ),
                writer=lambda rec: upsert_alert(db, rec),
                expected_min_records=0,
                show_progress=not self.no_progress,
            )

            result = executor.run()

            # exit code is recorded in result; CLI will handle process exit policy centrally
            if result.status_code not in (StatusCode.OK, StatusCode.OK_NO_DATA, StatusCode.OK_PARTIAL):
                raise SystemExit(1)

        finally:
            db.close()


# ----------------------------------------------------------------------
# fetch + normalize
# ----------------------------------------------------------------------
def fetch_alerts(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
    since: Optional[str] = None,
    ingested_at: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch alerts from BitSight.
    Deterministic pagination using limit/offset (and honors links.next if present).
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    base_url = (base_url or "").rstrip("/")
    url = f"{base_url}{BITSIGHT_ALERTS_ENDPOINT}"
    headers = {"Accept": "application/json"}

    records: List[Dict[str, Any]] = []
    ingested_at = ingested_at or datetime.now(timezone.utc)

    limit = 100
    offset = 0

    while True:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if since:
            params["since"] = since

        logging.info("Fetching alerts: %s (limit=%d, offset=%d)", url, limit, offset)

        resp = session.get(
            url,
            headers=headers,
            auth=(api_key, ""),
            params=params,
            timeout=timeout,
            proxies=proxies,
        )
        resp.raise_for_status()

        payload = resp.json() or {}
        results = payload.get("results") or []

        for obj in results:
            records.append(_normalize_alert(obj, ingested_at))

        links = payload.get("links") or {}
        next_link = links.get("next")

        if next_link:
            # If BitSight returns an absolute next URL, follow it; otherwise keep offset loop deterministic.
            if isinstance(next_link, str) and (next_link.startswith("http://") or next_link.startswith("https://")):
                url = next_link
                offset += limit
                continue

        if len(results) < limit:
            break

        offset += limit

    logging.info("Total alerts fetched: %d", len(records))
    return records


def _normalize_alert(obj: Dict[str, Any], ingested_at: datetime) -> Dict[str, Any]:
    """
    Map alert object into dbo.bitsight_alerts schema:
      - alert_guid (PK)
      - ingested_at
      - raw_payload (JSON string)
    """
    alert_guid = obj.get("guid")
    raw_payload = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return {
        "alert_guid": alert_guid,
        "ingested_at": ingested_at,
        "raw_payload": raw_payload,
    }


# ----------------------------------------------------------------------
# db write (MSSQL)
# ----------------------------------------------------------------------
def upsert_alert(db: MSSQLDatabase, rec: Dict[str, Any]) -> None:
    """
    Upsert into dbo.bitsight_alerts by alert_guid (PK).
    """
    sql = """
    MERGE dbo.bitsight_alerts AS target
    USING (SELECT CAST(? AS UNIQUEIDENTIFIER) AS alert_guid) AS source
      ON target.alert_guid = source.alert_guid
    WHEN MATCHED THEN
      UPDATE SET
        ingested_at = ?,
        raw_payload = ?
    WHEN NOT MATCHED THEN
      INSERT (alert_guid, ingested_at, raw_payload)
      VALUES (source.alert_guid, ?, ?);
    """

    alert_guid = rec.get("alert_guid")
    if not alert_guid:
        raise ValueError("alert_guid missing from record")

    ingested_at = rec.get("ingested_at")
    raw_payload = rec.get("raw_payload")

    db.execute(
        sql,
        (
            str(alert_guid),
            ingested_at,
            raw_payload,
            ingested_at,
            raw_payload,
        ),
    )
    db.commit()
