#!/usr/bin/env python3

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

import requests

from core.ingestion import IngestionExecutor
from core.transport import TransportConfig, build_session, validate_bitsight_api, TransportError
from core.status_codes import StatusCode
from db.mssql import MSSQLDatabase

# BitSight Asset Risk Matrix endpoint (per company)
BITSIGHT_ASSET_RISK_MATRIX_ENDPOINT = "/ratings/v1/companies/{company_guid}/asset-risk-matrix"


# ----------------------------------------------------------------------
# ingest entrypoint(s)
# ----------------------------------------------------------------------
def main(args) -> None:
    company_guid = getattr(args, "company_guid", None)
    if not company_guid:
        raise SystemExit("Missing required --company-guid")
    AssetRiskMatrixIngest(company_guid=company_guid, no_progress=getattr(args, "no_progress", False)).run(args)


def run(args) -> None:
    main(args)


# ----------------------------------------------------------------------
# ingestion implementation
# ----------------------------------------------------------------------
class AssetRiskMatrixIngest:
    def __init__(self, company_guid: str, no_progress: bool = False):
        self.company_guid = company_guid
        self.no_progress = no_progress

    def run(self, args) -> None:
        api_key = getattr(args, "api_key", None)
        base_url = getattr(args, "base_url", None) or "https://api.bitsighttech.com"
        timeout = int(getattr(args, "timeout", None) or 60)

        server = getattr(args, "server", None)
        database = getattr(args, "database", None)
        username = getattr(args, "username", None)
        password = getattr(args, "password", None)

        if not api_key:
            raise SystemExit("Missing required --api-key")
        if not (server and database and username and password):
            raise SystemExit("Missing MSSQL connection args: --server --database --username --password")

        ingested_at = datetime.now(timezone.utc)

        cfg = TransportConfig(
            base_url=base_url,
            api_key=api_key,
            timeout=timeout,
            proxy_url=getattr(args, "proxy_url", None),
            verify_ssl=True,
        )
        session, proxies = build_session(cfg)

        try:
            validate_bitsight_api(session, cfg, proxies)
        except TransportError as te:
            logging.error(
                "API validation failed | status=%s http=%s message=%s",
                te.status_code.name,
                te.http_status,
                str(te),
            )
            raise SystemExit(1)

        db = MSSQLDatabase(
            server=server,
            database=database,
            username=username,
            password=password,
        )

        try:
            executor = IngestionExecutor(
                fetcher=lambda: fetch_asset_risk_matrix(
                    session=session,
                    base_url=cfg.base_url,
                    api_key=cfg.api_key,
                    company_guid=self.company_guid,
                    timeout=cfg.timeout,
                    proxies=proxies,
                    ingested_at=ingested_at,
                ),
                writer=lambda rec: upsert_asset_risk_matrix(db, rec),
                expected_min_records=0,
                show_progress=not self.no_progress,
            )

            result = executor.run()
            if result.status_code not in (StatusCode.OK, StatusCode.OK_NO_DATA, StatusCode.OK_PARTIAL):
                raise SystemExit(1)

        finally:
            db.close()


# ----------------------------------------------------------------------
# fetch + normalize
# ----------------------------------------------------------------------
def fetch_asset_risk_matrix(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
    ingested_at: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch Asset Risk Matrix for a single company.

    Endpoint:
        GET /ratings/v1/companies/{company_guid}/asset-risk-matrix

    This endpoint is NOT paginated and returns a matrix-style payload.

    Auth:
        HTTP Basic Auth using api_key as username and blank password.
    """

    base_url = (base_url or "").rstrip("/")
    ingested_at = ingested_at or datetime.now(timezone.utc)

    endpoint = BITSIGHT_ASSET_RISK_MATRIX_ENDPOINT.format(company_guid=company_guid)
    url = f"{base_url}{endpoint}"

    headers = {"Accept": "application/json"}

    logging.info("Fetching asset risk matrix for company %s: %s", company_guid, url)

    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    payload = resp.json()

    raw_payload = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return [
        {
            "company_guid": company_guid,
            "ingested_at": ingested_at,
            "raw_payload": raw_payload,
        }
    ]


# ----------------------------------------------------------------------
# db write (MSSQL)
# ----------------------------------------------------------------------
def upsert_asset_risk_matrix(db: MSSQLDatabase, rec: Dict[str, Any]) -> None:
    """
    Upsert into dbo.bitsight_asset_risk_matrix by company_guid (PK).

    Expected table:
      - company_guid UNIQUEIDENTIFIER NOT NULL (PK)
      - ingested_at  DATETIME2 NOT NULL
      - raw_payload  NVARCHAR(MAX) NOT NULL
    """
    sql = """
    MERGE dbo.bitsight_asset_risk_matrix AS target
    USING (SELECT CAST(? AS UNIQUEIDENTIFIER) AS company_guid) AS source
      ON target.company_guid = source.company_guid
    WHEN MATCHED THEN
      UPDATE SET
        ingested_at = ?,
        raw_payload = ?
    WHEN NOT MATCHED THEN
      INSERT (company_guid, ingested_at, raw_payload)
      VALUES (source.company_guid, ?, ?);
    """

    company_guid = rec.get("company_guid")
    if not company_guid:
        raise ValueError("company_guid missing from record")

    ingested_at = rec.get("ingested_at")
    raw_payload = rec.get("raw_payload")

    db.execute(
        sql,
        (
            str(company_guid),
            ingested_at,
            raw_payload,
            ingested_at,
            raw_payload,
        ),
    )
    db.commit()
