#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# BitSight Asset Risk Matrix endpoint (per company)
BITSIGHT_ASSET_RISK_MATRIX_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/asset-risk-matrix"
)


def fetch_asset_risk_matrix(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Fetch Asset Risk Matrix for a single company.

    Endpoint:
        GET /ratings/v1/companies/{company_guid}/asset-risk-matrix

    This endpoint is NOT paginated and returns a matrix-style payload.

    Auth:
        HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    endpoint = BITSIGHT_ASSET_RISK_MATRIX_ENDPOINT.format(
        company_guid=company_guid
    )
    url = f"{base_url}{endpoint}"

    headers = {"Accept": "application/json"}
    ingested_at = datetime.utcnow()

    logging.info(
        f"Fetching asset risk matrix for company {company_guid}: {url}"
    )

    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    payload = resp.json()

    return {
        "company_guid": company_guid,
        "ingested_at": ingested_at,
        "raw_payload": payload,
    }
