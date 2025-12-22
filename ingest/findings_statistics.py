#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# BitSight Findings Statistics endpoint (per company)
BITSIGHT_FINDINGS_STATISTICS_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/findings/statistics"
)


def fetch_findings_statistics(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Fetch findings statistics for a single company.

    Endpoint:
        GET /ratings/v1/companies/{company_guid}/findings/statistics

    This endpoint is non-paginated and returns aggregate statistics.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    endpoint = BITSIGHT_FINDINGS_STATISTICS_ENDPOINT.format(
        company_guid=company_guid
    )
    url = f"{base_url}{endpoint}"

    headers = {"Accept": "application/json"}
    ingested_at = datetime.utcnow()

    logging.info(
        "Fetching findings statistics for company %s: %s",
        company_guid,
        url,
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
        "open_findings": payload.get("open"),
        "closed_findings": payload.get("closed"),
        "risk_vector_breakdown": payload.get("risk_vector"),
        "severity_breakdown": payload.get("severity"),
        "grade_breakdown": payload.get("grade"),
        "ingested_at": ingested_at,
        "raw_payload": payload,
    }
