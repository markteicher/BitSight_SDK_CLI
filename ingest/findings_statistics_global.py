#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# BitSight Global Findings Statistics endpoint
BITSIGHT_FINDINGS_STATISTICS_GLOBAL_ENDPOINT = "/ratings/v1/findings/statistics"


def fetch_findings_statistics_global(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Fetch global findings statistics across all companies.

    Endpoint:
        GET /ratings/v1/findings/statistics

    This endpoint is non-paginated and returns aggregate statistics.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_FINDINGS_STATISTICS_GLOBAL_ENDPOINT}"
    headers = {"Accept": "application/json"}
    ingested_at = datetime.utcnow()

    logging.info("Fetching global findings statistics: %s", url)

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
        "open_findings": payload.get("open"),
        "closed_findings": payload.get("closed"),
        "risk_vector_breakdown": payload.get("risk_vector"),
        "severity_breakdown": payload.get("severity"),
        "grade_breakdown": payload.get("grade"),
        "ingested_at": ingested_at,
        "raw_payload": payload,
    }
