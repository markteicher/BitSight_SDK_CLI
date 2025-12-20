#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# BitSight Threat Statistics (Summaries) endpoint (v2)
# Docs: GET /threats/summaries
BITSIGHT_THREAT_STATISTICS_ENDPOINT = "/ratings/v2/threats/summaries"


def fetch_threat_statistics(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Fetch BitSight threat statistics (summaries).
    Endpoint: GET /ratings/v2/threats/summaries

    This endpoint returns aggregated metrics and is typically non-paginated.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_THREAT_STATISTICS_ENDPOINT}"
    headers = {"Accept": "application/json"}
    ingested_at = datetime.utcnow()

    logging.info(f"Fetching threat statistics: {url}")

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
        "ingested_at": ingested_at,
        "raw_payload": payload,
    }
