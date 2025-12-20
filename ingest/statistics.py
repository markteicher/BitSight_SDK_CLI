#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# BitSight Statistics endpoint
BITSIGHT_STATISTICS_ENDPOINT = "/ratings/v1/statistics"


def fetch_statistics(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Fetch global BitSight statistics.
    This endpoint returns platform-wide aggregated metrics.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_STATISTICS_ENDPOINT}"
    headers = {"Accept": "application/json"}
    ingested_at = datetime.utcnow()

    logging.info(f"Fetching BitSight statistics: {url}")

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
