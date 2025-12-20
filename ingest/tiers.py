#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

# BitSight Tiers endpoint
BITSIGHT_TIERS_ENDPOINT = "/ratings/v1/tiers"


def fetch_tiers(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch tiers from BitSight.
    This endpoint is non-paginated.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_TIERS_ENDPOINT}"
    headers = {"Accept": "application/json"}

    ingested_at = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    logging.info(f"Fetching tiers: {url}")

    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    payload = resp.json()
    results = payload.get("results", payload)

    for obj in results:
        records.append({
            "tier_slug": obj.get("slug"),
            "ingested_at": ingested_at,
            "raw_payload": obj,
        })

    logging.info(f"Total tiers fetched: {len(records)}")
    return records
