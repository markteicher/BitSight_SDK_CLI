#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# BitSight Static Data endpoint
BITSIGHT_STATIC_DATA_ENDPOINT = "/ratings/v1/static-data"


def fetch_static_data(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Fetch static data from BitSight.
    This endpoint returns reference / lookup datasets.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_STATIC_DATA_ENDPOINT}"
    headers = {"Accept": "application/json"}

    ingested_at = datetime.utcnow()

    logging.info(f"Fetching static data: {url}")

    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    payload = resp.json()

    record = {
        "ingested_at": ingested_at,
        "raw_payload": payload,
    }

    return record
