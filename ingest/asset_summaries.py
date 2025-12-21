#!/usr/bin/env python3

import logging
import requests
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

BITSIGHT_ASSET_SUMMARIES_ENDPOINT = "/ratings/v1/assets/summaries"


def fetch_asset_summaries(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch asset summaries from BitSight.

    Endpoint:
        GET /ratings/v1/assets/summaries

    This endpoint is NOT paginated and returns a summary-style payload.

    Auth:
        HTTP Basic Auth using api_key as username and blank password.
    """

    if not isinstance(base_url, str) or not base_url.strip():
        raise ValueError("base_url must be a non-empty string")

    base_url = base_url.rstrip("/")

    url = f"{base_url}{BITSIGHT_ASSET_SUMMARIES_ENDPOINT}"
    headers = {"Accept": "application/json"}
    ingested_at = datetime.now(timezone.utc)

    logging.info("Fetching asset summaries: %s", url)

    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    payload = resp.json()

    return [
        {
            "ingested_at": ingested_at,
            "raw_payload": payload,
        }
    ]
