#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional

# BitSight User Quota endpoint
BITSIGHT_USER_QUOTA_ENDPOINT = "/ratings/v1/users/quota"


def fetch_user_quota(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch user quota information from BitSight.

    Endpoint:
        GET /ratings/v1/users/quota

    This endpoint is non-paginated and returns quota objects keyed by type.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_USER_QUOTA_ENDPOINT}"
    headers = {"Accept": "application/json"}

    ingested_at = datetime.utcnow()

    logging.info(f"Fetching user quota: {url}")

    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    payload = resp.json()

    records: List[Dict[str, Any]] = []

    for quota_type, values in payload.items():
        records.append(
            {
                "quota_type": quota_type,
                "total": values.get("total"),
                "used": values.get("used"),
                "remaining": values.get("remaining"),
                "ingested_at": ingested_at,
                "raw_payload": {quota_type: values},
            }
        )

    logging.info(f"User quota types fetched: {len(records)}")
    return records
