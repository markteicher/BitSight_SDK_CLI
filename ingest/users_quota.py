#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# BitSight User Quota endpoint
BITSIGHT_USER_QUOTA_ENDPOINT = "/ratings/v1/users/quota"


def fetch_user_quota(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Fetch user quota summary from BitSight.

    Endpoint:
        GET /ratings/v1/users/quota

    This endpoint returns a single, non-paginated summary object describing:
      - active/inactive user counts
      - total users
      - active license quota and remaining capacity
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_USER_QUOTA_ENDPOINT}"
    headers = {"Accept": "application/json"}

    ingested_at = datetime.utcnow()

    logging.info(f"Fetching user quota summary: {url}")

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
        "active_user_count": payload.get("active_user_count"),
        "inactive_user_count": payload.get("inactive_user_count"),
        "total_user_count": payload.get("total_user_count"),
        "total_active_quota": payload.get("total_active_quota"),
        "remaining_active_quota": payload.get("remaining_active_quota"),
        "ingested_at": ingested_at,
        "raw_payload": payload,
    }

    logging.info("User quota summary fetched successfully")
    return record
