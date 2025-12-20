#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List

BITSIGHT_USER_QUOTA_ENDPOINT = "/ratings/v1/users/quota"


def fetch_user_quota(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60
) -> List[Dict[str, Any]]:
    """
    Fetch user quota information from BitSight.
    This endpoint is non-paginated.
    """

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json"
    }

    url = f"{base_url}{BITSIGHT_USER_QUOTA_ENDPOINT}"
    logging.info(f"Fetching user quota: {url}")

    response = session.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()

    payload = response.json()
    ingested_at = datetime.utcnow()

    quota_records = []

    for quota_type, values in payload.items():
        quota_records.append({
            "quota_type": quota_type,
            "total": values.get("total"),
            "used": values.get("used"),
            "remaining": values.get("remaining"),
            "ingested_at": ingested_at,
            "raw_payload": {
                quota_type: values
            }
        })

    logging.info(f"User quota types fetched: {len(quota_records)}")
    return quota_records
