#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

# BitSight My Infrastructure endpoint
BITSIGHT_MY_INFRASTRUCTURE_ENDPOINT = "/ratings/v1/my-infrastructure"


def fetch_my_infrastructure(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch infrastructure assets associated with the authenticated organization.

    Endpoint:
        GET /ratings/v1/my-infrastructure

    Deterministic pagination using links.next when present.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_MY_INFRASTRUCTURE_ENDPOINT}"
    headers = {"Accept": "application/json"}

    records: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        logging.info(
            "Fetching my infrastructure: %s (limit=%d, offset=%d)",
            url,
            limit,
            offset,
        )

        resp = session.get(
            url,
            headers=headers,
            auth=(api_key, ""),
            params=params,
            timeout=timeout,
            proxies=proxies,
        )
        resp.raise_for_status()

        payload = resp.json()
        results = payload.get("results", [])

        for obj in results:
            records.append(_normalize_my_infrastructure(obj, ingested_at))

        links = payload.get("links") or {}
        next_link = links.get("next")

        if next_link:
            url = _absolutize_next(url, next_link)
            offset += limit
            continue

        if len(results) < limit:
            break

        offset += limit

    logging.info("Total infrastructure assets fetched: %d", len(records))
    return records


def _normalize_my_infrastructure(
    obj: Dict[str, Any],
    ingested_at: datetime,
) -> Dict[str, Any]:
    """
    Map my-infrastructure object into dbo.bitsight_my_infrastructure schema.
    """

    return {
        "asset_guid": obj.get("guid"),
        "asset_type": obj.get("type"),
        "ip_address": obj.get("ip_address"),
        "domain": obj.get("domain"),
        "first_seen_date": obj.get("first_seen_date"),
        "last_seen_date": obj.get("last_seen_date"),
        "ingested_at": ingested_at,
        "raw_payload": obj,
    }


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
