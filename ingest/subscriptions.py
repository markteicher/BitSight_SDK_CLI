#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

# BitSight Subscriptions endpoint
BITSIGHT_SUBSCRIPTIONS_ENDPOINT = "/ratings/v1/subscriptions"


def fetch_subscriptions(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch subscriptions from BitSight.
    Deterministic pagination using links.next when present.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_SUBSCRIPTIONS_ENDPOINT}"
    headers = {"Accept": "application/json"}

    records: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        logging.info(
            f"Fetching subscriptions: {url} (limit={limit}, offset={offset})"
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
            records.append(_normalize_subscription(obj, ingested_at))

        links = payload.get("links") or {}
        next_link = links.get("next")

        if next_link:
            url = _absolutize_next(url, next_link)
            offset += limit
            continue

        if len(results) < limit:
            break

        offset += limit

    logging.info(f"Total subscriptions fetched: {len(records)}")
    return records


def _normalize_subscription(
    obj: Dict[str, Any],
    ingested_at: datetime,
) -> Dict[str, Any]:
    """
    Map subscription object into dbo.bitsight_subscriptions schema.
    """

    company = obj.get("company") or {}
    subscription_type = obj.get("subscription_type") or {}
    life_cycle = obj.get("life_cycle") or {}

    return {
        "subscription_guid": obj.get("guid"),
        "company_guid": company.get("guid"),
        "subscription_type_name": subscription_type.get("name"),
        "subscription_type_slug": subscription_type.get("slug"),
        "life_cycle_name": life_cycle.get("name"),
        "life_cycle_slug": life_cycle.get("slug"),
        "start_date": obj.get("start_date"),
        "end_date": obj.get("end_date"),
        "ingested_at": ingested_at,
        "raw_payload": obj,
    }


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
