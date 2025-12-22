#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

# BitSight Portfolio endpoint
BITSIGHT_PORTFOLIO_ENDPOINT = "/ratings/v2/portfolio"


def fetch_portfolio(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch the BitSight portfolio.
    Deterministic pagination using links.next when present.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_PORTFOLIO_ENDPOINT}"
    headers = {"Accept": "application/json"}

    records: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        logging.info(
            "Fetching portfolio: %s (limit=%d, offset=%d)",
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

        for item in results:
            records.append(_normalize_portfolio_record(item, ingested_at))

        links = payload.get("links") or {}
        next_link = links.get("next")

        if next_link:
            url = _absolutize_next(url, next_link)
            offset += limit
            continue

        if len(results) < limit:
            break

        offset += limit

    logging.info("Total portfolio records fetched: %d", len(records))
    return records


def _normalize_portfolio_record(obj: Dict[str, Any], ingested_at: datetime) -> Dict[str, Any]:
    """
    Map portfolio object into dbo.bitsight_portfolio schema.
    """

    company = obj.get("company") or {}
    rating = obj.get("rating") or {}

    tier = obj.get("tier") or {}
    relationship = obj.get("relationship") or {}

    subscription_type = obj.get("subscription_type")
    life_cycle = obj.get("life_cycle")

    # Some tenants return objects, some return strings/null. Handle both.
    subscription_type_name = subscription_type.get("name") if isinstance(subscription_type, dict) else subscription_type
    subscription_type_slug = subscription_type.get("slug") if isinstance(subscription_type, dict) else None

    life_cycle_name = life_cycle.get("name") if isinstance(life_cycle, dict) else life_cycle
    life_cycle_slug = life_cycle.get("slug") if isinstance(life_cycle, dict) else None

    return {
        "company_guid": company.get("guid"),
        "name": company.get("name"),
        "rating": rating.get("rating"),
        "rating_date": rating.get("rating_date"),
        "tier_name": tier.get("name"),
        "relationship_name": relationship.get("name"),
        "subscription_type_name": subscription_type_name,
        "subscription_type_slug": subscription_type_slug,
        "life_cycle_name": life_cycle_name,
        "life_cycle_slug": life_cycle_slug,
        "network_size_v4": obj.get("network_size_v4"),
        "added_date": obj.get("added_date"),
        "ingested_at": ingested_at,
        "raw_payload": obj,
    }


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
