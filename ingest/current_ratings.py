#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

# BitSight Current Ratings endpoint
BITSIGHT_CURRENT_RATINGS_ENDPOINT = "/ratings/v1/current-ratings"


def fetch_current_ratings(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch current security ratings for all companies.
    Deterministic pagination using links.next.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_CURRENT_RATINGS_ENDPOINT}"
    headers = {"Accept": "application/json"}

    records: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        logging.info(f"Fetching current ratings: {url} (limit={limit}, offset={offset})")

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
            records.append(_normalize_current_rating(obj, ingested_at))

        links = payload.get("links") or {}
        next_link = links.get("next")

        if next_link:
            url = _absolutize_next(url, next_link)
            offset += limit
            continue

        if len(results) < limit:
            break

        offset += limit

    logging.info(f"Total current ratings fetched: {len(records)}")
    return records


def _normalize_current_rating(obj: Dict[str, Any], ingested_at: datetime) -> Dict[str, Any]:
    """
    Map current rating object into dbo.bitsight_current_ratings schema.
    """

    company = obj.get("company") or {}
    industry = obj.get("industry") or {}
    sub_industry = obj.get("sub_industry") or {}

    return {
        "company_guid": company.get("guid"),
        "rating": obj.get("rating"),
        "rating_date": obj.get("rating_date"),
        "network_size_v4": obj.get("network_size_v4"),
        "industry_name": industry.get("name"),
        "industry_slug": industry.get("slug"),
        "sub_industry_name": sub_industry.get("name"),
        "sub_industry_slug": sub_industry.get("slug"),
        "ingested_at": ingested_at,
        "raw_payload": obj,
    }


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
