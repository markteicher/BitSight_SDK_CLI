#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs

# BitSight Current Ratings v2 endpoint
BITSIGHT_CURRENT_RATINGS_V2_ENDPOINT = "/ratings/v2/current-ratings"


def fetch_current_ratings_v2(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch current ratings (v2).
    Deterministic pagination using links.next.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_CURRENT_RATINGS_V2_ENDPOINT}"
    headers = {"Accept": "application/json"}

    records: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        logging.info(
            f"Fetching current ratings v2: {url} (limit={limit}, offset={offset})"
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
            records.append(
                {
                    "company_guid": (obj.get("company") or {}).get("guid"),
                    "rating": obj.get("rating"),
                    "rating_date": obj.get("rating_date"),
                    "ingested_at": ingested_at,
                    "raw_payload": obj,
                }
            )

        links = payload.get("links") or {}
        next_link = links.get("next")

        if next_link:
            url = _absolutize_next(url, next_link)
            next_offset = _extract_offset(url)
            if next_offset is not None:
                offset = next_offset
            else:
                offset += limit
            continue

        if len(results) < limit:
            break

        offset += limit

    logging.info(f"Total current ratings v2 fetched: {len(records)}")
    return records


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)


def _extract_offset(url: str) -> Optional[int]:
    try:
        qs = parse_qs(urlparse(url).query)
        if "offset" in qs and qs["offset"]:
            return int(qs["offset"][0])
    except Exception:
        return None
    return None
