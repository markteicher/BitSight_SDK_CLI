#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

# BitSight Threats (v2) endpoint
BITSIGHT_THREATS_ENDPOINT = "/ratings/v2/threats"


def fetch_threats(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch threat intelligence (v2).
    Deterministic pagination using links.next.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_THREATS_ENDPOINT}"
    headers = {"Accept": "application/json"}

    records: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        logging.info(f"Fetching threats: {url} (limit={limit}, offset={offset})")

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
            records.append(_normalize_threat(obj, ingested_at))

        links = payload.get("links") or {}
        next_link = links.get("next")

        if next_link:
            url = _absolutize_next(url, next_link)
            offset += limit
            continue

        if len(results) < limit:
            break

        offset += limit

    logging.info(f"Total threats fetched: {len(records)}")
    return records


def _normalize_threat(obj: Dict[str, Any], ingested_at: datetime) -> Dict[str, Any]:
    """
    Map threat object into dbo.bitsight_threat_intel schema.
    """

    category = obj.get("category") or {}
    severity = obj.get("severity") or {}
    epss = obj.get("epss") or {}

    return {
        "threat_guid": obj.get("guid"),
        "name": obj.get("name"),
        "category_name": category.get("name"),
        "severity_level": severity.get("level"),
        "first_seen_date": obj.get("first_seen_date"),
        "last_seen_date": obj.get("last_seen_date"),
        "exposed_count": obj.get("exposed_count"),
        "mitigated_count": obj.get("mitigated_count"),
        "epss_score": epss.get("score"),
        "epss_percentile": epss.get("percentile"),
        "evidence_certainty": obj.get("evidence_certainty"),
        "ingested_at": ingested_at,
        "raw_payload": obj,
    }


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
