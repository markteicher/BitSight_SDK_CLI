#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs

# BitSight Threats endpoint (v2)
BITSIGHT_THREATS_ENDPOINT = "/ratings/v2/threats"


def fetch_threats(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch BitSight Threats (v2).
    Deterministic pagination using links.next when present, else limit/offset.
    Auth: HTTP Basic Auth using api_key as username and blank password.

    Returns rows aligned to dbo.bitsight_threats:
      - threat_guid
      - name
      - ingested_at
      - raw_payload
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
            next_offset = _extract_offset(url)
            if next_offset is not None:
                offset = next_offset
            else:
                offset += limit
            continue

        if len(results) < limit:
            break

        offset += limit

    logging.info(f"Total threats fetched: {len(records)}")
    return records


def _normalize_threat(obj: Dict[str, Any], ingested_at: datetime) -> Dict[str,
