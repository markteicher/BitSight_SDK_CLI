#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional

# BitSight Peer Analytics endpoint
BITSIGHT_PEER_ANALYTICS_ENDPOINT = "/ratings/v1/peer-analytics"


def fetch_peer_analytics(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: Optional[str] = None,
    industry_slug: Optional[str] = None,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch peer analytics from BitSight.

    Endpoint:
        GET /ratings/v1/peer-analytics

    This endpoint returns comparative analytics versus peers.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_PEER_ANALYTICS_ENDPOINT}"
    headers = {"Accept": "application/json"}

    params: Dict[str, Any] = {}
    if company_guid:
        params["company_guid"] = company_guid
    if industry_slug:
        params["industry"] = industry_slug

    ingested_at = datetime.utcnow()

    logging.info(
        "Fetching peer analytics: %s (company_guid=%s, industry=%s)",
        url,
        company_guid,
        industry_slug,
    )

    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        params=params if params else None,
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    payload = resp.json()
    results = payload.get("results", [])

    records: List[Dict[str, Any]] = []

    for obj in results:
        records.append(
            {
                "company_guid": company_guid,
                "industry_slug": industry_slug,
                "ingested_at": ingested_at,
                "raw_payload": obj,
            }
        )

    logging.info("Total peer analytics records fetched: %d", len(records))
    return records
