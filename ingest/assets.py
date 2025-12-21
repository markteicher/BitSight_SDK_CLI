#!/usr/bin/env python3

import logging
import requests
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

# BitSight Assets endpoint (per company)
BITSIGHT_COMPANY_ASSETS_ENDPOINT = "/ratings/v1/companies/{company_guid}/assets"


def fetch_assets(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch assets for a single company.

    Endpoint:
        GET /ratings/v1/companies/{company_guid}/assets

    Deterministic pagination using links.next when present.

    Auth:
        HTTP Basic Auth using api_key as username and blank password.
    """

    if not isinstance(base_url, str) or not base_url.strip():
        raise ValueError("base_url must be a non-empty string")

    if not company_guid or not isinstance(company_guid, str):
        raise ValueError("company_guid must be a non-empty string")

    base_url = base_url.rstrip("/")

    endpoint = BITSIGHT_COMPANY_ASSETS_ENDPOINT.format(company_guid=company_guid)
    url = f"{base_url}{endpoint}"
    headers = {"Accept": "application/json"}

    records: List[Dict[str, Any]] = []
    ingested_at = datetime.now(timezone.utc)

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        logging.info(
            "Fetching assets for company %s: %s (limit=%d, offset=%d)",
            company_guid,
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
            records.append(
                {
                    "asset_guid": obj.get("guid"),
                    "company_guid": company_guid,
                    "ingested_at": ingested_at,
                    "raw_payload": obj,
                }
            )

        links = payload.get("links") or {}
        next_link = links.get("next")

        if next_link:
            url = _absolutize_next(url, next_link)
            offset += limit
            continue

        if len(results) < limit:
            break

        offset += limit

    logging.info(
        "Total assets fetched for company %s: %d",
        company_guid,
        len(records),
    )
    return records


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
