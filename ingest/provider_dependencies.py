#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

BITSIGHT_PROVIDER_DEPENDENCIES_ENDPOINT = (
    "/ratings/v1/ratings-tree/providers/{provider_guid}/companies"
)


def fetch_provider_dependencies(
    session: requests.Session,
    base_url: str,
    api_key: str,
    provider_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch companies dependent on a specific service provider
    in the BitSight Ratings Tree.

    Endpoint:
        GET /ratings/v1/ratings-tree/providers/{provider_guid}/companies
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    endpoint = BITSIGHT_PROVIDER_DEPENDENCIES_ENDPOINT.format(
        provider_guid=provider_guid
    )
    url = f"{base_url}{endpoint}"
    headers = {"Accept": "application/json"}

    records: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}

        logging.info(
            "Fetching provider dependencies for provider %s: %s (limit=%d, offset=%d)",
            provider_guid,
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
                    "provider_guid": provider_guid,
                    "company_guid": obj.get("company_guid"),
                    "company_name": obj.get("company_name"),
                    "domain_count": obj.get("domain_count"),
                    "percent_dependent": obj.get("percent_dependent"),
                    "relationship_source": obj.get("relationship_source"),
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
        "Total dependent companies fetched for provider %s: %d",
        provider_guid,
        len(records),
    )
    return records


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
