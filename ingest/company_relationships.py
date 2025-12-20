#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

# BitSight Company Relationship Details endpoint
BITSIGHT_COMPANY_RELATIONSHIPS_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/relationships"
)


def fetch_company_relationships(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch relationship details for a single company.

    Endpoint:
        GET /ratings/v1/companies/{company_guid}/relationships

    Full 1:1 physical mapping of results[] fields:
        guid, company_guid, company_name, relationship_type,
        creator, last_editor, created_time, last_edited_time
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_COMPANY_RELATIONSHIPS_ENDPOINT.format(company_guid=company_guid)}"
    headers = {"Accept": "application/json"}

    records: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}

        logging.info(
            f"Fetching company relationship details for {company_guid}: {url} "
            f"(limit={limit}, offset={offset})"
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
                    "relationship_guid": obj.get("guid"),
                    "company_guid": obj.get("company_guid"),
                    "company_name": obj.get("company_name"),
                    "relationship_type": obj.get("relationship_type"),
                    "creator": obj.get("creator"),
                    "last_editor": obj.get("last_editor"),
                    "created_time": obj.get("created_time"),
                    "last_edited_time": obj.get("last_edited_time"),
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

    logging.info(f"Total company relationship records fetched: {len(records)}")
    return records


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
