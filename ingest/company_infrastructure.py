#!/usr/bin/env python3

import json
import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

BITSIGHT_COMPANY_INFRASTRUCTURE_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/infrastructure"
)


def fetch_company_infrastructure(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    asset_type: Optional[str] = None,   # e.g. DOMAIN, IP
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch infrastructure inventory for a company.

    Endpoint:
        GET /ratings/v1/companies/{company_guid}/infrastructure

    Supports pagination via links.next and optional ?type= filter.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_COMPANY_INFRASTRUCTURE_ENDPOINT.format(company_guid=company_guid)}"
    headers = {"Accept": "application/json"}

    ingested_at = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        if asset_type:
            params["type"] = asset_type

        logging.info(
            f"Fetching infrastructure for company {company_guid}: "
            f"{url} (limit={limit}, offset={offset})"
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
            attributed = obj.get("attributed_to") or {}
            records.append(
                {
                    "company_guid": company_guid,
                    "temporary_id": obj.get("temporary_id"),
                    "value": obj.get("value"),
                    "asset_type": obj.get("type"),
                    "source": obj.get("source"),
                    "country": obj.get("country"),
                    "start_date": obj.get("start_date"),
                    "end_date": obj.get("end_date"),
                    "is_active": obj.get("is_active"),
                    "attributed_guid": attributed.get("guid"),
                    "attributed_name": attributed.get("name"),
                    "ip_count": obj.get("ip_count"),
                    "is_suppressed": obj.get("is_suppressed"),
                    "asn": obj.get("asn"),
                    "tags": json.dumps(obj.get("tags")),
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
        f"Total infrastructure records fetched for company {company_guid}: {len(records)}"
    )
    return records


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
