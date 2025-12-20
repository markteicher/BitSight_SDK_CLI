#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urljoin

# BitSight Companies endpoint (v1)
# Base URL example: https://api.bitsighttech.com/ratings
BITSIGHT_COMPANIES_ENDPOINT = "/ratings/v1/companies"


def fetch_companies(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch all companies from BitSight.
    Handles pagination deterministically via limit/offset and/or links.next.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    # Normalize base_url: allow either https://api.bitsighttech.com or https://api.bitsighttech.com/ratings
    if base_url.endswith("/"):
        base_url = base_url[:-1]

    # Ensure we can join correctly if user passes base_url without /ratings
    if base_url.endswith("/ratings"):
        url = f"{base_url}{BITSIGHT_COMPANIES_ENDPOINT.replace('/ratings', '', 1)}"
    else:
        url = f"{base_url}{BITSIGHT_COMPANIES_ENDPOINT}"

    headers = {"Accept": "application/json"}

    companies: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        logging.info(f"Fetching companies: {url} (limit={limit}, offset={offset})")

        resp = session.get(
            url,
            headers=headers,
            auth=(api_key, ""),  # BitSight: Basic Auth with token:
            params=params,
            timeout=timeout,
            proxies=proxies,
        )
        resp.raise_for_status()
        payload = resp.json()

        results = payload.get("results", [])
        for c in results:
            companies.append(_normalize_company_record(c, ingested_at))

        # Prefer pagination links when provided
        links = payload.get("links") or {}
        next_link = links.get("next")

        # If next is provided, follow it (it already encodes limit/offset)
        if next_link:
            # Some APIs return absolute URLs; some return relative.
            url = _absolutize_next(url, next_link)
            offset += limit  # keep deterministic state even if url contains offset
            continue

        # Fallback: if no links.next, stop when we receive < limit
        if len(results) < limit:
            break

        offset += limit

    logging.info(f"Total companies fetched: {len(companies)}")
    return companies


def _normalize_company_record(company_obj: Dict[str, Any], ingested_at: datetime) -> Dict[str, Any]:
    """
    Map BitSight company object into dbo.bitsight_companies schema fields.
    Leaves anything uncertain in raw_payload (always).
    """
    company_guid = company_obj.get("guid") or company_obj.get("company_guid")
    name = company_obj.get("name")
    domain = company_obj.get("domain") or company_obj.get("primary_domain")

    # Country may be absent depending on API/product; keep safe.
    country = company_obj.get("country")

    # Some shapes include added_date / rating; keep safe.
    added_date = company_obj.get("added_date")
    rating = company_obj.get("rating")

    return {
        "company_guid": company_guid,
        "name": name,
        "domain": domain,
        "country": country,
        "added_date": added_date,
        "rating": rating,
        "ingested_at": ingested_at,
        "raw_payload": company_obj,
    }


def _absolutize_next(current_url: str, next_link: str) -> str:
    """
    Convert relative 'next' links to absolute URLs when needed.
    """
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
