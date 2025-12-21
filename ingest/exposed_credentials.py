#!/usr/bin/env python3
"""
ingest/exposed_credentials.py

BitSight SDK + CLI — Exposed Credentials ingestion unit.

Endpoint:
    GET /ratings/v1/exposed-credentials

Behavior:
- Uses HTTP Basic Auth (api_key as username, blank password)
- Fetches all records deterministically using limit/offset
- If links.next is present, it is followed (absolute or relative)
- Each record is normalized into a 1:1 MSSQL-ready dict, plus raw_payload
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests

# ------------------------------------------------------------
# Endpoint
# ------------------------------------------------------------
BITSIGHT_EXPOSED_CREDENTIALS_ENDPOINT = "/ratings/v1/exposed-credentials"


# ------------------------------------------------------------
# Public fetcher
# ------------------------------------------------------------
def fetch_exposed_credentials(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch exposed credentials from BitSight.

    Pagination:
      - Primary: limit/offset
      - Secondary: follow payload.links.next when present

    Returns:
      List of normalized records suitable for dbo.bitsight_exposed_credentials,
      each containing raw_payload for full fidelity retention.
    """

    # Normalize base_url (never allow trailing slash to double-slash join)
    base_url = (base_url or "").rstrip("/")
    url = f"{base_url}{BITSIGHT_EXPOSED_CREDENTIALS_ENDPOINT}"

    headers = {"Accept": "application/json"}

    ingested_at = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    # Deterministic page sizing
    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}

        logging.info(
            "Fetching exposed credentials: %s (limit=%d, offset=%d)",
            url,
            limit,
            offset,
        )

        resp = session.get(
            url,
            headers=headers,
            auth=(api_key, ""),  # BitSight: Basic Auth token as username, blank password
            params=params,
            timeout=timeout,
            proxies=proxies,
        )
        resp.raise_for_status()

        payload = resp.json() or {}
        results = payload.get("results") or []

        # Normalize each record into a 1:1 schema-friendly shape
        for obj in results:
            records.append(_normalize_exposed_credential(obj, ingested_at))

        # Prefer links.next when provided
        links = payload.get("links") or {}
        next_link = links.get("next")

        if next_link:
            url = _absolutize_next(url, next_link)
            offset += limit
            continue

        # Fallback stop condition when links are absent
        if len(results) < limit:
            break

        offset += limit

    logging.info("Total exposed credentials fetched: %d", len(records))
    return records


# ------------------------------------------------------------
# Normalization
# ------------------------------------------------------------
def _normalize_exposed_credential(
    obj: Dict[str, Any],
    ingested_at: datetime,
) -> Dict[str, Any]:
    """
    Map exposed credential object into dbo.bitsight_exposed_credentials fields.

    Raw payload is preserved verbatim for auditing / schema drift tolerance.
    """

    company = obj.get("company") or {}
    breach = obj.get("breach") or {}

    return {
        "credential_guid": obj.get("guid"),
        "company_guid": company.get("guid"),
        "exposure_type": obj.get("exposure_type"),
        "breach_name": breach.get("name"),
        "first_seen_date": obj.get("first_seen_date"),
        "last_seen_date": obj.get("last_seen_date"),
        "ingested_at": ingested_at,
        "raw_payload": obj,
    }


# ------------------------------------------------------------
# Pagination helpers
# ------------------------------------------------------------
def _absolutize_next(current_url: str, next_link: str) -> str:
    """
    BitSight links.next may be absolute or relative. This normalizes it.

    current_url is used as the base for joining relative paths.
    """
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
