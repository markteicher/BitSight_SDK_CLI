#!/usr/bin/env python3
"""
ingest/finding_comments.py

BitSight SDK + CLI — Finding Comments ingestion unit.

Endpoint:
    GET /ratings/v1/findings/{finding_guid}/comments

Behavior:
- Uses HTTP Basic Auth (api_key as username, blank password)
- Deterministic pagination using limit/offset
- Follows links.next when present (absolute or relative)
- Each comment is mapped 1:1 into a relational-friendly structure
- Full raw_payload is always preserved
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests


# ------------------------------------------------------------
# Endpoint
# ------------------------------------------------------------
BITSIGHT_FINDING_COMMENTS_ENDPOINT = "/ratings/v1/findings/{finding_guid}/comments"


# ------------------------------------------------------------
# Public fetcher
# ------------------------------------------------------------
def fetch_finding_comments(
    session: requests.Session,
    base_url: str,
    api_key: str,
    finding_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch all comments for a specific finding.

    Pagination:
      - Primary: limit/offset
      - Secondary: follow payload.links.next when present

    Returns:
      List of normalized comment records suitable for
      dbo.bitsight_finding_comments.
    """

    # Normalize base_url
    base_url = (base_url or "").rstrip("/")
    url = f"{base_url}{BITSIGHT_FINDING_COMMENTS_ENDPOINT.format(finding_guid=finding_guid)}"

    headers = {"Accept": "application/json"}

    ingested_at = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}

        logging.info(
            "Fetching finding comments for %s: %s (limit=%d, offset=%d)",
            finding_guid,
            url,
            limit,
            offset,
        )

        resp = session.get(
            url,
            headers=headers,
            auth=(api_key, ""),  # BitSight: Basic Auth token as username
            params=params,
            timeout=timeout,
            proxies=proxies,
        )
        resp.raise_for_status()

        payload = resp.json() or {}
        results = payload.get("results") or []

        for obj in results:
            records.append(
                _normalize_finding_comment(
                    obj=obj,
                    finding_guid=finding_guid,
                    ingested_at=ingested_at,
                )
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
        "Total finding comments fetched for %s: %d",
        finding_guid,
        len(records),
    )
    return records


# ------------------------------------------------------------
# Normalization
# ------------------------------------------------------------
def _normalize_finding_comment(
    obj: Dict[str, Any],
    finding_guid: str,
    ingested_at: datetime,
) -> Dict[str, Any]:
    """
    Normalize a finding comment object into dbo.bitsight_finding_comments fields.
    """

    author = obj.get("author") or {}
    company = obj.get("company") or {}

    return {
        "finding_guid": finding_guid,
        "comment_guid": obj.get("guid"),
        "thread_guid": obj.get("thread_guid"),
        "created_time": obj.get("created_time"),
        "last_update_time": obj.get("last_update_time"),
        "message": obj.get("message"),
        "is_public": obj.get("is_public", False),
        "is_deleted": obj.get("is_deleted", False),
        "parent_guid": obj.get("parent_guid"),
        "author_guid": author.get("guid"),
        "author_name": author.get("name"),
        "company_guid": company.get("guid"),
        "company_name": company.get("name"),
        "tagged_users": json.dumps(obj.get("tagged_users")),
        "remediation": json.dumps(obj.get("remediation")),
        "ingested_at": ingested_at,
        "raw_payload": obj,
    }


# ------------------------------------------------------------
# Pagination helpers
# ------------------------------------------------------------
def _absolutize_next(current_url: str, next_link: str) -> str:
    """
    Normalize BitSight links.next which may be absolute or relative.
    """
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
