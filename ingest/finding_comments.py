#!/usr/bin/env python3

import json
import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

BITSIGHT_FINDING_COMMENTS_ENDPOINT = "/ratings/v1/findings/{finding_guid}/comments"


def fetch_finding_comments(
    session: requests.Session,
    base_url: str,
    api_key: str,
    finding_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch comments for a specific finding.

    Endpoint:
        GET /ratings/v1/findings/{finding_guid}/comments

    Deterministic pagination via links.next + limit/offset.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_FINDING_COMMENTS_ENDPOINT.format(finding_guid=finding_guid)}"
    headers = {"Accept": "application/json"}

    records: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        logging.info(
            f"Fetching finding comments for {finding_guid}: {url} "
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
            author = obj.get("author") or {}
            company = obj.get("company") or {}

            records.append(
                {
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
        f"Total finding comments fetched for {finding_guid}: {len(records)}"
    )
    return records


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
