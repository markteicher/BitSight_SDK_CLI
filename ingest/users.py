#!/usr/bin/env python3

import logging
import requests
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

# BitSight Users endpoint (v2)
BITSIGHT_USERS_ENDPOINT = "/ratings/v2/users"


def fetch_users(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch all users from BitSight.

    Endpoint:
        GET /ratings/v2/users

    Deterministic pagination using links.next.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_USERS_ENDPOINT}"
    headers = {"Accept": "application/json"}

    records: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}

        logging.info(
            f"Fetching users: {url} (limit={limit}, offset={offset})"
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

        for user in results:
            group = user.get("group") or {}

            records.append(
                {
                    "user_guid": user.get("guid"),
                    "friendly_name": user.get("friendly_name"),
                    "formal_name": user.get("formal_name"),
                    "email": user.get("email"),
                    "group_guid": group.get("guid"),
                    "group_name": group.get("name"),
                    "landing_page": user.get("landing_page"),
                    "status": user.get("status"),
                    "last_login_time": user.get("last_login_time"),
                    "joined_time": user.get("joined_time"),
                    "mfa_status": user.get("mfa_status"),
                    "is_available_for_contact": user.get("is_available_for_contact"),
                    "is_company_api_token": user.get("is_company_api_token"),
                    "roles": json.dumps(user.get("roles")),
                    "features": json.dumps(user.get("features")),
                    "preferred_contact_for_entities": json.dumps(
                        user.get("preferred_contact_for_entities")
                    ),
                    "ingested_at": ingested_at,
                    "raw_payload": user,
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

    logging.info(f"Total users fetched: {len(records)}")
    return records


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
