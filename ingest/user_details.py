#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# BitSight User Details endpoint (v2)
BITSIGHT_USER_DETAILS_ENDPOINT = "/ratings/v2/users/{user_guid}"


def fetch_user_details(
    session: requests.Session,
    base_url: str,
    api_key: str,
    user_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Fetch full details for a single BitSight user.

    Endpoint:
        GET /ratings/v2/users/{user_guid}

    This endpoint returns the authoritative user record.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_USER_DETAILS_ENDPOINT.format(user_guid=user_guid)}"
    headers = {"Accept": "application/json"}
    ingested_at = datetime.utcnow()

    logging.info(f"Fetching user details for user {user_guid}: {url}")

    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    payload = resp.json()
    group = payload.get("group") or {}

    return {
        "user_guid": payload.get("guid"),
        "friendly_name": payload.get("friendly_name"),
        "formal_name": payload.get("formal_name"),
        "email": payload.get("email"),
        "group_guid": group.get("guid"),
        "group_name": group.get("name"),
        "landing_page": payload.get("landing_page"),
        "status": payload.get("status"),
        "last_login_time": payload.get("last_login_time"),
        "joined_time": payload.get("joined_time"),
        "mfa_status": payload.get("mfa_status"),
        "is_available_for_contact": payload.get("is_available_for_contact"),
        "is_company_api_token": payload.get("is_company_api_token"),
        "roles": payload.get("roles"),
        "features": payload.get("features"),
        "preferred_contact_for_entities": payload.get(
            "preferred_contact_for_entities"
        ),
        "ingested_at": ingested_at,
        "raw_payload": payload,
    }
