#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List

BITSIGHT_USERS_ENDPOINT = "/ratings/v2/users"


def fetch_users(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60
) -> List[Dict[str, Any]]:
    """
    Fetch all users from BitSight.
    Handles pagination deterministically.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json"
    }

    users = []
    url = f"{base_url}{BITSIGHT_USERS_ENDPOINT}"
    params = {"limit": 100, "offset": 0}

    while url:
        logging.info(f"Fetching users: {url}")
        response = session.get(url, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()

        payload = response.json()
        results = payload.get("results", [])

        for user in results:
            users.append({
                "user_guid": user.get("guid"),
                "friendly_name": user.get("friendly_name"),
                "formal_name": user.get("formal_name"),
                "email": user.get("email"),
                "status": user.get("status"),
                "landing_page": user.get("landing_page"),
                "mfa_status": user.get("mfa_status"),
                "last_login_time": user.get("last_login_time"),
                "joined_time": user.get("joined_time"),
                "ingested_at": datetime.utcnow(),
                "raw_payload": user
            })

        links = payload.get("links", {})
        url = links.get("next")
        params = None  # next already includes pagination

    logging.info(f"Total users fetched: {len(users)}")
    return users
