#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# BitSight Use Current Ratings License endpoint
BITSIGHT_USE_CURRENT_RATINGS_LICENSE_ENDPOINT = (
    "/ratings/v2/current-ratings/use-license"
)


def use_current_ratings_license(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Consume a Current Ratings license for a company.
    POST action endpoint.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_USE_CURRENT_RATINGS_LICENSE_ENDPOINT}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    payload = {
        "company_guid": company_guid
    }

    requested_at = datetime.utcnow()

    logging.info(
        f"Using Current Ratings license for company {company_guid}"
    )

    resp = session.post(
        url,
        headers=headers,
        auth=(api_key, ""),
        json=payload,
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    response_payload = resp.json()

    return {
        "company_guid": company_guid,
        "requested_at": requested_at,
        "raw_payload": response_payload,
    }
