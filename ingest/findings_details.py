#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# BitSight Finding Details endpoint
BITSIGHT_FINDING_DETAILS_ENDPOINT = "/ratings/v1/findings/{finding_guid}"


def fetch_finding_details(
    session: requests.Session,
    base_url: str,
    api_key: str,
    finding_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Fetch detailed information for a single finding.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    endpoint = BITSIGHT_FINDING_DETAILS_ENDPOINT.format(
        finding_guid=finding_guid
    )
    url = f"{base_url}{endpoint}"

    headers = {"Accept": "application/json"}
    ingested_at = datetime.utcnow()

    logging.info(f"Fetching finding details: {url}")

    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    payload = resp.json()

    record = {
        "finding_guid": finding_guid,
        "ingested_at": ingested_at,
        "raw_payload": payload,
    }

    return record
