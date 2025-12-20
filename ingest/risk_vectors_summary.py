#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# BitSight Risk Vectors Summary endpoint
BITSIGHT_RISK_VECTORS_SUMMARY_ENDPOINT = "/ratings/v1/risk-vectors/summary"


def fetch_risk_vectors_summary(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: Optional[str] = None,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Fetch risk vectors summary.
    Supports optional company_guid filtering.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_RISK_VECTORS_SUMMARY_ENDPOINT}"
    headers = {"Accept": "application/json"}

    params: Dict[str, Any] = {}
    if company_guid:
        params["company_guid"] = company_guid

    ingested_at = datetime.utcnow()

    logging.info(
        f"Fetching risk vectors summary "
        f"(company_guid={company_guid})"
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

    return {
        "company_guid": company_guid,
        "ingested_at": ingested_at,
        "raw_payload": payload,
    }
