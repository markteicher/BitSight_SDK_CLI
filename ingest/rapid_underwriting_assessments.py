#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# BitSight Rapid Underwriting Assessments endpoint
BITSIGHT_RUA_ENDPOINT = "/ratings/v1/rapid-underwriting-assessments"


def request_rapid_underwriting_assessment(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_name: str,
    domain: Optional[str] = None,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Submit a Rapid Underwriting Assessment (RUA) request.
    POST action endpoint that returns a job descriptor.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_RUA_ENDPOINT}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    payload: Dict[str, Any] = {
        "company_name": company_name
    }
    if domain:
        payload["domain"] = domain

    requested_at = datetime.utcnow()

    logging.info(
        f"Requesting Rapid Underwriting Assessment "
        f"(company_name={company_name}, domain={domain})"
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
        "company_name": company_name,
        "domain": domain,
        "requested_at": requested_at,
        "raw_payload": response_payload,
    }
