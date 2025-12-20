#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# BitSight Company Overview Report endpoint
BITSIGHT_COMPANY_OVERVIEW_REPORT_ENDPOINT = (
    "/ratings/v1/reports/company-overview"
)


def request_company_overview_report(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    report_format: str = "pdf",
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Request a Company Overview Report.
    This is a POST endpoint that returns a report job descriptor.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_COMPANY_OVERVIEW_REPORT_ENDPOINT}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    payload = {
        "company_guid": company_guid,
        "format": report_format,
    }

    requested_at = datetime.utcnow()

    logging.info(
        f"Requesting Company Overview Report "
        f"(company_guid={company_guid}, format={report_format})"
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

    record = {
        "company_guid": company_guid,
        "report_format": report_format,
        "requested_at": requested_at,
        "raw_payload": response_payload,
    }

    return record
