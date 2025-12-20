#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# BitSight Download Report endpoint
BITSIGHT_DOWNLOAD_REPORT_ENDPOINT = "/ratings/v1/reports/download"


def request_report_download(
    session: requests.Session,
    base_url: str,
    api_key: str,
    report_type: str,
    company_guid: Optional[str] = None,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Submit a report download request to BitSight.
    This is a POST endpoint that returns a job / download descriptor.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_DOWNLOAD_REPORT_ENDPOINT}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    payload: Dict[str, Any] = {
        "report_type": report_type
    }

    if company_guid:
        payload["company_guid"] = company_guid

    ingested_at = datetime.utcnow()

    logging.info(
        f"Requesting BitSight report download "
        f"(type={report_type}, company_guid={company_guid})"
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
        "report_type": report_type,
        "company_guid": company_guid,
        "requested_at": ingested_at,
        "raw_payload": response_payload,
    }

    return record
