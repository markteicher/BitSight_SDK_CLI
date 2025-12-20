#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Optional, Dict, Any

# BitSight NIST CSF Report endpoint
BITSIGHT_NIST_CSF_REPORT_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/reports/nist-csf"
)


def fetch_nist_csf_report(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Fetch the NIST CSF report for a company.
    This is a report-style endpoint returning structured compliance data.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_NIST_CSF_REPORT_ENDPOINT.format(company_guid=company_guid)}"
    headers = {"Accept": "application/json"}
    ingested_at = datetime.utcnow()

    logging.info(
        f"Fetching NIST CSF report for company {company_guid}"
    )

    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    return {
        "company_guid": company_guid,
        "ingested_at": ingested_at,
        "raw_payload": resp.json(),
    }
