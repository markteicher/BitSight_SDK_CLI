#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

BITSIGHT_COMPANY_FINDINGS_SUMMARY_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/findings/summary"
)


def fetch_company_findings_summary(
    session,
    base_url: str,
    api_key: str,
    company_guid: str,
    timeout: int = 60,
    proxies: Optional[dict] = None,
) -> Dict[str, Any]:

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_COMPANY_FINDINGS_SUMMARY_ENDPOINT.format(company_guid=company_guid)}"
    headers = {"Accept": "application/json"}
    ingested_at = datetime.utcnow()

    logging.info(
        f"Fetching findings summary for company {company_guid}: {url}"
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
