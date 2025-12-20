#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

BITSIGHT_COMPANY_OBSERVATIONS_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/observations"
)


def fetch_company_observations(
    session,
    base_url: str,
    api_key: str,
    company_guid: str,
    timeout: int = 60,
    proxies: Optional[dict] = None,
) -> List[Dict[str, Any]]:

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_COMPANY_OBSERVATIONS_ENDPOINT.format(company_guid=company_guid)}"
    headers = {"Accept": "application/json"}

    records = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        logging.info(
            f"Fetching observations for company {company_guid} "
            f"(limit={limit}, offset={offset})"
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
        results = payload.get("results", [])

        for obj in results:
            records.append({
                "company_guid": company_guid,
                "ingested_at": ingested_at,
                "raw_payload": obj,
            })

        links = payload.get("links") or {}
        if links.get("next"):
            offset += limit
            continue

        break

    return records
