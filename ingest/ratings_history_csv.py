#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Optional

# BitSight Ratings History (CSV) endpoint
BITSIGHT_RATINGS_HISTORY_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/reports/ratings-history"
)


def fetch_ratings_history_csv(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    timeout: int = 60,
    proxies: Optional[dict] = None,
) -> dict:
    """
    Fetch ratings history for a company as CSV.
    This endpoint ONLY supports format=csv.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_RATINGS_HISTORY_ENDPOINT.format(company_guid=company_guid)}"
    headers = {"Accept": "text/csv"}

    params = {"format": "csv"}
    ingested_at = datetime.utcnow()

    logging.info(
        f"Fetching ratings history CSV for company {company_guid}"
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

    return {
        "company_guid": company_guid,
        "ingested_at": ingested_at,
        "csv_payload": resp.text,
    }
