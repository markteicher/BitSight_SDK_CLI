#!/usr/bin/env python3

import logging
import requests
import csv
import io
from datetime import datetime
from typing import Dict, Any, List, Optional

# BitSight Ratings History endpoint (CSV response)
BITSIGHT_RATINGS_HISTORY_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/reports/ratings-history"
)


def fetch_ratings_history_for_company(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch ratings history (daily ratings, ~1 year) for a single company.

    Endpoint:
        GET /ratings/v1/companies/{company_guid}/reports/ratings-history

    Response format:
        CSV (not JSON)

    Notes:
        - This endpoint is non-paginated
        - Each CSV row represents a single rating snapshot
        - raw_payload stores the parsed CSV row for traceability
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_RATINGS_HISTORY_ENDPOINT.format(company_guid=company_guid)}"
    headers = {"Accept": "text/csv"}
    ingested_at = datetime.utcnow()

    logging.info(
        f"Fetching ratings history for company {company_guid}: {url}"
    )

    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        params={"format": "csv"},
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    csv_data = resp.text
    reader = csv.DictReader(io.StringIO(csv_data))

    records: List[Dict[str, Any]] = []

    for row in reader:
        rating_date = row.get("date") or row.get("rating_date")
        rating = row.get("rating")

        records.append(
            {
                "company_guid": company_guid,
                "rating_date": rating_date,
                "rating": int(rating) if rating and rating.isdigit() else None,
                "ingested_at": ingested_at,
                "raw_payload": row,  # CSV row preserved verbatim
            }
        )

    logging.info(
        f"Total ratings history records fetched for company {company_guid}: "
        f"{len(records)}"
    )

    return records
