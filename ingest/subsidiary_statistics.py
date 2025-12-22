#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional

# BitSight Subsidiary Statistics endpoint
BITSIGHT_SUBSIDIARY_STATISTICS_ENDPOINT = "/subsidiaries/statistics"


def fetch_subsidiary_statistics(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch subsidiary statistics from BitSight.

    Endpoint:
        GET /subsidiaries/statistics

    Notes:
      - This endpoint is treated as non-paginated. It returns a list of subsidiaries/companies,
        each including a stats[] series (date/value).
      - Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_SUBSIDIARY_STATISTICS_ENDPOINT}"
    headers = {"Accept": "application/json"}
    ingested_at = datetime.utcnow()

    logging.info("Fetching subsidiary statistics: %s", url)

    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    payload = resp.json()

    # Expected shape: a top-level list of company/subsidiary objects
    if not isinstance(payload, list):
        logging.error(
            "Unexpected response shape for %s: expected list, got %s",
            BITSIGHT_SUBSIDIARY_STATISTICS_ENDPOINT,
            type(payload).__name__,
        )
        raise ValueError("Unexpected subsidiary statistics response shape")

    records: List[Dict[str, Any]] = []
    for obj in payload:
        records.append(_normalize_subsidiary_statistics(obj, ingested_at))

    logging.info("Total subsidiary statistics records fetched: %s", len(records))
    return records


def _normalize_subsidiary_statistics(
    obj: Dict[str, Any],
    ingested_at: datetime,
) -> Dict[str, Any]:
    """
    Extract stable, top-level fields while preserving the full object in raw_payload.

    We intentionally keep stats[] as part of raw_payload (lossless ingestion),
    but also surface company identity for indexing/joining.
    """

    company = obj.get("company") or obj.get("subsidiary") or {}

    return {
        "company_guid": company.get("guid"),
        "company_name": company.get("name"),
        "ingested_at": ingested_at,
        "raw_payload": obj,
    }
