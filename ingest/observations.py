#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

# BitSight Observations endpoint (per company)
BITSIGHT_COMPANY_OBSERVATIONS_ENDPOINT = "/ratings/v1/companies/{company_guid}/observations"


def fetch_observations(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch observations for a single company.
    Deterministic pagination using links.next.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    endpoint = BITSIGHT_COMPANY_OBSERVATIONS_ENDPOINT.format(company_guid=company_guid)
    url = f"{base_url}{endpoint}"
    headers = {"Accept": "application/json"}

    records: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        logging.info(
            f"Fetching observations for company {company_guid}: {url} "
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
            records.append(_normalize_observation(obj, company_guid, ingested_at))

        links = payload.get("links") or {}
        next_link = links.get("next")

        if next_link:
            url = _absolutize_next(url, next_link)
            offset += limit
            continue

        if len(results) < limit:
            break

        offset += limit

    logging.info(
        f"Total observations fetched for company {company_guid}: {len(records)}"
    )
    return records


def _normalize_observation(
    obj: Dict[str, Any],
    company_guid: str,
    ingested_at: datetime,
) -> Dict[str, Any]:
    """
    Map an observation object into dbo.bitsight_observations schema.
    """

    return {
        "observation_guid": obj.get("guid"),
        "finding_guid": obj.get("finding_guid"),
        "company_guid": company_guid,
        "observed_date": obj.get("observed_date"),
        "observation_type": obj.get("type"),
        "ingested_at": ingested_at,
        "raw_payload": obj,
    }


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
