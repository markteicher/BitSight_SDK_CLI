#!/usr/bin/env python3

import logging
import requests
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

# BitSight Service Providers endpoint
BITSIGHT_SERVICE_PROVIDERS_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/service-providers"
)


def fetch_service_providers(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch service providers for a third-party company.

    Endpoint:
        GET /ratings/v1/companies/{company_guid}/service-providers

    Deterministic pagination using links.next.
    Full-field, lossless ingestion.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    endpoint = BITSIGHT_SERVICE_PROVIDERS_ENDPOINT.format(
        company_guid=company_guid
    )
    url = f"{base_url}{endpoint}"
    headers = {"Accept": "application/json"}

    records: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}

        logging.info(
            f"Fetching service providers for company {company_guid}: "
            f"{url} (limit={limit}, offset={offset})"
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
            records.append(
                {
                    "company_guid": company_guid,
                    "provider_guid": obj.get("provider_guid"),
                    "provider_name": obj.get("provider_name"),
                    "provider_industry": obj.get("provider_industry"),
                    "product_types": json.dumps(obj.get("product_types")),
                    "company_count": obj.get("company_count"),
                    "product_count": obj.get("product_count"),
                    "domain_count": obj.get("domain_count"),
                    "relative_importance": obj.get("relative_importance"),
                    "percent_dependent": obj.get("percent_dependent"),
                    "percent_dependent_company": obj.get(
                        "percent_dependent_company"
                    ),
                    "relative_criticality": obj.get("relative_criticality"),
                    "ingested_at": ingested_at,
                    "raw_payload": obj,
                }
            )

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
        f"Total service providers fetched for company {company_guid}: "
        f"{len(records)}"
    )
    return records


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
