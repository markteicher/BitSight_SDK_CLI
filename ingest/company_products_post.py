#!/usr/bin/env python3

import json
import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional

BITSIGHT_COMPANY_PRODUCTS_POST_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/products"
)


def fetch_company_products_post(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    POST: Get Products of a Company

    Endpoint:
        POST /ratings/v1/companies/{company_guid}/products

    Response:
        Top-level JSON array of product objects.

    Notes:
      - This endpoint supports body-based filters (e.g., product_guid[], product_types[],
        provider_guid[], provider_name[], relative_criticality[], relationship_source[]),
        plus query params like fields/limit/offset/q/sort.  [oai_citation:3‡Bitsight Knowledge Base](https://help.bitsighttech.com/hc/en-us/articles/14500448609303-POST-Get-Products-of-a-Company?utm_source=chatgpt.com)
      - Pagination is deterministic via limit/offset when used.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_COMPANY_PRODUCTS_POST_ENDPOINT.format(company_guid=company_guid)}"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    ingested_at = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    body = body or {}
    params = params.copy() if params else {}

    # Deterministic pagination if caller supplies limit; otherwise fetch "all" once
    limit = params.get("limit")
    offset = params.get("offset", 0)

    while True:
        if limit is not None:
            params["limit"] = limit
            params["offset"] = offset

            logging.info(
                f"POST company products for {company_guid}: {url} (limit={limit}, offset={offset})"
            )
        else:
            logging.info(f"POST company products for {company_guid}: {url}")

        resp = session.post(
            url,
            headers=headers,
            auth=(api_key, ""),
            params=params if params else None,
            data=json.dumps(body) if body else None,
            timeout=timeout,
            proxies=proxies,
        )
        resp.raise_for_status()

        payload = resp.json()  # list[dict]

        for obj in payload:
            records.append(
                {
                    "company_guid": company_guid,
                    "product_guid": obj.get("product_guid"),
                    "product_name": obj.get("product_name"),
                    "provider_guid": obj.get("provider_guid"),
                    "provider_name": obj.get("provider_name"),
                    "provider_industry": obj.get("provider_industry"),
                    "product_types": json.dumps(obj.get("product_types")),
                    "company_count": obj.get("company_count"),
                    "domain_count": obj.get("domain_count"),
                    "percent_dependent": obj.get("percent_dependent"),
                    "percent_dependent_company": obj.get("percent_dependent_company"),
                    "relative_importance": obj.get("relative_importance"),
                    "relative_criticality": obj.get("relative_criticality"),
                    "relationship_source": obj.get("relationship_source"),
                    "ingested_at": ingested_at,
                    "raw_payload": obj,
                }
            )

        if limit is None:
            break

        if len(payload) < int(limit):
            break

        offset = int(offset) + int(limit)

    logging.info(
        f"Total POST company products fetched for company {company_guid}: {len(records)}"
    )
    return records
