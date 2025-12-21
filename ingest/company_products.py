#!/usr/bin/env python3

import json
import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional

BITSIGHT_COMPANY_PRODUCTS_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/products"
)


def fetch_company_products(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch products used by a company.

    Endpoint:
        /ratings/v1/companies/{company_guid}/products

    Response:
        Top-level JSON array of product dependency objects.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_COMPANY_PRODUCTS_ENDPOINT.format(company_guid=company_guid)}"
    headers = {"Accept": "application/json"}

    ingested_at = datetime.utcnow()

    logging.info(
        f"Fetching products for company {company_guid}: {url}"
    )

    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    payload = resp.json()  # list[dict]

    records: List[Dict[str, Any]] = []
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
                "percent_dependent_company": obj.get(
                    "percent_dependent_company"
                ),
                "relative_importance": obj.get("relative_importance"),
                "relative_criticality": obj.get("relative_criticality"),
                "relationship_source": obj.get("relationship_source"),
                "ingested_at": ingested_at,
                "raw_payload": obj,
            }
        )

    logging.info(
        f"Total company products fetched for company {company_guid}: {len(records)}"
    )
    return records
