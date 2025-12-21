#!/usr/bin/env python3

import json
import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional

BITSIGHT_DOMAIN_PRODUCTS_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/domains/{domain_name}/products"
)


def fetch_domain_products(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    domain_name: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    GET enterprise products used by a single domain from a particular company.

    Endpoint:
        /ratings/v1/companies/{company_guid}/domains/{domain_name}/products

    Response:
        Top-level JSON array of product objects (no pagination).
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_DOMAIN_PRODUCTS_ENDPOINT.format(company_guid=company_guid, domain_name=domain_name)}"
    headers = {"Accept": "application/json"}

    ingested_at = datetime.utcnow()

    logging.info(
        f"Fetching domain products for company {company_guid}, domain {domain_name}: {url}"
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
                "domain_name": domain_name,
                "product_guid": obj.get("product_guid"),
                "product_name": obj.get("product_name"),
                "provider_name": obj.get("provider_name"),
                "product_types": json.dumps(obj.get("product_types")),
                "ingested_at": ingested_at,
                "raw_payload": obj,
            }
        )

    logging.info(
        f"Total domain products fetched for company {company_guid}, domain {domain_name}: {len(records)}"
    )
    return records
