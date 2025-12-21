#!/usr/bin/env python3
"""
ingest/domain_products.py

GET enterprise products used by a single domain from a particular company.

Endpoint:
    GET /ratings/v1/companies/{company_guid}/domains/{domain_name}/products

Response:
    Top-level JSON array of product objects (no pagination).
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, List

from core.status_codes import StatusCode
from core.transport import TransportError
from ingest.base import BitSightIngestBase


BITSIGHT_DOMAIN_PRODUCTS_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/domains/{domain_name}/products"
)


def fetch_domain_products(
    ingest: BitSightIngestBase,
    company_guid: str,
    domain_name: str,
) -> List[Dict[str, Any]]:
    """
    Fetch domain products for a single company + domain.

    Returns:
        List of records mapped 1:1 into dbo.bitsight_domain_products (raw preserved).
    """

    ingested_at = datetime.utcnow()
    path = BITSIGHT_DOMAIN_PRODUCTS_ENDPOINT.format(
        company_guid=company_guid,
        domain_name=domain_name,
    )

    logging.info(
        "Fetching domain products for company %s, domain %s",
        company_guid,
        domain_name,
    )

    try:
        payload = ingest.request(path)

    except TransportError:
        raise

    except Exception as exc:
        raise TransportError(
            str(exc),
            StatusCode.INGESTION_FETCH_FAILED,
        ) from exc

    if not isinstance(payload, list):
        raise TransportError(
            "Unexpected response type (expected list)",
            StatusCode.API_UNEXPECTED_RESPONSE,
        )

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
        "Total domain products fetched for company %s, domain %s: %d",
        company_guid,
        domain_name,
        len(records),
    )
    return records
