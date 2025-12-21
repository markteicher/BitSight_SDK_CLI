#!/usr/bin/env python3

import json
import logging
from datetime import datetime
from typing import Dict, Any, List

from core.status_codes import StatusCode
from core.transport import TransportError
from ingest.base import BitSightIngestBase


BITSIGHT_COMPANY_PRODUCTS_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/products"
)


def fetch_company_products(
    ingest: BitSightIngestBase,
    company_guid: str,
) -> List[Dict[str, Any]]:
    """
    Fetch products used by a company.

    Endpoint:
        GET /ratings/v1/companies/{company_guid}/products

    Returns:
        List of records mapped 1:1 into dbo.bitsight_company_products.
    """

    ingested_at = datetime.utcnow()
    path = BITSIGHT_COMPANY_PRODUCTS_ENDPOINT.format(
        company_guid=company_guid
    )

    logging.info(
        "Fetching products for company %s",
        company_guid,
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
        "Total company products fetched for company %s: %d",
        company_guid,
        len(records),
    )

    return records
