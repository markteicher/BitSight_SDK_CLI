#!/usr/bin/env python3

import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from core.status_codes import StatusCode
from core.transport import TransportError
from ingest.base import BitSightIngestBase


BITSIGHT_COMPANY_PRODUCTS_POST_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/products"
)


def fetch_company_products_post(
    ingest: BitSightIngestBase,
    company_guid: str,
    body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    POST: Get Products of a Company

    Endpoint:
        POST /ratings/v1/companies/{company_guid}/products

    Response:
        Top-level JSON array of product objects.

    Notes:
      - Supports body-based filters and query params like fields/limit/offset/q/sort.
      - Pagination is deterministic via limit/offset when limit is provided.
    """

    ingested_at = datetime.utcnow()
    path = BITSIGHT_COMPANY_PRODUCTS_POST_ENDPOINT.format(
        company_guid=company_guid
    )

    body = body or {}
    params = params.copy() if params else {}

    limit = params.get("limit")
    offset = params.get("offset", 0)

    records: List[Dict[str, Any]] = []

    while True:
        if limit is not None:
            params["limit"] = limit
            params["offset"] = offset
            logging.info(
                "POST company products for %s: %s (limit=%s, offset=%s)",
                company_guid,
                path,
                str(limit),
                str(offset),
            )
        else:
            logging.info(
                "POST company products for %s: %s",
                company_guid,
                path,
            )

        try:
            payload = ingest.request(
                path,
                method="POST",
                params=params if params else None,
                json_body=body if body else None,
            )

        except TransportError:
            raise

        except Exception as exc:
            raise TransportError(
                str(exc),
                StatusCode.INGESTION_FETCH_FAILED,
            ) from exc

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
        "Total POST company products fetched for company %s: %d",
        company_guid,
        len(records),
    )

    return records
