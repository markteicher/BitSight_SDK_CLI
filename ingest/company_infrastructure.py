#!/usr/bin/env python3

import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from core.status_codes import StatusCode
from core.transport import TransportError
from ingest.base import BitSightIngestBase


BITSIGHT_COMPANY_INFRASTRUCTURE_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/infrastructure"
)


def fetch_company_infrastructure(
    ingest: BitSightIngestBase,
    company_guid: str,
    asset_type: Optional[str] = None,   # e.g. DOMAIN, IP
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """
    Fetch infrastructure inventory for a company.

    Endpoint:
        GET /ratings/v1/companies/{company_guid}/infrastructure

    Supports pagination via links.next and optional ?type= filter.
    """

    ingested_at = datetime.utcnow()
    path = BITSIGHT_COMPANY_INFRASTRUCTURE_ENDPOINT.format(company_guid=company_guid)

    records: List[Dict[str, Any]] = []
    offset = 0

    while True:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if asset_type:
            params["type"] = asset_type

        logging.info(
            "Fetching infrastructure for company %s: %s (limit=%d, offset=%d)",
            company_guid,
            path,
            limit,
            offset,
        )

        try:
            payload = ingest.request(path, params=params)

        except TransportError:
            raise

        except Exception as exc:
            raise TransportError(
                str(exc),
                StatusCode.INGESTION_FETCH_FAILED,
            ) from exc

        results = payload.get("results", []) or []

        for obj in results:
            attributed = obj.get("attributed_to") or {}
            records.append(
                {
                    "company_guid": company_guid,
                    "temporary_id": obj.get("temporary_id"),
                    "value": obj.get("value"),
                    "asset_type": obj.get("type"),
                    "source": obj.get("source"),
                    "country": obj.get("country"),
                    "start_date": obj.get("start_date"),
                    "end_date": obj.get("end_date"),
                    "is_active": obj.get("is_active"),
                    "attributed_guid": attributed.get("guid"),
                    "attributed_name": attributed.get("name"),
                    "ip_count": obj.get("ip_count"),
                    "is_suppressed": obj.get("is_suppressed"),
                    "asn": obj.get("asn"),
                    "tags": json.dumps(obj.get("tags")),
                    "ingested_at": ingested_at,
                    "raw_payload": obj,
                }
            )

        links = payload.get("links") or {}
        next_link = links.get("next")

        if next_link:
            offset += limit
            continue

        if len(results) < limit:
            break

        offset += limit

    logging.info(
        "Total infrastructure records fetched for company %s: %d",
        company_guid,
        len(records),
    )
    return records
