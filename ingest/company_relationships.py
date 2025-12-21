#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import Dict, Any, List

from core.status_codes import StatusCode
from core.transport import TransportError
from ingest.base import BitSightIngestBase


BITSIGHT_COMPANY_RELATIONSHIPS_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/relationships"
)


def fetch_company_relationships(
    ingest: BitSightIngestBase,
    company_guid: str,
) -> List[Dict[str, Any]]:
    """
    Fetch relationship details for a single company.

    Endpoint:
        GET /ratings/v1/companies/{company_guid}/relationships

    Full 1:1 physical mapping of results[] fields.
    """

    ingested_at = datetime.utcnow()
    path = BITSIGHT_COMPANY_RELATIONSHIPS_ENDPOINT.format(
        company_guid=company_guid
    )

    records: List[Dict[str, Any]] = []

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}

        logging.info(
            "Fetching company relationship details for %s (limit=%d, offset=%d)",
            company_guid,
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

        results = payload.get("results", [])

        for obj in results:
            records.append(
                {
                    "relationship_guid": obj.get("guid"),
                    "company_guid": obj.get("company_guid"),
                    "company_name": obj.get("company_name"),
                    "relationship_type": obj.get("relationship_type"),
                    "creator": obj.get("creator"),
                    "last_editor": obj.get("last_editor"),
                    "created_time": obj.get("created_time"),
                    "last_edited_time": obj.get("last_edited_time"),
                    "ingested_at": ingested_at,
                    "raw_payload": obj,
                }
            )

        links = payload.get("links") or {}
        next_link = links.get("next")

        if not next_link:
            break

        offset += limit

    logging.info(
        "Total company relationship records fetched for %s: %d",
        company_guid,
        len(records),
    )

    return records
