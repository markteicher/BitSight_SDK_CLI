#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import Dict, Any, List

from core.status_codes import StatusCode
from core.transport import TransportError
from ingest.base import BitSightIngestBase


BITSIGHT_COMPANY_REQUESTS_ENDPOINT = "/ratings/v1/company-requests"


def fetch_company_requests(
    ingest: BitSightIngestBase,
) -> List[Dict[str, Any]]:
    """
    Fetch all company access requests.

    Endpoint:
        GET /ratings/v1/company-requests

    Deterministic pagination via limit/offset and links.next.
    """

    ingested_at = datetime.utcnow()
    path = BITSIGHT_COMPANY_REQUESTS_ENDPOINT

    records: List[Dict[str, Any]] = []

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}

        logging.info(
            "Fetching company requests (limit=%d, offset=%d)",
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
                    "request_guid": obj.get("guid"),
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
        "Total company requests fetched: %d",
        len(records),
    )

    return records
