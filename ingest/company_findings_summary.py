#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import Dict, Any

from core.status_codes import StatusCode
from core.transport import TransportError
from ingest.base import BitSightIngestBase


BITSIGHT_COMPANY_FINDINGS_SUMMARY_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/findings/summary"
)


def fetch_company_findings_summary(
    ingest: BitSightIngestBase,
    company_guid: str,
) -> Dict[str, Any]:
    """
    Fetch findings summary for a single company.

    Endpoint:
        GET /ratings/v1/companies/{company_guid}/findings/summary

    Returns a single record mapped 1:1 into dbo.bitsight_company_findings_summary.
    """

    ingested_at = datetime.utcnow()
    path = BITSIGHT_COMPANY_FINDINGS_SUMMARY_ENDPOINT.format(
        company_guid=company_guid
    )

    logging.info("Fetching findings summary for company %s", company_guid)

    try:
        payload = ingest.request(path)

    except TransportError:
        raise

    except Exception as exc:
        raise TransportError(
            str(exc),
            StatusCode.INGESTION_FETCH_FAILED,
        ) from exc

    return {
        "company_guid": company_guid,
        "ingested_at": ingested_at,
        "raw_payload": payload,
    }
