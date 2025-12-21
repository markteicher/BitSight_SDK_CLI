#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import Dict, Any

from core.status_codes import StatusCode
from core.transport import TransportError
from ingest.base import BitSightIngestBase


BITSIGHT_COMPANY_OVERVIEW_REPORT_ENDPOINT = "/ratings/v1/reports/company-overview"


def request_company_overview_report(
    ingest: BitSightIngestBase,
    company_guid: str,
    report_format: str = "pdf",
) -> Dict[str, Any]:
    """
    Request a Company Overview Report.

    Endpoint:
        POST /ratings/v1/reports/company-overview

    Returns a single record mapped 1:1 into dbo.bitsight_company_overview_report_jobs.
    """

    requested_at = datetime.utcnow()
    path = BITSIGHT_COMPANY_OVERVIEW_REPORT_ENDPOINT

    payload = {
        "company_guid": company_guid,
        "format": report_format,
    }

    logging.info(
        "Requesting Company Overview Report (company_guid=%s, format=%s)",
        company_guid,
        report_format,
    )

    try:
        response_payload = ingest.post(path, json_body=payload)

    except TransportError:
        raise

    except Exception as exc:
        raise TransportError(
            str(exc),
            StatusCode.INGESTION_FETCH_FAILED,
        ) from exc

    return {
        "company_guid": company_guid,
        "report_format": report_format,
        "requested_at": requested_at,
        "raw_payload": response_payload,
    }
