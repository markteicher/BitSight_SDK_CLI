#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import Dict, Any, Optional

from core.status_codes import StatusCode
from core.transport import TransportError
from ingest.base import BitSightIngestBase

# BitSight Download Report endpoint
BITSIGHT_DOWNLOAD_REPORT_ENDPOINT = "/ratings/v1/reports/download"


def request_report_download(
    ingest: BitSightIngestBase,
    report_type: str,
    company_guid: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Submit a report download request to BitSight.

    This endpoint initiates an asynchronous report generation job.
    It does NOT download the report content.

    Endpoint:
        POST /ratings/v1/reports/download
    """

    if not report_type or not isinstance(report_type, str):
        raise TransportError(
            "Invalid report_type",
            StatusCode.DATA_VALIDATION_ERROR,
        )

    payload: Dict[str, Any] = {
        "report_type": report_type,
    }

    if company_guid:
        payload["company_guid"] = company_guid

    requested_at = datetime.utcnow()

    logging.info(
        "Requesting report download | type=%s company_guid=%s",
        report_type,
        company_guid,
    )

    try:
        response = ingest.session.post(
            f"{ingest.base_url}{BITSIGHT_DOWNLOAD_REPORT_ENDPOINT}",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            auth=(ingest.api_key, ""),
            json=payload,
            timeout=ingest.timeout,
            proxies=ingest.proxies,
        )

    except TransportError:
        raise

    except Exception as exc:
        raise TransportError(
            str(exc),
            StatusCode.TRANSPORT_CONNECTION_FAILED,
        ) from exc

    if response.status_code not in (200, 201):
        raise TransportError(
            f"Unexpected HTTP status {response.status_code}",
            StatusCode.API_UNEXPECTED_RESPONSE,
            http_status=response.status_code,
        )

    try:
        response_payload = response.json()
    except Exception as exc:
        raise TransportError(
            "Failed to parse report download response",
            StatusCode.DATA_PARSE_ERROR,
        ) from exc

    if not isinstance(response_payload, dict):
        raise TransportError(
            "Malformed report download response payload",
            StatusCode.DATA_SCHEMA_MISMATCH,
        )

    return {
        "report_type": report_type,
        "company_guid": company_guid,
        "requested_at": requested_at,
        "raw_payload": response_payload,
    }
