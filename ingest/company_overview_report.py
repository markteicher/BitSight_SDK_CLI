#!/usr/bin/env python3

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from core.status_codes import StatusCode
from core.transport import TransportError
from ingest.base import BitSightIngestBase


BITSIGHT_COMPANY_OVERVIEW_REPORT_ENDPOINT = "/ratings/v1/reports/company-overview"


def ingest_company_overview_report(
    ingest: BitSightIngestBase,
    *,
    company_guid: str,
    report_format: str = "pdf",
) -> Dict[str, Any]:
    """
    Ingest Company Overview Report job.

    Semantics:
    - Idempotent per (company_guid, report_format)
    - Net-new job creation only
    - Status updates allowed
    - No deletion semantics (append/update only)
    """

    now = datetime.now(timezone.utc)

    # ------------------------------------------------------------
    # Check for existing job
    # ------------------------------------------------------------
    existing: Optional[Dict[str, Any]] = ingest.db.fetch_one(
        """
        SELECT job_guid, status
        FROM dbo.bitsight_company_overview_report_jobs
        WHERE company_guid = ? AND report_format = ?
        ORDER BY requested_at DESC
        """,
        (company_guid, report_format),
    )

    if existing:
        status = existing.get("status")

        if status in ("QUEUED", "RUNNING"):
            logging.info(
                "Company overview report already in progress "
                "(company_guid=%s, format=%s, status=%s)",
                company_guid,
                report_format,
                status,
            )
            return {
                "company_guid": company_guid,
                "report_format": report_format,
                "status": status,
                "requested_at": None,
                "action": "SKIPPED_IN_PROGRESS",
            }

        if status == "COMPLETED":
            logging.info(
                "Company overview report already completed "
                "(company_guid=%s, format=%s)",
                company_guid,
                report_format,
            )
            return {
                "company_guid": company_guid,
                "report_format": report_format,
                "status": status,
                "requested_at": None,
                "action": "SKIPPED_COMPLETED",
            }

    # ------------------------------------------------------------
    # Request new report
    # ------------------------------------------------------------
    payload = {
        "company_guid": company_guid,
        "format": report_format,
    }

    logging.info(
        "Requesting Company Overview Report "
        "(company_guid=%s, format=%s)",
        company_guid,
        report_format,
    )

    try:
        response = ingest.post(
            BITSIGHT_COMPANY_OVERVIEW_REPORT_ENDPOINT,
            json_body=payload,
        )

    except TransportError:
        raise

    except Exception as exc:
        raise TransportError(
            str(exc),
            StatusCode.INGESTION_FETCH_FAILED,
        ) from exc

    record = {
        "company_guid": company_guid,
        "report_format": report_format,
        "requested_at": now,
        "status": response.get("status", "QUEUED"),
        "job_guid": response.get("guid"),
        "raw_payload": response,
    }

    ingest.db.insert(
        "dbo.bitsight_company_overview_report_jobs",
        record,
    )

    return {
        "company_guid": company_guid,
        "report_format": report_format,
        "status": record["status"],
        "requested_at": now,
        "action": "REQUESTED",
    }
