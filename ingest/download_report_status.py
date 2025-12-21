#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# ----------------------------------------------------------------------
# BitSight Report Status endpoint
#
# This endpoint is used AFTER a report request has been created via:
#   POST /ratings/v1/reports/download
#
# It returns the execution status of the report job and, when complete,
# includes a time-bound download URL.
# ----------------------------------------------------------------------
BITSIGHT_REPORT_STATUS_ENDPOINT = "/ratings/v1/reports/{report_guid}/status"


def fetch_report_status(
    session: requests.Session,
    base_url: str,
    api_key: str,
    report_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Fetch the execution status of a previously requested BitSight report.

    Endpoint:
        GET /ratings/v1/reports/{report_guid}/status

    This function performs a SINGLE deterministic status check.
    It does not poll, retry, wait, or download artifacts.

    Auth:
        HTTP Basic Auth using api_key as username and blank password.

    Returns:
        A single record mapped 1:1 for storage, with the full API
        response preserved in raw_payload.
    """

    # ------------------------------------------------------------------
    # Normalize base_url to prevent double slashes
    # ------------------------------------------------------------------
    if base_url.endswith("/"):
        base_url = base_url[:-1]

    # ------------------------------------------------------------------
    # Construct full request URL
    # ------------------------------------------------------------------
    url = f"{base_url}{BITSIGHT_REPORT_STATUS_ENDPOINT.format(report_guid=report_guid)}"
    headers = {"Accept": "application/json"}

    checked_at = datetime.utcnow()

    logging.info(
        "Checking report status for report_guid=%s via %s",
        report_guid,
        url,
    )

    # ------------------------------------------------------------------
    # Execute request
    # ------------------------------------------------------------------
    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        timeout=timeout,
        proxies=proxies,
    )

    # Any non-2xx here is a real failure and should surface immediately
    resp.raise_for_status()

    payload = resp.json()

    # ------------------------------------------------------------------
    # Normalize response into a stable record
    #
    # All fields that may change shape or expand over time remain
    # preserved in raw_payload.
    # ------------------------------------------------------------------
    record = {
        "report_guid": report_guid,
        "status": payload.get("status"),
        "download_url": payload.get("download_url"),
        "expires_at": payload.get("expires_at"),
        "checked_at": checked_at,
        "raw_payload": payload,
    }

    logging.info(
        "Report status retrieved | report_guid=%s status=%s",
        report_guid,
        record.get("status"),
    )

    return record
