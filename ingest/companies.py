#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import Dict, Any, List

from core.status_codes import StatusCode
from core.transport import TransportError
from ingest.base import BitSightIngestBase


# BitSight Companies endpoint (v1)
BITSIGHT_COMPANIES_ENDPOINT = "/ratings/v1/companies"


def fetch_companies(
    ingest: BitSightIngestBase,
) -> List[Dict[str, Any]]:
    """
    Fetch all companies from BitSight.

    Deterministic pagination using links.next when present.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    companies: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    logging.info("Fetching companies")

    try:
        for obj in ingest.paginate(BITSIGHT_COMPANIES_ENDPOINT):
            companies.append(_normalize_company_record(obj, ingested_at))

    except TransportError:
        raise

    except Exception as exc:
        raise TransportError(
            str(exc),
            StatusCode.INGESTION_FETCH_FAILED,
        ) from exc

    logging.info("Total companies fetched: %d", len(companies))
    return companies


def _normalize_company_record(
    company_obj: Dict[str, Any],
    ingested_at: datetime,
) -> Dict[str, Any]:
    """
    Map BitSight company object into dbo.bitsight_companies schema fields.
    Leaves anything uncertain in raw_payload (always).
    """

    return {
        "company_guid": company_obj.get("guid") or company_obj.get("company_guid"),
        "name": company_obj.get("name"),
        "domain": company_obj.get("domain") or company_obj.get("primary_domain"),
        "country": company_obj.get("country"),
        "added_date": company_obj.get("added_date"),
        "rating": company_obj.get("rating"),
        "ingested_at": ingested_at,
        "raw_payload": company_obj,
    }
