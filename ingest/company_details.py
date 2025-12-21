#!/usr/bin/env python3

import logging
import json
from datetime import datetime
from typing import Dict, Any

from core.status_codes import StatusCode
from core.transport import TransportError
from ingest.base import BitSightIngestBase


BITSIGHT_COMPANY_DETAILS_ENDPOINT = "/ratings/v1/companies/{company_guid}"


def fetch_company_details(
    ingest: BitSightIngestBase,
    company_guid: str,
) -> Dict[str, Any]:
    """
    Fetch full company details from BitSight.

    Endpoint:
        GET /ratings/v1/companies/{company_guid}

    Returns a single record mapped 1:1 into dbo.bitsight_company_details.
    """

    ingested_at = datetime.utcnow()
    path = BITSIGHT_COMPANY_DETAILS_ENDPOINT.format(company_guid=company_guid)

    logging.info("Fetching company details for %s", company_guid)

    try:
        obj = ingest.request(path)

    except TransportError:
        raise

    except Exception as exc:
        raise TransportError(
            str(exc),
            StatusCode.INGESTION_FETCH_FAILED,
        ) from exc

    primary_company = obj.get("primary_company") or {}

    record = {
        "company_guid": obj.get("guid"),
        "custom_id": obj.get("custom_id"),
        "name": obj.get("name"),
        "shortname": obj.get("shortname"),
        "description": obj.get("description"),
        "primary_domain": obj.get("primary_domain"),
        "homepage": obj.get("homepage"),
        "industry": obj.get("industry"),
        "industry_slug": obj.get("industry_slug"),
        "sub_industry": obj.get("sub_industry"),
        "sub_industry_slug": obj.get("sub_industry_slug"),
        "ipv4_count": obj.get("ipv4_count"),
        "people_count": obj.get("people_count"),
        "search_count": obj.get("search_count"),
        "customer_monitoring_count": obj.get("customer_monitoring_count"),
        "company_type": obj.get("type"),
        "confidence": obj.get("confidence"),
        "bulk_email_sender_status": obj.get("bulk_email_sender_status"),
        "subscription_type": obj.get("subscription_type"),
        "subscription_type_key": obj.get("subscription_type_key"),
        "subscription_end_date": obj.get("subscription_end_date"),
        "sparkline_url": obj.get("sparkline"),
        "display_url": obj.get("display_url"),
        "service_provider": obj.get("service_provider"),
        "has_company_tree": obj.get("has_company_tree"),
        "has_preferred_contact": obj.get("has_preferred_contact"),
        "is_bundle": obj.get("is_bundle"),
        "is_primary": obj.get("is_primary"),
        "in_spm_portfolio": obj.get("in_spm_portfolio"),
        "is_mycomp_mysubs_bundle": obj.get("is_mycomp_mysubs_bundle"),
        "rating_industry_median": obj.get("rating_industry_median"),
        "primary_company_guid": primary_company.get("guid"),
        "primary_company_name": primary_company.get("name"),
        "permissions": json.dumps(obj.get("permissions")),
        "rating_details": json.dumps(obj.get("rating_details")),
        "ratings": json.dumps(obj.get("ratings")),
        "company_features": json.dumps(obj.get("company_features")),
        "compliance_claim": json.dumps(obj.get("compliance_claim")),
        "related_companies": json.dumps(obj.get("related_companies")),
        "ingested_at": ingested_at,
        "raw_payload": obj,
    }

    return record
