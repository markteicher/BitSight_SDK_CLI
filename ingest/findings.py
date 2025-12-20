#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

# BitSight Findings endpoint (per company)
BITSIGHT_COMPANY_FINDINGS_ENDPOINT = "/ratings/v1/companies/{company_guid}/findings"


def fetch_findings(
    session: requests.Session,
    base_url: str,
    api_key: str,
    company_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch findings for a single company.
    Deterministic pagination using links.next.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    endpoint = BITSIGHT_COMPANY_FINDINGS_ENDPOINT.format(company_guid=company_guid)
    url = f"{base_url}{endpoint}"
    headers = {"Accept": "application/json"}

    records: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        logging.info(
            f"Fetching findings for company {company_guid}: {url} "
            f"(limit={limit}, offset={offset})"
        )

        resp = session.get(
            url,
            headers=headers,
            auth=(api_key, ""),
            params=params,
            timeout=timeout,
            proxies=proxies,
        )
        resp.raise_for_status()

        payload = resp.json()
        results = payload.get("results", [])

        for obj in results:
            records.append(_normalize_finding(obj, company_guid, ingested_at))

        links = payload.get("links") or {}
        next_link = links.get("next")

        if next_link:
            url = _absolutize_next(url, next_link)
            offset += limit
            continue

        if len(results) < limit:
            break

        offset += limit

    logging.info(
        f"Total findings fetched for company {company_guid}: {len(records)}"
    )
    return records


def _normalize_finding(
    obj: Dict[str, Any],
    company_guid: str,
    ingested_at: datetime,
) -> Dict[str, Any]:
    """
    Map a finding object into dbo.bitsight_findings schema.
    """

    return {
        "finding_guid": obj.get("guid"),
        "company_guid": company_guid,
        "risk_vector": obj.get("risk_vector"),
        "severity": (obj.get("severity") or {}).get("level")
        if isinstance(obj.get("severity"), dict)
        else obj.get("severity"),
        "status": obj.get("status"),
        "first_seen_date": obj.get("first_seen_date"),
        "last_seen_date": obj.get("last_seen_date"),
        "remediation_date": obj.get("remediation_date"),
        "ingested_at": ingested_at,
        "raw_payload": obj,
    }


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
