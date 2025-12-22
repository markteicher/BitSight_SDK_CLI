#!/usr/bin/env python3

import json
import logging
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs

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
    Deterministic pagination using links.next when present.
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
            records.append(_normalize_finding(obj=obj, company_guid=company_guid, ingested_at=ingested_at))

        links = payload.get("links") or {}
        next_link = links.get("next")

        if next_link:
            url = _absolutize_next(url, next_link)
            next_offset = _extract_offset(url)
            if next_offset is not None:
                offset = next_offset
            else:
                offset += limit
            continue

        if len(results) < limit:
            break

        offset += limit

    logging.info(f"Total findings fetched for company {company_guid}: {len(records)}")
    return records


def _normalize_finding(obj: Dict[str, Any], company_guid: str, ingested_at: datetime) -> Dict[str, Any]:
    """
    Map finding object into dbo.bitsight_findings-style fields.
    Always retains raw_payload.
    """

    return {
        "finding_guid": obj.get("guid"),
        "company_guid": company_guid,
        "title": obj.get("title"),
        "category": obj.get("category"),
        "risk_vector": obj.get("risk_vector"),
        "severity": obj.get("severity"),
        "grade": obj.get("grade"),
        "status": obj.get("status"),
        "first_seen": obj.get("first_seen"),
        "last_seen": obj.get("last_seen"),
        "remediation_status": obj.get("remediation_status"),
        "observations": json.dumps(obj.get("observations")),
        "ingested_at": ingested_at,
        "raw_payload": obj,
    }


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)


def _extract_offset(url: str) -> Optional[int]:
    try:
        qs = parse_qs(urlparse(url).query)
        if "offset" in qs and qs["offset"]:
            return int(qs["offset"][0])
    except (ValueError, TypeError):
        return None
    return None
