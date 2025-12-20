#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

# BitSight Threat Evidence endpoint (v2)
BITSIGHT_THREAT_EVIDENCE_ENDPOINT = (
    "/ratings/v2/threats/{threat_guid}/companies/{entity_guid}/evidence"
)


def fetch_threat_evidence(
    session: requests.Session,
    base_url: str,
    api_key: str,
    threat_guid: str,
    entity_guid: str,
    timeout: int = 60,
    proxies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Fetch threat evidence for a specific company affected by a specific threat.

    Endpoint:
        GET /ratings/v2/threats/{threat_guid}/companies/{entity_guid}/evidence

    This endpoint is NOT paginated and returns a single evidence payload.

    Auth:
        HTTP Basic Auth using api_key as username and blank password.
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    endpoint = BITSIGHT_THREAT_EVIDENCE_ENDPOINT.format(
        threat_guid=threat_guid,
        entity_guid=entity_guid,
    )
    url = f"{base_url}{endpoint}"

    headers = {"Accept": "application/json"}
    ingested_at = datetime.utcnow()

    logging.info(
        f"Fetching threat evidence: threat={threat_guid}, entity={entity_guid}"
    )

    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    payload = resp.json()

    return {
        "threat_guid": threat_guid,
        "entity_guid": entity_guid,
        "ingested_at": ingested_at,
        "raw_payload": payload,
    }
