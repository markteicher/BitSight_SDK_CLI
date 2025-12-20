#!/usr/bin/env python3

import logging
import requests
from datetime import datetime
from typing import Dict, Any, Optional

BITSIGHT_FINDINGS_STATISTICS_GLOBAL_ENDPOINT = "/ratings/v1/findings/statistics"


def fetch_findings_statistics_global(
    session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[dict] = None,
) -> Dict[str, Any]:

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_FINDINGS_STATISTICS_GLOBAL_ENDPOINT}"
    headers = {"Accept": "application/json"}
    ingested_at = datetime.utcnow()

    logging.info(f"Fetching global findings statistics: {url}")

    resp = session.get(
        url,
        headers=headers,
        auth=(api_key, ""),
        timeout=timeout,
        proxies=proxies,
    )
    resp.raise_for_status()

    return {
        "ingested_at": ingested_at,
        "raw_payload": resp.json(),
    }
