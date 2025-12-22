#!/usr/bin/env python3

import logging
import requests
import json
from datetime import datetime
from typing import Any, List, Optional
from urllib.parse import urljoin

# BitSight Ratings Tree Product Types endpoint
BITSIGHT_RATINGS_TREE_PRODUCT_TYPES_ENDPOINT = (
    "/ratings/v1/ratings-tree/product-types"
)


def fetch_ratings_tree_product_types(
    session: requests.Session,
    base_url: str,
    api_key: str,
    timeout: int = 60,
    proxies: Optional[dict] = None,
) -> List[dict]:
    """
    Fetch product types in the BitSight ratings tree.

    Endpoint:
        GET /ratings/v1/ratings-tree/product-types

    Deterministic pagination using limit / offset and links.next.

    Full 1:1 physical field mapping of results[]:
        - product_type
        - company_guids
    """

    if base_url.endswith("/"):
        base_url = base_url[:-1]

    url = f"{base_url}{BITSIGHT_RATINGS_TREE_PRODUCT_TYPES_ENDPOINT}"
    headers = {"Accept": "application/json"}

    records: List[dict] = []
    ingested_at = datetime.utcnow()

    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}

        logging.info(
            f"Fetching ratings tree product types: {url} "
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
            records.append(
                {
                    "product_type": obj.get("product_type"),
                    "company_guids": json.dumps(obj.get("company_guids")),
                    "ingested_at": ingested_at,
                    "raw_payload": obj,
                }
            )

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
        f"Total ratings tree product types fetched: {len(records)}"
    )
    return records


def _absolutize_next(current_url: str, next_link: str) -> str:
    if next_link.startswith("http://") or next_link.startswith("https://"):
        return next_link
    return urljoin(current_url, next_link)
