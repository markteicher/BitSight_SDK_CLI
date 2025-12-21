#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from core.status_codes import StatusCode
from core.transport import TransportError
from ingest.base import BitSightIngestBase


# BitSight Client Access Links endpoint
BITSIGHT_CLIENT_ACCESS_LINKS_ENDPOINT = "/ratings/v1/client-access-links"


def fetch_client_access_links(
    ingest: BitSightIngestBase,
) -> List[Dict[str, Any]]:
    """
    Fetch client access links from BitSight.

    Deterministic pagination using links.next when present.
    Auth: HTTP Basic Auth using api_key as username and blank password.
    """

    records: List[Dict[str, Any]] = []
    ingested_at = datetime.utcnow()

    logging.info("Fetching client access links")

    try:
        for obj in ingest.paginate(BITSIGHT_CLIENT_ACCESS_LINKS_ENDPOINT):
            records.append(
                {
                    "access_link_guid": obj.get("guid"),
                    "company_guid": (obj.get("company") or {}).get("guid"),
                    "company_name": (obj.get("company") or {}).get("name"),
                    "created_at": obj.get("created_at"),
                    "expires_at": obj.get("expires_at"),
                    "status": obj.get("status"),
                    "created_by": (obj.get("created_by") or {}).get("email"),
                    "ingested_at": ingested_at,
                    "raw_payload": obj,
                }
            )

    except TransportError:
        raise

    except Exception as exc:
        raise TransportError(
            str(exc),
            StatusCode.INGESTION_FETCH_FAILED,
        ) from exc

    logging.info("Total client access links fetched: %d", len(records))
    return records
