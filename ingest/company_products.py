#!/usr/bin/env python3

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

from core.status_codes import StatusCode
from core.transport import TransportError
from ingest.base import BitSightIngestBase


BITSIGHT_COMPANY_PRODUCTS_ENDPOINT = (
    "/ratings/v1/companies/{company_guid}/products"
)


def ingest_company_products(
    ingest: BitSightIngestBase,
    *,
    company_guid: str,
) -> Dict[str, int]:
    """
    Ingest company product relationships.

    Semantics:
    - Net-new products inserted
    - Existing products updated if changed
    - Removed products deleted
    - Snapshot-diff per company
    """

    now = datetime.now(timezone.utc)
    path = BITSIGHT_COMPANY_PRODUCTS_ENDPOINT.format(
        company_guid=company_guid
    )

    logging.info("Ingesting company products (company_guid=%s)", company_guid)

    # ------------------------------------------------------------
    # Fetch remote snapshot
    # ------------------------------------------------------------
    try:
        payload = ingest.request(path)

    except TransportError:
        raise

    except Exception as exc:
        raise TransportError(
            str(exc),
            StatusCode.INGESTION_FETCH_FAILED,
        ) from exc

    remote: Dict[str, Dict[str, Any]] = {}

    for obj in payload:
        product_guid = obj.get("product_guid")
        if not product_guid:
            continue

        remote[product_guid] = {
            "company_guid": company_guid,
            "product_guid": product_guid,
            "product_name": obj.get("product_name"),
            "provider_guid": obj.get("provider_guid"),
            "provider_name": obj.get("provider_name"),
            "provider_industry": obj.get("provider_industry"),
            "product_types": json.dumps(obj.get("product_types")),
            "company_count": obj.get("company_count"),
            "domain_count": obj.get("domain_count"),
            "percent_dependent": obj.get("percent_dependent"),
            "percent_dependent_company": obj.get(
                "percent_dependent_company"
            ),
            "relative_importance": obj.get("relative_importance"),
            "relative_criticality": obj.get("relative_criticality"),
            "relationship_source": obj.get("relationship_source"),
            "ingested_at": now,
            "raw_payload": obj,
        }

    # ------------------------------------------------------------
    # Fetch existing DB snapshot
    # ------------------------------------------------------------
    existing_rows = ingest.db.fetch_all(
        """
        SELECT *
        FROM dbo.bitsight_company_products
        WHERE company_guid = ?
        """,
        (company_guid,),
    )

    existing: Dict[str, Dict[str, Any]] = {
        row["product_guid"]: row for row in existing_rows
    }

    inserted = 0
    updated = 0
    deleted = 0

    # ------------------------------------------------------------
    # Insert + Update
    # ------------------------------------------------------------
    for product_guid, record in remote.items():
        if product_guid not in existing:
            ingest.db.insert(
                "dbo.bitsight_company_products",
                record,
            )
            inserted += 1
            continue

        db_row = existing[product_guid]

        # Compare excluding ingestion timestamp + raw payload
        changed = False
        for field, value in record.items():
            if field in ("ingested_at", "raw_payload"):
                continue
            if db_row.get(field) != value:
                changed = True
                break

        if changed:
            ingest.db.update(
                "dbo.bitsight_company_products",
                record,
                where={
                    "company_guid": company_guid,
                    "product_guid": product_guid,
                },
            )
            updated += 1

    # ------------------------------------------------------------
    # Deletions
    # ------------------------------------------------------------
    for product_guid in existing.keys():
        if product_guid not in remote:
            ingest.db.delete_where(
                "dbo.bitsight_company_products",
                {
                    "company_guid": company_guid,
                    "product_guid": product_guid,
                },
            )
            deleted += 1

    logging.info(
        "Company products ingest complete (company_guid=%s): "
        "inserted=%d updated=%d deleted=%d",
        company_guid,
        inserted,
        updated,
        deleted,
    )

    return {
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
    }
