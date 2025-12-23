#!/usr/bin/env python3
"""
ingest/companies.py

Net-new / delta-aware ingestion for BitSight companies.

Semantics:
- Fetch authoritative company snapshot from BitSight
- Load current DB state
- Reconcile:
    NEW       -> insert
    UPDATED   -> update
    REMOVED   -> mark inactive
    UNCHANGED -> no-op
- Execute actions through IngestionExecutor for deterministic counts
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, Tuple

from core.ingestion import IngestionExecutor, IngestionResult
from core.db_router import DatabaseRouter

from ingest.companies_api import fetch_companies  # existing API fetcher


# ============================================================
# Normalization
# ============================================================

def _normalize_company(api_company: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize API payload into a comparable DB-shaped dict.
    """
    return {
        "company_guid": api_company["company_guid"],
        "name": api_company.get("name"),
        "domain": api_company.get("domain"),
        "country": api_company.get("country"),
        "added_date": api_company.get("added_date"),
    }


# ============================================================
# DB helpers
# ============================================================

def _load_db_companies(db) -> Dict[str, Dict[str, Any]]:
    """
    Load existing companies keyed by company_guid.
    """
    rows = db.fetch_all(
        """
        SELECT
            company_guid,
            name,
            domain,
            country,
            added_date,
            is_active
        FROM companies
        """
    )
    return {row["company_guid"]: dict(row) for row in rows}


def _companies_equal(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """
    Compare two normalized company records (ignores is_active).
    """
    for k in a.keys():
        if a.get(k) != b.get(k):
            return False
    return True


# ============================================================
# Writer
# ============================================================

class CompanyWriter:
    """
    Stateful writer used by IngestionExecutor.
    """

    def __init__(self, db):
        self.db = db

    def insert(self, company: Dict[str, Any]) -> None:
        self.db.execute(
            """
            INSERT INTO companies (
                company_guid,
                name,
                domain,
                country,
                added_date,
                is_active
            )
            VALUES (?, ?, ?, ?, ?, 1)
            """,
            (
                company["company_guid"],
                company["name"],
                company["domain"],
                company["country"],
                company["added_date"],
            ),
        )

    def update(self, company: Dict[str, Any]) -> None:
        self.db.execute(
            """
            UPDATE companies SET
                name = ?,
                domain = ?,
                country = ?,
                added_date = ?,
                is_active = 1
            WHERE company_guid = ?
            """,
            (
                company["name"],
                company["domain"],
                company["country"],
                company["added_date"],
                company["company_guid"],
            ),
        )

    def deactivate(self, company_guid: str) -> None:
        self.db.execute(
            "UPDATE companies SET is_active = 0 WHERE company_guid = ?",
            (company_guid,),
        )


# ============================================================
# Ingest entrypoint
# ============================================================

def run(args) -> IngestionResult:
    """
    CLI entrypoint.
    """

    # --------------------------------------------------------
    # DB
    # --------------------------------------------------------
    db = DatabaseRouter.get_database(
        backend="mssql",
        server=args.mssql_server,
        database=args.mssql_database,
        username=args.mssql_username,
        password=args.mssql_password,
    )
    db.connect()

    try:
        # ----------------------------------------------------
        # Fetch API companies
        # ----------------------------------------------------
        api_companies = fetch_companies(args)
        normalized_api = {
            c["company_guid"]: _normalize_company(c)
            for c in api_companies
        }

        # ----------------------------------------------------
        # Load DB companies
        # ----------------------------------------------------
        db_companies = _load_db_companies(db)

        writer = CompanyWriter(db)

        # ----------------------------------------------------
        # Build reconciliation plan
        # ----------------------------------------------------
        actions: list[Tuple[str, Dict[str, Any]]] = []

        # NEW + UPDATED
        for guid, api_company in normalized_api.items():
            db_company = db_companies.get(guid)

            if not db_company:
                actions.append(("insert", api_company))
                continue

            if not _companies_equal(api_company, db_company):
                actions.append(("update", api_company))

        # REMOVED
        for guid, db_company in db_companies.items():
            if guid not in normalized_api and db_company.get("is_active"):
                actions.append(("deactivate", {"company_guid": guid}))

        # ----------------------------------------------------
        # Executor wiring
        # ----------------------------------------------------
        def fetcher() -> Iterable[Any]:
            return actions

        def writer_fn(action: Tuple[str, Dict[str, Any]]) -> None:
            op, payload = action
            if op == "insert":
                writer.insert(payload)
            elif op == "update":
                writer.update(payload)
            elif op == "deactivate":
                writer.deactivate(payload["company_guid"])
            else:
                raise RuntimeError(f"Unknown operation {op}")

        executor = IngestionExecutor(
            fetcher=fetcher,
            writer=writer_fn,
            expected_min_records=0,
            show_progress=not args.no_progress,
        )

        result = executor.run()
        db.commit()
        return result

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()
