#!/usr/bin/env python3
"""
ingest/users.py

Net-new / delta-aware ingestion for BitSight users.

Semantics:
- Fetch authoritative user snapshot from BitSight
- Load current DB state
- Reconcile:
    NEW       -> insert
    UPDATED   -> update
    REMOVED   -> mark inactive
    UNCHANGED -> no-op
- Emit deterministic counts via IngestionExecutor
"""

from __future__ import annotations

import logging
from typing import Dict, Iterable, Any, Tuple

from core.ingestion import IngestionExecutor, IngestionResult
from core.status_codes import StatusCode
from core.exit_codes import ExitCode
from core.db_router import DatabaseRouter

from ingest.users_api import fetch_users   # existing API fetcher


# ============================================================
# Normalization
# ============================================================

def _normalize_user(api_user: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize API payload into a comparable DB-shaped dict.
    """
    return {
        "user_guid": api_user["user_guid"],
        "email": api_user.get("email"),
        "friendly_name": api_user.get("friendly_name"),
        "formal_name": api_user.get("formal_name"),
        "status": api_user.get("status"),
        "mfa_status": api_user.get("mfa_status"),
        "is_available_for_contact": api_user.get("is_available_for_contact"),
        "is_company_api_token": api_user.get("is_company_api_token"),
    }


# ============================================================
# DB helpers
# ============================================================

def _load_db_users(db) -> Dict[str, Dict[str, Any]]:
    """
    Load existing users keyed by user_guid.
    """
    rows = db.fetch_all(
        """
        SELECT
            user_guid,
            email,
            friendly_name,
            formal_name,
            status,
            mfa_status,
            is_available_for_contact,
            is_company_api_token,
            is_active
        FROM users
        """
    )

    return {row["user_guid"]: dict(row) for row in rows}


def _users_equal(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """
    Compare two normalized user records (ignores is_active).
    """
    for k in a.keys():
        if a.get(k) != b.get(k):
            return False
    return True


# ============================================================
# Writer
# ============================================================

class UserWriter:
    """
    Stateful writer used by IngestionExecutor.
    """

    def __init__(self, db):
        self.db = db

    def insert(self, user: Dict[str, Any]) -> None:
        self.db.execute(
            """
            INSERT INTO users (
                user_guid,
                email,
                friendly_name,
                formal_name,
                status,
                mfa_status,
                is_available_for_contact,
                is_company_api_token,
                is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            tuple(user.values()),
        )

    def update(self, user: Dict[str, Any]) -> None:
        self.db.execute(
            """
            UPDATE users SET
                email = ?,
                friendly_name = ?,
                formal_name = ?,
                status = ?,
                mfa_status = ?,
                is_available_for_contact = ?,
                is_company_api_token = ?,
                is_active = 1
            WHERE user_guid = ?
            """,
            (
                user["email"],
                user["friendly_name"],
                user["formal_name"],
                user["status"],
                user["mfa_status"],
                user["is_available_for_contact"],
                user["is_company_api_token"],
                user["user_guid"],
            ),
        )

    def deactivate(self, user_guid: str) -> None:
        self.db.execute(
            "UPDATE users SET is_active = 0 WHERE user_guid = ?",
            (user_guid,),
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
        # Fetch API users
        # ----------------------------------------------------
        api_users = fetch_users(args)
        normalized_api = {
            u["user_guid"]: _normalize_user(u)
            for u in api_users
        }

        # ----------------------------------------------------
        # Load DB users
        # ----------------------------------------------------
        db_users = _load_db_users(db)

        writer = UserWriter(db)

        # ----------------------------------------------------
        # Build reconciliation plan
        # ----------------------------------------------------
        actions: Iterable[Tuple[str, Dict[str, Any]]] = []

        # NEW + UPDATED
        for guid, api_user in normalized_api.items():
            db_user = db_users.get(guid)

            if not db_user:
                actions.append(("insert", api_user))
                continue

            if not _users_equal(api_user, db_user):
                actions.append(("update", api_user))

        # REMOVED
        for guid, db_user in db_users.items():
            if guid not in normalized_api and db_user.get("is_active"):
                actions.append(("deactivate", {"user_guid": guid}))

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
                writer.deactivate(payload["user_guid"])
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
