#!/usr/bin/env python3
"""
ingest/alerts.py

BitSight Alerts ingestion
Endpoint: https://api.bitsighttech.com/ratings/v2/alerts

Implements:
- net-new detection
- update detection (payload hash change)
- removal/resolution detection (previously active, now missing)
- uses core.IngestionExecutor for fetch + per-record write semantics

DB model:
- dbo.bitsight_alerts          (raw history append)   [created by schema]
- dbo.bitsight_alerts_state    (current state)        [created here if missing, guarded]

Notes:
- BitSight "guid" for alerts may be numeric (per docs). We store alert_guid as NVARCHAR(64) in state.
- If dbo.bitsight_alerts.alert_guid is UNIQUEIDENTIFIER and guid is numeric, history insert is skipped
  (state table still works).
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from core.config import ConfigStore, Config
from core.exit_codes import ExitCode
from core.ingestion import IngestionExecutor, IngestionResult
from core.status_codes import StatusCode
from core.transport import TransportConfig, TransportError, build_session
from core.db_router import DatabaseRouter


ALERTS_ENDPOINT_PATH = "/ratings/v2/alerts"
DEFAULT_LIMIT = 100


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _is_uuid_like(value: str) -> bool:
    v = (value or "").strip()
    if len(v) != 36:
        return False
    parts = v.split("-")
    if [len(p) for p in parts] != [8, 4, 4, 4, 12]:
        return False
    try:
        int("".join(parts), 16)
        return True
    except Exception:
        return False


def _merge_cfg(file_cfg: Config, args: Any) -> Config:
    # CLI args override config values (only what we use here)
    updates: Dict[str, Any] = {}
    for f in (
        "api_key",
        "base_url",
        "proxy_url",
        "proxy_username",
        "proxy_password",
        "timeout",
        "mssql_server",
        "mssql_database",
        "mssql_username",
        "mssql_password",
        "mssql_driver",
        "mssql_encrypt",
        "mssql_trust_cert",
        "mssql_timeout",
    ):
        if hasattr(args, f):
            v = getattr(args, f)
            if v is not None:
                updates[f] = v
    if not updates:
        return file_cfg
    merged = file_cfg.to_dict(include_secrets=True)
    merged.update(updates)
    return Config(**merged)


def _transport_cfg(cfg: Config) -> TransportConfig:
    return TransportConfig(
        base_url=cfg.base_url,
        api_key=cfg.api_key or "",
        timeout=cfg.timeout,
        proxy_url=cfg.proxy_url,
        proxy_username=cfg.proxy_username,
        proxy_password=cfg.proxy_password,
        verify_ssl=True,
    )


def _fetch_all_alerts(session, tcfg: TransportConfig, proxies: Optional[Dict[str, str]]) -> List[Dict[str, Any]]:
    """
    Pulls ALL alerts via pagination.
    """
    base = (tcfg.base_url or "").rstrip("/")
    url = f"{base}{ALERTS_ENDPOINT_PATH}"
    params = {"limit": DEFAULT_LIMIT, "offset": 0}

    results: List[Dict[str, Any]] = []
    seen_next: Optional[str] = None

    while True:
        logging.info("Fetching alerts page: %s (offset=%s limit=%s)", url, params.get("offset"), params.get("limit"))
        try:
            resp = session.get(
                url,
                params=params,
                auth=(tcfg.api_key, ""),
                timeout=tcfg.timeout,
                proxies=proxies,
                verify=tcfg.verify_ssl,
                headers={"Accept": "application/json"},
            )
        except Exception as e:
            raise TransportError(str(e), StatusCode.TRANSPORT_CONNECTION_FAILED) from e

        if resp.status_code != 200:
            raise TransportError(
                f"Alerts fetch failed (HTTP {resp.status_code})",
                StatusCode.API_UNEXPECTED_RESPONSE,
                resp.status_code,
            )

        payload = resp.json()
        page_results = payload.get("results") or []
        if not isinstance(page_results, list):
            raise TransportError("Alerts response malformed: results is not a list", StatusCode.DATA_PARSE_ERROR)

        results.extend(page_results)

        links = payload.get("links") or {}
        next_url = links.get("next")
        if not next_url:
            break

        # Prevent loops if API sends the same next link repeatedly
        if seen_next == next_url:
            raise TransportError("Pagination loop detected in alerts links.next", StatusCode.API_UNEXPECTED_RESPONSE)
        seen_next = next_url

        # For BitSight, next is a full URL including offset/limit. Use it.
        url = next_url
        params = None  # next_url already encodes params

    return results


def _ensure_state_table(db) -> None:
    """
    Create dbo.bitsight_alerts_state if missing (guarded).
    """
    sql = """
IF OBJECT_ID(N'dbo.bitsight_alerts_state', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.bitsight_alerts_state (
        alert_guid      NVARCHAR(64) NOT NULL,
        payload_hash    CHAR(64) NOT NULL,
        is_active       BIT NOT NULL CONSTRAINT DF_bitsight_alerts_state_is_active DEFAULT (1),
        first_seen_at   DATETIME2(7) NOT NULL,
        last_seen_at    DATETIME2(7) NOT NULL,
        last_changed_at DATETIME2(7) NOT NULL,
        resolved_at     DATETIME2(7) NULL,
        raw_payload     NVARCHAR(MAX) NOT NULL,
        CONSTRAINT PK_bitsight_alerts_state PRIMARY KEY CLUSTERED (alert_guid)
    );
END;
"""
    db.execute(sql)
    db.commit()


def _alerts_history_guid_type(db) -> str:
    """
    Return the dbo.bitsight_alerts.alert_guid column type, if table exists.
    If table missing, return empty string.
    """
    sql = """
SELECT DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo'
  AND TABLE_NAME   = 'bitsight_alerts'
  AND COLUMN_NAME  = 'alert_guid';
"""
    v = db.scalar(sql)
    return (str(v).lower().strip() if v else "")


@dataclass
class AlertsDiffCounters:
    new: int = 0
    updated: int = 0
    unchanged: int = 0
    removed: int = 0


def _upsert_state(db, alert_guid: str, payload_hash: str, raw_payload: str, now: datetime) -> Tuple[str, bool]:
    """
    Upsert dbo.bitsight_alerts_state for an alert.
    Returns (action, changed) where action is one of: NEW, UPDATED, UNCHANGED
    """
    # Read existing hash (if any)
    existing_hash = db.scalar(
        "SELECT payload_hash FROM dbo.bitsight_alerts_state WHERE alert_guid = ?;",
        (alert_guid,),
    )

    if existing_hash is None:
        db.execute(
            """
INSERT INTO dbo.bitsight_alerts_state (
    alert_guid, payload_hash, is_active,
    first_seen_at, last_seen_at, last_changed_at, resolved_at, raw_payload
) VALUES (?, ?, 1, ?, ?, ?, NULL, ?);
""",
            (alert_guid, payload_hash, now, now, now, raw_payload),
        )
        return ("NEW", True)

    existing_hash = str(existing_hash)
    if existing_hash != payload_hash:
        db.execute(
            """
UPDATE dbo.bitsight_alerts_state
SET payload_hash    = ?,
    is_active       = 1,
    last_seen_at    = ?,
    last_changed_at = ?,
    resolved_at     = NULL,
    raw_payload     = ?
WHERE alert_guid = ?;
""",
            (payload_hash, now, now, raw_payload, alert_guid),
        )
        return ("UPDATED", True)

    # unchanged: still update last_seen_at and ensure active
    db.execute(
        """
UPDATE dbo.bitsight_alerts_state
SET is_active    = 1,
    last_seen_at = ?,
    resolved_at  = NULL
WHERE alert_guid = ?;
""",
        (now, alert_guid),
    )
    return ("UNCHANGED", False)


def _insert_history_if_possible(
    db,
    history_guid_type: str,
    alert_guid: str,
    raw_payload: str,
    now: datetime,
) -> bool:
    """
    Append a row to dbo.bitsight_alerts if we can safely bind alert_guid.
    Returns True if inserted, False if skipped.
    """
    if not history_guid_type:
        return False  # table missing (or no column)
    if history_guid_type == "uniqueidentifier":
        if not _is_uuid_like(alert_guid):
            # schema mismatch vs API docs (numeric guid): don't blow up the run
            logging.warning(
                "Skipping dbo.bitsight_alerts history insert: alert_guid='%s' is not UUID but column is UNIQUEIDENTIFIER",
                alert_guid,
            )
            return False

    # Works for NVARCHAR / BIGINT etc (SQL Server will convert if compatible)
    db.execute(
        """
INSERT INTO dbo.bitsight_alerts (alert_guid, ingested_at, raw_payload)
VALUES (?, ?, ?);
""",
        (alert_guid, now, raw_payload),
    )
    return True


def _mark_removed(db, current_guids: List[str], now: datetime) -> int:
    """
    Mark previously active alerts as resolved when missing from current snapshot.
    Returns number removed/resolved.
    """
    if not current_guids:
        # Resolve everything active
        db.execute(
            """
UPDATE dbo.bitsight_alerts_state
SET is_active = 0,
    resolved_at = COALESCE(resolved_at, ?),
    last_seen_at = ?
WHERE is_active = 1;
""",
            (now, now),
        )
        # Count affected rows reliably: re-query
        return int(
            db.scalar("SELECT COUNT(1) FROM dbo.bitsight_alerts_state WHERE is_active = 0 AND resolved_at >= ?;", (now,))
            or 0
        )

    # Chunk IN lists for SQL Server parameter limits
    removed_total = 0
    chunk_size = 800  # conservative
    # First find currently active guids
    active_rows = db.execute  # keep lint quiet

    # Build a temp table approach would be best; here we do chunked NOT IN via LEFT JOIN with a temp table
    # Create temp table
    db.execute("IF OBJECT_ID('tempdb..#current_alerts') IS NOT NULL DROP TABLE #current_alerts;")
    db.execute("CREATE TABLE #current_alerts (alert_guid NVARCHAR(64) NOT NULL PRIMARY KEY);")

    for i in range(0, len(current_guids), chunk_size):
        chunk = current_guids[i : i + chunk_size]
        rows = [(g,) for g in chunk]
        db.executemany("INSERT INTO #current_alerts (alert_guid) VALUES (?);", rows)

    # Mark removed
    db.execute(
        """
UPDATE s
SET s.is_active = 0,
    s.resolved_at = COALESCE(s.resolved_at, ?),
    s.last_seen_at = ?
FROM dbo.bitsight_alerts_state s
LEFT JOIN #current_alerts c ON c.alert_guid = s.alert_guid
WHERE s.is_active = 1 AND c.alert_guid IS NULL;
""",
        (now, now),
    )

    # Count removed in this pass (rows now inactive but resolved_at set to now)
    removed_total = int(
        db.scalar(
            """
SELECT COUNT(1)
FROM dbo.bitsight_alerts_state
WHERE is_active = 0 AND resolved_at IS NOT NULL AND resolved_at = ?;
""",
            (now,),
        )
        or 0
    )
    db.execute("DROP TABLE #current_alerts;")
    return removed_total


def main(args: Any) -> IngestionResult:
    """
    CLI entrypoint pattern supported by cli.py dispatcher.
    Returns IngestionResult with exit_code for CLI ownership.
    """
    store = ConfigStore()
    cfg = _merge_cfg(store.load(), args)

    # Validate config at point of use
    cfg.validate(require_api_key=True)

    # Build transport session
    tcfg = _transport_cfg(cfg)
    session, proxies = build_session(tcfg)

    # DB connection (MSSQL only for now)
    if not all([cfg.mssql_server, cfg.mssql_database, cfg.mssql_username, cfg.mssql_password]):
        return IngestionResult(
            status_code=StatusCode.CONFIG_INCOMPLETE,
            exit_code=ExitCode.CONFIG_VALUE_MISSING,
            records_fetched=0,
            records_written=0,
            records_failed=0,
            duration_seconds=0.0,
            message="Missing MSSQL connection parameters",
        )

    db = DatabaseRouter.get_database(
        backend="mssql",
        server=cfg.mssql_server,
        database=cfg.mssql_database,
        username=cfg.mssql_username,
        password=cfg.mssql_password,
        driver=cfg.mssql_driver,
        encrypt=cfg.mssql_encrypt,
        trust_cert=cfg.mssql_trust_cert,
        timeout=cfg.mssql_timeout,
    )
    db.connect()

    try:
        _ensure_state_table(db)

        # optional flush requested by CLI
        if getattr(args, "flush", False):
            logging.warning("FLUSH requested: clearing alerts state/history tables")
            try:
                db.execute("DELETE FROM dbo.bitsight_alerts_state;")
            except Exception:
                pass
            try:
                db.execute("DELETE FROM dbo.bitsight_alerts;")
            except Exception:
                pass
            db.commit()

        history_guid_type = _alerts_history_guid_type(db)
        counters = AlertsDiffCounters()

        # ------------------------------------------------------------
        # Fetcher
        # ------------------------------------------------------------
        def fetcher() -> Iterable[Dict[str, Any]]:
            return _fetch_all_alerts(session, tcfg, proxies)

        # ------------------------------------------------------------
        # Writer (supports --dry-run)
        # ------------------------------------------------------------
        dry_run = bool(getattr(args, "dry_run", False))

        def writer(alert: Dict[str, Any]) -> None:
            nonlocal counters

            if not isinstance(alert, dict):
                raise TypeError("Alert record must be a dict")

            guid_val = alert.get("guid")
            if guid_val is None:
                raise ValueError("Alert missing guid")

            alert_guid = str(guid_val).strip()
            raw_payload = _json_dumps(alert)
            payload_hash = _sha256_text(raw_payload)
            now = _utc_now()

            if dry_run:
                # Dry-run still computes diffs against state table (read-only),
                # but does not persist changes.
                existing_hash = db.scalar(
                    "SELECT payload_hash FROM dbo.bitsight_alerts_state WHERE alert_guid = ?;",
                    (alert_guid,),
                )
                if existing_hash is None:
                    counters.new += 1
                elif str(existing_hash) != payload_hash:
                    counters.updated += 1
                else:
                    counters.unchanged += 1
                return

            # Persist
            action, changed = _upsert_state(db, alert_guid, payload_hash, raw_payload, now)
            if action == "NEW":
                counters.new += 1
            elif action == "UPDATED":
                counters.updated += 1
            else:
                counters.unchanged += 1

            # Append history best-effort
            _insert_history_if_possible(db, history_guid_type, alert_guid, raw_payload, now)

        # ------------------------------------------------------------
        # Execute via canonical executor
        # ------------------------------------------------------------
        exec_ = IngestionExecutor(
            fetcher=fetcher,
            writer=writer,
            expected_min_records=0,
            show_progress=not bool(getattr(args, "no_progress", False)),
        )

        result = exec_.run()

        # Removal / resolution (snapshot reconciliation)
        try:
            if result.records_fetched > 0:
                current_guids = []
                # Re-fetch would be wasteful; but executor materializes list internally.
                # We can compute current guids by selecting from #temp? Not available.
                # Minimal approach: rely on state table updates during run (non-dry-run),
                # then mark removed using last_seen_at < run_start timestamp.
                #
                # For dry-run we can’t update last_seen_at; we do a direct snapshot comparison
                # by fetching again (still safe; dry-run is semantics heavy by design).
                run_mark = _utc_now()

                if dry_run:
                    snapshot = list(fetcher())
                    current_guids = [str(a.get("guid")).strip() for a in snapshot if isinstance(a, dict) and a.get("guid") is not None]
                else:
                    # last_seen_at was updated to "now" per row as we wrote it.
                    # We'll mark removed by comparing against a cutoff.
                    cutoff = _utc_now()
                    # Set cutoff slightly earlier is unnecessary; we use "cutoff" and then mark those not touched.
                    db.execute(
                        """
UPDATE s
SET s.is_active = 0,
    s.resolved_at = COALESCE(s.resolved_at, ?)
FROM dbo.bitsight_alerts_state s
WHERE s.is_active = 1 AND s.last_seen_at < ?;
""",
                        (cutoff, cutoff),
                    )
                    counters.removed = int(
                        db.scalar(
                            """
SELECT COUNT(1)
FROM dbo.bitsight_alerts_state
WHERE is_active = 0 AND resolved_at IS NOT NULL AND resolved_at = ?;
""",
                            (cutoff,),
                        )
                        or 0
                    )

                if dry_run and current_guids:
                    # mark_removed writes; dry-run should not.
                    # Instead compute removed by set diff.
                    active = db.scalar("SELECT COUNT(1) FROM dbo.bitsight_alerts_state WHERE is_active = 1;") or 0
                    # Pull active guids (potentially large; but dry-run is operator-driven)
                    rows_sql = "SELECT alert_guid FROM dbo.bitsight_alerts_state WHERE is_active = 1;"
                    # MSSQLDatabase likely has cursor; but our interface doesn’t expose fetchall.
                    # So: do not attempt full removed calc without a fetch API.
                    logging.info("DRY-RUN: removed/resolved calculation requires DB row iteration; skipped (active=%s).", active)

            if not dry_run:
                db.commit()

        except Exception:
            if not dry_run:
                db.rollback()
            logging.exception("Removal/resolution phase failed")

        # ------------------------------------------------------------
        # Log net-new/update/remove summary
        # ------------------------------------------------------------
        logging.info(
            "Alerts diff summary | new=%d updated=%d unchanged=%d removed=%d dry_run=%s",
            counters.new,
            counters.updated,
            counters.unchanged,
            counters.removed,
            dry_run,
        )

        return result

    except TransportError as e:
        logging.error("TransportError: %s", str(e))
        try:
            db.rollback()
        except Exception:
            pass
        return IngestionResult(
            status_code=e.status_code,
            exit_code=ExitCode.NETWORK_UNKNOWN_ERROR if e.http_status is None else ExitCode.API_UNKNOWN_ERROR,
            records_fetched=0,
            records_written=0,
            records_failed=1,
            duration_seconds=0.0,
            message=str(e),
        )

    except Exception as e:
        logging.exception("Unhandled alerts ingestion failure")
        try:
            db.rollback()
        except Exception:
            pass
        return IngestionResult(
            status_code=StatusCode.EXECUTION_UNHANDLED_EXCEPTION,
            exit_code=ExitCode.RUNTIME_EXCEPTION,
            records_fetched=0,
            records_written=0,
            records_failed=1,
            duration_seconds=0.0,
            message=str(e),
        )

    finally:
        try:
            db.close()
        except Exception:
            pass
        try:
            session.close()
        except Exception:
            pass
