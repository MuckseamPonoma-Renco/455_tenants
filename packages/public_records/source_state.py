"""Durable evidence of which official sources actually completed a refresh."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from packages.automation_status import watchdog_max_age_seconds
from packages.db import PublicRecordSourceState, PublicRecordWatch
from packages.public_records.config import source_configs
from packages.timeutil import parse_ts_to_epoch


def source_max_age_seconds() -> int:
    return watchdog_max_age_seconds()


def source_state_map(session) -> dict[str, PublicRecordSourceState]:
    return {row.source_key: row for row in session.scalars(select(PublicRecordSourceState)).all()}


def record_source_results(
    session,
    results: dict[str, dict[str, Any]],
    seen: set[tuple[str, str, str]],
) -> dict[str, Any]:
    """Record complete query unions; absence never certifies physical correction.

    Partial/failed queries preserve the previous success and presence evidence.
    Results cover a whole source (base query and every required linked query).
    """
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    states = source_state_map(session)
    all_records = list(session.scalars(select(PublicRecordWatch)).all())
    for source in source_configs():
        result = results.get(source.key)
        row = states.get(source.key)
        if row is None:
            row = PublicRecordSourceState(source_key=source.key)
            session.add(row)
        row.last_attempt_at = now
        row.query_count = int((result or {}).get("query_count", 0))
        row.source_errors = int((result or {}).get("source_errors", 0))
        row.row_count = sum(1 for key in seen if key[0] == source.key)
        complete = bool(result and result.get("complete") and not row.source_errors)
        if complete:
            row.state = "ready" if row.row_count else "empty"
            row.last_success_at = now
            row.error_summary = None
            for record in all_records:
                if record.source_system != source.key:
                    continue
                key = (record.source_system, record.record_type, record.record_key)
                record.source_presence_status = "present" if key in seen else "not_seen"
                record.source_checked_at = now
        else:
            row.state = "partial" if result and row.row_count else "error"
            row.source_errors = max(1, row.source_errors)
            row.error_summary = json.dumps((result or {}).get("errors") or ["Source refresh did not complete."], ensure_ascii=False)[:2000]
    session.flush()
    return watchdog_source_health(session)


def watchdog_source_health(session, *, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    now_epoch = int(now.timestamp())
    max_age = source_max_age_seconds()
    states = source_state_map(session)
    sources = []
    for source in source_configs():
        row = states.get(source.key)
        epoch = parse_ts_to_epoch(row.last_success_at) if row else None
        fresh = epoch is not None and -300 <= now_epoch - epoch <= max_age
        state = row.state if row else "unknown"
        if state in {"ready", "empty"} and not fresh:
            state = "stale"
        sources.append({
            "source_key": source.key,
            "state": state,
            "last_attempt_at": row.last_attempt_at if row else None,
            "last_success_at": row.last_success_at if row else None,
            "row_count": row.row_count if row else 0,
            "query_count": row.query_count if row else 0,
            "source_errors": row.source_errors if row else 0,
            "ok": state in {"ready", "empty"} and fresh,
        })
    ok = bool(sources) and all(item["ok"] for item in sources)
    attempts = [item["last_attempt_at"] for item in sources if item["last_attempt_at"]]
    successes = [item["last_success_at"] for item in sources if item["last_success_at"]]
    return {
        "ok": ok,
        "state": "ready" if ok else "degraded",
        "last_attempt_at": max(attempts) if attempts else None,
        "last_success_at": min(successes) if len(successes) == len(sources) else None,
        "source_errors": sum(item["source_errors"] for item in sources),
        "max_age_seconds": max_age,
        "sources": sources,
    }
