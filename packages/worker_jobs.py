from sqlalchemy import select

from packages.audit import append_audit_event, daily_hash_chain
from packages.db import Incident, MessageDecision, RawMessage, get_session
from packages.incident.extractor import classify_and_upsert_incident
from packages.nyc311.legal_export import export_legal_bundle as export_bundle_impl
from packages.nyc311.planner import ensure_filing_jobs
from packages.nyc311.tracker import sync_all_case_statuses
from packages.sheets.sync import (
    sync_311_cases_to_sheets,
    sync_311_queue_to_sheets,
    sync_coverage_to_sheets,
    sync_dashboard_to_sheets,
    sync_decisions_to_sheets,
    sync_incidents_to_sheets,
)


def _safe_sync_sheets():
    try:
        sync_incidents_to_sheets()
        sync_dashboard_to_sheets()
        sync_coverage_to_sheets()
        sync_311_cases_to_sheets()
        sync_311_queue_to_sheets()
        sync_decisions_to_sheets()
    except Exception as exc:
        append_audit_event("SHEETS_SYNC_SKIPPED", None, {"error": str(exc)[:300]})


def process_message(message_id: str, *, sync_sheets: bool = True):
    with get_session() as session:
        raw = session.get(RawMessage, message_id)
        if not raw:
            return {"ok": False, "error": "missing raw message"}
        incident_id = classify_and_upsert_incident(session, raw)
        queued = ensure_filing_jobs(session)
        session.commit()

    append_audit_event("PROCESS_MESSAGE", message_id, {"incident_id": incident_id, "queued_jobs": len(queued)})
    if sync_sheets:
        _safe_sync_sheets()
    daily_hash_chain()
    return {"ok": True, "incident_id": incident_id, "queued_jobs": len(queued)}


def process_pending_messages(limit: int = 100, *, latest_first: bool = False, resync_sheets: bool = True):
    order = RawMessage.ts_epoch.desc().nullslast() if latest_first else RawMessage.ts_epoch.asc().nullsfirst()

    with get_session() as session:
        pending_ids = [
            mid
            for (mid,) in session.execute(
                select(RawMessage.message_id)
                .outerjoin(MessageDecision, MessageDecision.message_id == RawMessage.message_id)
                .where(MessageDecision.message_id.is_(None))
                .order_by(order)
                .limit(limit)
            ).all()
        ]

    processed = 0
    errors = 0
    for message_id in pending_ids:
        try:
            with get_session() as session:
                raw = session.get(RawMessage, message_id)
                if raw is None or session.get(MessageDecision, message_id) is not None:
                    continue
                classify_and_upsert_incident(session, raw)
                session.commit()
            processed += 1
        except Exception as exc:
            errors += 1
            append_audit_event("PROCESS_PENDING_MESSAGE_ERROR", message_id, {"error": str(exc)[:300]})

    with get_session() as session:
        queued = ensure_filing_jobs(session)
        session.commit()

        remaining = session.query(RawMessage).outerjoin(
            MessageDecision, MessageDecision.message_id == RawMessage.message_id
        ).filter(MessageDecision.message_id.is_(None)).count()
        incidents = session.query(Incident).count()
        decisions = session.query(MessageDecision).count()

    if resync_sheets:
        _safe_sync_sheets()

    result = {
        "ok": True,
        "pending_selected": len(pending_ids),
        "processed_total": processed,
        "errors_total": errors,
        "remaining_pending": remaining,
        "incidents_total": incidents,
        "decisions_total": decisions,
        "queued_jobs": len(queued),
    }
    append_audit_event("PROCESS_PENDING_MESSAGES", None, result)
    daily_hash_chain()
    return result


def full_resync_sheets():
    _safe_sync_sheets()
    append_audit_event("FULL_RESYNC_SHEETS", None, {})
    daily_hash_chain()
    return {"ok": True}


def reprocess_last_n(n: int):
    with get_session() as session:
        msgs = session.query(RawMessage).order_by(RawMessage.ts_epoch.desc().nullslast()).limit(n).all()
        for raw in reversed(msgs):
            classify_and_upsert_incident(session, raw)
        queued = ensure_filing_jobs(session)
        session.commit()
    _safe_sync_sheets()
    append_audit_event("REPROCESS_LAST_N", None, {"n": n, "queued_jobs": len(queued)})
    daily_hash_chain()
    return {"ok": True, "queued_jobs": len(queued)}


def queue_311_jobs():
    with get_session() as session:
        jobs = ensure_filing_jobs(session)
        session.commit()
    _safe_sync_sheets()
    append_audit_event("QUEUE_311_JOBS", None, {"queued": len(jobs)})
    return {"ok": True, "queued": len(jobs)}


def sync_311_statuses():
    with get_session() as session:
        results = sync_all_case_statuses(session)
        session.commit()
    _safe_sync_sheets()
    append_audit_event("SYNC_311_STATUSES", None, {"updated": len(results)})
    return {"ok": True, "updated": len(results)}


def export_legal_bundle():
    with get_session() as session:
        result = export_bundle_impl(session)
    append_audit_event("EXPORT_LEGAL_BUNDLE", None, result)
    return {"ok": True, **result}
