from __future__ import annotations

import json
from typing import Any

from packages.audit import append_audit_event
from packages.db import FilingJob, get_session
from packages.nyc311.planner import claim_next_job
from packages.nyc311.portal import lookup_service_request_status, submit_elevator_complaint
from packages.nyc311.tracker import create_case_from_filing_job
from packages.worker_jobs import _safe_sync_sheets


def _append_note(existing: str | None, extra: str) -> str:
    extra = extra.strip()
    if not extra:
        return (existing or "")[:2000]
    if not existing:
        return extra[:2000]
    return f"{existing} | {extra}"[:2000]


def run_portal_filing_once(*, headless: bool = True, verify_lookup: bool = True) -> dict[str, Any]:
    with get_session() as session:
        job, skipped = claim_next_job(session)
        session.commit()
        if skipped:
            _safe_sync_sheets()
        if not job:
            return {"ok": True, "job": None}
        job_id = int(job.job_id)
        job_meta = {"job_id": job_id, "incident_id": job.incident_id}
        payload = json.loads(job.payload_json or "{}")

    try:
        submission = submit_elevator_complaint(payload, headless=headless)
        if not submission.service_request_number:
            raise RuntimeError("Portal submission finished without a service request number")
    except Exception as exc:
        with get_session() as session:
            job = session.get(FilingJob, job_id)
            if job:
                job.state = "failed"
                job.last_error = str(exc)[:2000]
                job.notes = _append_note(job.notes, "failed in portal_playwright")
                session.commit()
        _safe_sync_sheets()
        append_audit_event("PORTAL_FILING_FAILED", str(job_id), {"error": str(exc)[:500]})
        return {"ok": False, "job": job_meta, "job_id": job_id, "error": str(exc)}

    lookup = None
    if verify_lookup:
        try:
            lookup = lookup_service_request_status(submission.service_request_number, headless=headless)
        except Exception as exc:
            lookup = {"error": str(exc)}

    with get_session() as session:
        job = session.get(FilingJob, job_id)
        if not job:
            raise RuntimeError(f"Claimed filing job {job_id} disappeared before completion")
        job.filing_channel = "portal_playwright"
        if submission.review_screenshot_path:
            job.notes = _append_note(job.notes, f"review={submission.review_screenshot_path}")
        if submission.confirmation_screenshot_path:
            job.notes = _append_note(job.notes, f"confirmation={submission.confirmation_screenshot_path}")
        if lookup and isinstance(lookup, dict) and lookup.get("error"):
            job.notes = _append_note(job.notes, f"lookup_error={lookup['error']}")
        case = create_case_from_filing_job(session, job=job, sr_number=submission.service_request_number)
        session.commit()

    _safe_sync_sheets()
    append_audit_event(
        "PORTAL_FILING_SUBMITTED",
        str(job_id),
        {
            "service_request_number": submission.service_request_number,
            "address_id": submission.address_id,
            "login_used": submission.login_used,
        },
    )
    return {
        "ok": True,
        "job": job_meta,
        "job_id": job_id,
        "service_request_number": submission.service_request_number,
        "address_text": submission.address_text,
        "address_id": submission.address_id,
        "review_screenshot_path": submission.review_screenshot_path,
        "confirmation_screenshot_path": submission.confirmation_screenshot_path,
        "lookup": lookup.__dict__ if hasattr(lookup, "__dict__") else lookup,
    }
