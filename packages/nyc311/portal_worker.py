from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select

from packages.audit import append_audit_event
from packages.db import FilingJob, Incident, ServiceRequestCase, get_session
from packages.nyc311.planner import (
    SUBMISSION_UNCERTAIN_JOB_STATES,
    claim_next_job,
    claimed_filing_job_is_current,
    now_iso,
    recover_cancelled_filing_job,
)
from packages.nyc311.portal import PortalSubmissionCancelled, PortalSubmissionUncertain, lookup_service_request_status, submit_elevator_complaint
from packages.nyc311.tracker import apply_portal_lookup_status, create_case_from_filing_job
from packages.worker_jobs import _safe_sync_sheets


def _append_note(existing: str | None, extra: str) -> str:
    extra = extra.strip()
    if not extra:
        return (existing or "")[:2000]
    if not existing:
        return extra[:2000]
    extra = extra[:2000]
    available = max(0, 2000 - len(extra) - 3)
    return f"{existing[:available]} | {extra}" if available else extra


def _begin_submission(job_id: int) -> bool:
    """Durably fence the final click before any external submission can occur."""
    with get_session() as session:
        job = session.scalar(select(FilingJob).where(FilingJob.job_id == job_id).with_for_update())
        if job is None:
            return False
        if job.incident_id:
            session.scalar(select(Incident).where(Incident.incident_id == job.incident_id).with_for_update())
        if not claimed_filing_job_is_current(session, job):
            return False
        job.state = "submitting"
        job.updated_at = now_iso()
        job.notes = _append_note(job.notes, "final submit armed; receipt reconciliation required before any retry")
        session.commit()
        return True


def _audit(kind: str, job_id: int, details: dict[str, Any]) -> None:
    # Audit/reporting failures cannot change a known external submission into
    # retryable work. The job state and receipt are committed independently.
    try:
        append_audit_event(kind, str(job_id), details)
    except Exception:
        pass


def _submission_failure(job_id: int, exc: Exception, *, receipt=None) -> dict[str, Any]:
    unknown = receipt is not None or isinstance(exc, PortalSubmissionUncertain)
    state = "submission_unknown" if unknown else "failed"
    persistence_error = None
    try:
        with get_session() as session:
            job = session.scalar(select(FilingJob).where(FilingJob.job_id == job_id).with_for_update())
            if job is not None:
                unknown = unknown or job.state in SUBMISSION_UNCERTAIN_JOB_STATES
                if job.state == "submitted":
                    # Another receipt reconciliation may already have completed.
                    state = "submitted"
                elif job.state not in {"claimed", "failed", "pending"} | SUBMISSION_UNCERTAIN_JOB_STATES:
                    state = job.state
                else:
                    state = "submission_unknown" if unknown else "failed"
                    job.state = state
                    job.updated_at = now_iso()
                    job.last_error = str(exc)[:2000]
                    job.notes = _append_note(job.notes, "receipt reconciliation required; do not retry" if unknown else "failed before final submit in portal_playwright")
                    if receipt is not None:
                        job.notes = _append_note(job.notes, f"portal_receipt={receipt.service_request_number}")
                    session.commit()
    except Exception as persist_exc:
        # In the uncertain path, the earlier committed 'submitting' fence still
        # prevents stale-claim recovery even when the database is unavailable.
        persistence_error = str(persist_exc)[:500]
    meta = {"error": str(exc)[:500], "state": state}
    if receipt is not None:
        meta["service_request_number"] = receipt.service_request_number
    if persistence_error:
        meta["persistence_error"] = persistence_error
    _audit("PORTAL_FILING_SUBMISSION_UNKNOWN" if unknown else "PORTAL_FILING_FAILED", job_id, meta)
    return {"ok": False, "state": state, "submission_unknown": unknown, **meta}


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
        submission = submit_elevator_complaint(
            payload,
            headless=headless,
            submit_live=True,
            before_submit=lambda: _begin_submission(job_id),
        )
        if not submission.service_request_number:
            raise RuntimeError("Portal submission finished without a service request number")
    except PortalSubmissionCancelled as exc:
        with get_session() as session:
            job = session.scalar(select(FilingJob).where(FilingJob.job_id == job_id).with_for_update())
            state = "missing"
            if job:
                state = recover_cancelled_filing_job(session, job)
                if state == "skipped":
                    job.last_error = None
                    job.notes = _append_note(job.notes, "cancelled at portal review; filing is ineligible or already has a receipt")
                session.commit()
        _safe_sync_sheets()
        _audit("PORTAL_FILING_CANCELLED", job_id, {"reason": str(exc)[:500], "state": state})
        return {"ok": state in {"pending", "skipped", "submitted"}, "job": job_meta, "job_id": job_id, "skipped": state == "skipped", "requeued": state == "pending", "state": state, "reason": str(exc)}
    except Exception as exc:
        failure = _submission_failure(job_id, exc)
        _safe_sync_sheets()
        return {"job": job_meta, "job_id": job_id, **failure}

    # Store the externally issued receipt first. A slow/broken optional lookup
    # must never leave a successfully submitted request in a retryable state.
    try:
        with get_session() as session:
            job = session.scalar(select(FilingJob).where(FilingJob.job_id == job_id).with_for_update())
            if not job:
                raise RuntimeError(f"Claimed filing job {job_id} disappeared before completion")
            job.filing_channel = "portal_playwright"
            if submission.review_screenshot_path:
                job.notes = _append_note(job.notes, f"review={submission.review_screenshot_path}")
            if submission.confirmation_screenshot_path:
                job.notes = _append_note(job.notes, f"confirmation={submission.confirmation_screenshot_path}")
            create_case_from_filing_job(session, job=job, sr_number=submission.service_request_number)
            session.commit()
    except Exception as exc:
        failure = _submission_failure(job_id, exc, receipt=submission)
        _safe_sync_sheets()
        return {"job": job_meta, "job_id": job_id, **failure}

    lookup = None
    lookup_verified = False
    if verify_lookup:
        try:
            lookup = lookup_service_request_status(submission.service_request_number, headless=headless)
            if not (isinstance(lookup, dict) and lookup.get("error")):
                with get_session() as session:
                    case = session.scalar(select(ServiceRequestCase).where(ServiceRequestCase.service_request_number == submission.service_request_number))
                    if case is not None:
                        lookup_verified = apply_portal_lookup_status(case, lookup)
                        session.commit()
        except Exception as exc:
            lookup_verified = False
            lookup = {"error": str(exc)}
            _audit("PORTAL_FILING_LOOKUP_FAILED", job_id, {"service_request_number": submission.service_request_number, "error": str(exc)[:500]})

    _safe_sync_sheets()
    _audit(
        "PORTAL_FILING_SUBMITTED",
        job_id,
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
        "lookup_verified": lookup_verified,
        "lookup": lookup.__dict__ if hasattr(lookup, "__dict__") else lookup,
    }
