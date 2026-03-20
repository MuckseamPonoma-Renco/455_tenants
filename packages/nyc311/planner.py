from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone
from sqlalchemy import or_, select
from packages.db import FilingJob, Incident, ServiceRequestCase
from packages.nyc311.drafts import build_filing_draft


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return default


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def incident_is_auto_eligible(inc: Incident) -> bool:
    if not _env_bool("AUTO_FILE_ENABLED", True):
        return False
    if _env_bool("AUTO_FILE_ELEVATOR_ONLY", True) and inc.category != "elevator":
        return False
    if inc.status == "closed":
        return False

    max_age_hours = _env_int("AUTO_FILE_MAX_INCIDENT_AGE_HOURS", 168)
    if max_age_hours > 0 and inc.last_ts_epoch:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=max_age_hours)
        if int(inc.last_ts_epoch) < int(cutoff.timestamp()):
            return False

    min_confidence = _env_int("AUTO_FILE_MIN_CONFIDENCE", 60)
    if int(inc.confidence or 0) < min_confidence:
        return False

    if _env_bool("AUTO_FILE_SKIP_NEEDS_REVIEW", False) and bool(inc.needs_review):
        return False

    min_witnesses = _env_int("AUTO_FILE_MIN_WITNESSES", 1)
    min_reports = _env_int("AUTO_FILE_MIN_REPORTS", 1)
    return int(inc.witness_count or 0) >= min_witnesses or int(inc.report_count or 0) >= min_reports or int(inc.severity or 0) >= 5


def _dedupe_key(inc: Incident) -> str:
    return f"311:{inc.incident_id}"


def ensure_filing_job_for_incident(session, inc: Incident) -> FilingJob | None:
    if not incident_is_auto_eligible(inc):
        return None

    existing_job = session.scalar(select(FilingJob).where(FilingJob.dedupe_key == _dedupe_key(inc)))
    if existing_job:
        return existing_job

    existing_case = session.scalar(select(ServiceRequestCase).where(ServiceRequestCase.incident_id == inc.incident_id))
    if existing_case:
        return None

    draft = build_filing_draft(inc)
    if not draft:
        return None

    job = FilingJob(
        dedupe_key=_dedupe_key(inc),
        incident_id=inc.incident_id,
        job_type="nyc311_file",
        state="pending",
        priority=max(1, 100 - int(inc.severity or 0) * 10),
        filing_channel="android_tasker",
        complaint_type=draft.complaint_type,
        form_target=draft.form_target,
        payload_json=draft.payload_json(),
        notes=draft.description[:2000],
        attempts=0,
        created_at=now_iso(),
        updated_at=now_iso(),
    )
    session.add(job)
    return job


def ensure_filing_jobs(session) -> list[FilingJob]:
    incidents = session.scalars(select(Incident).where(Incident.status != "closed").order_by(Incident.last_ts_epoch.desc().nullslast())).all()
    jobs = []
    for inc in incidents:
        job = ensure_filing_job_for_incident(session, inc)
        if job:
            jobs.append(job)
    return jobs


def claim_next_job(session) -> FilingJob | None:
    row = session.scalar(
        select(FilingJob)
        .where(FilingJob.state.in_(["pending", "failed"]))
        .order_by(FilingJob.priority.asc(), FilingJob.created_at.asc())
    )
    if not row:
        return None
    row.state = "claimed"
    row.claimed_at = now_iso()
    row.updated_at = now_iso()
    row.attempts = int(row.attempts or 0) + 1
    return row
