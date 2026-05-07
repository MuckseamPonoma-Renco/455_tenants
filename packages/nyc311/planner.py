from __future__ import annotations
import os
import re
from datetime import datetime, timedelta, timezone
from sqlalchemy import or_, select
from sqlalchemy.orm import object_session
from packages.db import FilingJob, Incident, MessageDecision, RawMessage, ServiceRequestCase, get_session
from packages.incident.reconcile import close_superseded_open_elevator_incidents
from packages.nyc311.drafts import build_filing_draft


ACTIONABLE_ELEVATOR_EVENTS = {"outage", "still_out", "new_issue"}


ELEVATOR_ACTIONABLE_COMPLAINT_RE = re.compile(
    r"\b("
    r"out\s+of\s+(?:service|order)|not\s+working|broken|stuck|dead|"
    r"no\s+(?:the\s+)?(?:north|south|left|right)\s+(?:elevator|lift|one|side)|"
    r"not\s+(?:the\s+)?(?:north|south|left|right)\s+(?:elevator|lift)|"
    r"(?:the\s+)?(?:north|south|left|right)\s+(?:one|side)\s+(?:is\s+|are\s+|was\s+|were\s+|still\s+)?(?:out|down|dead|broken|stuck|not\s+working)|"
    r"only\s+(?:the\s+)?(?:north|south|left|right)\s+(?:elevator|lift|one|side)?\s*(?:is\s+)?(?:working|functioning|operational|running|in\s+service)|"
    r"(?:elevators?|lifts?|north|south|left|right|they|it)\s+(?:is\s+|are\s+|was\s+|were\s+|still\s+)?out|"
    r"(?:elevators?|lifts?|north|south|left|right|they|it)\s+(?:is\s+|are\s+|was\s+|were\s+|still\s+)?down|"
    r"shutdown|shut\s*off|trapped|entrapment|"
    r"alarm|"
    r"stopping\s+on\s+(?:each|every|all)\s+floor|floor[- ]by[- ]floor|"
    r"skip(?:s|ped|ping)?\s+(?:a\s+)?floor|irregular\s+floor|"
    r"doors?\s+stuck|one\s+(?:working\s+)?(?:elevator|lift)|"
    r"down\s+to\s+one|only\s+one\s+(?:working\s+)?(?:elevator|lift)|"
    r"reduced\s+service|malfunction(?:ing)?"
    r")\b",
    re.IGNORECASE,
)


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


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _proof_message_text(inc: Incident) -> str:
    refs = [ref.strip() for ref in (inc.proof_refs or "").split(",") if ref.strip()]
    if not refs:
        return ""
    try:
        with get_session() as session:
            rows = session.scalars(select(RawMessage).where(RawMessage.message_id.in_(refs))).all()
    except Exception:
        return ""
    return " ".join(" ".join((row.text or "").split()) for row in rows if row.text)


def _elevator_complaint_text(inc: Incident) -> str:
    return " ".join(
        part
        for part in (
            inc.title or "",
            inc.summary or "",
            _proof_message_text(inc),
        )
        if part
    )


def _classifier_says_actionable_elevator(inc: Incident) -> bool | None:
    session = object_session(inc)
    if session is None:
        return None
    refs = [ref.strip() for ref in (inc.proof_refs or "").split(",") if ref.strip()]
    query = select(MessageDecision).where(
        MessageDecision.is_issue.is_(True),
        MessageDecision.category == "elevator",
        or_(
            MessageDecision.incident_id == inc.incident_id,
            MessageDecision.message_id.in_(refs or [""]),
        ),
    )
    decisions = list(session.scalars(query).all())
    if not decisions:
        return None
    return any((row.event_type or "new_issue") in ACTIONABLE_ELEVATOR_EVENTS for row in decisions)


def incident_is_auto_eligible(inc: Incident) -> bool:
    if not _env_bool("AUTO_FILE_ENABLED", True):
        return False
    if _env_bool("AUTO_FILE_ELEVATOR_ONLY", True) and inc.category != "elevator":
        return False
    if inc.category == "elevator":
        classified_actionable = _classifier_says_actionable_elevator(inc)
        if classified_actionable is False:
            return False
        if classified_actionable is None:
            complaint_text = _elevator_complaint_text(inc)
            if not ELEVATOR_ACTIONABLE_COMPLAINT_RE.search(complaint_text):
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

    dedupe_key = _dedupe_key(inc)
    for row in session.new:
        if isinstance(row, FilingJob) and row.dedupe_key == dedupe_key:
            return row

    existing_job = session.scalar(select(FilingJob).where(FilingJob.dedupe_key == dedupe_key))
    if existing_job:
        return existing_job

    existing_case = session.scalar(select(ServiceRequestCase).where(ServiceRequestCase.incident_id == inc.incident_id))
    if existing_case:
        return None

    draft = build_filing_draft(inc)
    if not draft:
        return None

    job = FilingJob(
        dedupe_key=dedupe_key,
        incident_id=inc.incident_id,
        job_type="nyc311_file",
        state="pending",
        priority=max(1, 100 - int(inc.severity or 0) * 10),
        filing_channel="portal_playwright",
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
    close_superseded_open_elevator_incidents(session)
    incidents = session.scalars(select(Incident).where(Incident.status != "closed").order_by(Incident.last_ts_epoch.desc().nullslast())).all()
    jobs = []
    for inc in incidents:
        job = ensure_filing_job_for_incident(session, inc)
        if job:
            jobs.append(job)
    return jobs


def claim_next_job(session) -> tuple[FilingJob | None, int]:
    stale_after_min = _env_int("CLAIM_STALE_MINUTES", 30)
    requeued = False
    if stale_after_min > 0:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=stale_after_min)
        claimed_rows = session.scalars(select(FilingJob).where(FilingJob.state == "claimed")).all()
        for row in claimed_rows:
            claimed_at = _parse_iso(row.claimed_at) or _parse_iso(row.updated_at) or _parse_iso(row.created_at)
            if claimed_at and claimed_at <= cutoff:
                row.state = "pending"
                row.claimed_at = None
                row.updated_at = now_iso()
                note = "auto-requeued because a claimed job went stale"
                row.notes = f"{row.notes} | {note}"[:2000] if row.notes else note
                requeued = True
    if requeued:
        session.flush()

    rows = session.scalars(
        select(FilingJob)
        .where(FilingJob.state.in_(["pending", "failed"]))
        .order_by(FilingJob.priority.asc(), FilingJob.created_at.asc())
    ).all()
    skipped = 0
    for row in rows:
        incident = session.get(Incident, row.incident_id) if row.incident_id else None
        if incident is None or not incident_is_auto_eligible(incident):
            row.state = "skipped"
            row.updated_at = now_iso()
            note = "auto-skipped because incident is no longer auto-eligible"
            row.notes = f"{row.notes} | {note}"[:2000] if row.notes else note
            skipped += 1
            continue
        row.state = "claimed"
        row.claimed_at = now_iso()
        row.updated_at = now_iso()
        row.attempts = int(row.attempts or 0) + 1
        return row, skipped
    return None, skipped
