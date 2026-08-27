from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone
from sqlalchemy import or_, select
from sqlalchemy.orm import object_session
from packages.db import FilingJob, Incident, MessageDecision, RawMessage, ServiceRequestCase, get_session
from packages.incident.dedupe import dedupe_open_elevator_continuations
from packages.incident.reconcile import (
    close_superseded_open_elevator_incidents,
    repair_unbounded_elevator_restore_attachments,
)
from packages.nyc311.drafts import build_filing_draft


ACTIONABLE_ELEVATOR_EVENTS = {"outage", "still_out", "new_issue"}
EQUIVALENT_JOB_STATES = frozenset({"awaiting_approval", "approved", "pending", "claimed", "submitted"})
CLAIM_BLOCKING_EQUIVALENT_JOB_STATES = frozenset({"approved", "claimed", "submitted"})
PRE_SUBMISSION_JOB_STATES = frozenset({"awaiting_approval", "approved", "pending", "claimed", "failed"})
REFRESHABLE_JOB_STATES = frozenset({"awaiting_approval", "approved", "pending", "failed"})
FILING_APPROVAL_PHRASE = "APPROVED \u2014 GO LIVE"
APPROVAL_HASH_PREFIX = "approval_payload_sha256="


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
    r"reduced\s+service|malfunction(?:ing)?|"
    r"clunk(?:ed|ing)?|bang(?:ed|ing)?|bounce[sd]?|jolt(?:ed|ing)?|shake[sn]?|shook|"
    r"rough\s+ride|door\s+(?:opened|opening|opens)\s+(?:slow(?:ly)?|in\s+slo-?mo)|slow\s+door"
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
    if not _env_bool("AUTO_FILE_ENABLED", False):
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


def _normalized_filing_text(value: object) -> str:
    return " ".join(str(value or "").casefold().split())


def _filing_signature(
    complaint_type: str | None,
    form_target: str | None,
    payload_json: str | None,
) -> tuple[str, str, str, str, str] | None:
    try:
        payload = json.loads(payload_json or "{}")
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    incident = payload.get("incident")
    asset = incident.get("asset") if isinstance(incident, dict) else ""
    start_ts = _parse_iso(incident.get("start_ts")) if isinstance(incident, dict) else None
    description = _normalized_filing_text(payload.get("description"))
    if not description:
        return None
    return (
        _normalized_filing_text(complaint_type),
        _normalized_filing_text(form_target),
        _normalized_filing_text(asset),
        description,
        str(int(start_ts.timestamp()) // 900) if start_ts else "",
    )


def _job_filing_signature(job: FilingJob) -> tuple[str, str, str, str, str] | None:
    return _filing_signature(job.complaint_type, job.form_target, job.payload_json)


def _equivalent_job_blocks_filing(
    job: FilingJob,
    *,
    signature: tuple[str, str, str, str, str],
    now: datetime,
) -> bool:
    if job.state in {"pending", "claimed"}:
        return True
    if job.state != "submitted":
        return False

    # A matching observed-event bucket means the same event was already filed,
    # even if a weekly export reaches the system days later.
    if signature[-1]:
        return True

    duplicate_window_minutes = _env_int("AUTO_FILE_EQUIVALENT_DUPLICATE_MINUTES", 180)
    if duplicate_window_minutes <= 0:
        return True
    submitted_at = _parse_iso(job.completed_at) or _parse_iso(job.updated_at) or _parse_iso(job.created_at)
    if submitted_at is None:
        return True
    return submitted_at >= now - timedelta(minutes=duplicate_window_minutes)


def _find_equivalent_filing_job(
    session,
    signature: tuple[str, str, str, str, str] | None,
    *,
    states: frozenset[str],
    exclude: FilingJob | None = None,
) -> FilingJob | None:
    if signature is None:
        return None
    candidates = [
        row
        for row in session.new
        if isinstance(row, FilingJob) and row.state in states
    ]
    candidates.extend(session.scalars(select(FilingJob).where(FilingJob.state.in_(states))).all())

    seen: set[tuple[str, object]] = set()
    now = datetime.now(tz=timezone.utc)
    for candidate in candidates:
        if candidate is exclude:
            continue
        identity = ("job", candidate.job_id) if candidate.job_id is not None else ("object", id(candidate))
        if identity in seen:
            continue
        seen.add(identity)
        if _job_filing_signature(candidate) != signature:
            continue
        if _equivalent_job_blocks_filing(candidate, signature=signature, now=now):
            return candidate
    return None


def _append_job_note(existing: str | None, note: str) -> str:
    return f"{existing} | {note}"[:2000] if existing else note


def _payload_sha256(job: FilingJob) -> str:
    return hashlib.sha256((job.payload_json or "{}").encode("utf-8")).hexdigest()


def filing_job_preview(job: FilingJob) -> dict[str, object]:
    try:
        payload = json.loads(job.payload_json or "{}")
    except (TypeError, ValueError):
        payload = {}
    incident = payload.get("incident") if isinstance(payload, dict) else {}
    return {
        "job_id": job.job_id,
        "incident_id": job.incident_id,
        "state": job.state,
        "complaint_type": job.complaint_type,
        "form_target": job.form_target,
        "description": payload.get("description") if isinstance(payload, dict) else "",
        "incident": incident if isinstance(incident, dict) else {},
        "payload_sha256": _payload_sha256(job),
    }


def _approval_payload_sha256(job: FilingJob) -> str:
    for part in reversed((job.notes or "").split(" | ")):
        if part.startswith(APPROVAL_HASH_PREFIX):
            return part.removeprefix(APPROVAL_HASH_PREFIX).strip()
    return ""


def claimed_filing_job_is_current(session, job: FilingJob) -> bool:
    if job.state != "claimed" or _approval_payload_sha256(job) != _payload_sha256(job):
        return False
    incident = session.get(Incident, job.incident_id) if job.incident_id else None
    return bool(incident is not None and incident_is_auto_eligible(incident))


def approve_filing_job(
    session,
    job_id: int,
    *,
    expected_payload_sha256: str,
    approval_phrase: str,
) -> FilingJob:
    if approval_phrase != FILING_APPROVAL_PHRASE:
        raise ValueError("The exact filing approval phrase is required")
    job = session.get(FilingJob, job_id)
    if job is None:
        raise ValueError(f"Filing job {job_id} does not exist")
    if job.state not in {"awaiting_approval", "failed"}:
        raise ValueError(f"Filing job {job_id} cannot be approved from state {job.state}")
    current_payload_sha256 = _payload_sha256(job)
    if expected_payload_sha256 != current_payload_sha256:
        raise ValueError("The filing preview changed; review the current payload before approving")
    incident = session.get(Incident, job.incident_id) if job.incident_id else None
    if incident is None or not incident_is_auto_eligible(incident):
        raise ValueError("The filing incident is no longer eligible")
    approval_note = f"{APPROVAL_HASH_PREFIX}{current_payload_sha256}"
    existing_notes = job.notes or ""
    available = max(0, 2000 - len(approval_note) - 3)
    job.notes = f"{existing_notes[:available]} | {approval_note}" if existing_notes else approval_note
    job.state = "approved"
    job.claimed_at = None
    job.last_error = None
    job.updated_at = now_iso()
    return job


def _equivalent_job_note(job: FilingJob) -> str:
    reference = str(job.job_id) if job.job_id is not None else job.dedupe_key
    return f"auto-skipped equivalent 311 filing job {reference} ({job.dedupe_key})"


def _retire_ineligible_job(job: FilingJob) -> bool:
    if job.state not in PRE_SUBMISSION_JOB_STATES:
        return False
    job.state = "skipped"
    job.claimed_at = None
    job.updated_at = now_iso()
    note = "auto-skipped because incident is no longer auto-eligible"
    if note not in (job.notes or ""):
        job.notes = _append_job_note(job.notes, note)
    return True


def retire_ineligible_filing_jobs(session) -> int:
    jobs = session.scalars(select(FilingJob).where(FilingJob.state.in_(PRE_SUBMISSION_JOB_STATES))).all()
    retired = 0
    for job in jobs:
        incident = session.get(Incident, job.incident_id) if job.incident_id else None
        if incident is None or not incident_is_auto_eligible(incident):
            retired += int(_retire_ineligible_job(job))
    return retired


def _refresh_filing_job_draft(job: FilingJob, inc: Incident, draft, payload_json: str) -> FilingJob:
    if job.state not in REFRESHABLE_JOB_STATES:
        return job
    changed = any(
        (
            job.complaint_type != draft.complaint_type,
            job.form_target != draft.form_target,
            job.payload_json != payload_json,
        )
    )
    if not changed:
        return job

    job.priority = max(1, 100 - int(inc.severity or 0) * 10)
    job.filing_channel = "portal_playwright"
    job.complaint_type = draft.complaint_type
    job.form_target = draft.form_target
    job.payload_json = payload_json
    job.notes = draft.description[:2000]
    job.state = "awaiting_approval"
    job.claimed_at = None
    job.last_error = None
    job.updated_at = now_iso()
    return job


def ensure_filing_job_for_incident(session, inc: Incident) -> FilingJob | None:
    dedupe_key = _dedupe_key(inc)
    pending_job = None
    for row in session.new:
        if isinstance(row, FilingJob) and row.dedupe_key == dedupe_key:
            pending_job = row
            break

    existing_job = pending_job or session.scalar(select(FilingJob).where(FilingJob.dedupe_key == dedupe_key))
    if not incident_is_auto_eligible(inc):
        if existing_job is not None:
            _retire_ineligible_job(existing_job)
        return None
    if existing_job and existing_job.state not in REFRESHABLE_JOB_STATES:
        return existing_job

    existing_case = session.scalar(select(ServiceRequestCase).where(ServiceRequestCase.incident_id == inc.incident_id))
    if existing_case:
        return None

    draft = build_filing_draft(inc)
    if not draft:
        return None

    payload_json = draft.payload_json()
    if existing_job is not None:
        return _refresh_filing_job_draft(existing_job, inc, draft, payload_json)

    equivalent_job = _find_equivalent_filing_job(
        session,
        _filing_signature(draft.complaint_type, draft.form_target, payload_json),
        states=EQUIVALENT_JOB_STATES,
    )
    created_at = now_iso()
    if equivalent_job is not None:
        job = FilingJob(
            dedupe_key=dedupe_key,
            incident_id=inc.incident_id,
            job_type="nyc311_file",
            state="skipped",
            priority=max(1, 100 - int(inc.severity or 0) * 10),
            filing_channel="portal_playwright",
            complaint_type=draft.complaint_type,
            form_target=draft.form_target,
            payload_json=payload_json,
            notes=_append_job_note(draft.description[:1800], _equivalent_job_note(equivalent_job)),
            attempts=0,
            created_at=created_at,
            updated_at=created_at,
        )
        session.add(job)
        return job

    job = FilingJob(
        dedupe_key=dedupe_key,
        incident_id=inc.incident_id,
        job_type="nyc311_file",
        state="awaiting_approval",
        priority=max(1, 100 - int(inc.severity or 0) * 10),
        filing_channel="portal_playwright",
        complaint_type=draft.complaint_type,
        form_target=draft.form_target,
        payload_json=payload_json,
        notes=draft.description[:2000],
        attempts=0,
        created_at=created_at,
        updated_at=created_at,
    )
    session.add(job)
    return job


def ensure_filing_jobs(session) -> list[FilingJob]:
    repair_unbounded_elevator_restore_attachments(session)
    dedupe_open_elevator_continuations(session)
    close_superseded_open_elevator_incidents(session)
    retire_ineligible_filing_jobs(session)
    incidents = session.scalars(select(Incident).where(Incident.status != "closed").order_by(Incident.last_ts_epoch.desc().nullslast())).all()
    jobs = []
    for inc in incidents:
        job = ensure_filing_job_for_incident(session, inc)
        if job:
            jobs.append(job)
    return jobs


def claim_next_job(session) -> tuple[FilingJob | None, int]:
    skipped = retire_ineligible_filing_jobs(session)
    stale_after_min = _env_int("CLAIM_STALE_MINUTES", 30)
    requeued = False
    if stale_after_min > 0:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=stale_after_min)
        claimed_rows = session.scalars(select(FilingJob).where(FilingJob.state == "claimed")).all()
        for row in claimed_rows:
            claimed_at = _parse_iso(row.claimed_at) or _parse_iso(row.updated_at) or _parse_iso(row.created_at)
            if claimed_at and claimed_at <= cutoff:
                row.state = "awaiting_approval"
                row.claimed_at = None
                row.updated_at = now_iso()
                note = "approval reset because a claimed job went stale"
                row.notes = f"{row.notes} | {note}"[:2000] if row.notes else note
                requeued = True
    if requeued:
        session.flush()

    rows = session.scalars(
        select(FilingJob)
        .where(FilingJob.state == "approved")
        .order_by(FilingJob.priority.asc(), FilingJob.created_at.asc())
    ).all()
    for row in rows:
        if _approval_payload_sha256(row) != _payload_sha256(row):
            row.state = "awaiting_approval"
            row.claimed_at = None
            row.updated_at = now_iso()
            row.notes = _append_job_note(row.notes, "approval reset because the filing payload changed")
            skipped += 1
            continue
        incident = session.get(Incident, row.incident_id) if row.incident_id else None
        if incident is None or not incident_is_auto_eligible(incident):
            row.state = "skipped"
            row.updated_at = now_iso()
            note = "auto-skipped because incident is no longer auto-eligible"
            row.notes = _append_job_note(row.notes, note)
            skipped += 1
            continue
        equivalent_job = _find_equivalent_filing_job(
            session,
            _job_filing_signature(row),
            states=CLAIM_BLOCKING_EQUIVALENT_JOB_STATES,
            exclude=row,
        )
        if equivalent_job is not None:
            row.state = "skipped"
            row.updated_at = now_iso()
            row.notes = _append_job_note(row.notes, _equivalent_job_note(equivalent_job))
            skipped += 1
            continue
        row.state = "claimed"
        row.claimed_at = now_iso()
        row.updated_at = now_iso()
        row.attempts = int(row.attempts or 0) + 1
        return row, skipped
    return None, skipped
