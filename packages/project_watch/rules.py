from __future__ import annotations

import json
import re
from datetime import datetime, timezone

from sqlalchemy import select

from packages.db import (
    FilingJob,
    Incident,
    MessageDecision,
    PublicRecordWatch,
    RawMessage,
    ServiceRequestCase,
    WatchdogAction,
)
from packages.public_records.elevator_scope import describes_elevator_replacement_scope
from packages.timeutil import parse_ts_to_epoch


ACTIVE_ELEVATOR_OBSERVATION_MAX_AGE_HOURS = 7 * 24
ELEVATOR_DOWN_EVENT_TYPES = frozenset({"outage", "still_out"})
ELEVATOR_DEGRADED_TEXT = re.compile(
    r"\b(?:super|very|really|unusually)\s+slow\b"
    r"|\b(?:moving|running|operating|travel(?:ing|ling))\s+slow(?:ly)?\b"
    r"|\b(?:clunk(?:ed|ing)?|bang(?:ed|ing)?|bounc(?:e[sd]?|ing)|jolt(?:ed|ing)?|shake[sn]?|shook)\b"
    r"|\brough\s+ride\b"
    r"|\b(?:door|doors)\s+(?:opened|opening|opens)\s+(?:slow(?:ly)?|in\s+slo-?mo)\b"
    r"|\bslow\s+(?:door|doors)\b"
    r"|\b(?:floor[- ]by[- ]floor|skipping\s+(?:a\s+)?floor|irregular\s+floor)\b"
    r"|\bstopping\s+(?:(?:at|on)\s+)?(?:each|every|all)\s+floor\b",
    re.IGNORECASE,
)
ELEVATOR_DOWN_TEXT = re.compile(
    r"\b(?:out\s+of\s+(?:service|order)|not\s+working|stuck|dead|shutdown|shut\s*off)\b"
    r"|\b(?:elevators?|lifts?|it|they)\s+(?:(?:is|are|was|were|remain(?:s|ed)?)\s+)?"
    r"(?:currently\s+)?(?:(?:still|again)\s+)?(?:out|down)\b"
    r"|\b(?:north|south|left|right)(?:\s+(?:elevator|lift|one|side))?\s+"
    r"(?:(?:is|are|was|were|remain(?:s|ed)?)\s+)?(?:currently\s+)?(?:(?:still|again)\s+)?(?:out|down)\b"
    r"|\b(?:both|all)\s+(?:(?:elevators?|lifts?)\s+)?(?:(?:are|were|remain(?:s|ed)?)\s+)?"
    r"(?:currently\s+)?(?:(?:still|again)\s+)?(?:out|down)\b"
    r"|\b(?:still|again)\s+(?:out|down|dead|stuck|not\s+working)\b"
    r"|\b(?:out|down|dead|stuck)\s+again\b"
    r"|\b(?:down|back)\s+to\s+(?:one|1)(?:\s+working)?\s+(?:elevator|lift)\b"
    r"|\bonly\s+(?:one|1)\s+(?:working\s+)?(?:elevator|lift)\b"
    r"|\b(?:zero|no)\s+(?:elevators|lifts)\b",
    re.IGNORECASE,
)
ELEVATOR_WORKING_TEXT = re.compile(
    r"\b(?:working\s+(?:now|normal(?:ly)?|again)|currently\s+(?:working|functioning|running)|"
    r"operational\s+again|restored|back\s+(?:up|on|in\s+service)|"
    r"both\s+(?:elevators|lifts)?\s*(?:are\s+|were\s+)?(?:working|functioning|operational|running))\b",
    re.IGNORECASE,
)


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _deadline_from(anchor: str, days: int) -> str:
    anchor_epoch = parse_ts_to_epoch(anchor)
    if anchor_epoch is None:
        anchor_epoch = int(datetime.now(tz=timezone.utc).timestamp())
    return datetime.fromtimestamp(
        anchor_epoch + (days * 86400),
        tz=timezone.utc,
    ).replace(microsecond=0).isoformat()


def _earlier_deadline(current: str | None, candidate: str | None) -> str | None:
    if not current:
        return candidate
    if not candidate:
        return current
    current_epoch = parse_ts_to_epoch(current)
    candidate_epoch = parse_ts_to_epoch(candidate)
    if current_epoch is None:
        return candidate
    if candidate_epoch is None:
        return current
    return candidate if candidate_epoch < current_epoch else current


def ensure_action(
    session,
    *,
    action_type: str,
    severity: str,
    title: str,
    detail: str,
    due_at: str | None = None,
    due_in_days: int | None = None,
    owner_role: str = "volunteer",
    source_record_id: int | None = None,
    related_incident_id: str | None = None,
    draft_message: str | None = None,
) -> WatchdogAction:
    if due_at is not None and due_in_days is not None:
        raise ValueError("Provide due_at or due_in_days, not both")
    existing = session.scalar(
        select(WatchdogAction)
        .where(
            WatchdogAction.action_type == action_type,
            WatchdogAction.status.in_(["open", "pending"]),
            WatchdogAction.source_record_id == source_record_id,
            WatchdogAction.related_incident_id == related_incident_id,
        )
        .order_by(WatchdogAction.created_at.desc().nullslast())
    )
    if existing:
        anchored_due_at = (
            _deadline_from(existing.created_at or now_iso(), due_in_days)
            if due_in_days is not None
            else due_at
        )
        updates = {
            "severity": severity,
            "title": title,
            "detail": detail,
            "due_at": _earlier_deadline(existing.due_at, anchored_due_at),
            "owner_role": owner_role,
            "draft_message": draft_message,
        }
        changed = False
        for field, value in updates.items():
            if getattr(existing, field) != value:
                setattr(existing, field, value)
                changed = True
        if changed:
            existing.updated_at = now_iso()
        return existing
    created_at = now_iso()
    resolved_due_at = (
        _deadline_from(created_at, due_in_days)
        if due_in_days is not None
        else due_at
    )
    action = WatchdogAction(
        action_type=action_type,
        severity=severity,
        title=title,
        detail=detail,
        due_at=resolved_due_at,
        owner_role=owner_role,
        status="open",
        source_record_id=source_record_id,
        related_incident_id=related_incident_id,
        draft_message=draft_message,
        created_at=created_at,
        updated_at=created_at,
    )
    session.add(action)
    return action


def action_for_new_record(session, record: PublicRecordWatch) -> WatchdogAction:
    return ensure_action(
        session,
        action_type="new_record_needs_verification",
        severity="info",
        title="New official record needs source match",
        detail=(
            f"The system imported official record {record.record_key}, but it has not reached the automatic "
            "confidence threshold yet. A person only needs to help if the public view marks this as weak or conflicting."
        ),
        due_in_days=3,
        owner_role="volunteer",
        source_record_id=record.id,
        draft_message="Please check this one record only if the automatic official-source match stays weak or conflicting.",
    )


def action_for_changed_record(session, record: PublicRecordWatch) -> WatchdogAction:
    return ensure_action(
        session,
        action_type="changed_public_record",
        severity="watch",
        title="Official record changed",
        detail=(
            f"The system detected a change on official record {record.record_key}. The public view should update "
            "the plain-language answer before asking any resident to do manual checking."
        ),
        due_in_days=2,
        owner_role="operator",
        source_record_id=record.id,
        draft_message="A public record changed. Update tenants only with the plain-language meaning of the record.",
    )


def _is_elevator_public_record(record: PublicRecordWatch) -> bool:
    text = " ".join(
        str(value or "")
        for value in (
            record.record_type,
            record.filing_type,
            record.status,
            record.status_detail,
            record.device_number,
        )
    ).casefold()
    return "elevator" in text or "elev" in text or (record.device_number or "").casefold().startswith(("3p6189", "3p6190"))


def _record_text(record: PublicRecordWatch) -> str:
    try:
        raw = json.loads(record.raw_json or "{}")
    except Exception:
        raw = {}
    return " ".join(
        str(value or "")
        for value in (
            record.record_type,
            record.record_key,
            record.filing_type,
            record.status,
            record.status_detail,
            record.device_number,
            raw.get("descriptionofwork") if isinstance(raw, dict) else "",
            raw.get("filingstatus_or_filingincludes") if isinstance(raw, dict) else "",
        )
    ).casefold()


def _permit_is_closed_or_expired(record: PublicRecordWatch) -> bool:
    status = (record.status or "").casefold()
    if any(word in status for word in ("signed off", "loc issued", "co issued")):
        return True
    expiry_epoch = parse_ts_to_epoch(record.expires_at)
    return bool(expiry_epoch and expiry_epoch < int(datetime.now(tz=timezone.utc).timestamp()))


def _is_current_replacement_permit(record: PublicRecordWatch) -> bool:
    if not record.permit_issued_at or not _is_current_replacement_filing(record):
        return False
    return True


def _is_current_replacement_filing(record: PublicRecordWatch) -> bool:
    if record.record_type != "elevator_permit_application":
        return False
    if _permit_is_closed_or_expired(record):
        return False
    text = _record_text(record)
    if "door lock monitoring" in text or "dlm" in text:
        return False
    return describes_elevator_replacement_scope(text)


def _complete_open_actions(session, action_type: str, *, keep_related_incident_ids: set[str] | None = None) -> None:
    keep_related_incident_ids = keep_related_incident_ids or set()
    for action in session.scalars(
        select(WatchdogAction).where(
            WatchdogAction.action_type == action_type,
            WatchdogAction.status.in_(["open", "pending"]),
        )
    ).all():
        if action.related_incident_id and action.related_incident_id in keep_related_incident_ids:
            continue
        action.status = "completed"
        action.completed_at = now_iso()
        action.updated_at = now_iso()


def _incident_has_automated_followup(session, incident_id: str) -> bool:
    cases = session.scalars(select(ServiceRequestCase).where(ServiceRequestCase.incident_id == incident_id)).all()
    for case in cases:
        status = (case.status or "").casefold()
        if not any(word in status for word in ("closed", "resolved", "dismissed", "cancel")):
            return True
    jobs = session.scalars(select(FilingJob).where(FilingJob.incident_id == incident_id)).all()
    return any(
        (job.state or "").casefold() in {"pending", "claimed", "awaiting_approval", "approved", "submitted"}
        for job in jobs
    )


def _latest_elevator_state(session, incident: Incident) -> str:
    """Return the current tenant-observed state without treating every open row as an outage."""
    latest = session.execute(
        select(MessageDecision, RawMessage)
        .join(RawMessage, RawMessage.message_id == MessageDecision.message_id)
        .where(MessageDecision.incident_id == incident.incident_id)
        .order_by(
            RawMessage.ts_epoch.desc().nullslast(),
            RawMessage.ts_iso.desc().nullslast(),
            MessageDecision.created_at.desc().nullslast(),
            MessageDecision.message_id.desc(),
        )
        .limit(1)
    ).first()

    if latest is None:
        event_type = ""
        evidence_text = f"{incident.title or ''} {incident.summary or ''}"
    else:
        decision, raw_message = latest
        event_type = (decision.event_type or "").casefold()
        evidence_text = raw_message.text or ""

    # Explicit descriptions of slow/irregular operation are degraded service,
    # even if an older classifier called the message an outage. Reduced service
    # (for example, "only one elevator working") remains an outage.
    text_is_down = bool(ELEVATOR_DOWN_TEXT.search(evidence_text))
    if ELEVATOR_DEGRADED_TEXT.search(evidence_text) and not text_is_down:
        return "degraded"
    if ELEVATOR_WORKING_TEXT.search(evidence_text) and not text_is_down:
        return "working"
    if event_type in ELEVATOR_DOWN_EVENT_TYPES or text_is_down:
        return "down"
    if event_type == "restore":
        return "working"
    return "unknown"


def evaluate_project_rules(session) -> list[WatchdogAction]:
    session.flush()
    actions: list[WatchdogAction] = []
    records = session.scalars(select(PublicRecordWatch)).all()
    elevator_filing_records = [
        row for row in records
        if row.record_type in {"elevator_permit_application", "elevator_device_detail", "elevator_safety_compliance"}
    ]
    permit_records = [row for row in elevator_filing_records if row.record_type == "elevator_permit_application"]
    current_replacement_filing_ids: set[int] = set()
    current_replacement_permit_ids: set[int] = set()

    for record in permit_records:
        status = (record.status or "").casefold()
        detail = (record.status_detail or "").casefold()
        if _is_current_replacement_filing(record):
            current_replacement_filing_ids.add(record.id)
        if any(word in status or word in detail for word in ("objection", "incomplete", "hold")):
            actions.append(
                ensure_action(
                    session,
                    action_type="objection_or_hold",
                    severity="watch",
                    title="Ask management for correction/resubmission date",
                    detail=f"Permit filing {record.record_key} appears to have an objection, incomplete item, or hold.",
                    due_in_days=3,
                    owner_role="operator",
                    source_record_id=record.id,
                    draft_message="What is the correction or resubmission date for the DOB filing issue on the elevator replacement?",
                )
            )
        if "approved" in status and not record.permit_issued_at:
            actions.append(
                ensure_action(
                    session,
                    action_type="approved_no_permit",
                    severity="watch",
                    title="Approved filing has no permit date yet",
                    detail=(
                        f"The system can see that filing {record.record_key} is approved, but no permit-issued "
                        "date is stored. Treat it as not construction-ready until a permit date appears or management provides an official permit."
                    ),
                    due_in_days=5,
                    owner_role="operator",
                    source_record_id=record.id,
                )
            )
        if _is_current_replacement_permit(record):
            current_replacement_permit_ids.add(record.id)
            actions.append(
                ensure_action(
                    session,
                    action_type="permit_issued",
                    severity="info",
                    title="Resident photo needed: lobby/start-date notice",
                    detail=(
                        f"The system found a permit-issued signal for {record.record_key}. This is now a hallway-only "
                        "check: a resident photo or note is needed because the system cannot see lobby postings or start-date notices."
                    ),
                    due_in_days=2,
                    owner_role="resident",
                    source_record_id=record.id,
                )
            )
        expiry_epoch = parse_ts_to_epoch(record.expires_at)
        if expiry_epoch:
            days_left = (expiry_epoch - int(datetime.now(tz=timezone.utc).timestamp())) / 86400
            if 0 <= days_left <= 30:
                actions.append(
                    ensure_action(
                        session,
                        action_type="permit_expiring_soon_30_days",
                        severity="watch",
                        title="Ask for permit renewal or extension plan",
                        detail=f"Permit or filing {record.record_key} expires within 30 days.",
                        due_in_days=2,
                        owner_role="operator",
                        source_record_id=record.id,
                )
            )

    if not current_replacement_filing_ids:
        actions.append(
            ensure_action(
                session,
                action_type="no_public_filing_after_30_days",
                severity="watch",
                title="Ask management whether the DOB filing has been submitted",
                detail=(
                    "Management described a replacement project, but automatic DOB/NYC checks have not found "
                    "a current full-replacement elevator filing for 455 Ocean Parkway. A tenant representative "
                    "should ask whether a DOB NOW filing exists yet. If it exists, management should provide the "
                    "filing number and status; if it does not, management should provide the expected filing date "
                    "and what approvals, drawings, contracts, or equipment decisions remain before submission."
                ),
                due_in_days=7,
                owner_role="tenant_association",
                draft_message=(
                    "Please confirm whether a DOB NOW elevator filing has been submitted for the full elevator "
                    "replacement at 455 Ocean Parkway. If yes, please share the filing number, current status, "
                    "expected start date, and required posting plan. If not, please share the expected filing date "
                    "and what approvals, drawings, contracts, or equipment decisions remain before submission. "
                    "Tenants are tracking management claims, official public records, and observed elevator service "
                    "separately so updates stay accurate."
                ),
            )
        )
    else:
        _complete_open_actions(session, "no_public_filing_after_30_days")

    for stale_action in session.scalars(
        select(WatchdogAction).where(
            WatchdogAction.action_type == "permit_issued",
            WatchdogAction.status.in_(["open", "pending"]),
        )
    ).all():
        if stale_action.source_record_id not in current_replacement_permit_ids:
            stale_action.status = "completed"
            stale_action.completed_at = now_iso()
            stale_action.updated_at = now_iso()

    _complete_open_actions(session, "active_official_elevator_record")

    now_epoch = int(datetime.now(tz=timezone.utc).timestamp())
    open_elevator_incidents = session.scalars(
        select(Incident).where(Incident.category == "elevator", Incident.status != "closed")
    ).all()
    active_one_elevator_incident_ids: set[str] = set()
    active_both_elevator_incident_ids: set[str] = set()
    active_degraded_elevator_incident_ids: set[str] = set()
    for incident in open_elevator_incidents:
        observed_epoch = int(incident.last_ts_epoch or incident.start_ts_epoch or now_epoch)
        observation_age_hours = (now_epoch - observed_epoch) / 3600.0
        if observation_age_hours > ACTIVE_ELEVATOR_OBSERVATION_MAX_AGE_HOURS:
            continue
        current_state = _latest_elevator_state(session, incident)
        if current_state == "degraded":
            if not _incident_has_automated_followup(session, incident.incident_id):
                active_degraded_elevator_incident_ids.add(incident.incident_id)
                actions.append(
                    ensure_action(
                        session,
                        action_type="active_elevator_degraded_service",
                        severity="watch",
                        title="Degraded elevator service needs follow-up",
                        detail=(
                            f"{incident.title} is reported as slow or irregular while still operating. "
                            "Track the service problem without presenting it as an elevator outage."
                        ),
                        due_in_days=1,
                        owner_role="operator",
                        related_incident_id=incident.incident_id,
                    )
                )
            continue
        if current_state != "down":
            continue
        age_hours = ((now_epoch - int(incident.start_ts_epoch or observed_epoch)) / 3600.0)
        if incident.asset == "elevator_both":
            active_both_elevator_incident_ids.add(incident.incident_id)
            actions.append(
                ensure_action(
                    session,
                    action_type="both_elevators_down",
                    severity="critical",
                    title="Both elevators down: file 311 and escalate",
                    detail="Tenant-observed reality indicates both elevators are down. Trigger 311, management notice, and escalation packet.",
                    due_in_days=0,
                    owner_role="operator",
                    related_incident_id=incident.incident_id,
                )
            )
        elif age_hours >= 24 and not _incident_has_automated_followup(session, incident.incident_id):
            active_one_elevator_incident_ids.add(incident.incident_id)
            actions.append(
                ensure_action(
                    session,
                    action_type="active_phase_one_elevator_down",
                    severity="critical" if age_hours >= 72 else "yellow",
                    title="One elevator down during replacement watch",
                    detail=f"{incident.title} has remained open for about {int(age_hours)} hours.",
                    due_in_days=1,
                    owner_role="operator",
                    related_incident_id=incident.incident_id,
                )
            )
    _complete_open_actions(
        session,
        "active_phase_one_elevator_down",
        keep_related_incident_ids=active_one_elevator_incident_ids,
    )
    _complete_open_actions(
        session,
        "both_elevators_down",
        keep_related_incident_ids=active_both_elevator_incident_ids,
    )
    _complete_open_actions(
        session,
        "active_elevator_degraded_service",
        keep_related_incident_ids=active_degraded_elevator_incident_ids,
    )

    replacement_progress_records = [
        row for row in permit_records
        if _is_current_replacement_filing(row)
    ]
    latest_change_epoch = max(
        (parse_ts_to_epoch(row.last_changed_at) or 0 for row in replacement_progress_records),
        default=0,
    )
    if (
        replacement_progress_records
        and latest_change_epoch
        and (now_epoch - latest_change_epoch) >= 14 * 86400
    ):
        actions.append(
            ensure_action(
                session,
                action_type="no_public_movement_14_days",
                severity="watch",
                title="Ask management for two-week project update",
                detail="No public-record movement on the current replacement filing has been detected for at least 14 days.",
                due_in_days=2,
                owner_role="operator",
            )
        )
    else:
        _complete_open_actions(session, "no_public_movement_14_days")
    return actions
