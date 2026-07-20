from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from packages.db import FilingJob, Incident, PublicRecordWatch, ServiceRequestCase, WatchdogAction
from packages.timeutil import parse_ts_to_epoch


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _due(days: int) -> str:
    return (datetime.now(tz=timezone.utc) + timedelta(days=days)).isoformat()


def ensure_action(
    session,
    *,
    action_type: str,
    severity: str,
    title: str,
    detail: str,
    due_at: str | None = None,
    owner_role: str = "volunteer",
    source_record_id: int | None = None,
    related_incident_id: str | None = None,
    draft_message: str | None = None,
) -> WatchdogAction:
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
        existing.severity = severity
        existing.title = title
        existing.detail = detail
        existing.due_at = due_at
        existing.owner_role = owner_role
        existing.draft_message = draft_message
        existing.updated_at = now_iso()
        return existing
    action = WatchdogAction(
        action_type=action_type,
        severity=severity,
        title=title,
        detail=detail,
        due_at=due_at,
        owner_role=owner_role,
        status="open",
        source_record_id=source_record_id,
        related_incident_id=related_incident_id,
        draft_message=draft_message,
        created_at=now_iso(),
        updated_at=now_iso(),
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
        due_at=_due(3),
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
        due_at=_due(2),
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
    return any(
        phrase in text
        for phrase in (
            "full elevator replacement",
            "elevator replacement",
            "replace elevator",
            "replace existing elevator",
            "replacement of elevator",
            "new elevator",
        )
    )


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
    return any((job.state or "").casefold() in {"pending", "claimed", "submitted"} for job in jobs)


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
                    due_at=_due(3),
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
                    due_at=_due(5),
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
                    due_at=_due(2),
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
                        due_at=_due(2),
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
                due_at=_due(7),
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
    for incident in open_elevator_incidents:
        age_hours = ((now_epoch - int(incident.start_ts_epoch or incident.last_ts_epoch or now_epoch)) / 3600.0)
        if incident.asset == "elevator_both":
            active_both_elevator_incident_ids.add(incident.incident_id)
            actions.append(
                ensure_action(
                    session,
                    action_type="both_elevators_down",
                    severity="critical",
                    title="Both elevators down: file 311 and escalate",
                    detail="Tenant-observed reality indicates both elevators are down. Trigger 311, management notice, and escalation packet.",
                    due_at=_due(0),
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
                    due_at=_due(1),
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

    latest_change_epoch = max((parse_ts_to_epoch(row.last_changed_at) or 0 for row in records), default=0)
    if records and latest_change_epoch and (now_epoch - latest_change_epoch) >= 14 * 86400:
        actions.append(
            ensure_action(
                session,
                action_type="no_public_movement_14_days",
                severity="watch",
                title="Ask management for two-week project update",
                detail="No public-record movement has been detected for at least 14 days.",
                due_at=_due(2),
                owner_role="operator",
            )
        )
    return actions
