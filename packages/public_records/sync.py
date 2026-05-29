from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select

from packages.db import (
    CapitalProject,
    ComplianceCheck,
    Incident,
    ProjectMilestone,
    PublicRecordWatch,
    ServiceRequestCase,
    WatchdogAction,
    WeeklyDigest,
)
from packages.project_watch.rules import action_for_changed_record, action_for_new_record, evaluate_project_rules, now_iso
from packages.public_records.config import building_bbl_compact, building_bin, source_configs
from packages.public_records.normalize import normalize_record
from packages.public_records.nyc_open_data import fetch_rows, query_url
from packages.public_records.verification import apply_machine_verification
from packages.timeutil import normalize_timestamp, parse_ts_to_epoch


BUILDING_KEY = "455-ocean-parkway"
VISIBLE_ACTION_STATUSES = ("open", "pending", "failed")


def _source_map():
    return {source.key: source for source in source_configs()}


def ensure_default_project(session) -> CapitalProject:
    project = session.scalar(select(CapitalProject).where(CapitalProject.building_key == BUILDING_KEY))
    ts = now_iso()
    summary = (
        "Management states that both elevators will be fully replaced by PS Marcato Elevator Co., Inc. "
        "with VDA Associates as consultant. Preparation is expected to take about four months for parts "
        "and permits. Each elevator replacement is expected to take about 3.5 months while the other "
        "elevator remains running, with on-site work expected approximately late 2026."
    )
    if not project:
        project = CapitalProject(
            building_key=BUILDING_KEY,
            title="455 Ocean Parkway elevator replacement",
            phase="pre_permit_watch",
            management_summary=summary,
            risk_level="watch",
            current_bottleneck="Waiting for verifiable public filings, permits, and project schedule detail.",
            next_expected_record="DOB NOW elevator permit application, permit issuance, or DOB NOW Safety/compliance update.",
            management_contact_email="mgmt@weinreb.com",
            superintendent_email="super455OP@weinreb.com",
            created_at=ts,
            updated_at=ts,
        )
        session.add(project)
        session.flush()
        _seed_default_milestones(session, project)
        return project

    project.management_summary = project.management_summary or summary
    project.management_contact_email = project.management_contact_email or "mgmt@weinreb.com"
    project.superintendent_email = project.superintendent_email or "super455OP@weinreb.com"
    project.updated_at = ts
    if not project.milestones:
        _seed_default_milestones(session, project)
    return project


def _seed_default_milestones(session, project: CapitalProject) -> None:
    ts = now_iso()
    rows = [
        ProjectMilestone(
            project_id=project.id,
            phase="preparation",
            elevator_asset=None,
            management_claimed_start="management claimed approximately 4 months before on-site work",
            management_claimed_end="late 2026 approximate",
            status="claimed",
            source_type="management_pdf",
            notes="Parts and permits preparation period stated by management.",
            created_at=ts,
            updated_at=ts,
        ),
        ProjectMilestone(
            project_id=project.id,
            phase="elevator_1_replacement",
            elevator_asset="elevator_1",
            management_claimed_start="late 2026 approximate",
            management_claimed_end="about 3.5 months after start",
            status="claimed",
            source_type="management_pdf",
            notes="Management claims the second elevator remains running during elevator #1 replacement.",
            created_at=ts,
            updated_at=ts,
        ),
        ProjectMilestone(
            project_id=project.id,
            phase="elevator_2_replacement",
            elevator_asset="elevator_2",
            management_claimed_start="after elevator #1 returns to service",
            management_claimed_end="about 3.5 months after phase start",
            status="claimed",
            source_type="management_pdf",
            notes="Management claims the first elevator returns to service before elevator #2 replacement.",
            created_at=ts,
            updated_at=ts,
        ),
    ]
    session.add_all(rows)


def _query_specs() -> list[tuple[str, dict[str, str]]]:
    bbl = building_bbl_compact()
    bin_value = building_bin()
    return [
        ("dob_now_elevator_applications", {"bbl": bbl}),
        ("dob_now_elevator_safety_compliance", {"bbl": bbl}),
        ("dob_complaints", {"bin": bin_value, "unit": "ELEVR"}),
        ("dob_violations", {"bin": bin_value}),
        ("dob_ecb_violations", {"bin": bin_value}),
        ("oath_hearings", {"issuing_agency": "DEPT. OF BUILDINGS", "violation_location_house": "455", "violation_location_street_name": "OCEAN PARKWAY"}),
        ("nyc_311", {"bbl": bbl, "agency": "DOB", "complaint_type": "Elevator"}),
        ("hpd_building", {"bin": bin_value}),
        ("hpd_violations", {"bbl": bbl, "violationstatus": "Open"}),
    ]


def fetch_public_record_rows() -> list[tuple[str, dict[str, Any], str]]:
    sources = _source_map()
    fetched: list[tuple[str, dict[str, Any], str]] = []
    elevator_jobs: set[str] = set()
    device_ids: set[str] = set()

    for source_key, params in _query_specs():
        source = sources[source_key]
        url = query_url(source, {"$limit": "500", **params})
        rows = fetch_rows(source, params, limit=500)
        for row in rows:
            fetched.append((source_key, row, url))
            if source_key == "dob_now_elevator_applications" and row.get("job_filing_number"):
                elevator_jobs.add(str(row["job_filing_number"]))
            if source_key == "dob_now_elevator_safety_compliance" and row.get("device_number"):
                device_ids.add(str(row["device_number"]))

    device_source = sources["dob_now_elevator_device_details"]
    for job in sorted(elevator_jobs):
        params = {"job_filing_number": job}
        url = query_url(device_source, {"$limit": "500", **params})
        for row in fetch_rows(device_source, params, limit=500):
            fetched.append(("dob_now_elevator_device_details", row, url))
    for device_id in sorted(device_ids):
        params = {"device_id": device_id}
        url = query_url(device_source, {"$limit": "500", **params})
        for row in fetch_rows(device_source, params, limit=500):
            fetched.append(("dob_now_elevator_device_details", row, url))

    return fetched


def upsert_public_record(
    session,
    source_key: str,
    row: dict[str, Any],
    *,
    source_url: str | None = None,
    create_new_action: bool = True,
) -> tuple[PublicRecordWatch, str]:
    source = _source_map()[source_key]
    normalized = normalize_record(source, row, source_url=source_url)
    ts = now_iso()
    existing = session.scalar(
        select(PublicRecordWatch).where(
            PublicRecordWatch.source_system == normalized["source_system"],
            PublicRecordWatch.record_type == normalized["record_type"],
            PublicRecordWatch.record_key == normalized["record_key"],
        )
    )
    if not existing:
        record = PublicRecordWatch(
            **normalized,
            first_seen_at=ts,
            last_seen_at=ts,
            last_changed_at=ts,
        )
        session.add(record)
        session.flush()
        if create_new_action:
            action_for_new_record(session, record)
        return record, "created"

    existing.last_seen_at = ts
    if existing.raw_hash == normalized["raw_hash"]:
        for field, value in normalized.items():
            if field in {"needs_human_verification", "visible_public", "raw_json", "raw_hash"}:
                continue
            if getattr(existing, field) != value:
                setattr(existing, field, value)
        return existing, "unchanged"

    for field, value in normalized.items():
        if field in {"needs_human_verification", "visible_public"}:
            continue
        setattr(existing, field, value)
    existing.needs_human_verification = True
    existing.human_verified_at = None
    existing.human_verified_by = None
    existing.last_changed_at = ts
    session.flush()
    action_for_changed_record(session, existing)
    return existing, "changed"


def sync_public_records(session, *, baseline: bool | None = None) -> dict[str, int]:
    counts = {"fetched": 0, "created": 0, "baseline_created": 0, "changed": 0, "unchanged": 0}
    seen: set[tuple[str, str, str]] = set()
    existing_sources = {
        source_system
        for (source_system,) in session.execute(select(PublicRecordWatch.source_system).distinct()).all()
    }
    for source_key, row, url in fetch_public_record_rows():
        source = _source_map()[source_key]
        normalized = normalize_record(source, row, source_url=url)
        seen_key = (normalized["source_system"], normalized["record_type"], normalized["record_key"])
        if seen_key in seen:
            continue
        seen.add(seen_key)
        source_is_baseline = baseline if baseline is not None else source_key not in existing_sources
        _record, state = upsert_public_record(
            session,
            source_key,
            row,
            source_url=url,
            create_new_action=not source_is_baseline,
        )
        counts["fetched"] += 1
        counts[state] += 1
        if state == "created" and source_is_baseline:
            counts["baseline_created"] += 1
    counts.update(apply_machine_verification(session))
    return counts


def sync_replacement_watchdog(session) -> dict[str, int]:
    ensure_default_project(session)
    counts = sync_public_records(session)
    actions = evaluate_project_rules(session)
    session.flush()
    counts["actions_open"] = session.query(WatchdogAction).filter(WatchdogAction.status == "open").count()
    counts["actions_touched"] = len(actions)
    return counts


def verify_public_record(session, record_id: int, *, verified_by: str | None = None) -> PublicRecordWatch | None:
    record = session.get(PublicRecordWatch, record_id)
    if not record:
        return None
    record.needs_human_verification = False
    record.human_verified_at = now_iso()
    record.human_verified_by = verified_by or "admin"
    for action in session.scalars(
        select(WatchdogAction).where(
            WatchdogAction.source_record_id == record.id,
            WatchdogAction.action_type.in_(["new_record_needs_verification", "changed_public_record"]),
            WatchdogAction.status == "open",
        )
    ).all():
        action.status = "completed"
        action.completed_at = now_iso()
        action.updated_at = now_iso()
    return record


def add_watchdog_check(
    session,
    *,
    check_type: str,
    status: str,
    checked_by: str | None = None,
    photo_url: str | None = None,
    source_url: str | None = None,
    notes: str | None = None,
) -> ComplianceCheck:
    check = ComplianceCheck(
        check_type=check_type,
        status=status,
        checked_at=now_iso(),
        checked_by=checked_by,
        photo_url=photo_url,
        source_url=source_url,
        notes=notes,
    )
    session.add(check)
    return check


def _raw_record(row: PublicRecordWatch) -> dict[str, Any]:
    try:
        raw = json.loads(row.raw_json or "{}")
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _plain_date(value: str | None) -> str:
    normalized = normalize_timestamp(value)
    return normalized[:10] if normalized else ""


def _source_last_seen(records: list[PublicRecordWatch]) -> str:
    return max((normalize_timestamp(row.last_seen_at) or "" for row in records), default="")


def _record_description(row: PublicRecordWatch) -> str:
    raw = _raw_record(row)
    for key in (
        "descriptionofwork",
        "violation_description",
        "violation_details",
        "device_job_description",
        "resolution_description",
        "novdescription",
        "filingstatus_or_filingincludes",
        "description",
    ):
        value = str(raw.get(key) or "").strip()
        if value:
            return value
    return (row.status_detail or "").strip()


def _record_text(row: PublicRecordWatch) -> str:
    raw = _raw_record(row)
    pieces = [
        row.record_type,
        row.record_key,
        row.filing_type,
        row.status,
        row.status_detail,
        row.device_number,
        _record_description(row),
        json.dumps(raw, sort_keys=True),
    ]
    return " ".join(str(piece or "") for piece in pieces).casefold()


def _is_elevator_record(row: PublicRecordWatch) -> bool:
    text = _record_text(row)
    return row.record_type in {
        "elevator_permit_application",
        "elevator_device_detail",
        "elevator_safety_compliance",
    } or "elevator" in text or "elev" in text or (row.device_number or "").casefold().startswith(("3p6189", "3p6190"))


def _is_active_or_pending(row: PublicRecordWatch) -> bool:
    status = f"{row.status or ''} {row.status_detail or ''}".casefold()
    return any(word in status for word in ("active", "open", "pending"))


def _is_closed_or_expired(row: PublicRecordWatch) -> bool:
    status = (row.status or "").casefold()
    if any(word in status for word in ("signed off", "loc issued", "co issued", "resolved", "dismissed")):
        return True
    expiry = parse_ts_to_epoch(row.expires_at)
    if expiry and expiry < int(datetime.now(tz=timezone.utc).timestamp()):
        return True
    raw = _raw_record(row)
    return bool(raw.get("signedoff_date") or raw.get("signoff_date"))


def _looks_like_full_elevator_replacement(row: PublicRecordWatch) -> bool:
    if row.record_type != "elevator_permit_application":
        return False
    text = _record_text(row)
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


def _record_sort_key(row: PublicRecordWatch) -> tuple[str, str]:
    return (normalize_timestamp(row.filed_at) or normalize_timestamp(row.last_changed_at) or "", row.record_key)


def public_elevator_watch_items(session) -> list[dict[str, Any]]:
    records = session.scalars(select(PublicRecordWatch)).all()
    elevator_records = [row for row in records if row.visible_public and _is_elevator_record(row)]
    permit_records = sorted(
        [row for row in elevator_records if row.record_type == "elevator_permit_application"],
        key=_record_sort_key,
        reverse=True,
    )
    current_replacement_permits = [
        row for row in permit_records
        if _looks_like_full_elevator_replacement(row) and not _is_closed_or_expired(row)
    ]
    active_official_records = sorted(
        [
            row for row in elevator_records
            if row.record_type in {"dob_ecb_violation", "dob_violation", "dob_complaint"}
            and _is_active_or_pending(row)
        ],
        key=_record_sort_key,
        reverse=True,
    )
    devices = sorted(
        [row for row in elevator_records if row.record_type == "elevator_safety_compliance"],
        key=lambda row: row.device_number or row.record_key,
    )
    open_incidents = session.scalars(
        select(Incident)
        .where(Incident.category == "elevator", Incident.status != "closed")
        .order_by(Incident.last_ts_epoch.desc().nullslast())
    ).all()

    last_official_sync = _source_last_seen(elevator_records)

    items: list[dict[str, Any]] = []

    if current_replacement_permits:
        record = current_replacement_permits[0]
        permit_date = _plain_date(record.permit_issued_at)
        answer = f"Yes: DOB filing {record.record_key} appears to be an active elevator replacement filing."
        if permit_date:
            answer += f" Permit date: {permit_date}."
        items.append({
            "topic": "Full elevator replacement permit",
            "answer": answer,
            "why_it_matters": "This is the official signal that replacement work can move from management claim toward permitted construction.",
            "checked_by": "Automatic DOB/Open Data check",
            "last_checked_at": last_official_sync,
            "human_needed": "No DOB lookup needed. A resident photo is only needed for lobby notices.",
            "source_url": record.source_url,
        })
    elif permit_records:
        record = permit_records[0]
        description = _record_description(record)
        date_bits = ", ".join(
            bit for bit in [
                f"filed {_plain_date(record.filed_at)}" if _plain_date(record.filed_at) else "",
                f"status {record.status}" if record.status else "",
            ]
            if bit
        )
        items.append({
            "topic": "Full elevator replacement permit",
            "answer": "No current full-replacement permit found in the official elevator permit records.",
            "why_it_matters": (
                f"The system found elevator filing {record.record_key}"
                f"{f' ({date_bits})' if date_bits else ''}, but it appears to be older or different work"
                f"{f': {description[:180]}' if description else ''}."
            ),
            "checked_by": "Automatic DOB/Open Data check",
            "last_checked_at": last_official_sync,
            "human_needed": "No resident DOB search needed. Ask management for the real filing number if they claim replacement is already permitted.",
            "source_url": record.source_url,
        })
    else:
        items.append({
            "topic": "Full elevator replacement permit",
            "answer": "No DOB elevator replacement permit has been found yet.",
            "why_it_matters": "Until a matching official filing appears, the replacement schedule is still a management claim, not a public-record-confirmed permit.",
            "checked_by": "Automatic DOB/Open Data check",
            "last_checked_at": last_official_sync,
            "human_needed": "No resident DOB search needed. Ask management for the DOB filing number.",
            "source_url": "",
        })

    if active_official_records:
        record = active_official_records[0]
        issue_date = _plain_date(record.filed_at)
        description = _record_description(record)
        items.append({
            "topic": "Active official elevator violation",
            "answer": f"Yes: official record {record.record_key} is {record.status or 'active/pending'}{f' from {issue_date}' if issue_date else ''}.",
            "why_it_matters": (description[:260] if description else "This is a current official elevator enforcement record."),
            "checked_by": "Automatic DOB/ECB/Open Data check",
            "last_checked_at": normalize_timestamp(record.last_seen_at) or last_official_sync,
            "human_needed": "No DOB lookup needed. Use this public fact when escalating current service problems.",
            "source_url": record.source_url,
        })
    else:
        items.append({
            "topic": "Active official elevator violation",
            "answer": "No active official elevator violation is currently imported.",
            "why_it_matters": "If a new violation appears, the system should show it here without asking residents to search DOB manually.",
            "checked_by": "Automatic DOB/ECB/Open Data check",
            "last_checked_at": last_official_sync,
            "human_needed": "No resident DOB search needed.",
            "source_url": "",
        })

    active_devices = [row for row in devices if (row.status or "").casefold() == "active"]
    if active_devices:
        device_list = ", ".join(row.device_number or row.record_key for row in active_devices)
        latest_inspection = max((_plain_date(row.inspection_date) for row in active_devices), default="")
        items.append({
            "topic": "Elevators listed by DOB",
            "answer": f"DOB lists {len(active_devices)} active elevator device(s): {device_list}.",
            "why_it_matters": (
                f"Latest imported inspection date: {latest_inspection}." if latest_inspection
                else "Device registry status is not the same thing as today's real service condition."
            ),
            "checked_by": "Automatic DOB elevator device check",
            "last_checked_at": last_official_sync,
            "human_needed": "No, unless today's actual service differs; then use the tenant report form.",
            "source_url": active_devices[0].source_url,
        })

    if open_incidents:
        both_down = any(row.asset == "elevator_both" for row in open_incidents)
        if both_down:
            answer = "Tenant reports show an open both-elevators outage."
        else:
            answer = f"Tenant reports show {len(open_incidents)} open elevator issue(s)."
        latest = open_incidents[0]
        items.append({
            "topic": "Actual elevator service reported by tenants",
            "answer": answer,
            "why_it_matters": latest.summary or latest.title or "Tenant reports are the live condition; DOB records can lag behind.",
            "checked_by": "Automatic tenant report/incident check",
            "last_checked_at": normalize_timestamp(latest.updated_at, fallback=latest.last_ts_epoch) or "",
            "human_needed": "Residents should only report what they personally observe; no manual record lookup needed.",
            "source_url": "",
        })
    else:
        items.append({
            "topic": "Actual elevator service reported by tenants",
            "answer": "No open tenant-reported elevator outage is currently in the system.",
            "why_it_matters": "If service changes, residents should report the real condition; the automation handles sorting and escalation.",
            "checked_by": "Automatic tenant report/incident check",
            "last_checked_at": "",
            "human_needed": "Only submit a report when something is actually happening.",
            "source_url": "",
        })

    if current_replacement_permits:
        items.append({
            "topic": "Lobby posting / start-date notice",
            "answer": "Resident photo/check needed if notices are posted.",
            "why_it_matters": "The system can read official DOB records, but it cannot see the building lobby or hallway postings.",
            "checked_by": "Human-only physical check",
            "last_checked_at": "",
            "human_needed": "Yes: one clear photo or note from the lobby/hallway.",
            "source_url": "",
        })
    else:
        items.append({
            "topic": "Lobby posting / start-date notice",
            "answer": "No hallway check is needed yet for replacement work.",
            "why_it_matters": "There is no current full-replacement permit in the official records, so residents should not be asked to hunt for postings yet.",
            "checked_by": "Automatic rule from permit status",
            "last_checked_at": last_official_sync,
            "human_needed": "Not now.",
            "source_url": "",
        })

    items.append({
        "topic": "What residents should do",
        "answer": "Use the tenant report form for real elevator outages; do not manually search DOB unless this view says a human check is needed.",
        "why_it_matters": "The system should do the public-record checking automatically and keep resident effort focused on facts only people in the building can see.",
        "checked_by": "System policy",
        "last_checked_at": now_iso(),
        "human_needed": "Only for real-world observations: outage, posted notice, unsafe condition, or management-only answer.",
        "source_url": "",
    })
    return items


def _project_payload(project: CapitalProject | None) -> dict[str, Any]:
    if not project:
        return {}
    return {
        "id": project.id,
        "building_key": project.building_key,
        "title": project.title,
        "phase": project.phase,
        "management_summary": project.management_summary,
        "risk_level": project.risk_level,
        "current_bottleneck": project.current_bottleneck,
        "next_expected_record": project.next_expected_record,
        "management_contact_email": project.management_contact_email,
        "superintendent_email": project.superintendent_email,
        "updated_at": normalize_timestamp(project.updated_at),
    }


def _milestone_payload(row: ProjectMilestone) -> dict[str, Any]:
    return {
        "id": row.id,
        "phase": row.phase,
        "elevator_asset": row.elevator_asset,
        "management_claimed_start": row.management_claimed_start,
        "management_claimed_end": row.management_claimed_end,
        "publicly_verified_start": normalize_timestamp(row.publicly_verified_start),
        "publicly_verified_end": normalize_timestamp(row.publicly_verified_end),
        "status": row.status,
        "source_type": row.source_type,
        "source_url": row.source_url,
        "notes": row.notes,
    }


def public_record_payload(row: PublicRecordWatch) -> dict[str, Any]:
    return {
        "id": row.id,
        "source_system": row.source_system,
        "record_type": row.record_type,
        "record_key": row.record_key,
        "bbl": row.bbl,
        "bin": row.bin,
        "address": row.address,
        "job_number": row.job_number,
        "permit_number": row.permit_number,
        "device_number": row.device_number,
        "filing_type": row.filing_type,
        "status": row.status,
        "status_detail": row.status_detail,
        "filed_at": normalize_timestamp(row.filed_at),
        "approved_at": normalize_timestamp(row.approved_at),
        "permit_issued_at": normalize_timestamp(row.permit_issued_at),
        "inspection_date": normalize_timestamp(row.inspection_date),
        "expires_at": normalize_timestamp(row.expires_at),
        "source_url": row.source_url,
        "first_seen_at": normalize_timestamp(row.first_seen_at),
        "last_seen_at": normalize_timestamp(row.last_seen_at),
        "last_changed_at": normalize_timestamp(row.last_changed_at),
        "needs_human_verification": bool(row.needs_human_verification),
        "human_verified_at": normalize_timestamp(row.human_verified_at),
        "human_verified_by": row.human_verified_by,
        "machine_verification_status": row.machine_verification_status,
        "machine_confidence": row.machine_confidence,
        "machine_verified_at": normalize_timestamp(row.machine_verified_at),
        "machine_verified_by": row.machine_verified_by,
        "machine_verification_summary": row.machine_verification_summary,
        "corroborating_records": json.loads(row.corroborating_records_json or "[]"),
        "visible_public": bool(row.visible_public),
        "notes": row.notes,
    }


def action_payload(row: WatchdogAction) -> dict[str, Any]:
    return {
        "id": row.id,
        "action_type": row.action_type,
        "severity": row.severity,
        "title": row.title,
        "detail": row.detail,
        "due_at": normalize_timestamp(row.due_at),
        "owner_role": row.owner_role,
        "status": row.status,
        "source_record_id": row.source_record_id,
        "related_incident_id": row.related_incident_id,
        "draft_message": row.draft_message,
        "completed_at": normalize_timestamp(row.completed_at),
        "created_at": normalize_timestamp(row.created_at),
        "updated_at": normalize_timestamp(row.updated_at),
    }


def project_state(session) -> dict[str, Any]:
    project = session.scalar(select(CapitalProject).where(CapitalProject.building_key == BUILDING_KEY))
    if not project:
        project = ensure_default_project(session)
        session.flush()
    milestones = session.scalars(select(ProjectMilestone).where(ProjectMilestone.project_id == project.id)).all()
    records = session.scalars(select(PublicRecordWatch).order_by(PublicRecordWatch.last_changed_at.desc().nullslast())).all()
    actions = session.scalars(
        select(WatchdogAction)
        .where(WatchdogAction.status.in_(VISIBLE_ACTION_STATUSES))
        .order_by(WatchdogAction.created_at.desc().nullslast())
    ).all()
    checks = session.scalars(select(ComplianceCheck).order_by(ComplianceCheck.checked_at.desc().nullslast())).all()
    elevator_incidents = session.scalars(
        select(Incident)
        .where(Incident.category == "elevator")
        .order_by(Incident.last_ts_epoch.desc().nullslast())
        .limit(25)
    ).all()
    service_requests = session.scalars(
        select(ServiceRequestCase)
        .order_by(ServiceRequestCase.submitted_at.desc().nullslast())
        .limit(25)
    ).all()
    return {
        "project": _project_payload(project),
        "management_claims": {
            "project": _project_payload(project),
            "milestones": [_milestone_payload(row) for row in milestones],
        },
        "official_records": [public_record_payload(row) for row in records],
        "tenant_reality": {
            "elevator_incidents": [
                {
                    "incident_id": row.incident_id,
                    "asset": row.asset,
                    "status": row.status,
                    "severity": row.severity,
                    "start_ts": normalize_timestamp(row.start_ts, fallback=row.start_ts_epoch),
                    "end_ts": normalize_timestamp(row.end_ts, fallback=row.end_ts_epoch),
                    "title": row.title,
                    "summary": row.summary,
                    "report_count": int(row.report_count or 0),
                    "witness_count": int(row.witness_count or 0),
                }
                for row in elevator_incidents
            ],
            "service_requests": [
                {
                    "service_request_number": row.service_request_number,
                    "status": row.status,
                    "agency": row.agency,
                    "complaint_type": row.complaint_type,
                    "submitted_at": normalize_timestamp(row.submitted_at),
                    "closed_at": normalize_timestamp(row.closed_at),
                }
                for row in service_requests
            ],
        },
        "actions": [action_payload(row) for row in actions],
        "public_view": public_elevator_watch_items(session),
        "checks": [
            {
                "id": row.id,
                "check_type": row.check_type,
                "status": row.status,
                "checked_at": normalize_timestamp(row.checked_at),
                "checked_by": row.checked_by,
                "photo_url": row.photo_url,
                "source_url": row.source_url,
                "notes": row.notes,
            }
            for row in checks
        ],
    }


def project_briefing(session) -> dict[str, Any]:
    state = project_state(session)
    records = state["official_records"]
    actions = [row for row in state["actions"] if row["status"] == "open"]
    unverified = [row for row in records if row["needs_human_verification"]]
    machine_verified = [row for row in records if row.get("machine_verified_at")]
    verified = [row for row in records if not row["needs_human_verification"]]
    tenant_incidents = state["tenant_reality"]["elevator_incidents"]
    next_action = sorted(actions, key=lambda row: ({"critical": 0, "yellow": 1, "watch": 2, "info": 3}.get(row["severity"], 4), row["due_at"] or ""))[0] if actions else None
    public_view = {row["topic"]: row for row in state.get("public_view", [])}
    permit_answer = (public_view.get("Full elevator replacement permit") or {}).get("answer")
    violation_answer = (public_view.get("Active official elevator violation") or {}).get("answer")

    tenant_draft = (
        "Elevator watch update: "
        f"{permit_answer or 'The system is checking official DOB permit records automatically.'} "
        f"{violation_answer or ''} "
        f"Tenant reports show {len(tenant_incidents)} recent elevator record(s). Residents only need to report real conditions or send a hallway-posting photo when the public view asks for it."
    )
    management_draft = (
        "Please provide the current DOB filing number, permit status, expected start date, and posting plan for the "
        "455 Ocean Parkway elevator replacement. Tenants are tracking management claims, official public records, "
        "and observed elevator service separately."
    )
    if next_action and next_action.get("draft_message"):
        management_draft = next_action["draft_message"]

    return {
        "project_state": state,
        "tenant_update_draft": tenant_draft,
        "management_followup_draft": management_draft,
        "next_best_action": next_action,
        "official_record_counts": {
            "total": len(records),
            "verified": len(verified),
            "machine_verified": len(machine_verified),
            "needs_human_verification": len(unverified),
        },
        "used_llm": False,
    }


def generate_weekly_digest(session) -> WeeklyDigest:
    end = datetime.now(tz=timezone.utc)
    start = end - timedelta(days=7)
    briefing = project_briefing(session)
    digest = WeeklyDigest(
        period_start=start.isoformat(),
        period_end=end.isoformat(),
        public_summary=briefing["tenant_update_draft"],
        management_followup_draft=briefing["management_followup_draft"],
        tenant_update_draft=briefing["tenant_update_draft"],
        generated_at=now_iso(),
        used_llm=False,
    )
    session.add(digest)
    return digest
