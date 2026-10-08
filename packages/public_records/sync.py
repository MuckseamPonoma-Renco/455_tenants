from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import object_session

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
from packages.project_watch.rules import action_for_changed_record, action_for_new_record, ensure_action, evaluate_project_rules, now_iso
from packages.public_records.config import building_bbl_compact, building_bin, source_configs
from packages.public_records.elevator_scope import describes_elevator_replacement_scope
from packages.public_records.hearings import current_oath_hearing_outcome
from packages.public_records.normalize import normalize_record, semantic_raw_hash
from packages.public_records.nyc_open_data import fetch_rows, query_url
from packages.public_records.verification import apply_machine_verification
from packages.public_records.source_state import record_source_results, watchdog_source_health
from packages.public_records.source_queries import query_specs, oath_ticket_query_specs, electrical_is_elevator_related, normalize_ticket_reference
from packages.timeutil import normalize_timestamp, parse_ts_to_epoch


BUILDING_KEY = "455-ocean-parkway"
VISIBLE_ACTION_STATUSES = ("open", "pending", "failed")
LAST_FETCH_ERRORS: list[dict[str, str]] = []
LAST_SUCCESSFUL_CONTACT_REGISTRATION_IDS: set[str] = set()
LAST_SEEN_CONTACT_KEYS_BY_REGISTRATION: dict[str, set[str]] = {}


class PublicRecordFetchBatch(list):
    """Keep completion evidence with its rows, even if another worker fetches."""

    def __init__(self):
        super().__init__()
        self.errors: list[dict[str, str]] = []
        self.source_results: dict[str, dict[str, Any]] = {}
        self.contact_registration_ids: set[str] = set()
        self.contact_keys: dict[str, set[str]] = {}


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
    return query_specs()


def _fetch_error_payload(source_key: str, params: dict[str, str], exc: Exception) -> dict[str, str]:
    return {
        "source_key": source_key,
        "params": json.dumps(params, sort_keys=True),
        "error": str(exc)[:500],
        "checked_at": now_iso(),
    }


def fetch_public_record_rows() -> list[tuple[str, dict[str, Any], str]]:
    global LAST_FETCH_ERRORS, LAST_SUCCESSFUL_CONTACT_REGISTRATION_IDS, LAST_SEEN_CONTACT_KEYS_BY_REGISTRATION
    sources = _source_map()
    fetched = PublicRecordFetchBatch()
    errors = fetched.errors
    source_results = fetched.source_results
    elevator_jobs: set[str] = set()
    device_ids: set[str] = set()
    registration_ids: set[str] = set()
    ecb_tickets: set[str] = set()
    observed_oath_tickets: set[str] = set()
    successful_contact_registration_ids: set[str] = set()
    seen_contact_keys_by_registration: dict[str, set[str]] = {}

    def fetch_source(
        source_key: str,
        params: dict[str, str],
        *,
        limit: int = 500,
    ) -> tuple[str, list[dict[str, Any]], bool]:
        source = sources[source_key]
        receipt = source_results.setdefault(source_key, {
            "query_count": 0, "source_errors": 0, "complete": True, "errors": [],
        })
        receipt["query_count"] += 1
        url = query_url(source, {"$limit": str(limit), **params})
        try:
            return url, fetch_rows(source, params, limit=limit), True
        except Exception as exc:
            error = _fetch_error_payload(source_key, params, exc)
            errors.append(error)
            receipt["source_errors"] += 1
            receipt["complete"] = False
            receipt["errors"].append(error["error"])
            return url, [], False

    for source_key, params in _query_specs():
        url, rows, _succeeded = fetch_source(source_key, params)
        for row in rows:
            if source_key == "dob_now_electrical_applications" and not electrical_is_elevator_related(row):
                continue
            fetched.append((source_key, row, url))
            if source_key == "dob_ecb_violations" and row.get("ecb_violation_number"):
                ecb_tickets.add(normalize_ticket_reference(row["ecb_violation_number"]))
            if source_key == "oath_hearings" and row.get("ticket_number"):
                observed_oath_tickets.add(normalize_ticket_reference(row["ticket_number"]))
            if source_key == "dob_now_elevator_applications" and row.get("job_filing_number"):
                elevator_jobs.add(str(row["job_filing_number"]))
            if source_key == "dob_now_elevator_safety_compliance" and row.get("device_number"):
                device_ids.add(str(row["device_number"]))
            if source_key == "hpd_building" and row.get("registrationid"):
                registration_ids.add(str(row["registrationid"]))

    for params in oath_ticket_query_specs(ecb_tickets - observed_oath_tickets):
        url, rows, _succeeded = fetch_source("oath_hearings", params)
        fetched.extend(("oath_hearings", row, url) for row in rows)

    for job in sorted(elevator_jobs):
        params = {"job_filing_number": job}
        url, rows, _succeeded = fetch_source("dob_now_elevator_device_details", params)
        for row in rows:
            fetched.append(("dob_now_elevator_device_details", row, url))
    for device_id in sorted(device_ids):
        params = {"device_id": device_id}
        url, rows, _succeeded = fetch_source("dob_now_elevator_device_details", params)
        for row in rows:
            fetched.append(("dob_now_elevator_device_details", row, url))
    for registration_id in sorted(registration_ids):
        params = {"registrationid": registration_id}
        url, rows, succeeded = fetch_source("hpd_registration_contacts", params)
        if succeeded:
            successful_contact_registration_ids.add(registration_id)
            seen_contact_keys_by_registration[registration_id] = {
                str(row.get("registrationcontactid") or "").strip()
                for row in rows
                if str(row.get("registrationcontactid") or "").strip()
            }
        for row in rows:
            fetched.append(("hpd_registration_contacts", row, url))

    # Linked sources are not complete when discovery failed, even if another
    # query happened to return rows. Never retire evidence on a partial union.
    dependencies = {
        "oath_hearings": ("dob_ecb_violations",),
        "dob_now_elevator_device_details": ("dob_now_elevator_applications", "dob_now_elevator_safety_compliance"),
        "hpd_registration_contacts": ("hpd_building",),
    }
    for source_key in sources:
        receipt = source_results.setdefault(source_key, {
            "query_count": 0, "source_errors": 0, "complete": True, "errors": [],
        })
        missing = [key for key in dependencies.get(source_key, ()) if not source_results.get(key, {}).get("complete")]
        if not receipt["query_count"] or missing:
            error = _fetch_error_payload(source_key, {}, RuntimeError(
                "Required source queries were not completed" + (": " + ", ".join(missing) if missing else ".")
            ))
            errors.append(error)
            receipt["complete"] = False
            receipt["source_errors"] += 1
            receipt["errors"].append(error["error"])
    fetched.contact_registration_ids = successful_contact_registration_ids
    fetched.contact_keys = seen_contact_keys_by_registration
    LAST_FETCH_ERRORS = errors
    LAST_SUCCESSFUL_CONTACT_REGISTRATION_IDS = successful_contact_registration_ids
    LAST_SEEN_CONTACT_KEYS_BY_REGISTRATION = seen_contact_keys_by_registration
    return fetched


def _retire_unseen_registration_contacts(session, batch=None) -> int:
    registration_ids = batch.contact_registration_ids if isinstance(batch, PublicRecordFetchBatch) else LAST_SUCCESSFUL_CONTACT_REGISTRATION_IDS
    contact_keys = batch.contact_keys if isinstance(batch, PublicRecordFetchBatch) else LAST_SEEN_CONTACT_KEYS_BY_REGISTRATION
    if isinstance(batch, PublicRecordFetchBatch) and not batch.source_results.get("hpd_registration_contacts", {}).get("complete"):
        return 0
    if not registration_ids:
        return 0
    retired = 0
    contacts = session.scalars(
        select(PublicRecordWatch).where(
            PublicRecordWatch.source_system == "hpd_registration_contacts"
        )
    ).all()
    ts = now_iso()
    for contact in contacts:
        try:
            raw = json.loads(contact.raw_json or "{}")
        except json.JSONDecodeError:
            continue
        if not isinstance(raw, dict):
            continue
        registration_id = str(raw.get("registrationid") or "").strip()
        if registration_id not in registration_ids:
            continue
        seen_keys = contact_keys.get(registration_id, set())
        # An empty upstream snapshot is not proof that every registered contact
        # was removed. Preserve that history; source presence conveys absence.
        if not seen_keys:
            continue
        if contact.record_key in seen_keys:
            continue
        if contact.status == "not_in_current_hpd_export":
            continue
        contact.status = "not_in_current_hpd_export"
        contact.visible_public = False
        contact.needs_human_verification = True
        contact.machine_verified_at = None
        contact.machine_verified_by = None
        contact.machine_verification_status = "retired_from_current_registration"
        contact.machine_verification_summary = (
            "This contact is not present in the latest successful HPD registration-contact export."
        )
        contact.last_changed_at = ts
        action_for_changed_record(session, contact)
        retired += 1
    return retired


def _sync_source_error_action(session, errors: list[dict[str, str]]) -> None:
    open_actions = session.scalars(
        select(WatchdogAction).where(
            WatchdogAction.action_type == "public_record_source_error",
            WatchdogAction.status.in_(["open", "pending"]),
        )
    ).all()
    if not errors:
        for action in open_actions:
            action.status = "completed"
            action.completed_at = now_iso()
            action.updated_at = now_iso()
        return

    source_names = sorted({row["source_key"] for row in errors})
    detail_lines = [
        f"{row['source_key']} {row['params']}: {row['error']}"
        for row in errors[:6]
    ]
    if len(errors) > 6:
        detail_lines.append(f"...and {len(errors) - 6} more source/query failure(s).")
    ensure_action(
        session,
        action_type="public_record_source_error",
        severity="watch",
        title="Public-record sync had partial source failures",
        detail=(
            "The watchdog kept using the sources that responded, but these official-source queries failed: "
            + "; ".join(detail_lines)
        ),
        due_in_days=1,
        owner_role="system",
        draft_message=f"Retry public-record sync; affected source(s): {', '.join(source_names)}.",
    )


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
    existing_semantic_hash = existing.raw_hash
    try:
        existing_raw = json.loads(existing.raw_json or "{}")
        if isinstance(existing_raw, dict):
            existing_semantic_hash = semantic_raw_hash(source_key, existing_raw)
    except (TypeError, ValueError):
        pass
    if existing_semantic_hash == normalized["raw_hash"]:
        for field, value in normalized.items():
            if field in {"needs_human_verification", "visible_public"}:
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


def sync_public_records(session, *, baseline: bool | None = None) -> dict[str, Any]:
    counts = {"fetched": 0, "created": 0, "baseline_created": 0, "changed": 0, "unchanged": 0, "source_errors": 0}
    seen: set[tuple[str, str, str]] = set()
    existing_sources = {
        source_system
        for (source_system,) in session.execute(select(PublicRecordWatch.source_system).distinct()).all()
    }
    fetched_rows = fetch_public_record_rows()
    fetch_errors = list(fetched_rows.errors if isinstance(fetched_rows, PublicRecordFetchBatch) else LAST_FETCH_ERRORS)
    counts["source_errors"] = len(fetch_errors)
    _sync_source_error_action(session, fetch_errors)
    for source_key, row, url in fetched_rows:
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
    counts["source_health"] = record_source_results(
        session, fetched_rows.source_results if isinstance(fetched_rows, PublicRecordFetchBatch) else {}, seen,
    )
    counts["source_errors"] = max(counts["source_errors"], counts["source_health"]["source_errors"])
    counts.update(apply_machine_verification(session))
    counts["registration_contacts_retired"] = _retire_unseen_registration_contacts(session, fetched_rows)
    return counts


def sync_replacement_watchdog(session) -> dict[str, Any]:
    project = ensure_default_project(session)
    counts = sync_public_records(session)
    actions = evaluate_project_rules(session)
    progress = _filing_progress(_latest_replacement_filing(session.scalars(select(PublicRecordWatch)).all()))
    for key in ("phase", "current_bottleneck", "next_expected_record"):
        setattr(project, key, progress[key])
    digest = ensure_recent_weekly_digest(session)
    session.flush()
    open_actions = session.query(WatchdogAction).filter(WatchdogAction.status == "open").all()
    counts["actions_open"] = sum(1 for action in open_actions if action_is_tenant_visible(action))
    counts["actions_touched"] = len(actions)
    counts["weekly_digest_created"] = 1 if digest else 0
    return counts


def ensure_recent_weekly_digest(session, *, max_age_days: int = 7) -> WeeklyDigest | None:
    latest = session.scalars(
        select(WeeklyDigest).order_by(WeeklyDigest.generated_at.desc().nullslast())
    ).first()
    latest_epoch = parse_ts_to_epoch(latest.generated_at) if latest else None
    now_epoch = int(datetime.now(tz=timezone.utc).timestamp())
    if latest_epoch and (now_epoch - latest_epoch) < max_age_days * 86400:
        return None
    return generate_weekly_digest(session)


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
        "job_description",
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


def _description_excerpt(description: str, *, max_chars: int) -> str:
    normalized = " ".join((description or "").split())
    first_sentence = normalized.split(".", maxsplit=1)[0].strip()
    if len(first_sentence) <= max_chars:
        return first_sentence
    return first_sentence[:max_chars].rsplit(" ", maxsplit=1)[0].rstrip(" ,;:")


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
    # Details may mention an old pending hearing on a resolved record.
    status = (row.status or "").casefold()
    if any(word in status for word in ("inactive", "closed", "resolved", "dismissed")):
        return False
    return any(word in status for word in ("active", "open", "pending"))


def _is_closed_or_expired(row: PublicRecordWatch) -> bool:
    status = (row.status or "").casefold()
    if any(word in status for word in ("signed off", "loc issued", "co issued", "resolved", "dismissed", "withdrawn", "cancelled", "canceled", "revoked")):
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
    return describes_elevator_replacement_scope(text)


def _record_sort_key(row: PublicRecordWatch) -> tuple[str, str]:
    return (normalize_timestamp(row.filed_at) or normalize_timestamp(row.last_changed_at) or "", row.record_key)


def _current_trusted_records(records: list[PublicRecordWatch]) -> list[PublicRecordWatch]:
    return [row for row in records if public_record_is_tenant_trusted(row)
            and getattr(row, "source_presence_status", None) != "not_seen"]


def _verification_counts(records: list[PublicRecordWatch]) -> dict[str, int]:
    elevator_rows = [row for row in records if _is_elevator_record(row)]
    return {
        "total_imported_elevator_records": len(elevator_rows),
        "needs_human_verification": sum(1 for row in elevator_rows if row.needs_human_verification
                                        or not (row.machine_verified_at or row.human_verified_at)),
        "official_conflicts": sum(1 for row in elevator_rows if row.machine_verification_status == "official_conflict"),
    }


def _latest_replacement_filing(records: list[PublicRecordWatch]) -> PublicRecordWatch | None:
    # History still establishes that an application was submitted. Completion,
    # withdrawal, expiration or a missing refresh must not reset the project to
    # "no filing", or allow an older active application to replace the latest.
    candidates = [row for row in records if public_record_is_tenant_trusted(row)
                  and _looks_like_full_elevator_replacement(row)]
    return max(candidates, key=_record_sort_key, default=None)


def _filing_progress(record: PublicRecordWatch | None) -> dict[str, str]:
    if not record:
        return {
            "phase": "pre_permit_watch",
            "current_bottleneck": "No current verified full-replacement elevator application is available in the monitored records.",
            "next_expected_record": "Matching DOB NOW elevator application, followed by approval and permit issuance.",
            "current_milestone": "Awaiting verified elevator application",
        }
    status = (record.status or "filed").casefold()
    raw = _raw_record(record)
    if getattr(record, "source_presence_status", None) == "not_seen":
        return {
            "phase": "filing_status_unconfirmed",
            "current_bottleneck": f"Previously verified elevator application {record.record_key} was absent from the latest source refresh; its last recorded status was {record.status or 'filed'}.",
            "next_expected_record": "Restored official-source confirmation of this filing or its replacement; absence does not establish withdrawal, correction or completion.",
            "current_milestone": f"{record.record_key}: current status unconfirmed (last recorded: {record.status or 'filed'})",
        }
    signoff_date = _plain_date(raw.get("signedoff_date") or raw.get("signoff_date"))
    if signoff_date or any(word in status.replace("-", " ") for word in ("signed off", "loc issued", "co issued")):
        return {
            "phase": "official_signoff_recorded",
            "current_bottleneck": f"DOB records sign-off for elevator application {record.record_key}{' on ' + signoff_date if signoff_date else ''}; confirm each elevator's return to service and any remaining project items.",
            "next_expected_record": "Final inspection/sign-off documents and confirmation of each car's return to service; official sign-off alone does not establish current operation or safety.",
            "current_milestone": f"{record.record_key}: official sign-off recorded{' ' + signoff_date if signoff_date else ''}",
        }
    if any(word in status for word in ("withdrawn", "cancelled", "canceled", "revoked")):
        return {
            "phase": "filing_inactive",
            "current_bottleneck": f"The latest elevator application {record.record_key} is {record.status}; management must identify the filing and approvals now intended for the project.",
            "next_expected_record": "Official replacement/reactivated filing or other current project authorization, with an updated schedule.",
            "current_milestone": f"{record.record_key}: {record.status}",
        }
    expiry = parse_ts_to_epoch(record.expires_at)
    if "expired" in status or (expiry is not None and expiry < int(datetime.now(timezone.utc).timestamp())):
        return {
            "phase": "permit_expired",
            "current_bottleneck": f"The monitored permit/application {record.record_key} is expired; current renewal or replacement authorization is unconfirmed.",
            "next_expected_record": "Official renewal/extension or replacement filing and confirmation of the revised work schedule.",
            "current_milestone": f"{record.record_key}: expired{' ' + _plain_date(record.expires_at) if record.expires_at else ''}",
        }
    if any(word in status for word in ("objection", "incomplete", "hold")):
        return {
            "phase": "objections_pending",
            "current_bottleneck": f"Elevator application {record.record_key} is {record.status}; correction and resubmission are needed.",
            "next_expected_record": "Applicant correction/resubmission and DOB approval, then elevator permit issuance.",
            "current_milestone": f"{record.record_key}: {record.status}",
        }
    if record.permit_issued_at:
        return {
            "phase": "permit_issued_watch",
            "current_bottleneck": "Elevator permit issuance is recorded; equipment delivery, the car-by-car schedule and actual work start require confirmation.",
            "next_expected_record": "Posted work/start notices, verified site progress, inspections and final sign-off.",
            "current_milestone": f"{record.record_key}: elevator permit issued {_plain_date(record.permit_issued_at)}",
        }
    if "approved" in status:
        return {
            "phase": "approved_awaiting_permit",
            "current_bottleneck": f"Elevator application {record.record_key} is approved; no issued elevator permit date is recorded.",
            "next_expected_record": "Issued elevator permit and a dated car-by-car work schedule.",
            "current_milestone": f"{record.record_key}: approved, awaiting elevator permit",
        }
    return {
        "phase": "plan_review",
        "current_bottleneck": f"Elevator application {record.record_key} is {record.status or 'filed'}; approval and elevator permit issuance remain unconfirmed.",
        "next_expected_record": "DOB plan-review decision or status change, then elevator permit issuance.",
        "current_milestone": f"{record.record_key}: {record.status or 'filed'}",
    }


def _source_check_time(source_keys: tuple[str, ...], records: list[PublicRecordWatch], health: dict[str, Any]) -> str:
    """The oldest relevant source controls freshness; unrelated rows cannot renew it."""
    states = {row["source_key"]: row for row in health.get("sources", [])}
    stamps = []
    for key in source_keys:
        if key in states and (states[key].get("last_success_at") or states[key].get("last_attempt_at")):
            stamp = normalize_timestamp(states[key].get("last_success_at")) or ""
        else:
            # Legacy stores have no completed-fetch receipt, only record sightings.
            sightings = [normalize_timestamp(row.last_seen_at) or "" for row in records if row.source_system == key]
            stamp = min(sightings, default="")
        if not stamp:
            return ""
        stamps.append(stamp)
    return min(stamps, default="")


def _record_check_time(record: PublicRecordWatch, health: dict[str, Any]) -> str:
    seen = normalize_timestamp(record.last_seen_at) or ""
    checked = _source_check_time((record.source_system,), [record], health)
    return min(seen, checked) if seen and checked else ""


def _building_match_explanation(record: PublicRecordWatch) -> str:
    if str(record.bin or "") == building_bin() and str(record.bbl or "") == building_bbl_compact():
        return (f"The DOB address {record.address or 'on this record'} belongs to this building: "
                f"BIN {record.bin} and BBL {record.bbl} both match 455 Ocean Parkway. ")
    if str(record.bin or "") == building_bin():
        return f"BIN {record.bin} matches 455 Ocean Parkway. "
    return "The record passed the official building/device identity match. "


def _linked_hearing_outcome(record: PublicRecordWatch, related_records: list[PublicRecordWatch] | None = None) -> dict[str, Any] | None:
    if record.record_type != "dob_ecb_violation":
        return None
    if related_records is None:
        session = object_session(record)
        related_records = list(session.scalars(select(PublicRecordWatch).where(PublicRecordWatch.source_system == "oath_hearings"))) if session else []
    return current_oath_hearing_outcome(record, related_records)


def _enforcement_details(record: PublicRecordWatch, related_records: list[PublicRecordWatch] | None = None) -> str:
    raw = _raw_record(record)
    facts = []
    if raw.get("severity"):
        facts.append(f"Severity: {raw['severity']}")
    if _plain_date(raw.get("hearing_date")):
        facts.append(f"Hearing: {_plain_date(raw.get('hearing_date'))} ({raw.get('hearing_status') or 'status not shown'})")
    outcome = _linked_hearing_outcome(record, related_records)
    if outcome:
        facts.append(f"OATH hearing result: {outcome['hearing_result']}; decision date: {outcome['decision_date']}")
        facts.append("The OATH decision supplies the hearing outcome while the ECB hearing-status field may lag")
    elif record.source_system == "oath_hearings" and raw.get("hearing_result"):
        facts.append(f"OATH hearing result: {raw['hearing_result']}")
        if _plain_date(raw.get("decision_date")):
            facts.append(f"Decision date: {_plain_date(raw['decision_date'])}")
    if outcome or (record.source_system == "oath_hearings" and raw.get("hearing_result")):
        facts.append("A hearing decision does not establish physical correction or accepted DOB certification")
    if record.record_type == "dob_ecb_violation":
        certification = str(raw.get("certification_status") or "").strip()
        facts.append(f"Certification: {certification}" if certification else "No certification status is shown in this feed")
    if record.record_type == "dob_complaint":
        facts.append(f"Inspection: {_plain_date(record.inspection_date)}" if record.inspection_date else "No inspection date is shown")
        disposition = str(raw.get("disposition_code") or raw.get("disposition_comments") or "").strip()
        facts.append(f"Disposition: {disposition}" if disposition else "No disposition is shown")
    return "; ".join(facts)


def public_elevator_watch_items(session) -> list[dict[str, Any]]:
    records = session.scalars(select(PublicRecordWatch)).all()
    elevator_records = _current_trusted_records(records)
    health = watchdog_source_health(session)
    permit_records = sorted(
        [row for row in elevator_records if row.record_type == "elevator_permit_application"],
        key=_record_sort_key,
        reverse=True,
    )
    latest_replacement = _latest_replacement_filing(records)
    filing_progress = _filing_progress(latest_replacement)
    current_replacement_filings = [latest_replacement] if latest_replacement and filing_progress["phase"] in {
        "objections_pending", "plan_review", "approved_awaiting_permit", "permit_issued_watch",
    } else []
    issued_replacement_permits = [row for row in current_replacement_filings if row.permit_issued_at]
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

    filing_check = _source_check_time(("dob_now_elevator_applications",), records, health)
    enforcement_check = _source_check_time(("dob_ecb_violations", "dob_violations", "dob_complaints"), records, health)
    device_check = _source_check_time(("dob_now_elevator_safety_compliance",), records, health)

    items: list[dict[str, Any]] = []

    if latest_replacement:
        record = latest_replacement
        permit_date = _plain_date(record.permit_issued_at)
        answer = f"DOB filing {record.record_key} matches the announced two-elevator project."
        inactive_phase = filing_progress["phase"] not in {"objections_pending", "plan_review", "approved_awaiting_permit", "permit_issued_watch"}
        if inactive_phase:
            answer += " " + filing_progress["current_milestone"] + "."
        elif permit_date:
            answer += f" Permit-issued date: {permit_date}."
        else:
            answer += f" It is not an issued permit yet; current status: {record.status or 'filed'}."
        description = _record_description(record)
        description_excerpt = _description_excerpt(description, max_chars=260)
        items.append({
            "topic": "Full elevator replacement permit",
            "answer": answer,
            "why_it_matters": (
                f"The official work description is: {description_excerpt}. " if description_excerpt else ""
            ) + _building_match_explanation(record) + (
                filing_progress["current_bottleneck"] + " " + filing_progress["next_expected_record"]
                if inactive_phase else "An elevator application records submitted scope. An electrical permit does not establish approval of this elevator application."
            ),
            "checked_by": "Automatic DOB/Open Data check",
            "last_checked_at": _record_check_time(record, health),
            "human_needed": (
                ("The operator should restore source confirmation; management should confirm this filing's current status."
                 if filing_progress["phase"] == "filing_status_unconfirmed"
                 else "Ask management for the official documents and service/schedule confirmation described above.")
                if inactive_phase else "No DOB lookup needed. A resident photo is only needed for lobby notices."
                if permit_date
                else ("Ask management for the exact DOB objections, who owns each correction, and the target correction/resubmission dates."
                      if any(word in (record.status or "").casefold() for word in ("objection", "hold", "incomplete"))
                      else "No DOB lookup needed. The automatic watcher will track plan review and permit issuance; management should confirm scope and expected start date.")
            ),
            "source_url": record.source_url,
        })
    elif permit_records:
        record = permit_records[0]
        description = _record_description(record)
        description_excerpt = _description_excerpt(description, max_chars=180)
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
                f"{f' ({date_bits})' if date_bits else ''}, but its public scope does not explicitly establish a full replacement"
                f"{f': {description_excerpt}' if description_excerpt else ''}."
            ),
            "checked_by": "Automatic DOB/Open Data check",
            "last_checked_at": filing_check,
            "human_needed": "No resident DOB search needed. Ask management for the real filing number if they claim replacement is already permitted.",
            "source_url": record.source_url,
        })
    else:
        items.append({
            "topic": "Full elevator replacement permit",
            "answer": "No DOB elevator replacement permit has been found yet.",
            "why_it_matters": "Until a matching official filing appears, the replacement schedule is still a management claim, not a public-record-confirmed permit.",
            "checked_by": "Automatic DOB/Open Data check",
            "last_checked_at": filing_check,
            "human_needed": "No resident DOB search needed. Ask management for the DOB filing number.",
            "source_url": "",
        })

    electrical_records = sorted(
        [row for row in elevator_records if row.record_type == "electrical_permit_application" and not _is_closed_or_expired(row)],
        key=_record_sort_key, reverse=True,
    )
    for record in electrical_records:
        raw = _raw_record(record)
        stated_start = _plain_date(raw.get("job_start_date"))
        items.append({
            "topic": f"Related electrical permit: {record.record_key}",
            "answer": (f"Electrical filing {record.record_key}: {record.status or 'status unavailable'}. "
                       + (f"Permit issued {_plain_date(record.permit_issued_at)}. " if record.permit_issued_at else "No electrical permit-issued date is shown. ")
                       + (f"Applicant-entered start: {stated_start}." if stated_start else "")),
            "why_it_matters": (f"{_description_excerpt(_record_description(record), max_chars=230)}. "
                               "This covers related electrical work; it does not establish elevator-modernization approval, an issued elevator permit, or actual construction start."),
            "checked_by": "Automatic DOB electrical permit check",
            "last_checked_at": _record_check_time(record, health),
            "human_needed": "Ask management what electrical work is scheduled and how it connects to the elevator permit and car-by-car schedule.",
            "source_url": record.source_url,
        })

    for index, record in enumerate(active_official_records):
        issue_date = _plain_date(record.filed_at)
        description = _record_description(record)
        is_complaint = record.record_type == "dob_complaint"
        class_one = bool(re.fullmatch(r"CLASS\s*-?\s*1", str(_raw_record(record).get("severity") or "").strip(), re.IGNORECASE))
        base_topic = "Pending official elevator complaint" if is_complaint else "Active official elevator violation"
        # Preserve the original headline key for the first violation while giving
        # every active record its own row and stable identifier.
        prior_same_type = any((earlier.record_type == "dob_complaint") == is_complaint for earlier in active_official_records[:index])
        items.append({
            "topic": f"{base_topic}: {record.record_key}" if prior_same_type or is_complaint else base_topic,
            "answer": f"Yes: official record {record.record_key} is {record.status or 'active/pending'}{f' from {issue_date}' if issue_date else ''}.",
            "why_it_matters": (f"{description[:260]}. " if description else "") + _enforcement_details(record, records)
                + (". A complaint is an allegation awaiting the agency's findings." if is_complaint
                   else ". The database status does not establish today's physical condition or prove whether repairs have been made."),
            "checked_by": "Automatic DOB/ECB/Open Data check",
            "last_checked_at": _record_check_time(record, health),
            "human_needed": ("Ask management to identify the affected elevator and provide its current status, dated repairs and correction/certification evidence."
                             if class_one else ("The official OATH hearing outcome is available. Request correction/certification evidence if still outstanding; report any real new or changed condition."
                                                if _linked_hearing_outcome(record, records) else "No DOB lookup needed. Request the hearing outcome and correction evidence when applicable; report any real new or changed condition.")),
            "source_url": record.source_url,
        })
    if not any(row.record_type != "dob_complaint" for row in active_official_records):
        items.append({
            "topic": "Active official elevator violation",
            "answer": "No active official elevator violation is currently imported.",
            "why_it_matters": "If a new violation appears, the system should show it here without asking residents to search DOB manually.",
            "checked_by": "Automatic DOB/ECB/Open Data check",
            "last_checked_at": enforcement_check,
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
                f"Latest imported inspection date: {latest_inspection}. " if latest_inspection else ""
            ) + "Active is the device registry status; it does not establish today's operation, safety, or completion of repairs.",
            "checked_by": "Automatic DOB elevator device check",
            "last_checked_at": device_check,
            "human_needed": "No, unless today's actual service differs; then use the tenant report form.",
            "source_url": active_devices[0].source_url,
        })

    if open_incidents:
        latest = open_incidents[0]
        asset_label = {
            "elevator_north": "the north elevator",
            "elevator_south": "the south elevator",
            "elevator_both": "both elevators",
        }.get(latest.asset, "at least one elevator")
        answer = f"The latest unresolved tenant report concerns {asset_label}."
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
            "answer": "No open tenant-reported elevator service issue is currently in the system.",
            "why_it_matters": "If service changes, residents should report the real condition; the automation handles sorting and escalation.",
            "checked_by": "Automatic tenant report/incident check",
            "last_checked_at": "",
            "human_needed": "Only submit a report when something is actually happening.",
            "source_url": "",
        })

    if issued_replacement_permits:
        items.append({
            "topic": "Lobby posting / start-date notice",
            "answer": "Resident photo/check needed if notices are posted.",
            "why_it_matters": "The system can read official DOB records, but it cannot see the building lobby or hallway postings.",
            "checked_by": "Human-only physical check",
            "last_checked_at": "",
            "human_needed": "Yes: one clear photo or note from the lobby/hallway.",
            "source_url": "",
        })
    elif latest_replacement and not current_replacement_filings:
        posting_answers = {
            "official_signoff_recorded": "Official sign-off is recorded; report new work notices or service changes if observed.",
            "filing_status_unconfirmed": "The latest source refresh could not confirm the known filing; current posting requirements remain unconfirmed.",
            "permit_expired": "The monitored filing is expired; management should identify any renewed authorization and its posting requirements.",
            "filing_inactive": "The latest filing is inactive; management should identify the filing now intended for the project and its posting requirements.",
        }
        items.append({
            "topic": "Lobby posting / start-date notice",
            "answer": posting_answers[filing_progress["phase"]],
            "why_it_matters": "Retain the known filing history and confirm the current project status before deriving a new notice request.",
            "checked_by": "Automatic rule from latest known filing status",
            "last_checked_at": _record_check_time(latest_replacement, health),
            "human_needed": "Report any newly posted notice or active work you observe; management should provide current project documents.",
            "source_url": latest_replacement.source_url,
        })
    else:
        items.append({
            "topic": "Lobby posting / start-date notice",
            "answer": "No hallway check is needed yet for replacement work.",
            "why_it_matters": "There is no issued full-replacement permit in the official records, so residents should not be asked to hunt for postings yet.",
            "checked_by": "Automatic rule from permit status",
            "last_checked_at": filing_check,
            "human_needed": "Not now.",
            "source_url": "",
        })

    absent = [row.record_key for row in records if public_record_is_tenant_trusted(row)
              and getattr(row, "source_presence_status", None) == "not_seen"]
    verification = _verification_counts(records)
    pending_review = verification["needs_human_verification"] or verification["official_conflicts"]
    if not health.get("ok") or absent or pending_review:
        problems = [f"{row['source_key']}: {row.get('state') or 'unverified'}"
                    for row in health.get("sources", []) if row.get("state") not in {"ready", "empty"}]
        items.append({
            "topic": "Monitoring coverage",
            "answer": "Some official-source checks need attention; displayed records are the last verified observations.",
            "why_it_matters": ("; ".join(problems) or ("Source checks completed." if health.get("ok") else "Completed source-check receipts are unavailable."))
                + (f" {len(absent)} previously tracked records were absent from their latest source; their last observations remain labeled in PublicRecords. Absence does not prove correction or resolution." if absent else "")
                + (f" {verification['needs_human_verification']} elevator-related records need identity review; {verification['official_conflicts']} have conflicting identifiers. Their details are excluded from current verified facts." if pending_review else ""),
            "checked_by": "Per-source fetch receipts",
            "last_checked_at": normalize_timestamp(health.get("last_attempt_at")) or "",
            "human_needed": "The operator should restore source checks; tenants should continue reporting observed conditions and request management-only answers.",
            "source_url": "",
        })

    items.append({
        "topic": "What residents should do",
        "answer": "Report real outages or degraded service. The tenant association should follow the current ActionQueue for management answers and project documents.",
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


def public_record_payload(row: PublicRecordWatch, related_records: list[PublicRecordWatch] | None = None) -> dict[str, Any]:
    raw = _raw_record(row)
    outcome = _linked_hearing_outcome(row, related_records)
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
        "source_presence_status": getattr(row, "source_presence_status", None) or "legacy_unconfirmed",
        "source_checked_at": normalize_timestamp(getattr(row, "source_checked_at", None)),
        "severity": raw.get("severity"),
        "hearing_date": _plain_date(raw.get("hearing_date")),
        "hearing_status": raw.get("hearing_status"),
        "hearing_result": outcome["hearing_result"] if outcome else raw.get("hearing_result"),
        "hearing_decision_date": outcome["decision_date"] if outcome else _plain_date(raw.get("decision_date")),
        "hearing_outcome_source_url": outcome["source_url"] if outcome else (row.source_url if row.source_system == "oath_hearings" else None),
        "certification_status": raw.get("certification_status"),
        "public_context": _enforcement_details(row, related_records),
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


def public_record_is_tenant_trusted(row: PublicRecordWatch) -> bool:
    if not row.visible_public:
        return False
    if not _is_elevator_record(row):
        return False
    if row.needs_human_verification:
        return False
    if row.machine_verification_status == "official_conflict":
        return False
    return bool(row.human_verified_at or row.machine_verified_at)


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


def action_is_tenant_visible(row: WatchdogAction) -> bool:
    return row.status in VISIBLE_ACTION_STATUSES and row.owner_role in {"resident", "tenant_association"}


def registered_owner_organizations(session) -> list[dict[str, Any]]:
    """Return only verified organization-level HPD owner entries for tenant views."""
    current_registration_ids: set[str] = set()
    building_rows = session.scalars(
        select(PublicRecordWatch).where(PublicRecordWatch.source_system == "hpd_building")
    ).all()
    for building in building_rows:
        if building.machine_verification_status == "official_conflict" or building.source_presence_status == "not_seen":
            continue
        try:
            building_raw = json.loads(building.raw_json or "{}")
        except json.JSONDecodeError:
            continue
        registration_id = str(
            building_raw.get("registrationid") if isinstance(building_raw, dict) else ""
        ).strip()
        if registration_id:
            current_registration_ids.add(registration_id)
    if not current_registration_ids:
        return []

    contacts = session.scalars(
        select(PublicRecordWatch)
        .where(PublicRecordWatch.source_system == "hpd_registration_contacts")
        .order_by(PublicRecordWatch.last_seen_at.desc().nullslast())
    ).all()
    owners: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for contact in contacts:
        if (
            not contact.visible_public
            or contact.needs_human_verification
            or contact.machine_verification_status == "official_conflict"
            or contact.source_presence_status == "not_seen"
            or not (contact.machine_verified_at or contact.human_verified_at)
        ):
            continue
        try:
            raw = json.loads(contact.raw_json or "{}")
        except json.JSONDecodeError:
            continue
        contact_type = re.sub(r"[^a-z]", "", str(raw.get("type") or "").casefold()) if isinstance(raw, dict) else ""
        if not isinstance(raw, dict) or contact_type != "corporateowner":
            continue
        name = str(raw.get("corporationname") or "").strip()
        registration_id = str(raw.get("registrationid") or "").strip()
        if not name or registration_id not in current_registration_ids:
            continue
        key = (name.casefold(), registration_id)
        if key in seen:
            continue
        seen.add(key)
        owners.append(
            {
                "organization": name,
                "role": "Corporate Owner",
                "registration_id": registration_id,
                "last_seen_at": normalize_timestamp(contact.last_seen_at),
                "source_url": contact.source_url,
                "verified_by": contact.machine_verified_by or contact.human_verified_by,
            }
        )
    return owners


def project_state(session) -> dict[str, Any]:
    project = session.scalar(select(CapitalProject).where(CapitalProject.building_key == BUILDING_KEY))
    if not project:
        project = ensure_default_project(session)
        session.flush()
    milestones = session.scalars(select(ProjectMilestone).where(ProjectMilestone.project_id == project.id)).all()
    imported_records = session.scalars(select(PublicRecordWatch).order_by(PublicRecordWatch.last_changed_at.desc().nullslast())).all()
    records = [row for row in imported_records if public_record_is_tenant_trusted(row)]
    actions = session.scalars(
        select(WatchdogAction)
        .where(WatchdogAction.status.in_(VISIBLE_ACTION_STATUSES))
        .order_by(WatchdogAction.created_at.desc().nullslast())
    ).all()
    tenant_actions = [row for row in actions if action_is_tenant_visible(row)]
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
    filing = _latest_replacement_filing(records)
    project_payload = {**_project_payload(project), **_filing_progress(filing)}
    project_payload["official_status_checked_at"] = normalize_timestamp(filing.last_seen_at) if filing else None
    health = watchdog_source_health(session)
    return {
        "project": project_payload,
        "management_claims": {
            "project": project_payload,
            "milestones": [_milestone_payload(row) for row in milestones],
        },
        "official_records": [public_record_payload(row, related_records=imported_records) for row in records],
        "registered_owners": registered_owner_organizations(session),
        "monitoring": health,
        "verification": _verification_counts(imported_records),
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
        "actions": [action_payload(row) for row in tenant_actions],
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
    actions = [row for row in state["actions"] if row["status"] in VISIBLE_ACTION_STATUSES]
    unverified = [row for row in records if row["needs_human_verification"]]
    machine_verified = [row for row in records if row.get("machine_verified_at")]
    verified = [row for row in records if not row["needs_human_verification"]]
    tenant_incidents = state["tenant_reality"]["elevator_incidents"]
    next_action = sorted(actions, key=lambda row: ({"critical": 0, "yellow": 1, "watch": 2, "info": 3}.get(row["severity"], 4), row["due_at"] or ""))[0] if actions else None
    public_view = {row["topic"]: row for row in state.get("public_view", [])}
    permit_answer = (public_view.get("Full elevator replacement permit") or {}).get("answer")
    enforcement_items = [row for row in state.get("public_view", [])
                         if row["topic"].startswith(("Active official elevator violation", "Pending official elevator complaint"))]
    enforcement_update = " ".join(f"{row['answer']} {row['why_it_matters']}" for row in enforcement_items)
    electrical_update = " ".join(row["answer"] for row in state.get("public_view", [])
                                 if row["topic"].startswith("Related electrical permit:"))
    service_answer = (public_view.get("Actual elevator service reported by tenants") or {}).get("answer") or ""
    monitoring_warning = (public_view.get("Monitoring coverage") or {}).get("answer") or ""

    tenant_draft = (
        "Elevator watch update: "
        f"{permit_answer or 'The system is checking official DOB permit records automatically.'} "
        f"{electrical_update} {enforcement_update} {service_answer} {monitoring_warning} "
        "Report observed outages or degraded service. See ActionQueue for the current requests to management."
    )
    management_draft = (
        "Please confirm whether a DOB NOW elevator filing has been submitted for the full elevator replacement "
        "at 455 Ocean Parkway. If yes, please share the filing number, current status, expected start date, and "
        "required posting plan. If not, please share the expected filing date and what approvals, drawings, "
        "contracts, or equipment decisions remain before submission. Tenants are tracking management claims, "
        "official public records, and observed elevator service separately."
    )
    filing = _latest_replacement_filing(session.scalars(select(PublicRecordWatch)).all())
    if filing:
        phase = _filing_progress(filing)["phase"]
        if phase == "filing_status_unconfirmed":
            management_draft = (
                f"Previously verified DOB elevator application {filing.record_key}, last recorded as {filing.status or 'filed'}, "
                "was absent from the latest official-source refresh. Please confirm its current status and provide the "
                "current official filing or replacement reference. Absence from a refresh does not establish resolution or withdrawal."
            )
        elif phase == "official_signoff_recorded":
            management_draft = (
                f"DOB records official sign-off for elevator application {filing.record_key}. Please provide the final "
                "inspection/sign-off documents, confirm when each elevator returned to service, and identify any outstanding "
                "project or service issues. Official sign-off alone does not establish today's operation or safety."
            )
        elif phase in {"filing_inactive", "permit_expired"}:
            management_draft = (
                f"The latest known elevator application {filing.record_key} is "
                f"{'expired' if phase == 'permit_expired' else filing.status}. Please identify the current renewal, "
                "replacement or reactivated filing and provide its official approval/permit documents and revised car-by-car schedule."
            )
        else:
            management_draft = (
                f"DOB elevator application {filing.record_key} is listed as {filing.status or 'filed'}. "
                + ("Please provide the exact examiner objections or hold items, who is responsible for each correction, "
                   "and the correction/resubmission dates. "
                   if any(word in (filing.status or "").casefold() for word in ("objection", "incomplete", "hold"))
                   else "Please provide the remaining steps and target dates for the current filing status. ")
                + "Please provide a dated schedule for each elevator: equipment delivery, elevator permit issuance, shutdown, "
                  "return to service, inspection and sign-off, and confirm the sequence and accessibility arrangements."
            )
    for action in actions:
        if action.get("draft_message") and action["action_type"] in {"class_one_correction_evidence", "hearing_outcome_request"}:
            management_draft += " " + action["draft_message"]
    if electrical_update:
        management_draft += " Please explain the related electrical work schedule and how it connects to the elevator application and work sequence."

    return {
        "project_state": state,
        "tenant_update_draft": tenant_draft,
        "management_followup_draft": management_draft,
        "next_best_action": next_action,
        "official_record_counts": {
            "total": len(records),
            "verified": len(verified),
            "machine_verified": len(machine_verified),
            "needs_human_verification": state["verification"]["needs_human_verification"],
            "official_conflicts": state["verification"]["official_conflicts"],
            "total_imported_elevator_records": state["verification"]["total_imported_elevator_records"],
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
        tenant_actions_json=json.dumps([
            {key: action.get(key) for key in ("title", "action_type", "status")}
            for action in briefing["project_state"]["actions"]
        ], ensure_ascii=False),
        generated_at=now_iso(),
        used_llm=False,
    )
    session.add(digest)
    return digest
