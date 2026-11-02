import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from packages.db import Base, WatchdogAction, PublicRecordSourceState
from packages.project_watch.rules import evaluate_project_rules
from packages.public_records.sync import _filing_check_time, project_briefing, upsert_public_record
from packages.public_records.source_state import record_source_results, watchdog_source_health


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _filing(session, key, date, status, **overrides):
    raw = {
        "job_filing_number": key, "filing_date": date, "filing_status": status,
        "bin": "3126839", "bbl": "3053900074", "house_number": "449",
        "street_name": "OCEAN PARKWAY", "descriptionofwork": "Modernization of two passenger elevators",
        "filingstatus_or_filingincludes": "Alteration/Replacement", **overrides,
    }
    return upsert_public_record(session, "dob_now_elevator_applications", raw)[0]


@pytest.mark.parametrize("status,overrides,expected_phase,expected_phrase", [
    ("Signed Off", {"signedoff_date": "2026-09-01"}, "official_signoff_recorded", "final inspection/sign-off documents"),
    ("Permit Issued", {"signedoff_date": "2026-09-01"}, "official_signoff_recorded", "final inspection/sign-off documents"),
    ("Withdrawn", {}, "filing_inactive", "replacement or reactivated filing"),
    ("Cancelled", {}, "filing_inactive", "replacement or reactivated filing"),
    ("Revoked", {}, "filing_inactive", "replacement or reactivated filing"),
    ("Expired", {}, "permit_expired", "current renewal"),
    ("Permit Issued", {"permit_expiration_date": "2026-09-01"}, "permit_expired", "current renewal"),
])
def test_terminal_latest_filing_preserves_history_and_does_not_restore_older_permit(session, status, overrides, expected_phase, expected_phrase):
    _filing(session, "OLDER-I1", "2026-06-01", "Permit Issued", permit_entire_date="2026-06-02")
    evaluate_project_rules(session)
    old_action = session.scalar(select(WatchdogAction).where(WatchdogAction.action_type == "permit_issued"))
    assert old_action is not None

    _filing(session, "LATEST-I1", "2026-08-21", status, **overrides)
    evaluate_project_rules(session)
    briefing = project_briefing(session)
    project = briefing["project_state"]["project"]
    assert project["phase"] == expected_phase
    assert "LATEST-I1" in project["current_milestone"]
    assert "Awaiting verified elevator application" not in project["current_milestone"]
    assert "whether a DOB" not in briefing["management_followup_draft"]
    assert expected_phrase in briefing["management_followup_draft"]
    assert old_action.status == "completed"
    assert not any(action["action_type"] in {"permit_issued", "no_public_filing_after_30_days"} for action in briefing["project_state"]["actions"])
    headline = next(item for item in briefing["project_state"]["public_view"] if item["topic"] == "Full elevator replacement permit")
    assert "LATEST-I1" in headline["answer"]
    assert "OLDER-I1" not in headline["answer"]
    assert "No DOB elevator replacement permit has been found" not in headline["answer"]
    if expected_phase == "official_signoff_recorded":
        assert "does not establish current operation or safety" in headline["why_it_matters"]


def test_absent_latest_filing_stays_known_with_unconfirmed_status(session):
    _filing(session, "OLDER-I1", "2026-06-01", "Permit Issued", permit_entire_date="2026-06-02")
    latest = _filing(session, "LATEST-I1", "2026-08-21", "Objections")
    evaluate_project_rules(session)
    latest.source_presence_status = "not_seen"
    evaluate_project_rules(session)
    briefing = project_briefing(session)
    project = briefing["project_state"]["project"]
    assert project["phase"] == "filing_status_unconfirmed"
    assert "LATEST-I1" in project["current_milestone"]
    assert "last recorded: Objections" in project["current_milestone"]
    draft = briefing["management_followup_draft"]
    assert "Previously verified DOB elevator application LATEST-I1" in draft
    assert "whether a DOB" not in draft
    assert "does not establish resolution or withdrawal" in draft
    assert not any(action["action_type"] in {"permit_issued", "no_public_filing_after_30_days"} for action in briefing["project_state"]["actions"])
    headline = next(item for item in briefing["project_state"]["public_view"] if item["topic"] == "Full elevator replacement permit")
    assert "current status unconfirmed" in headline["answer"]
    assert "OLDER-I1" not in headline["answer"]


def test_completed_absence_check_passes_sheet_audit_without_renewing_filing_facts(session):
    from datetime import datetime, timedelta, timezone
    from packages.public_records.sync import public_elevator_watch_items
    from packages.public_records.source_state import record_source_results
    from packages.public_records.config import source_configs
    from scripts import audit_public_watchdog_tabs as audit
    from packages.public_records.verification import apply_machine_verification

    record = _filing(session, "LATEST-I1", "2026-08-21", "Objections")
    apply_machine_verification(session)
    old = (datetime.now(timezone.utc) - timedelta(days=2)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    record.last_seen_at = old
    health = record_source_results(session, {
        source.key: {"query_count": 1, "source_errors": 0, "complete": True}
        for source in source_configs()
    }, set())
    assert health["ok"] is True
    view = {item["topic"]: item for item in public_elevator_watch_items(session)}
    topics = ("Full elevator replacement permit", "Lobby posting / start-date notice")
    for topic in topics:
        assert view[topic]["last_checked_at"] != old
        assert "unconfirmed" in view[topic]["answer"]
        assert old in view[topic]["why_it_matters"]
    assert record.last_seen_at == old
    assert record.status == "Objections"
    assert record.source_presence_status == "not_seen"
    assert project_briefing(session)["project_state"]["project"]["official_status_checked_at"] == view[topics[0]]["last_checked_at"]

    spec = next(spec for spec in audit._resolved_tab_specs() if spec.logical_name == "ElevatorWatch")
    values = [list(spec.headers)] + [
        [view[topic].get(key) or "" for key in
         ("topic", "answer", "why_it_matters", "checked_by", "last_checked_at", "human_needed", "source_url")]
        for topic in (*topics, audit.SYSTEM_WATCHDOG_FRESHNESS_TOPIC)
    ]
    result = audit._audit_tab(
        spec, audit.ExpectedTab(values, "USER_ENTERED"), audit.LiveTab(values, values),
        metadata={}, workbook_tz=timezone.utc, now=datetime.now(timezone.utc),
        max_age_seconds=90 * 60, max_drift_seconds=90 * 60, limit=10,
    )
    assert result["ok"] is True
    assert all(check["freshness_required"] for check in result["volatile_last_checked"])



@pytest.mark.parametrize("checked,succeeded,expected", [
    (None, "2026-10-12T12:00:00Z", ""),
    ("bad timestamp", "2026-10-12T12:00:00Z", ""),
    ("2026-10-12T12:00:00Z", None, ""),
    ("2026-10-12T12:00:00Z", "bad timestamp", ""),
    ("2026-10-11T12:00:00Z", "2026-10-12T12:00:00Z", "2026-10-11T12:00:00Z"),
    ("2026-10-12T12:00:00Z", "2026-10-11T12:00:00Z", "2026-10-11T12:00:00Z"),
])
def test_absent_filing_requires_both_valid_receipts_and_uses_older_time(session, checked, succeeded, expected):
    record = _filing(session, "LATEST-I1", "2026-08-21", "Objections")
    record.source_presence_status = "not_seen"
    record.source_checked_at = checked
    health = {"sources": [
        {"source_key": record.source_system, "last_success_at": succeeded},
        {"source_key": "unrelated_source", "last_success_at": "2026-10-12T13:00:00Z"},
    ]}
    assert _filing_check_time(record, health) == expected
    health["sources"].pop(0)
    assert _filing_check_time(record, health) == ""


def test_failed_source_retry_does_not_renew_absence_check(session):
    record = _filing(session, "LATEST-I1", "2026-08-21", "Objections")
    old = "2026-10-10T12:00:00Z"
    record.source_presence_status = "not_seen"
    record.source_checked_at = old
    session.add(PublicRecordSourceState(source_key=record.source_system, state="empty", last_success_at=old))
    session.flush()
    record_source_results(session, {
        record.source_system: {"query_count": 1, "source_errors": 1, "complete": False},
    }, set())
    assert record.source_checked_at == old
    assert _filing_check_time(record, watchdog_source_health(session)) == old


def test_present_filing_still_requires_recent_positive_sighting(session):
    record = _filing(session, "LATEST-I1", "2026-08-21", "Objections")
    record.source_presence_status = "present"
    record.last_seen_at = "2026-10-10T12:00:00Z"
    record.source_checked_at = "2026-10-12T12:00:00Z"
    health = {"sources": [{"source_key": record.source_system, "last_success_at": record.source_checked_at}]}
    assert _filing_check_time(record, health) == record.last_seen_at
