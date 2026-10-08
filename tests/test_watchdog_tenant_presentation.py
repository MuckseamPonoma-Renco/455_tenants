import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from packages.db import Base, PublicRecordSourceState, PublicRecordWatch, WatchdogAction, WeeklyDigest
from packages.project_watch.rules import evaluate_project_rules
from packages.project_watch import rules as project_rules
from packages.public_records.config import source_configs
from packages.public_records.sync import (
    generate_weekly_digest,
    project_briefing,
    public_elevator_watch_items,
    upsert_public_record,
)
from packages.public_records.verification import apply_machine_verification
from packages.sheets import sync as sheets_sync


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def seed_current_sources(session):
    now = datetime.now(timezone.utc).isoformat()
    for source in source_configs():
        session.add(PublicRecordSourceState(
            source_key=source.key, state="ready", last_attempt_at=now,
            last_success_at=now, row_count=1, query_count=1, source_errors=0,
        ))
    session.flush()


def filing(session, status="Objections", **overrides):
    raw = {
        "job_filing_number": "B01422304-I1", "filing_date": "2026-08-21",
        "filing_status": status, "bin": "3126839", "bbl": "3053900074",
        "house_number": "449", "street_name": "OCEAN PARKWAY",
        "descriptionofwork": "Modernization of Two (2) Passenger Elevators.",
        "filingstatus_or_filingincludes": "Alteration/Replacement", **overrides,
    }
    return upsert_public_record(session, "dob_now_elevator_applications", raw)[0]


def summons(session, number, severity, hearing, issue="20260826"):
    return upsert_public_record(session, "dob_ecb_violations", {
        "ecb_violation_number": number, "ecb_violation_status": "ACTIVE",
        "bin": "3126839", "boro": "3", "block": "05390", "lot": "0074",
        "issue_date": issue, "severity": severity, "violation_type": "Elevators",
        "violation_description": "CEASE USE ITEM: INOPERATIVE ZONE LOCK RESTRICTOR" if severity == "CLASS - 1" else "WORN ELEVATOR DOOR GIBS",
        "hearing_date": hearing, "hearing_status": "PENDING",
    })[0]


def test_verified_aug21_objection_drives_current_phase_and_tenant_request(session):
    seed_current_sources(session)
    record = filing(session)
    evaluate_project_rules(session)
    briefing = project_briefing(session)
    project = briefing["project_state"]["project"]
    assert project["phase"] == "objections_pending"
    assert "B01422304-I1" in project["current_milestone"]
    assert "resubmission" in project["current_bottleneck"]
    action = next(row for row in briefing["project_state"]["actions"] if row["action_type"] == "objection_or_hold")
    assert action["owner_role"] == "tenant_association"
    assert action["source_record_id"] == record.id
    assert "exact examiner objections" in briefing["management_followup_draft"]
    assert "whether a DOB" not in briefing["management_followup_draft"]
    view = next(row for row in briefing["project_state"]["public_view"] if row["topic"] == "Full elevator replacement permit")
    assert "449 OCEAN PARKWAY" in view["why_it_matters"]
    assert "3126839" in view["why_it_matters"] and "3053900074" in view["why_it_matters"]
    assert "not an issued permit yet" in view["answer"]
    assert "correction/resubmission" in view["human_needed"]


def test_related_electrical_permit_does_not_advance_elevator_permit(session):
    seed_current_sources(session)
    filing(session)
    upsert_public_record(session, "dob_now_electrical_applications", {
        "job_filing_number": "B01422331-I1-EL", "filing_date": "2026-06-25",
        "filing_status": "Permit Issued", "permit_issued_date": "2026-06-25",
        "job_status": "Job in Process", "job_start_date": "2026-09-15",
        "bin": "3126839", "gis_bbl": "3053900074",
        "job_description": "Controls and brakes related to the following elevators: city IDs 3P6189 and 3P6190.",
    })
    evaluate_project_rules(session)
    briefing = project_briefing(session)
    assert briefing["project_state"]["project"]["phase"] == "objections_pending"
    electrical = next(row for row in briefing["project_state"]["public_view"] if row["topic"].startswith("Related electrical permit"))
    assert "2026-06-25" in electrical["answer"]
    assert "Applicant-entered start: 2026-09-15" in electrical["answer"]
    assert "does not establish elevator-modernization approval" in electrical["why_it_matters"]
    assert not session.scalar(select(WatchdogAction).where(WatchdogAction.action_type == "permit_issued"))


def test_all_active_summonses_and_complaints_remain_visible(session, monkeypatch):
    class MeetingWeek(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 9, 6, 14, tzinfo=timezone.utc)

    monkeypatch.setattr(project_rules, "datetime", MeetingWeek)
    seed_current_sources(session)
    filing(session)
    summons(session, "39201971R", "CLASS - 1", "20261104")
    summons(session, "39194290Z", "CLASS - 2", "20260909", issue="20260630")
    for number, date in (("3A78096", "2026-08-27"), ("3A78243", "2026-08-28")):
        upsert_public_record(session, "dob_complaints", {
            "complaint_number": number, "status": "ACTIVE", "date_entered": date,
            "bin": "3126839", "unit": "ELEVR", "house_number": "455", "house_street": "OCEAN PARKWAY",
        })
    evaluate_project_rules(session)
    briefing = project_briefing(session)
    rendered = json.dumps(briefing["project_state"]["public_view"])
    for number in ("39201971R", "39194290Z", "3A78096", "3A78243"):
        assert number in rendered
        assert number in briefing["tenant_update_draft"]
    assert "CLASS - 1" in rendered and "CLASS - 2" in rendered
    assert "2026-09-09" in rendered and "2026-11-04" in rendered
    assert "No certification status is shown" in rendered
    assert "A complaint is an allegation" in rendered
    assert "does not establish today's physical condition" in rendered
    assert "affected elevator" in rendered
    assert "39201971R" in briefing["management_followup_draft"]
    hearing_action = next(row for row in briefing["project_state"]["actions"] if row["action_type"] == "hearing_outcome_request")
    assert "2026-09-09" in hearing_action["title"]
    assert hearing_action["due_at"].startswith("2026-09-10")


def test_newer_conflicting_filing_does_not_displace_verified_building_filing(session):
    seed_current_sources(session)
    filing(session)
    filing(session, "Permit Issued", job_filing_number="WRONG-BUILDING", bin="9999999", bbl="9999999999", filing_date="2026-09-06", permit_entire_date="2026-09-06")
    evaluate_project_rules(session)
    briefing = project_briefing(session)
    assert briefing["project_state"]["project"]["phase"] == "objections_pending"
    assert "WRONG-BUILDING" not in json.dumps(briefing["project_state"]["public_view"])
    assert briefing["official_record_counts"]["official_conflicts"] == 1


def test_older_issued_filing_does_not_override_latest_objections(session):
    seed_current_sources(session)
    filing(session, "Permit Issued", job_filing_number="OLDER-I1", filing_date="2026-06-01", permit_entire_date="2026-06-02")
    filing(session)
    evaluate_project_rules(session)
    briefing = project_briefing(session)
    assert briefing["project_state"]["project"]["phase"] == "objections_pending"
    lobby = next(row for row in briefing["project_state"]["public_view"] if row["topic"] == "Lobby posting / start-date notice")
    assert "No hallway check is needed yet" in lobby["answer"]
    assert not any(row["action_type"] == "permit_issued" for row in briefing["project_state"]["actions"])


def test_filing_freshness_cannot_be_renewed_by_unrelated_fresh_device_row(session):
    seed_current_sources(session)
    record = filing(session)
    old = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
    record.last_seen_at = old
    state = session.scalar(select(PublicRecordSourceState).where(PublicRecordSourceState.source_key == "dob_now_elevator_applications"))
    state.last_success_at = old
    upsert_public_record(session, "dob_now_elevator_safety_compliance", {
        "device_number": "3P6189", "device_status": "Active", "bin": "3126839", "bbl": "3053900074",
        "periodic_latest_inspection": "2026-03-11",
    })
    apply_machine_verification(session)
    view = public_elevator_watch_items(session)
    permit = next(row for row in view if row["topic"] == "Full elevator replacement permit")
    assert permit["last_checked_at"][:19] == old[:19]
    warning = next(row for row in view if row["topic"] == "Monitoring coverage")
    assert "dob_now_elevator_applications: stale" in warning["why_it_matters"]
    device = next(row for row in view if row["topic"] == "Elevators listed by DOB")
    assert "does not establish today's operation, safety" in device["why_it_matters"]


def test_objection_action_retires_on_status_change_but_absence_is_not_resolution(session):
    record = filing(session)
    evaluate_project_rules(session)
    action = session.scalar(select(WatchdogAction).where(WatchdogAction.action_type == "objection_or_hold"))
    record.source_presence_status = "not_seen"
    evaluate_project_rules(session)
    assert action.status == "pending" and action.owner_role == "operator"
    assert "does not establish correction or resolution" in action.detail
    assert not any(row["action_type"] == "no_public_filing_after_30_days" for row in project_briefing(session)["project_state"]["actions"])
    record.source_presence_status = "present"
    filing(session, "Approved")
    evaluate_project_rules(session)
    assert action.status == "completed"
    assert project_briefing(session)["project_state"]["project"]["phase"] == "approved_awaiting_permit"


def test_weekly_action_snapshot_is_not_replaced_with_todays_actions(session, monkeypatch):
    filing(session)
    evaluate_project_rules(session)
    digest = generate_weekly_digest(session)
    session.flush()
    saved_actions = json.loads(digest.tenant_actions_json)
    saved_title = saved_actions[0]["title"]
    session.add(WeeklyDigest(generated_at="2025-01-01", tenant_update_draft="Legacy summary"))
    for action in session.scalars(select(WatchdogAction)).all():
        action.status = "completed"
    session.add(WatchdogAction(action_type="today", title="A different action today", status="open", owner_role="resident", severity="info"))
    session.flush()

    @contextmanager
    def fake_session():
        yield session

    captured = []
    monkeypatch.setattr(sheets_sync, "get_session", fake_session)
    monkeypatch.setattr(sheets_sync, "_service", lambda: None)
    monkeypatch.setattr(sheets_sync, "_watchdog_sheet_id", lambda: "test")
    monkeypatch.setattr(sheets_sync, "_ensure_tab_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(sheets_sync, "_apply_tab_layout", lambda *args, **kwargs: None)
    monkeypatch.setattr(sheets_sync, "_replace_tab_values", lambda svc, sheet, tab, values, **kwargs: captured.extend(values))
    sheets_sync.sync_weekly_digest_to_sheets()
    assert captured[1][4] == saved_title
    assert captured[2][4] == "Historical action snapshot unavailable; see current ActionQueue."
    assert "A different action today" not in json.dumps(captured)
