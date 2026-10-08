import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from packages.db import Base, WatchdogAction
from packages.project_watch.rules import evaluate_project_rules
from packages.public_records.sync import project_briefing, upsert_public_record


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
