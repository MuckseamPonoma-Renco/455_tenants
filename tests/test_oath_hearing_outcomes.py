import pytest
from contextlib import contextmanager
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from packages.db import Base, PublicRecordWatch, WatchdogAction
from packages.project_watch.rules import evaluate_project_rules
from packages.public_records.config import source_configs
from packages.public_records.hearings import current_oath_hearing_outcome
from packages.public_records.normalize import normalize_record
from packages.public_records.sync import public_elevator_watch_items, public_record_payload, upsert_public_record
from packages.public_records.verification import apply_machine_verification
from packages.sheets import sync as sheets_sync


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def summons(session, hearing_date="20250905"):
    record = upsert_public_record(session, "dob_ecb_violations", {
        "ecb_violation_number": "39194290Z", "ecb_violation_status": "ACTIVE",
        "bin": "3126839", "boro": "3", "block": "05390", "lot": "0074",
        "issue_date": "20250901", "severity": "CLASS - 1", "violation_type": "Elevators",
        "violation_description": "ELEVATOR DOOR GIBS", "hearing_date": hearing_date,
        "hearing_status": "PENDING",
    })[0]
    record.source_presence_status = "present"
    session.flush()
    return record


def oath(session, **overrides):
    raw = {
        "ticket_number": "039194290Z", "issuing_agency": "DEPT. OF BUILDINGS",
        "violation_location_borough": "BROOKLYN", "violation_location_block_no": "05390",
        "violation_location_lot_no": "0074", "violation_location_house": "449",
        "violation_location_street_name": "OCEAN PARKWAY", "violation_date": "2025-09-01",
        "violation_details": "ELEVATOR DOOR GIBS", "hearing_date": "2025-09-05",
        "hearing_result": "IN VIOLATION", "decision_date": "2025-09-05", **overrides,
    }
    record = upsert_public_record(session, "oath_hearings", raw)[0]
    record.source_presence_status = "present"
    session.flush()
    return record


def test_oath_result_and_date_are_normalized_without_claiming_repair():
    source = next(source for source in source_configs() if source.key == "oath_hearings")
    normalized = normalize_record(source, {"ticket_number": "039194290Z", "hearing_result": "DISMISSED", "decision_date": "2025-09-05"})
    assert normalized["status"] == "DISMISSED"
    assert "Decision date: 2025-09-05" in normalized["status_detail"]
    assert "does not establish physical correction" in normalized["status_detail"]


def test_current_normalized_oath_decision_completes_hearing_request_and_surfaces_in_public_views(session):
    record = summons(session)
    evaluate_project_rules(session)
    hearing_action = session.scalar(select(WatchdogAction).where(WatchdogAction.action_type == "hearing_outcome_request"))
    assert hearing_action.status == "open"
    decision = oath(session)
    evaluate_project_rules(session)
    assert hearing_action.status == "completed"
    assert "IN VIOLATION" in hearing_action.detail
    correction = session.scalar(select(WatchdogAction).where(WatchdogAction.action_type == "class_one_correction_evidence"))
    assert correction.status == "open"
    payload = public_record_payload(record)
    assert payload["hearing_status"] == "PENDING"  # Preserve the ECB source's separate field.
    assert payload["hearing_result"] == "IN VIOLATION"
    assert payload["hearing_decision_date"] == "2025-09-05"
    assert payload["hearing_outcome_source_url"] == decision.source_url
    assert "ECB hearing-status field may lag" in payload["public_context"]
    assert "does not establish physical correction" in payload["public_context"]
    item = next(item for item in public_elevator_watch_items(session) if "39194290Z" in item["answer"])
    assert "IN VIOLATION" in item["why_it_matters"]
    assert "2025-09-05" in item["why_it_matters"]
    assert "correction/certification evidence" in item["human_needed"]


@pytest.mark.parametrize("override", [
    {"bin": "9999999"},
    {"ticket_number": "039201971R"},
    {"hearing_result": "PENDING", "decision_date": "2025-09-05"},
    {"decision_date": None},
    {"decision_date": "2099-09-05"},
    {"hearing_date": "2025-08-01", "decision_date": "2025-08-01"},
    {"issuing_agency": "DEPARTMENT OF SANITATION"},
])
def test_unrelated_conflicting_pending_or_old_decision_cannot_satisfy_current_hearing(session, override):
    record = summons(session)
    oath(session, **override)
    evaluate_project_rules(session)
    assert session.scalar(select(WatchdogAction).where(WatchdogAction.action_type == "hearing_outcome_request")).status == "open"
    assert public_record_payload(record)["hearing_result"] is None


@pytest.mark.parametrize("presence", ["not_seen", None])
def test_absent_or_legacy_oath_observation_cannot_satisfy_current_hearing(session, presence):
    record = summons(session)
    decision = oath(session)
    decision.source_presence_status = presence
    evaluate_project_rules(session)
    assert current_oath_hearing_outcome(record, [decision]) is None
    assert session.scalar(select(WatchdogAction).where(WatchdogAction.action_type == "hearing_outcome_request")).status == "open"


def test_newer_pending_hearing_supersedes_historical_decision_for_same_normalized_ticket(session):
    record = summons(session)
    historical = oath(session)
    latest = oath(session, ticket_number="39194290Z", hearing_date="2025-09-10", hearing_result="PENDING", decision_date=None)
    evaluate_project_rules(session)
    assert current_oath_hearing_outcome(record, [historical, latest]) is None
    assert session.scalar(select(WatchdogAction).where(WatchdogAction.action_type == "hearing_outcome_request")).status == "open"
    assert "IN VIOLATION" not in public_record_payload(record)["public_context"]


def test_unverified_or_explicitly_conflicting_oath_observation_is_not_a_join_candidate(session):
    record = summons(session)
    decision = oath(session)
    apply_machine_verification(session)
    decision.needs_human_verification = True
    assert current_oath_hearing_outcome(record, [decision]) is None
    decision.needs_human_verification = False
    decision.machine_verification_status = "official_conflict"
    assert current_oath_hearing_outcome(record, [decision]) is None


def test_public_sheet_includes_joined_oath_outcome_after_records_detach(session, monkeypatch):
    summons(session)
    oath(session)
    apply_machine_verification(session)

    @contextmanager
    def detached_read():
        yield session
        session.expunge_all()

    captured = []
    monkeypatch.setattr(sheets_sync, "get_session", detached_read)
    monkeypatch.setattr(sheets_sync, "_service", lambda: None)
    monkeypatch.setattr(sheets_sync, "_watchdog_sheet_id", lambda: "test")
    monkeypatch.setattr(sheets_sync, "_ensure_tab_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(sheets_sync, "_apply_tab_layout", lambda *args, **kwargs: None)
    monkeypatch.setattr(sheets_sync, "_replace_tab_values", lambda svc, sheet, tab, values, **kwargs: captured.extend(values))
    sheets_sync.sync_public_records_to_sheets()
    ecb_row = next(row for row in captured[1:] if row[0] == "dob_ecb_violations")
    assert "OATH hearing result: IN VIOLATION" in ecb_row[7]
    assert "decision date: 2025-09-05" in ecb_row[7]
    assert "does not establish physical correction" in ecb_row[7]
