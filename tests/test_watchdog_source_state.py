from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from packages.db import Base, PublicRecordSourceState, PublicRecordWatch
from packages.public_records.config import source_configs
from packages.public_records.source_state import record_source_results, watchdog_source_health
from packages.public_records.sync import PublicRecordFetchBatch, _retire_unseen_registration_contacts, registered_owner_organizations
from packages import worker_jobs


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def complete_results():
    return {source.key: {"query_count": 1, "source_errors": 0, "complete": True} for source in source_configs()}


def complete_source_health():
    return {"ok": True, "sources": [
        {"source_key": source.key, "state": "ready", "source_errors": 0,
         "last_success_at": datetime.now(timezone.utc).isoformat()}
        for source in source_configs()
    ]}


def test_successful_empty_query_preserves_history_without_claiming_correction(session):
    record = PublicRecordWatch(source_system="hpd_violations", record_type="hpd_violation", record_key="old", status="Open")
    session.add(record)
    session.flush()
    result = record_source_results(session, complete_results(), set())
    assert result["ok"] is True
    assert record.source_presence_status == "not_seen"
    assert record.status == "Open"
    assert session.get(PublicRecordWatch, record.id) is record


def test_partial_query_preserves_prior_success_and_presence(session):
    record = PublicRecordWatch(source_system="dob_complaints", record_type="dob_complaint", record_key="old", status="ACTIVE", source_presence_status="present", source_checked_at="2026-08-01T00:00:00Z")
    state = PublicRecordSourceState(source_key="dob_complaints", state="ready", last_success_at="2026-08-01T00:00:00Z")
    session.add_all([record, state])
    session.flush()
    results = complete_results()
    results["dob_complaints"] = {"query_count": 2, "source_errors": 1, "complete": False, "errors": ["timeout"]}
    health = record_source_results(session, results, set())
    assert health["ok"] is False
    assert state.last_success_at == "2026-08-01T00:00:00Z"
    assert record.source_presence_status == "present"
    assert record.source_checked_at == "2026-08-01T00:00:00Z"


def test_missing_source_receipts_are_degraded_and_never_last_success(session):
    health = record_source_results(session, {}, set())
    assert health["ok"] is False
    assert health["source_errors"] == len(source_configs())
    assert health["last_success_at"] is None


def test_source_health_expires_on_same_contract(session):
    record_source_results(session, complete_results(), set())
    health = watchdog_source_health(session, now=datetime.now(timezone.utc) + timedelta(days=1))
    assert health["ok"] is False
    assert all(source["state"] == "stale" for source in health["sources"])


def test_empty_contact_export_does_not_retire_every_contact(session):
    contact = PublicRecordWatch(source_system="hpd_registration_contacts", record_type="hpd_contact", record_key="1", status="current", visible_public=True, raw_json='{"registrationid":"123"}')
    session.add(contact)
    session.flush()
    batch = PublicRecordFetchBatch()
    batch.source_results = {"hpd_registration_contacts": {"complete": True}}
    batch.contact_registration_ids = {"123"}
    batch.contact_keys = {"123": set()}
    assert _retire_unseen_registration_contacts(session, batch) == 0
    assert contact.visible_public is True
    assert contact.status == "current"


@pytest.mark.parametrize("source_ok,sheet_ok,expected", [(True, True, True), (False, True, False), (True, False, False)])
def test_worker_requires_complete_sources_and_sheet_readback(monkeypatch, source_ok, sheet_ok, expected):
    from scripts import audit_public_watchdog_tabs
    monkeypatch.setattr(worker_jobs, "sync_replacement_watchdog_to_sheets", lambda: None)
    monkeypatch.setattr(worker_jobs, "append_audit_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(audit_public_watchdog_tabs, "run_audit", lambda **kwargs: {"ok": sheet_ok})
    source_health = complete_source_health()
    source_health["ok"] = source_ok
    result = worker_jobs._complete_watchdog_sync({"source_errors": 0 if source_ok else 1, "source_health": source_health})
    assert result["ok"] is expected
    assert result["sheet_errors"] == (0 if sheet_ok else 1)


def test_worker_does_not_swallow_sheet_write_failure(monkeypatch):
    def fail():
        raise RuntimeError("write failed")
    monkeypatch.setattr(worker_jobs, "sync_replacement_watchdog_to_sheets", fail)
    monkeypatch.setattr(worker_jobs, "append_audit_event", lambda *args, **kwargs: None)
    result = worker_jobs._complete_watchdog_sync({"source_errors": 0, "source_health": complete_source_health()})
    assert result["ok"] is False
    assert result["sheet_readback_ok"] is False
    assert result["sheet_errors"] == 1


def test_worker_rejects_success_summary_without_source_receipts(monkeypatch):
    from scripts import audit_public_watchdog_tabs
    monkeypatch.setattr(worker_jobs, "sync_replacement_watchdog_to_sheets", lambda: None)
    monkeypatch.setattr(audit_public_watchdog_tabs, "run_audit", lambda **kwargs: {"ok": True})
    result = worker_jobs._complete_watchdog_sync({"source_errors": 0, "source_health": {"ok": True}})
    assert result["ok"] is False


def _owner_rows(session):
    building = PublicRecordWatch(source_system="hpd_building", record_type="hpd_building", record_key="building",
        machine_verification_status="official_building_match", source_presence_status="present", raw_json='{"registrationid":"373786"}')
    contact = PublicRecordWatch(source_system="hpd_registration_contacts", record_type="hpd_contact", record_key="owner",
        visible_public=True, needs_human_verification=False, machine_verified_at="2026-09-06T09:00:00Z",
        machine_verification_status="official_building_match", source_presence_status="present",
        raw_json='{"registrationid":"373786","type":"CorporateOwner","corporationname":"VERIFIED OWNER LLC"}')
    session.add_all([building, contact])
    session.flush()
    assert len(registered_owner_organizations(session)) == 1
    return building, contact


@pytest.mark.parametrize("field,value", [("machine_verification_status", "official_conflict"), ("source_presence_status", "not_seen")])
def test_current_owner_excludes_conflicting_or_absent_contact_without_deleting_history(session, field, value):
    building, contact = _owner_rows(session)
    setattr(contact, field, value)
    assert registered_owner_organizations(session) == []
    assert session.get(PublicRecordWatch, contact.id) is contact
    assert session.get(PublicRecordWatch, building.id) is building


@pytest.mark.parametrize("field,value", [("machine_verification_status", "official_conflict"), ("source_presence_status", "not_seen")])
def test_current_owner_excludes_conflicting_or_absent_building_registration(session, field, value):
    building, contact = _owner_rows(session)
    setattr(building, field, value)
    assert registered_owner_organizations(session) == []
    assert session.get(PublicRecordWatch, contact.id) is contact
