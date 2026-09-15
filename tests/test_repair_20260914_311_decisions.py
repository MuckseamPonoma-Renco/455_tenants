import json
import stat

import pytest

from packages.db import (
    FilingJob, Incident, IncidentWitness, MessageDecision, RawMessage,
    ServiceRequestCase, get_session,
)
from scripts import repair_20260914_311_decisions as operator


MODELS = (RawMessage, MessageDecision, Incident, IncidentWitness, FilingJob, ServiceRequestCase)
MECHANIC_ID = "4bc722e77ecb5b7cbc5fd160cc182cf355a794bbfb4b14b8c71e41aab4c73c7e"


def _snapshot_tables(session):
    return {
        model.__tablename__: sorted(
            [operator.snapshot(row) for row in session.query(model).all()],
            key=lambda row: json.dumps(row, sort_keys=True),
        )
        for model in MODELS
    }


def _seed(session):
    incident = Incident(
        incident_id=operator.INCIDENT, category="elevator", asset=None,
        status="open", title="Elevator outage update", summary="Original evidence summary",
        report_count=3, witness_count=1, confidence=80, needs_review=False,
        start_ts="2026-09-14T18:20:00Z", start_ts_epoch=1789400400,
        last_ts_epoch=1789403460, updated_at="2026-09-14T19:11:00Z",
        proof_refs=json.dumps([entry[0] for entry in operator.TARGETS] + [MECHANIC_ID]),
    )
    unrelated = Incident(
        incident_id="already-filed-morning-event", category="elevator",
        asset="elevator_both", status="closed", title="Earlier elevator event",
        summary="Separate preserved event", report_count=3, witness_count=2,
    )
    session.add_all([incident, unrelated])
    session.flush()
    for index, (message_id, source_text, timestamp, asset) in enumerate(operator.TARGETS):
        session.add(RawMessage(
            message_id=message_id, chat_name="Fixture building chat", sender="Fixture tenant",
            sender_hash="fixture-witness", ts_iso=timestamp,
            ts_epoch=1789400400 + index * 1080, text=source_text, source="whatsapp_web",
        ))
        session.add(MessageDecision(
            message_id=message_id, incident_id=operator.INCIDENT,
            is_issue=True, category="elevator", event_type="status_update",
            confidence=78, needs_review=False, auto_file_candidate=False,
            chosen_source="rules_context", created_at=timestamp,
            rules_json=json.dumps({"is_issue": bool(index), "event_type": "outage" if index else None, "asset": asset}),
            llm_json=json.dumps({"fixture": "prior model evidence, preserved verbatim"}),
            final_json=json.dumps({"is_issue": True, "category": "elevator", "event_type": "status_update", "asset": asset, "confidence": 78}),
        ))
    session.add(RawMessage(
        message_id=MECHANIC_ID, sender_hash="fixture-witness", ts_iso="2026-09-14T19:11:00Z",
        text="mechanic is here", source="whatsapp_web",
    ))
    session.add(MessageDecision(
        message_id=MECHANIC_ID, incident_id=operator.INCIDENT, is_issue=True,
        category="elevator", event_type="status_update", chosen_source="rules_context",
        confidence=78, auto_file_candidate=False,
        final_json='{"event_type":"status_update","asset":null}',
    ))
    session.add(IncidentWitness(incident_id=operator.INCIDENT, sender_hash="fixture-witness"))
    job = FilingJob(
        dedupe_key="fixture-existing-morning-job", incident_id=unrelated.incident_id,
        state="submitted", attempts=1, payload_json='{"existing":"payload"}',
        notes="Original receipt evidence", completed_at="2026-09-14T11:42:56Z",
    )
    session.add(job)
    session.flush()
    session.add(ServiceRequestCase(
        service_request_number="311-12345678", incident_id=unrelated.incident_id,
        filing_job_id=job.job_id, source="portal_playwright", status="In Progress",
        raw_status_json='{"source":"nyc311_portal","status":"In Progress"}',
    ))
    session.commit()
    return incident


def _private_backup(tmp_path):
    directory = tmp_path / "private-evidence"
    directory.mkdir(mode=0o700)
    return directory / "before.json"


def test_dry_run_reports_exact_changes_without_mutation_or_backup(client, tmp_path):
    with get_session() as session:
        _seed(session)
        before = _snapshot_tables(session)
        backup = _private_backup(tmp_path)
        result = operator.repair(session, backup=backup)
        assert result == {"ok": True, "decision_updates": 2, "incident_updates": 1, "applied": False}
        assert _snapshot_tables(session) == before
        assert not backup.exists()


def test_apply_writes_exact_private_backup_and_preserves_original_evidence_and_jobs(client, tmp_path):
    with get_session() as session:
        _seed(session)
        before = _snapshot_tables(session)
        backup = _private_backup(tmp_path)
        result = operator.repair(session, apply=True, backup=backup)
        session.commit()
        assert result["applied"] is True
        assert result["decision_updates"] == 2
        assert stat.S_IMODE(backup.stat().st_mode) == 0o600
        saved = json.loads(backup.read_text())
        assert saved["repair"] == operator.REPAIR
        assert set(saved["tables"]) == {model.__tablename__ for model in MODELS}
        for table, rows in before.items():
            assert sorted(saved["tables"][table], key=lambda row: json.dumps(row, sort_keys=True)) == rows

        after = _snapshot_tables(session)
        for model in (RawMessage, IncidentWitness, FilingJob, ServiceRequestCase):
            assert after[model.__tablename__] == before[model.__tablename__]
        originals = {row["message_id"]: row for row in before[MessageDecision.__tablename__]}
        for message_id, _, _, asset in operator.TARGETS:
            decision = session.get(MessageDecision, message_id)
            final = json.loads(decision.final_json)
            assert decision.event_type == "outage"
            assert decision.auto_file_candidate is True
            assert final["event_type"] == "outage"
            assert final["asset"] == asset
            assert final["close_incident"] is False
            assert final["repair_311_context"]["prior_decision"] == originals[message_id]
            assert final["repair_311_context"]["id"] == operator.REPAIR
            current = operator.snapshot(decision)
            for key, value in originals[message_id].items():
                if key not in {"event_type", "auto_file_candidate", "final_json"}:
                    assert current[key] == value
        assert operator.snapshot(session.get(MessageDecision, MECHANIC_ID)) == originals[MECHANIC_ID]
        incident = session.get(Incident, operator.INCIDENT)
        original_incident = next(row for row in before[Incident.__tablename__] if row["incident_id"] == operator.INCIDENT)
        assert incident.asset == "elevator_south"
        assert incident.title == "South elevator outage"
        assert incident.status == "open"
        for key in ("report_count", "witness_count", "proof_refs", "start_ts", "end_ts", "last_ts_epoch", "confidence", "needs_review"):
            assert getattr(incident, key) == original_incident[key]


def test_second_apply_is_idempotent_without_creating_another_backup(client, tmp_path):
    with get_session() as session:
        _seed(session)
        operator.repair(session, apply=True, backup=_private_backup(tmp_path))
        session.commit()
        after_first = _snapshot_tables(session)
        result = operator.repair(session, apply=True)
        assert result == {"ok": True, "decision_updates": 0, "applied": False}
        assert _snapshot_tables(session) == after_first


@pytest.mark.parametrize("field,value", [("text", "changed source text"), ("ts_iso", "2026-09-14T18:21:00Z")])
def test_changed_source_rejected_before_backup_or_mutation(client, tmp_path, field, value):
    with get_session() as session:
        _seed(session)
        raw = session.get(RawMessage, operator.TARGETS[0][0])
        setattr(raw, field, value)
        session.commit()
        before = _snapshot_tables(session)
        backup = _private_backup(tmp_path)
        with pytest.raises(ValueError, match="Source identity changed"):
            operator.repair(session, apply=True, backup=backup)
        assert _snapshot_tables(session) == before
        assert not backup.exists()


@pytest.mark.parametrize("field,value", [("status", "closed"), ("asset", "elevator_north"), ("report_count", 4)])
def test_changed_incident_rejected_before_backup_or_mutation(client, tmp_path, field, value):
    with get_session() as session:
        incident = _seed(session)
        setattr(incident, field, value)
        session.commit()
        before = _snapshot_tables(session)
        backup = _private_backup(tmp_path)
        with pytest.raises(ValueError, match="Incident changed"):
            operator.repair(session, apply=True, backup=backup)
        assert _snapshot_tables(session) == before
        assert not backup.exists()


def test_apply_without_backup_rejected_without_mutation(client):
    with get_session() as session:
        _seed(session)
        before = _snapshot_tables(session)
        with pytest.raises(ValueError, match="Private backup path required"):
            operator.repair(session, apply=True)
        assert _snapshot_tables(session) == before


def test_apply_refuses_public_backup_directory(client, tmp_path):
    directory = tmp_path / "unsafe"
    directory.mkdir(mode=0o755)
    directory.chmod(0o755)
    with get_session() as session:
        _seed(session)
        before = _snapshot_tables(session)
        with pytest.raises(ValueError, match="Backup directory must be private"):
            operator.repair(session, apply=True, backup=directory / "before.json")
        assert _snapshot_tables(session) == before
        assert not (directory / "before.json").exists()


@pytest.mark.parametrize("kind", ["job", "receipt"])
def test_existing_target_filing_or_receipt_requires_reconciliation(client, tmp_path, kind):
    with get_session() as session:
        _seed(session)
        if kind == "job":
            session.add(FilingJob(dedupe_key="target-job", incident_id=operator.INCIDENT, state="pending"))
        else:
            session.add(ServiceRequestCase(service_request_number="311-87654321", incident_id=operator.INCIDENT, status="submitted"))
        session.commit()
        before = _snapshot_tables(session)
        backup = _private_backup(tmp_path)
        with pytest.raises(ValueError, match="existing filing/receipt"):
            operator.repair(session, apply=True, backup=backup)
        assert _snapshot_tables(session) == before
        assert not backup.exists()


@pytest.mark.parametrize("field,value", [("event_type", "outage"), ("chosen_source", "new-review"), ("is_issue", False)])
def test_changed_decision_rejected_before_backup_or_mutation(client, tmp_path, field, value):
    with get_session() as session:
        _seed(session)
        decision = session.get(MessageDecision, operator.TARGETS[0][0])
        setattr(decision, field, value)
        session.commit()
        before = _snapshot_tables(session)
        backup = _private_backup(tmp_path)
        with pytest.raises(ValueError, match="Decision starting state changed"):
            operator.repair(session, apply=True, backup=backup)
        assert _snapshot_tables(session) == before
        assert not backup.exists()
