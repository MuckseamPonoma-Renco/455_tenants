import json
import os
from types import SimpleNamespace
from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from packages.db import FilingJob, Incident, MessageDecision, RawMessage, get_session
from packages.nyc311 import cloud_filing, portal_worker
from packages.nyc311.planner import claim_next_job, ensure_filing_job_for_incident
from packages.nyc311.portal import PortalSubmissionCancelled, PortalSubmissionResult


def _choice(**changes):
    return {
        "is_issue": True, "signal_type": "report", "category": "elevator", "asset": "elevator_south",
        "event_type": "outage", "close_incident": False, "needs_review": False,
        "confidence": 95, "review_status": "completed", "severity": 4,
        "title": "South elevator outage", "summary": "South elevator is not working.", **changes,
    }


@pytest.fixture(autouse=True)
def _safe_environment(monkeypatch):
    monkeypatch.setenv("PUBLIC_UPDATES_CHAT_NAMES", "455 Tenants")
    monkeypatch.setenv("AUTO_FILE_MAX_INCIDENT_AGE_HOURS", "0")


def _seed(*, name="cloud", timestamp=None, create_job=False):
    timestamp = int(datetime.now(timezone.utc).timestamp()) - 120 if timestamp is None else timestamp
    iso = datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
    with get_session() as session:
        raw = RawMessage(message_id=f"{name}-raw", chat_name="455 Tenants", sender="Tenant",
                         sender_hash="test-sender", ts_epoch=timestamp, ts_iso=iso,
                         text="South elevator is not working.", source="chat_export")
        incident = Incident(incident_id=name, category="elevator", asset="elevator_south",
                            severity=4, status="open", start_ts=iso, start_ts_epoch=timestamp,
                            last_ts_epoch=timestamp, title="South elevator outage",
                            summary=raw.text, proof_refs=raw.message_id, report_count=1,
                            witness_count=1, confidence=95, needs_review=False, updated_at=iso)
        decision = MessageDecision(message_id=raw.message_id, incident_id=name, chosen_source="hybrid",
                                   is_issue=True, category="elevator", event_type="outage", confidence=95,
                                   needs_review=False, final_json=json.dumps(_choice()), llm_json=json.dumps(_choice()))
        session.add_all([raw, incident, decision])
        session.flush()
        job = ensure_filing_job_for_incident(session, incident) if create_job else None
        session.commit()
        return name, raw.message_id, job.job_id if job is not None else None


def _snapshot(incident_id):
    with get_session() as session:
        return cloud_filing.reviewed_condition_snapshot(session, session.get(Incident, incident_id))


def _receipt():
    return PortalSubmissionResult(
        service_request_number="311-77778888", confirmation_text="Confirmation 311-77778888",
        final_url="https://portal.311.nyc.gov/confirmation", address_id="address-id",
        address_text="455 OCEAN PARKWAY, BROOKLYN, NY, 11218", login_used=False,
        review_screenshot_path="/tmp/review.png", confirmation_screenshot_path="/tmp/confirmation.png",
    )


def test_real_extractor_completed_review_creates_cloud_candidate(client, monkeypatch):
    from packages.incident import extractor

    monkeypatch.setenv("LLM_MODE", "all")
    monkeypatch.setattr(extractor, "LLM_MODE", "all")
    monkeypatch.setattr(extractor, "llm_classify_message", lambda *_args, **_kwargs: _choice())
    now = int(datetime.now(timezone.utc).timestamp()) - 60
    with get_session() as session:
        raw = RawMessage(message_id="extractor-raw", chat_name="455 Tenants", sender="Tenant",
                         sender_hash="sender", ts_epoch=now,
                         ts_iso=datetime.fromtimestamp(now, timezone.utc).isoformat(),
                         text="South elevator is not working.", source="chat_export")
        session.add(raw)
        session.flush()
        incident_id = extractor.classify_and_upsert_incident(session, raw, allow_filing_job=False)
        session.commit()
    assert len(_snapshot(incident_id)) == 64


@pytest.mark.parametrize("age", [None, 0, 24 * 3600 + 10, -3600])
def test_missing_stale_or_future_evidence_never_qualifies(client, age):
    incident_id, raw_id, _ = _seed()
    timestamp = None if age is None else (0 if age == 0 else int(datetime.now(timezone.utc).timestamp()) - age)
    with get_session() as session:
        session.get(Incident, incident_id).last_ts_epoch = timestamp
        session.get(RawMessage, raw_id).ts_epoch = timestamp
        session.commit()
    assert _snapshot(incident_id) is None


@pytest.mark.parametrize("mutation", ["missing_model_status", "model_error", "missing_close", "disagreement", "changed_raw", "wrong_asset", "closed"])
def test_unsafe_or_incomplete_source_fails_closed(client, mutation):
    incident_id, raw_id, _ = _seed()
    assert _snapshot(incident_id)
    with get_session() as session:
        decision = session.get(MessageDecision, raw_id)
        model = json.loads(decision.llm_json)
        if mutation == "missing_model_status":
            model.pop("review_status")
        elif mutation == "model_error":
            model["review_status"] = "error"
        elif mutation == "missing_close":
            model.pop("close_incident")
        elif mutation == "disagreement":
            decision.chosen_source = "rules_with_llm_disagreement"
        elif mutation == "changed_raw":
            session.get(RawMessage, raw_id).text = "Elevator replacement starts next month."
        elif mutation == "wrong_asset":
            session.get(Incident, incident_id).asset = "elevator_north"
        elif mutation == "closed":
            session.get(Incident, incident_id).status = "closed"
        decision.llm_json = json.dumps(model)
        session.commit()
    assert _snapshot(incident_id) is None


@pytest.mark.parametrize("later_state", ["undecided", "unreviewed_other", "failed_other", "restore"])
def test_later_unlinked_unreviewed_or_restoration_message_blocks(client, later_state):
    incident_id, _, _ = _seed()
    now = int(datetime.now(timezone.utc).timestamp()) - 30
    with get_session() as session:
        session.add(RawMessage(message_id="later", chat_name="455 Tenants", sender="Tenant",
                               sender_hash="sender2", ts_epoch=now, text="Fixed FYI", source="chat_export"))
        if later_state != "undecided":
            choice = _choice(is_issue=False, category="other", asset=None, event_type="non_issue", signal_type="discussion")
            if later_state == "restore":
                choice = _choice(event_type="restore", close_incident=True)
            if later_state == "unreviewed_other":
                choice.pop("review_status")
            if later_state == "failed_other":
                choice["review_status"] = "error"
            session.add(MessageDecision(message_id="later", chosen_source="llm", is_issue=choice["is_issue"],
                                        category=choice["category"], event_type=choice["event_type"], needs_review=False,
                                        llm_json=json.dumps(choice), final_json=json.dumps(choice)))
        session.commit()
    assert _snapshot(incident_id) is None


def test_scoped_claim_does_not_retire_reclaim_or_migrate_unrelated_jobs(client, monkeypatch):
    incident_id, _, job_id = _seed(create_job=True)
    _, _, unrelated_id = _seed(name="unrelated", timestamp=int(datetime.now(timezone.utc).timestamp()) - 864000, create_job=True)
    with get_session() as session:
        unrelated = session.get(FilingJob, unrelated_id)
        unrelated.state = "claimed"
        unrelated.claimed_at = "2020-01-01T00:00:00+00:00"
        session.commit()
    monkeypatch.setenv("AUTO_FILE_MAX_INCIDENT_AGE_HOURS", "24")
    with get_session() as session:
        job, _ = claim_next_job(session, incident_ids={incident_id})
        assert job.job_id == job_id
        session.commit()
    with get_session() as session:
        unrelated = session.get(FilingJob, unrelated_id)
        assert unrelated.state == "claimed"
        assert unrelated.claimed_at == "2020-01-01T00:00:00+00:00"


def test_postgres_busy_claim_lock_does_not_scan_or_mutate_jobs():
    queries = []

    class BusySession:
        def get_bind(self):
            return SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

        def scalar(self, query):
            queries.append(str(query))
            return False

    assert claim_next_job(BusySession(), incident_ids={"current"}) == (None, 0)
    assert queries == ["SELECT pg_try_advisory_xact_lock(4550311)"]


def test_current_linked_source_can_be_newer_than_capped_proof_refs(client):
    incident_id, _, _ = _seed()
    now = int(datetime.now(timezone.utc).timestamp()) - 30
    with get_session() as session:
        incident = session.get(Incident, incident_id)
        incident.last_ts_epoch = now
        session.add(RawMessage(message_id="current-linked", chat_name="455 Tenants", sender="Tenant",
                               sender_hash="sender2", ts_epoch=now, text="South elevator is not working.", source="chat_export"))
        session.add(MessageDecision(message_id="current-linked", incident_id=incident_id, chosen_source="hybrid",
                                    is_issue=True, category="elevator", event_type="outage", confidence=95,
                                    needs_review=False, final_json=json.dumps(_choice()), llm_json=json.dumps(_choice())))
        session.commit()
    assert _snapshot(incident_id)


def test_attempt_exhaustion_is_blocked_not_successfully_empty(client):
    _, _, job_id = _seed(create_job=True)
    with get_session() as session:
        session.get(FilingJob, job_id).attempts = 3
        session.commit()
    result = cloud_filing.run_current_filings()
    assert result["ok"] is False and result["blocked"] == 1


def test_scoped_worker_rechecks_raw_before_submit(client, monkeypatch):
    incident_id, raw_id, _ = _seed()

    def submit(_payload, *, before_submit, **_kwargs):
        with get_session() as session:
            session.get(RawMessage, raw_id).text = "South elevator is working now."
            session.commit()
        assert before_submit() is False
        raise PortalSubmissionCancelled("source changed")

    monkeypatch.setattr(portal_worker, "submit_elevator_complaint", submit)
    result = cloud_filing.run_current_filings()
    assert result["ok"] is False
    assert result["submitted"] == 0
    assert result["blocked"] > 0
    with get_session() as session:
        assert session.scalar(select(FilingJob).where(FilingJob.incident_id == incident_id)).state != "submitting"


def test_receipt_gate_is_repeated_at_final_submit(client, monkeypatch):
    _seed()
    called = []

    def submit(_payload, *, before_submit, **_kwargs):
        assert before_submit() is False
        raise PortalSubmissionCancelled("pending upload")

    monkeypatch.setattr(portal_worker, "submit_elevator_complaint", submit)
    result = cloud_filing.run_current_filings(before_submit=lambda: called.append(True) or False)
    assert called == [True]
    assert result["submitted"] == 0
    assert result["blocked"] == 1


def test_success_counts_receipt_and_verified_lookup_separately(client, monkeypatch):
    _seed()
    monkeypatch.setenv("AUTO_FILE_ENABLED", "0")

    def submit(_payload, *, before_submit, **_kwargs):
        assert before_submit() is True
        return _receipt()

    monkeypatch.setattr(portal_worker, "submit_elevator_complaint", submit)
    monkeypatch.setattr(portal_worker, "lookup_service_request_status", lambda *_args, **_kwargs: {"error": "private lookup error"})
    result = cloud_filing.run_current_filings(max_jobs=1)
    assert result == {"ok": True, "submitted": 1, "lookup_verified": 0, "empty": 0, "blocked": 0}
    assert os.environ["AUTO_FILE_ENABLED"] == "0"


def test_failure_stops_batch_without_second_attempt_or_private_output(client, monkeypatch):
    _seed()
    monkeypatch.setenv("AUTO_FILE_ENABLED", "0")

    def fail(*_args, **_kwargs):
        raise RuntimeError("private tenant details")

    monkeypatch.setattr(portal_worker, "submit_elevator_complaint", fail)
    result = cloud_filing.run_current_filings()
    assert result == {"ok": False, "submitted": 0, "lookup_verified": 0, "empty": 0, "blocked": 1}
    assert os.environ["AUTO_FILE_ENABLED"] == "0"
    with get_session() as session:
        assert session.scalar(select(FilingJob)).attempts == 1


def test_blocked_candidates_are_not_reported_as_successfully_empty(client, monkeypatch):
    incident_id, raw_id, _ = _seed()
    with get_session() as session:
        session.get(MessageDecision, raw_id).llm_json = "{}"
        session.commit()
    monkeypatch.setattr(portal_worker, "submit_elevator_complaint", lambda *_args, **_kwargs: pytest.fail("must not file"))
    result = cloud_filing.run_current_filings()
    assert result["ok"] is False and result["blocked"] == 1
    with get_session() as session:
        assert session.scalar(select(FilingJob).where(FilingJob.incident_id == incident_id)) is None
