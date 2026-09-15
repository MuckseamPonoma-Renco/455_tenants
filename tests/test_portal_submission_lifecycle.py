from datetime import datetime, timedelta, timezone

import pytest

from packages.db import FilingJob, Incident, ServiceRequestCase, get_session
from packages.nyc311 import portal_worker
from packages.nyc311.drafts import build_filing_draft
from packages.nyc311.planner import claim_next_job, ensure_filing_job_for_incident
from packages.nyc311.portal import PortalSubmissionCancelled, PortalSubmissionResult, PortalSubmissionUncertain


def _seed_job():
    now = datetime.now(timezone.utc)
    with get_session() as session:
        incident = Incident(
            incident_id="lifecycle-elevator", category="elevator", asset="elevator_south",
            severity=4, status="open", start_ts=now.isoformat(),
            start_ts_epoch=int(now.timestamp()), last_ts_epoch=int(now.timestamp()),
            title="South elevator outage", summary="South elevator not working.",
            proof_refs="", report_count=1, witness_count=1, confidence=80, needs_review=False,
            updated_at=now.isoformat(),
        )
        session.add(incident)
        session.flush()
        job = ensure_filing_job_for_incident(session, incident)
        session.commit()
        return job.job_id


def _receipt():
    return PortalSubmissionResult(
        service_request_number="311-77778888", confirmation_text="Confirmation 311-77778888",
        final_url="https://portal.311.nyc.gov/confirmation", address_id="address-id",
        address_text="455 OCEAN PARKWAY, BROOKLYN, NY, 11218", login_used=False,
        review_screenshot_path="/tmp/review.png", confirmation_screenshot_path="/tmp/confirmation.png",
    )


def test_final_click_callback_persists_submitting_and_cannot_run_twice(client, monkeypatch):
    job_id = _seed_job()

    def submit(_payload, *, before_submit, **_kwargs):
        assert before_submit() is True
        with get_session() as session:
            assert session.get(FilingJob, job_id).state == "submitting"
        assert before_submit() is False
        return _receipt()

    monkeypatch.setattr(portal_worker, "submit_elevator_complaint", submit)
    result = portal_worker.run_portal_filing_once(verify_lookup=False)
    assert result["ok"] is True
    with get_session() as session:
        assert session.get(FilingJob, job_id).state == "submitted"


@pytest.mark.parametrize("error", [RuntimeError("connection lost after final click"), PortalSubmissionUncertain("receipt not confirmed")])
def test_post_click_failure_is_unknown_and_never_reclaimed(client, monkeypatch, error):
    job_id = _seed_job()

    def submit(_payload, *, before_submit, **_kwargs):
        assert before_submit()
        raise error

    monkeypatch.setattr(portal_worker, "submit_elevator_complaint", submit)
    result = portal_worker.run_portal_filing_once(verify_lookup=False)
    assert result["ok"] is False
    assert result["submission_unknown"] is True
    with get_session() as session:
        job = session.get(FilingJob, job_id)
        assert job.state == "submission_unknown"
        job.claimed_at = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        session.commit()
    with get_session() as session:
        assert claim_next_job(session)[0] is None
        assert session.get(FilingJob, job_id).state == "submission_unknown"


def test_pre_submit_failure_remains_retryable(client, monkeypatch):
    job_id = _seed_job()

    def submit(*_args, **_kwargs):
        raise RuntimeError("address widget unavailable before review")

    monkeypatch.setattr(portal_worker, "submit_elevator_complaint", submit)
    result = portal_worker.run_portal_filing_once(verify_lookup=False)
    assert result["submission_unknown"] is False
    with get_session() as session:
        assert session.get(FilingJob, job_id).state == "failed"
        assert claim_next_job(session)[0].job_id == job_id


def test_receipt_is_committed_before_optional_lookup(client, monkeypatch):
    job_id = _seed_job()

    def submit(_payload, *, before_submit, **_kwargs):
        assert before_submit()
        return _receipt()

    def lookup(*_args, **_kwargs):
        with get_session() as session:
            assert session.get(FilingJob, job_id).state == "submitted"
            assert session.query(ServiceRequestCase).one().service_request_number == "311-77778888"
        raise RuntimeError("status lookup unavailable")

    monkeypatch.setattr(portal_worker, "submit_elevator_complaint", submit)
    monkeypatch.setattr(portal_worker, "lookup_service_request_status", lookup)
    result = portal_worker.run_portal_filing_once()
    assert result["ok"] is True
    assert result["lookup_verified"] is False
    with get_session() as session:
        assert session.get(FilingJob, job_id).state == "submitted"
        assert claim_next_job(session)[0] is None


def test_receipt_persistence_failure_keeps_unknown_and_receipt_for_reconciliation(client, monkeypatch):
    job_id = _seed_job()

    def submit(_payload, *, before_submit, **_kwargs):
        assert before_submit()
        return _receipt()

    def fail_persistence(*_args, **_kwargs):
        raise RuntimeError("receipt transaction failed")

    monkeypatch.setattr(portal_worker, "submit_elevator_complaint", submit)
    monkeypatch.setattr(portal_worker, "create_case_from_filing_job", fail_persistence)
    result = portal_worker.run_portal_filing_once(verify_lookup=False)
    assert result["ok"] is False
    assert result["service_request_number"] == "311-77778888"
    with get_session() as session:
        job = session.get(FilingJob, job_id)
        assert job.state == "submission_unknown"
        assert "portal_receipt=311-77778888" in job.notes
        assert session.query(ServiceRequestCase).count() == 0
        assert claim_next_job(session)[0] is None


@pytest.mark.parametrize("state", ["submitting", "submission_unknown"])
def test_uncertain_jobs_block_equivalent_aliases_and_survive_incident_closure(client, state):
    job_id = _seed_job()
    with get_session() as session:
        job = session.get(FilingJob, job_id)
        job.state = state
        job.claimed_at = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        incident = session.get(Incident, job.incident_id)
        alias = Incident(
            incident_id="lifecycle-alias", category=incident.category, asset=incident.asset,
            severity=incident.severity, status="open", start_ts=incident.start_ts,
            start_ts_epoch=incident.start_ts_epoch, last_ts_epoch=incident.last_ts_epoch,
            title=incident.title, summary=incident.summary, proof_refs="",
            report_count=1, witness_count=1, confidence=80, needs_review=False,
        )
        session.add(alias)
        session.flush()
        alias_job = ensure_filing_job_for_incident(session, alias)
        assert alias_job.state == "skipped"
        # Even a legacy/manually queued duplicate must be blocked at claim time.
        alias_job.state = "pending"
        incident.status = "closed"
        session.commit()
    with get_session() as session:
        assert claim_next_job(session)[0] is None
        assert session.get(FilingJob, job_id).state == state
        assert session.query(FilingJob).filter_by(incident_id="lifecycle-alias").one().state == "skipped"


def test_new_report_at_review_refreshes_same_job_instead_of_permanently_skipping(client, monkeypatch):
    job_id = _seed_job()

    def submit(_payload, *, before_submit, **_kwargs):
        with get_session() as session:
            incident = session.get(Incident, "lifecycle-elevator")
            incident.report_count += 1
            session.commit()
        assert before_submit() is False
        raise PortalSubmissionCancelled("payload changed before final submit")

    monkeypatch.setattr(portal_worker, "submit_elevator_complaint", submit)
    result = portal_worker.run_portal_filing_once(verify_lookup=False)
    assert result["requeued"] is True
    assert result["skipped"] is False
    with get_session() as session:
        job = session.get(FilingJob, job_id)
        assert job.state == "pending"
        incident = session.get(Incident, job.incident_id)
        assert job.payload_json == build_filing_draft(incident).payload_json()
        assert claim_next_job(session)[0].job_id == job_id
        assert session.query(FilingJob).count() == 1


def test_repeated_review_cancellations_respect_attempt_limit(client, monkeypatch):
    job_id = _seed_job()
    monkeypatch.setenv("AUTO_FILE_MAX_PORTAL_ATTEMPTS", "1")

    def submit(_payload, *, before_submit, **_kwargs):
        with get_session() as session:
            incident = session.get(Incident, "lifecycle-elevator")
            incident.report_count += 1
            session.commit()
        assert before_submit() is False
        raise PortalSubmissionCancelled("payload changed before final submit")

    monkeypatch.setattr(portal_worker, "submit_elevator_complaint", submit)
    result = portal_worker.run_portal_filing_once(verify_lookup=False)
    assert result["ok"] is False
    assert result["state"] == "failed"
    with get_session() as session:
        assert claim_next_job(session)[0] is None
        assert session.get(FilingJob, job_id).state == "failed"


def test_new_manual_receipt_blocks_an_already_queued_job_at_claim(client):
    job_id = _seed_job()
    with get_session() as session:
        session.add(ServiceRequestCase(service_request_number="311-12345678", incident_id="lifecycle-elevator", source="whatsapp_message"))
        session.commit()
    with get_session() as session:
        job, skipped = claim_next_job(session)
        assert job is None
        assert skipped == 1
        queued = session.get(FilingJob, job_id)
        assert queued.state == "skipped"
        assert queued.attempts == 0
        assert "already has service request 311-12345678" in queued.notes


def test_manual_receipt_arriving_during_portal_review_cancels_without_refiling(client, monkeypatch):
    job_id = _seed_job()

    def submit(_payload, *, before_submit, **_kwargs):
        with get_session() as session:
            session.add(ServiceRequestCase(service_request_number="311-12345678", incident_id="lifecycle-elevator", source="whatsapp_message"))
            session.commit()
        assert before_submit() is False
        raise PortalSubmissionCancelled("tenant supplied an existing service request")

    monkeypatch.setattr(portal_worker, "submit_elevator_complaint", submit)
    result = portal_worker.run_portal_filing_once(verify_lookup=False)
    assert result["skipped"] is True
    assert result["requeued"] is False
    with get_session() as session:
        assert session.get(FilingJob, job_id).state == "skipped"
        assert session.query(ServiceRequestCase).one().service_request_number == "311-12345678"
        assert claim_next_job(session)[0] is None
