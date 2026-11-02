from datetime import datetime, timedelta, timezone

import pytest

from packages.db import FilingJob, Incident, MessageDecision, RawMessage, ServiceRequestCase, get_session
from packages.incident import extractor
from packages.nyc311 import planner
from packages.nyc311.drafts import build_filing_draft


def seed():
    now = datetime.now(timezone.utc)
    with get_session() as session:
        inc = Incident(incident_id="new-live-outage", category="elevator", asset="elevator_both", status="open",
            title="Elevator outage", summary="Both elevators down; one stuck on 17.",
            start_ts=(now - timedelta(minutes=20)).isoformat(), start_ts_epoch=int(now.timestamp())-1200,
            last_ts_epoch=int(now.timestamp())-600, report_count=1, witness_count=1, severity=4, confidence=90)
        raw = RawMessage(message_id="new-report", sender_hash="fixture", chat_name="Fixture",
            text="Both elevators down; one stuck on 17.", ts_epoch=int(now.timestamp())-600,
            ts_iso=(now-timedelta(minutes=10)).isoformat(), source="whatsapp_web")
        decision = MessageDecision(message_id=raw.message_id, incident_id=inc.incident_id,
            is_issue=True, category="elevator", event_type="outage", chosen_source="rules")
        job = FilingJob(dedupe_key="311:"+inc.incident_id, incident_id=inc.incident_id, state="skipped", attempts=0,
            payload_json='{"description":"Old invalid report"}',
            notes="Safety correction: false-positive elevator outage; no submission attempted.",
            updated_at=(now-timedelta(hours=1)).isoformat())
        session.add_all([inc,raw,decision,job]);session.commit()
        return job.job_id


def test_new_live_report_reopens_exact_never_attempted_job_with_current_payload(client):
    jid=seed()
    with get_session() as s:
        inc=s.get(Incident,"new-live-outage")
        job=planner.ensure_filing_job_for_incident(s,inc,trigger_message_id="new-report")
        s.commit()
        assert job.job_id==jid and job.state=="pending" and job.attempts==0
        assert "Both elevators down." in job.payload_json
        assert "false-positive" in job.notes and "new-report" in job.notes
        assert s.query(FilingJob).count()==1
        assert s.query(ServiceRequestCase).count()==0


@pytest.mark.parametrize("change", ["no_trigger","old_report","archive","future_report","attempted","manual_hold","nonissue","repair_update","closed","existing_case"])
def test_skip_recovery_requires_current_live_outage_and_no_receipt(client,change):
    jid=seed()
    with get_session() as s:
        job=s.get(FilingJob,jid);inc=s.get(Incident,"new-live-outage");raw=s.get(RawMessage,"new-report");d=s.get(MessageDecision,raw.message_id)
        if change=="old_report": raw.ts_epoch-=2*86400
        if change=="future_report": raw.ts_epoch+=2*86400
        if change=="archive": raw.source="zip_import"
        if change=="attempted": job.attempts=1
        if change=="manual_hold": job.notes="Operator hold: inspect before filing"
        if change=="nonissue": d.is_issue=False
        if change=="repair_update": d.event_type="status_update";raw.text="Mechanic called."
        if change=="closed": inc.status="closed"
        if change=="existing_case": s.add(ServiceRequestCase(incident_id=inc.incident_id,service_request_number="311-12345678"))
        s.commit()
        planner.ensure_filing_job_for_incident(s,inc,trigger_message_id=None if change=="no_trigger" else raw.message_id)
        s.commit()
        assert s.get(FilingJob,jid).state=="skipped"


@pytest.mark.parametrize("state", ["submitting","submission_unknown","submitted","awaiting_human_verification"])
def test_new_report_never_reopens_protected_state(client,state):
    jid=seed()
    with get_session() as s:
        s.get(FilingJob,jid).state=state;s.commit()
        planner.ensure_filing_job_for_incident(s,s.get(Incident,"new-live-outage"),trigger_message_id="new-report")
        s.commit()
        assert s.get(FilingJob,jid).state==state


@pytest.mark.parametrize("state", ["submitted","submission_unknown","awaiting_human_verification"])
def test_equivalent_existing_job_blocks_skip_recovery(client,state):
    jid=seed()
    with get_session() as s:
        inc=s.get(Incident,"new-live-outage");draft=build_filing_draft(inc)
        s.add(FilingJob(dedupe_key="equivalent",incident_id=inc.incident_id,state=state,
            complaint_type=draft.complaint_type,form_target=draft.form_target,payload_json=draft.payload_json()))
        s.commit()
        planner.ensure_filing_job_for_incident(s,inc,trigger_message_id="new-report");s.commit()
        assert s.get(FilingJob,jid).state=="skipped"


@pytest.mark.parametrize("allow_filing", [False, True])
def test_live_classification_can_recover_but_historical_replay_cannot(client,allow_filing):
    jid=seed()
    with get_session() as s:
        extractor.classify_and_upsert_incident(s,s.get(RawMessage,"new-report"),allow_filing_job=allow_filing)
        s.commit()
        assert s.get(FilingJob,jid).state==("pending" if allow_filing else "skipped")


def test_combined_outage_does_not_claim_both_cars_stuck(client):
    seed()
    with get_session() as s:
        assert build_filing_draft(s.get(Incident,"new-live-outage")).description=="Both elevators down."
