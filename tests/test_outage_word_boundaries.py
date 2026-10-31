import json
from datetime import datetime, timezone

import pytest

from packages.db import FilingJob, Incident, MessageDecision, RawMessage, get_session
from packages.incident.rules import classify_rules
from packages.nyc311.drafts import build_filing_draft
from packages.nyc311.planner import (
    _payload_sha256,
    claim_next_job,
    claimed_filing_job_is_current,
    incident_is_auto_eligible,
)


@pytest.fixture(autouse=True)
def filing_policy(monkeypatch):
    # These regressions exercise the normal enabled filing policy, regardless
    # of configuration-loader tests that run earlier in the full suite.
    monkeypatch.setenv("AUTO_FILE_ENABLED", "1")
    monkeypatch.setenv("AUTO_FILE_SKIP_NEEDS_REVIEW", "0")


@pytest.mark.parametrize("text", [
    "We discussed the elevators. When they were outside our apartment, the inspector checked the ceiling repair.",
    "The elevator replacement deadlines were discussed at the meeting.",
    "We discussed the elevators while they were outdoors.",
])
def test_outage_words_do_not_match_parts_of_unrelated_words(text):
    assert classify_rules(text)["is_issue"] is False


@pytest.mark.parametrize("text", [
    "Both elevators are out.",
    "The north one is stuck on 17.",
    "The south elevator is dead!",
    "They were out. Both elevators are still down.",
])
def test_whole_word_outages_remain_actionable(text):
    decision = classify_rules(text)
    assert decision["is_issue"] is True
    assert decision["category"] == "elevator"
    assert decision["event_type"] in {"outage", "still_out"}


def _stored_outage(session, text, *, source="rules_with_llm_disagreement", stored_rule_issue=True):
    now = datetime.now(timezone.utc)
    raw = RawMessage(message_id="old-rule-message", chat_name="Test Building", sender_hash="resident",
                     text=text, ts_iso=now.isoformat(), ts_epoch=int(now.timestamp()), source="test")
    incident = Incident(incident_id="old-rule-incident", category="elevator", status="open", severity=4,
                        title="Elevator outage", summary="Elevator not working.",
                        start_ts=now.isoformat(), start_ts_epoch=int(now.timestamp()),
                        last_ts_epoch=int(now.timestamp()), proof_refs=raw.message_id,
                        report_count=1, witness_count=1, confidence=85, needs_review=True)
    session.add_all([raw, incident])
    session.flush()
    rules = {"is_issue": stored_rule_issue, "category": "elevator" if stored_rule_issue else "other",
             "event_type": "outage" if stored_rule_issue else "non_issue"}
    session.add(MessageDecision(message_id=raw.message_id, incident_id=incident.incident_id,
                                chosen_source=source, is_issue=True, category="elevator", event_type="outage",
                                confidence=85, rules_json=json.dumps(rules)))
    draft = build_filing_draft(incident)
    job = FilingJob(dedupe_key="311:old-rule-incident", incident_id=incident.incident_id, state="pending",
                    complaint_type=draft.complaint_type, form_target=draft.form_target,
                    payload_json=draft.payload_json(), created_at=now.isoformat())
    session.add(job)
    session.flush()
    return incident, job


def test_claim_skips_stale_rule_false_positive_without_a_submission(client):
    with get_session() as session:
        incident, job = _stored_outage(session, "We discussed the elevators. They were outside our apartment checking the ceiling.")
        assert incident_is_auto_eligible(incident) is False
        session.commit()
        claimed, skipped = claim_next_job(session)
        assert claimed is None
        assert skipped == 1
        assert job.state == "skipped"
        assert job.attempts == 0


def test_final_submit_rechecks_source_after_a_rule_correction(client):
    with get_session() as session:
        _, job = _stored_outage(session, "We discussed the elevators. They were outside our apartment checking the ceiling.")
        job.state = "claimed"
        job.notes = "claimed_payload_sha256=" + _payload_sha256(job)
        session.flush()
        assert claimed_filing_job_is_current(session, job) is False


@pytest.mark.parametrize(("text", "source", "stored_rule_issue"), [
    ("The north elevator is stuck on 17.", "rules_with_llm_disagreement", True),
    ("Both still down.", "rules_context", False),
    ("Both still down.", "rules_with_llm_disagreement", False),
    ("Both still down.", "llm", False),
])
def test_current_direct_and_contextual_reports_remain_eligible(client, text, source, stored_rule_issue):
    with get_session() as session:
        incident, _ = _stored_outage(session, text, source=source, stored_rule_issue=stored_rule_issue)
        assert incident_is_auto_eligible(incident) is True
