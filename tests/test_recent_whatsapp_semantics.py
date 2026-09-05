import pytest

from packages.db import Incident, MessageDecision, RawMessage, get_session
from packages.incident.rules import classify_rules
import scripts.certify_20260905_reviewed_decisions as review_certifier
from scripts.repair_recent_whatsapp_semantics import (
    CROSS_SOURCE_TARGET_ALIASES,
    _align_cross_source_alias_decisions,
)


def _auth_headers():
    return {"Authorization": "Bearer test-token"}


@pytest.mark.parametrize(
    ("text", "category", "event_type"),
    [
        (
            "Washing machine 15 isn't reading cards; the door isn't connecting to register that it is shut.",
            "laundry",
            "new_issue",
        ),
        ("They came out and fixed washers 14 and 15.", "laundry", "restore"),
        ("There is a lack of fire hoses in the stairwells.", "fire_safety", "new_issue"),
        ("It looks like firehouses were removed again in stairwell B.", "fire_safety", "still_out"),
        ("North lift super slow descending. Really slow.", "elevator", "status_update"),
    ],
)
def test_recent_export_phrases_have_stable_noncontaminating_rules(text, category, event_type):
    result = classify_rules(text)

    assert result["is_issue"] is True
    assert result["category"] == category
    assert result["event_type"] == event_type


def test_fire_hose_short_followups_keep_one_fire_safety_incident(client, monkeypatch):
    monkeypatch.setattr("packages.incident.extractor.LLM_MODE", "off")
    messages = [
        (1788445860, "There is a lack of fire hoses in the stairwells."),
        (1788446160, "I was wondering the same thing!"),
        (1788449820, "Yay, looks like replaced or in process of being replaced while I was out."),
        (1788472320, "It looks like firehouses were removed again in stairwell B."),
    ]
    ids = []
    for ts_epoch, text in messages:
        response = client.post(
            "/ingest/whatsapp_web",
            headers=_auth_headers(),
            json={"chat_name": "455 Tenants", "sender": "Tenant", "ts_epoch": ts_epoch, "text": text},
        )
        assert response.status_code == 200, response.text
        ids.append(response.json()["message_id"])

    with get_session() as session:
        decisions = [session.get(MessageDecision, message_id) for message_id in ids]
        assert all(decision is not None and decision.category == "fire_safety" for decision in decisions)
        assert len({decision.incident_id for decision in decisions}) == 1
        incident = session.get(Incident, decisions[0].incident_id)
        assert incident is not None
        assert incident.status == "open"
        assert incident.asset == "stairwell_fire_hoses"


def test_laundry_restore_closes_laundry_incident_not_elevator(client, monkeypatch):
    monkeypatch.setattr("packages.incident.extractor.LLM_MODE", "off")
    issue = client.post(
        "/ingest/whatsapp_web",
        headers=_auth_headers(),
        json={
            "chat_name": "455 Tenants",
            "sender": "Tenant One",
            "ts_epoch": 1788351360,
            "text": "Washing machine 15 isn't reading cards and the door isn't connecting.",
        },
    )
    restore = client.post(
        "/ingest/whatsapp_web",
        headers=_auth_headers(),
        json={
            "chat_name": "455 Tenants",
            "sender": "Tenant Two",
            "ts_epoch": 1788442560,
            "text": "They came out and fixed washers 14 and 15.",
        },
    )
    assert issue.status_code == restore.status_code == 200

    with get_session() as session:
        issue_decision = session.get(MessageDecision, issue.json()["message_id"])
        restore_decision = session.get(MessageDecision, restore.json()["message_id"])
        assert issue_decision.category == restore_decision.category == "laundry"
        assert issue_decision.incident_id == restore_decision.incident_id
        incident = session.get(Incident, issue_decision.incident_id)
        assert incident.status == "closed"


def test_direction_followup_inherits_north_slow_elevator(client, monkeypatch):
    monkeypatch.setattr("packages.incident.extractor.LLM_MODE", "off")
    slow = client.post(
        "/ingest/whatsapp_web",
        headers=_auth_headers(),
        json={
            "chat_name": "455 Tenants",
            "sender": "Tenant One",
            "ts_epoch": 1788487200,
            "text": "North lift super slow descending. Really slow.",
        },
    )
    confirmation = client.post(
        "/ingest/whatsapp_web",
        headers=_auth_headers(),
        json={
            "chat_name": "455 Tenants",
            "sender": "Tenant Two",
            "ts_epoch": 1788487260,
            "text": "Same going up.",
        },
    )
    assert slow.status_code == confirmation.status_code == 200

    with get_session() as session:
        slow_decision = session.get(MessageDecision, slow.json()["message_id"])
        confirmation_decision = session.get(MessageDecision, confirmation.json()["message_id"])
        assert slow_decision.category == confirmation_decision.category == "elevator"
        assert slow_decision.incident_id == confirmation_decision.incident_id
        incident = session.get(Incident, slow_decision.incident_id)
        assert incident.asset == "elevator_north"
        assert incident.status == "open"


def test_limited_fire_stair_followup_attaches_to_exact_access_incident(client, monkeypatch):
    monkeypatch.setattr("packages.incident.extractor.LLM_MODE", "off")
    initial = client.post(
        "/ingest/whatsapp_web",
        headers=_auth_headers(),
        json={
            "chat_name": "455 Tenants",
            "sender": "Tenant One",
            "ts_epoch": 1784813160,
            "text": "One of the lobby fire stair doors is stuck closed.",
        },
    )
    followup = client.post(
        "/ingest/whatsapp_web",
        headers=_auth_headers(),
        json={
            "chat_name": "455 Tenants",
            "sender": "Tenant Two",
            "ts_epoch": 1784813400,
            "text": "1 lift, 1 functional fire stair.",
        },
    )
    assert initial.status_code == followup.status_code == 200

    with get_session() as session:
        initial_decision = session.get(MessageDecision, initial.json()["message_id"])
        followup_decision = session.get(MessageDecision, followup.json()["message_id"])
        assert initial_decision.category == followup_decision.category == "security_access"
        assert followup_decision.event_type == "status_update"
        assert followup_decision.chosen_source == "rules_context"
        assert followup_decision.incident_id == initial_decision.incident_id
        incident = session.get(Incident, initial_decision.incident_id)
        assert incident.status == "open"
        assert incident.severity == 4
        assert incident.report_count == 2


def test_limited_fire_stair_fragment_without_context_remains_nonissue(client, monkeypatch):
    monkeypatch.setattr("packages.incident.extractor.LLM_MODE", "off")
    response = client.post(
        "/ingest/whatsapp_web",
        headers=_auth_headers(),
        json={
            "chat_name": "Unrelated Chat",
            "sender": "Tenant",
            "ts_epoch": 1784813400,
            "text": "1 lift, 1 functional fire stair.",
        },
    )
    assert response.status_code == 200

    with get_session() as session:
        decision = session.get(MessageDecision, response.json()["message_id"])
        assert decision.is_issue is False
        assert decision.incident_id is None


def test_repair_aligns_archive_alias_with_contextually_classified_live_decision(client):
    archive_message_id, live_message_id = next(iter(CROSS_SOURCE_TARGET_ALIASES.items()))
    with get_session() as session:
        session.add_all(
            [
                MessageDecision(
                    message_id=archive_message_id,
                    is_issue=False,
                    event_type="non_issue",
                    chosen_source="rules",
                ),
                MessageDecision(
                    message_id=live_message_id,
                    incident_id="fire-incident",
                    is_issue=True,
                    category="fire_safety",
                    event_type="status_update",
                    chosen_source="rules_context",
                    confidence=88,
                    needs_review=False,
                    auto_file_candidate=False,
                    final_json='{"category":"fire_safety"}',
                ),
            ]
        )
        session.flush()

        aligned = _align_cross_source_alias_decisions(session)
        archive = session.get(MessageDecision, archive_message_id)

        assert aligned == [
            {
                "archive_message_id": archive_message_id,
                "live_message_id": live_message_id,
                "incident_id": "fire-incident",
            }
        ]
        assert archive.is_issue is True
        assert archive.category == "fire_safety"
        assert archive.event_type == "status_update"
        assert archive.incident_id == "fire-incident"
        assert archive.chosen_source == "rules_context"


def test_review_certifier_records_truthful_completed_provenance(client, monkeypatch):
    message_id = "reviewed-message"
    monkeypatch.setattr(
        review_certifier,
        "REVIEWED_DECISIONS",
        {message_id: (True, "elevator", "restore")},
    )
    with get_session() as session:
        session.add(
            Incident(
                incident_id="restored-elevator",
                category="elevator",
                title="Elevator restored",
                summary="",
                proof_refs=message_id,
                needs_review=True,
            )
        )
        session.add(
            RawMessage(
                message_id=message_id,
                sender_hash="tenant",
                text="Both work now",
                source="test",
            )
        )
        session.add(
            MessageDecision(
                message_id=message_id,
                incident_id="restored-elevator",
                chosen_source="rules_context",
                is_issue=True,
                category="elevator",
                event_type="restore",
                needs_review=True,
                final_json='{"category":"elevator","event_type":"restore","needs_review":true}',
            )
        )
        session.commit()

    result = review_certifier.certify(apply=True)

    assert result["applied"] is True
    assert result["errors"] == []
    with get_session() as session:
        decision = session.get(MessageDecision, message_id)
        final = review_certifier._json_object(decision.final_json)
        assert decision.chosen_source == "review_codex_semantic_audit"
        assert decision.needs_review is False
        assert final["review_status"] == "completed"
        assert final["review_kind"] == "codex_semantic_audit"
        assert final["reviewed_by"] == review_certifier.REVIEWED_BY
        assert final["needs_review"] is False
        assert session.get(Incident, "restored-elevator").needs_review is False


def test_review_certifier_records_truthful_nonissue_provenance(client, monkeypatch):
    message_id = "reviewed-nonissue"
    monkeypatch.setattr(review_certifier, "REVIEWED_DECISIONS", {message_id: (False, "", "")})
    with get_session() as session:
        session.add(
            RawMessage(
                message_id=message_id,
                sender_hash="tenant",
                text="Please respond if you would like to join us.",
                source="test",
            )
        )
        session.add(
            MessageDecision(
                message_id=message_id,
                chosen_source="none",
                is_issue=False,
                needs_review=False,
                final_json="{}",
            )
        )
        session.commit()

    result = review_certifier.certify(apply=True)

    assert result["applied"] is True
    with get_session() as session:
        decision = session.get(MessageDecision, message_id)
        final = review_certifier._json_object(decision.final_json)
        assert decision.incident_id is None
        assert decision.is_issue is False
        assert decision.chosen_source == "review_codex_semantic_audit"
        assert final["review_status"] == "completed"
