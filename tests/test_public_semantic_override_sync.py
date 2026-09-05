from packages.db import Incident, MessageDecision, RawMessage
from packages.sheets import sync as sheets_sync


RESTORE_MESSAGE_ID = "a1f33f3c5ea919e042d082a0a25768ffafe85230ce57490f155c16b1971086be"
TRANSIT_MESSAGE_ID = "d2abe256aac02ed84ffd4a7926ae5f8c500fdf7562cd14d842a3799275cf5c38"


def _incident() -> Incident:
    return Incident(
        incident_id="incident-under-test",
        category="elevator",
        asset="elevator_south",
        status="open",
        title="South elevator outage",
        summary="Internal summary",
        proof_refs=RESTORE_MESSAGE_ID,
    )


def _raw(message_id: str, text: str) -> RawMessage:
    return RawMessage(
        message_id=message_id,
        chat_name="455 Tenants",
        sender="Tenant",
        sender_hash="sender-hash",
        ts_iso="2026-06-01T12:00:00Z",
        ts_epoch=1780315200,
        text=text,
        source="whatsapp_zip",
    )


def test_audited_override_controls_inclusion_labels_and_summary():
    incident = _incident()
    raw = _raw(RESTORE_MESSAGE_ID, "Both currently working")
    stale_decision = MessageDecision(
        message_id=RESTORE_MESSAGE_ID,
        incident_id=incident.incident_id,
        is_issue=False,
        category="elevator",
        event_type="still_out",
    )

    assert sheets_sync._public_should_include_update(incident, raw, stale_decision) is True
    assert sheets_sync._public_event_issue_label(incident, raw) == "Both elevators working"
    assert sheets_sync._public_event_category_label(incident, raw) == "Elevator"
    assert sheets_sync._public_event_summary(incident, raw) == "Both elevators were reported working."


def test_audited_exclusion_wins_over_stale_positive_decision():
    incident = _incident()
    raw = _raw(TRANSIT_MESSAGE_ID, "I've been waiting on the Delancey platform for 20 minutes 😔")
    stale_decision = MessageDecision(
        message_id=TRANSIT_MESSAGE_ID,
        incident_id=incident.incident_id,
        is_issue=True,
        category="elevator",
        event_type="new_issue",
    )

    assert sheets_sync._public_should_include_update(incident, raw, stale_decision) is False


def test_audited_evidence_suppression_is_enforced_in_rendered_row(monkeypatch):
    incident = _incident()
    raw = _raw(RESTORE_MESSAGE_ID, "Both currently working")
    decision = MessageDecision(
        message_id=RESTORE_MESSAGE_ID,
        incident_id=incident.incident_id,
        is_issue=True,
        category="elevator",
        event_type="restore",
    )
    monkeypatch.setattr(
        sheets_sync,
        "_public_raw_evidence_cells",
        lambda _raw: ("sensitive preview", "sensitive link"),
    )

    rows = sheets_sync._public_update_rows(
        [incident],
        {raw.message_id: raw},
        {},
        {"455 tenants"},
        {incident.incident_id: [raw.message_id]},
        {raw.message_id: decision},
    )

    assert len(rows) == 1
    assert rows[0][1:3] == ["Both elevators working", "Elevator"]
    assert rows[0][4:7] == ["", "", "Both elevators were reported working."]
