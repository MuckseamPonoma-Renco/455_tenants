import pytest

from packages.db import Incident, MessageDecision, RawMessage
from packages.sheets import sync as sheets_sync
from packages.sheets.public_semantic_overrides import PublicSemanticOverride, PublicSemanticOverrideError


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


def _override(message_id: str, *, include: bool) -> PublicSemanticOverride:
    return PublicSemanticOverride(
        message_id=message_id,
        raw_text_sha256="0" * 64,
        include=include,
        issue_label="Issue" if include else "",
        category_label="Category" if include else "",
        summary="Neutral summary." if include else "",
        show_evidence=False,
        reason="Audited test entry.",
    )


def test_semantic_ledger_validation_rejects_a_partial_archive(monkeypatch):
    first_id = "1" * 64
    second_id = "2" * 64
    overrides = {
        first_id: _override(first_id, include=True),
        second_id: _override(second_id, include=False),
    }
    monkeypatch.setattr(sheets_sync, "load_public_semantic_overrides", lambda: overrides)

    with pytest.raises(PublicSemanticOverrideError, match="missing 1 raw message"):
        sheets_sync._validate_public_semantic_override_sources(
            {first_id: _raw(first_id, "first")},
            {},
            {"incident-under-test"},
        )


def test_semantic_ledger_validation_requires_included_rows_to_remain_linked(monkeypatch):
    message_id = "3" * 64
    overrides = {message_id: _override(message_id, include=True)}
    raw = _raw(message_id, "source")
    monkeypatch.setattr(sheets_sync, "load_public_semantic_overrides", lambda: overrides)
    monkeypatch.setattr(
        sheets_sync,
        "get_public_semantic_override",
        lambda selected_id, text, *, overrides: overrides[selected_id],
    )

    with pytest.raises(PublicSemanticOverrideError, match="without a live incident link"):
        sheets_sync._validate_public_semantic_override_sources(
            {message_id: raw},
            {},
            {"incident-under-test"},
        )
