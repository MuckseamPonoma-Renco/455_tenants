import json

from packages.db import Incident, MessageDecision, RawMessage, get_session
import scripts.repair_20260905_live_laundry_semantics as repair_module


RAW_TEXTS = {
    "116e13ef0a5356a49cb9be091cf030a78d07523302a6946dd80cb8b5efee897a":
        "Washer #15 stole my detergent ! I can call Hercules today",
    "26d0635ef3918e7e92233ce85d088f5600546e94fa9acc9fb2d790d1618786be":
        "That's the one I reported! Please tell them it happened again and that it was reported as fixed on Thursday.",
}


def _seed() -> None:
    with get_session() as session:
        session.add(
            Incident(
                incident_id=repair_module.INCIDENT_ID,
                category="laundry",
                asset=None,
                severity=2,
                status="open",
                title="Washer #15 stole detergent",
                summary="Legacy summary",
                proof_refs=",".join(repair_module.MESSAGE_SPECS),
                report_count=2,
                witness_count=2,
                confidence=90,
                needs_review=False,
            )
        )
        for message_id, spec in repair_module.MESSAGE_SPECS.items():
            session.add(
                RawMessage(
                    message_id=message_id,
                    chat_name="455 Tenants",
                    sender="Tenant",
                    sender_hash=f"hash-{message_id[:4]}",
                    ts_iso="2026-09-05T00:00:00Z",
                    ts_epoch=spec["ts_epoch"],
                    text=RAW_TEXTS[message_id],
                    source="whatsapp_web",
                )
            )
            session.add(
                MessageDecision(
                    message_id=message_id,
                    incident_id=repair_module.INCIDENT_ID,
                    chosen_source="llm",
                    is_issue=True,
                    category="laundry",
                    event_type=spec["event_type"],
                    confidence=90,
                    needs_review=False,
                    auto_file_candidate=False,
                    rules_json=json.dumps({"kind": "nonissue"}),
                    llm_json=json.dumps({"review_status": "completed"}),
                    final_json=json.dumps(
                        {
                            "is_issue": True,
                            "category": "laundry",
                            "asset": None,
                            "event_type": spec["event_type"],
                        }
                    ),
                )
            )
        session.commit()


def test_live_laundry_repair_is_fail_closed_atomic_and_idempotent(client, monkeypatch):
    _seed()
    events = []
    monkeypatch.setattr(repair_module, "append_audit_event", lambda *args: events.append(args))
    monkeypatch.setattr(repair_module, "daily_hash_chain", lambda: None)

    dry_run = repair_module.repair(apply=False)
    assert dry_run["errors"] == []
    assert dry_run["would_change"] is True
    assert dry_run["applied"] is False

    applied = repair_module.repair(apply=True)
    assert applied["errors"] == []
    assert applied["validation_errors"] == []
    assert applied["applied"] is True
    assert len(events) == 1

    with get_session() as session:
        incident = session.get(Incident, repair_module.INCIDENT_ID)
        assert incident.asset == "washer_15"
        assert incident.title == repair_module.DESIRED_TITLE
        assert incident.summary == repair_module.DESIRED_SUMMARY
        assert incident.report_count == 2
        for message_id in repair_module.MESSAGE_SPECS:
            decision = session.get(MessageDecision, message_id)
            final = json.loads(decision.final_json)
            assert decision.chosen_source == f"review:{repair_module.REPAIR_ID}"
            assert decision.auto_file_candidate is False
            assert final["asset"] == "washer_15"
            assert final["title"] == repair_module.MESSAGE_SPECS[message_id]["title"]
            assert final["summary"] == repair_module.MESSAGE_SPECS[message_id]["summary"]
            assert final["review_status"] == "completed"

    repeated = repair_module.repair(apply=True)
    assert repeated["errors"] == []
    assert repeated["already_repaired"] is True
    assert repeated["applied"] is False
    assert len(events) == 1


def test_live_laundry_repair_rejects_raw_drift(client, monkeypatch):
    _seed()
    monkeypatch.setattr(repair_module, "append_audit_event", lambda *args: None)
    with get_session() as session:
        row = session.get(RawMessage, next(iter(repair_module.MESSAGE_SPECS)))
        row.text += " changed"
        session.commit()

    result = repair_module.repair(apply=True)

    assert result["applied"] is False
    assert any("hash/timestamp mismatch" in error for error in result["errors"])
    with get_session() as session:
        incident = session.get(Incident, repair_module.INCIDENT_ID)
        assert incident.asset is None
