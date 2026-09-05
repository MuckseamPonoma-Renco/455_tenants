from __future__ import annotations

import hashlib
import json
from collections import Counter

from packages.db import FilingJob, Incident, MessageDecision, RawMessage, get_session
import scripts.certify_20260905_full_archive_reviews as certifier
from scripts.audit_whatsapp_export_decisions import llm_review_details


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _incident(incident_id: str) -> Incident:
    return Incident(
        incident_id=incident_id,
        category="elevator",
        asset="elevator_north",
        severity=2,
        status="open",
        title="Test incident",
        summary="",
        proof_refs="",
        report_count=1,
        witness_count=1,
        confidence=80,
        needs_review=False,
    )


def _raw(message_id: str, text: str, ordinal: int) -> RawMessage:
    return RawMessage(
        message_id=message_id,
        chat_name="Tenants WhatsApp",
        sender="Tenant",
        sender_hash=f"sender-{ordinal}",
        ts_iso=f"2026-01-01T00:{ordinal % 60:02d}:00Z",
        ts_epoch=ordinal,
        text=text,
        source="test",
    )


def _decision(
    message_id: str,
    *,
    is_issue: bool = False,
    category: str | None = None,
    asset: str | None = None,
    event_type: str | None = None,
    incident_id: str | None = None,
    final_extra: dict[str, object] | None = None,
) -> MessageDecision:
    final = {
        "is_issue": is_issue,
        "category": category,
        "asset": asset,
        "event_type": event_type,
        "signal_type": "report" if is_issue else "nonissue",
    }
    final.update(final_extra or {})
    return MessageDecision(
        message_id=message_id,
        incident_id=incident_id,
        chosen_source="rules",
        is_issue=is_issue,
        category=category,
        event_type=event_type,
        confidence=77,
        needs_review=False,
        auto_file_candidate=True,
        rules_json=json.dumps({"keep_rules": True}),
        llm_json=json.dumps({"review_status": "disabled", "keep_llm": True}),
        final_json=json.dumps(final),
    )


def _review(
    message_id: str,
    text: str,
    ordinal: int,
    *,
    outcome: str = "unchanged_correct",
    is_issue: bool = False,
    category: str | None = None,
    asset: str | None = None,
    event_type: str | None = None,
    incident_id: str | None = None,
) -> dict[str, object]:
    return {
        "export_ordinal": ordinal,
        "message_id": message_id,
        "raw_text_sha256": _sha(text),
        "review_outcome": outcome,
        "expected_decision": {
            "is_issue": is_issue,
            "category": category,
            "asset": asset,
            "event_type": event_type,
            "incident_id": incident_id,
        },
        "audit_rationale": f"Individually reviewed row {ordinal}.",
    }


def _write_ledger(tmp_path, reviews: list[dict[str, object]]):
    counts = Counter(row["review_outcome"] for row in reviews)
    payload = {
        "schema_version": 1,
        "certification_id": certifier.CERTIFICATION_ID,
        "reviewed_by": certifier.REVIEWED_BY,
        "expected_counts": {
            "total": len(reviews),
            "unchanged_correct": counts["unchanged_correct"],
            "deferred_missing_evidence": counts["deferred_missing_evidence"],
        },
        "reviews": reviews,
    }
    path = tmp_path / "review-ledger.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def _set_scope(monkeypatch, *, unchanged: int, deferred: int) -> None:
    monkeypatch.setattr(
        certifier,
        "EXPECTED_OUTCOME_COUNTS",
        {"unchanged_correct": unchanged, "deferred_missing_evidence": deferred},
    )


def _json(value: str | None) -> dict[str, object]:
    return json.loads(value or "{}")


def test_committed_ledger_is_the_exact_closed_world_scope():
    ledger = certifier.load_review_ledger()
    outcomes = Counter(entry.review_outcome for entry in ledger.entries)
    ids = {entry.message_id for entry in ledger.entries}

    assert len(ledger.entries) == 206
    assert outcomes == {"unchanged_correct": 199, "deferred_missing_evidence": 7}
    assert len(ids) == 206
    assert "c400f5b97089a35f4fcd6e6304ac9375f37a0d66a8d6f99bf8d1c9b05cfcbcad" not in ids
    assert "4ddabad9aedeb1e362ad048e14fb6978f38cf71d7d2755535aad59b58ba793cc" not in ids
    assert "9a29f78e3fab730079ae60903db277fa3d59f138fbb14845be2149db2e385f03" not in ids
    assert all(len(entry.raw_text_sha256) == 64 for entry in ledger.entries)


def test_apply_succeeds_and_preserves_decision_semantics_and_final_fields(
    client, monkeypatch, tmp_path
):
    unchanged_id = "a" * 64
    deferred_id = "b" * 64
    incident_id = "incident-exact"
    unchanged_text = "Acknowledged."
    deferred_text = "North elevator context needs a reply anchor."
    reviews = [
        _review(unchanged_id, unchanged_text, 1),
        _review(
            deferred_id,
            deferred_text,
            2,
            outcome="deferred_missing_evidence",
            is_issue=True,
            category="elevator",
            asset="elevator_north",
            event_type="outage",
            incident_id=incident_id,
        ),
    ]
    ledger_path = _write_ledger(tmp_path, reviews)
    _set_scope(monkeypatch, unchanged=1, deferred=1)
    monkeypatch.setattr(certifier, "_now_iso", lambda: "2026-09-05T20:00:00Z")
    audit_calls: list[tuple[object, ...]] = []
    monkeypatch.setattr(certifier, "append_audit_event", lambda *args: audit_calls.append(args))
    monkeypatch.setattr(certifier, "daily_hash_chain", lambda: audit_calls.append(("chain",)))

    with get_session() as session:
        session.add(_incident(incident_id))
        session.add_all(
            [
                _raw(unchanged_id, unchanged_text, 1),
                _raw(deferred_id, deferred_text, 2),
                _decision(
                    unchanged_id,
                    final_extra={"unrelated": {"nested": [1, 2, 3]}},
                ),
                _decision(
                    deferred_id,
                    is_issue=True,
                    category="elevator",
                    asset="elevator_north",
                    event_type="outage",
                    incident_id=incident_id,
                    final_extra={"unrelated": "preserve me"},
                ),
            ]
        )
        session.commit()

    result = certifier.certify(apply=True, ledger_path=ledger_path)

    assert result["errors"] == []
    assert result["applied"] is True
    assert result["changed"] is True
    assert result["to_certify"] == [unchanged_id, deferred_id]
    assert audit_calls[0][0] == "FULL_ARCHIVE_MANUAL_REVIEW_CERTIFIED"
    assert audit_calls[0][2]["outcome_counts"] == {
        "unchanged_correct": 1,
        "deferred_missing_evidence": 1,
    }
    assert audit_calls[-1] == ("chain",)

    with get_session() as session:
        unchanged = session.get(MessageDecision, unchanged_id)
        deferred = session.get(MessageDecision, deferred_id)
        unchanged_final = _json(unchanged.final_json)
        deferred_final = _json(deferred.final_json)

        assert (unchanged.is_issue, unchanged.category, unchanged.event_type, unchanged.incident_id) == (
            False,
            None,
            None,
            None,
        )
        assert unchanged_final["unrelated"] == {"nested": [1, 2, 3]}
        assert unchanged_final["review_outcome"] == "unchanged_correct"
        assert unchanged_final["review_status"] == "completed"
        assert unchanged.chosen_source == certifier.CHOSEN_SOURCE
        assert unchanged.needs_review is False
        assert llm_review_details(unchanged, text=unchanged_text)[0] == "completed"

        assert (deferred.is_issue, deferred.category, deferred.event_type, deferred.incident_id) == (
            True,
            "elevator",
            "outage",
            incident_id,
        )
        assert deferred_final["asset"] == "elevator_north"
        assert deferred_final["unrelated"] == "preserve me"
        assert deferred_final["review_outcome"] == "deferred_missing_evidence"
        assert deferred_final["review_status"] == "completed"
        assert deferred.chosen_source == certifier.CHOSEN_SOURCE
        assert deferred.needs_review is True
        assert llm_review_details(deferred, text=deferred_text)[0] == "completed"
        assert deferred.auto_file_candidate is True
        assert _json(deferred.rules_json) == {"keep_rules": True}
        assert _json(deferred.llm_json) == {"review_status": "disabled", "keep_llm": True}
        assert session.query(FilingJob).count() == 0


def test_dry_run_is_read_only_and_emits_no_audit_event(client, monkeypatch, tmp_path):
    message_id = "4" * 64
    text = "No condition stated."
    ledger_path = _write_ledger(tmp_path, [_review(message_id, text, 1)])
    _set_scope(monkeypatch, unchanged=1, deferred=0)
    audit_calls = []
    monkeypatch.setattr(certifier, "append_audit_event", lambda *args: audit_calls.append(args))
    monkeypatch.setattr(certifier, "daily_hash_chain", lambda: audit_calls.append(("chain",)))
    with get_session() as session:
        session.add_all([_raw(message_id, text, 1), _decision(message_id)])
        session.commit()

    result = certifier.certify(apply=False, ledger_path=ledger_path)

    assert result["errors"] == []
    assert result["applied"] is False
    assert result["changed"] is False
    assert result["to_certify"] == [message_id]
    assert audit_calls == []
    with get_session() as session:
        decision = session.get(MessageDecision, message_id)
        assert decision.chosen_source == "rules"
        assert "review_status" not in _json(decision.final_json)


def test_apply_is_idempotent_and_does_not_duplicate_audit_event(client, monkeypatch, tmp_path):
    message_id = "c" * 64
    text = "Thanks."
    ledger_path = _write_ledger(tmp_path, [_review(message_id, text, 1)])
    _set_scope(monkeypatch, unchanged=1, deferred=0)
    audit_calls: list[tuple[object, ...]] = []
    monkeypatch.setattr(certifier, "append_audit_event", lambda *args: audit_calls.append(args))
    monkeypatch.setattr(certifier, "daily_hash_chain", lambda: audit_calls.append(("chain",)))
    with get_session() as session:
        session.add_all([_raw(message_id, text, 1), _decision(message_id)])
        session.commit()

    first = certifier.certify(apply=True, ledger_path=ledger_path)
    with get_session() as session:
        first_reviewed_at = _json(session.get(MessageDecision, message_id).final_json)["reviewed_at"]
    audit_calls.clear()
    second = certifier.certify(apply=True, ledger_path=ledger_path)

    assert first["changed"] is True
    assert second["errors"] == []
    assert second["applied"] is True
    assert second["changed"] is False
    assert second["to_certify"] == []
    assert second["already_certified"] == [message_id]
    assert audit_calls == []
    with get_session() as session:
        assert _json(session.get(MessageDecision, message_id).final_json)["reviewed_at"] == first_reviewed_at


def test_hash_drift_fails_closed_without_partial_provenance(client, monkeypatch, tmp_path):
    first_id = "d" * 64
    second_id = "e" * 64
    reviews = [
        _review(first_id, "Reviewed first.", 1),
        _review(second_id, "Reviewed second.", 2),
    ]
    ledger_path = _write_ledger(tmp_path, reviews)
    _set_scope(monkeypatch, unchanged=2, deferred=0)
    audit_calls = []
    monkeypatch.setattr(certifier, "append_audit_event", lambda *args: audit_calls.append(args))
    monkeypatch.setattr(certifier, "daily_hash_chain", lambda: audit_calls.append(("chain",)))
    with get_session() as session:
        session.add_all(
            [
                _raw(first_id, "Reviewed first.", 1),
                _raw(second_id, "Changed after review.", 2),
                _decision(first_id),
                _decision(second_id),
            ]
        )
        session.commit()

    result = certifier.certify(apply=True, ledger_path=ledger_path)

    assert result["applied"] is False
    assert result["changed"] is False
    assert any("raw-text SHA256 mismatch" in error for error in result["errors"])
    assert audit_calls == []
    with get_session() as session:
        assert session.get(MessageDecision, first_id).chosen_source == "rules"
        assert "review_status" not in _json(session.get(MessageDecision, first_id).final_json)


def test_decision_drift_fails_closed_without_partial_provenance(client, monkeypatch, tmp_path):
    first_id = "f" * 64
    second_id = "1" * 64
    reviews = [
        _review(first_id, "Reviewed first.", 1),
        _review(second_id, "Reviewed second.", 2),
    ]
    ledger_path = _write_ledger(tmp_path, reviews)
    _set_scope(monkeypatch, unchanged=2, deferred=0)
    monkeypatch.setattr(certifier, "append_audit_event", lambda *_args: None)
    monkeypatch.setattr(certifier, "daily_hash_chain", lambda: None)
    with get_session() as session:
        session.add_all(
            [
                _raw(first_id, "Reviewed first.", 1),
                _raw(second_id, "Reviewed second.", 2),
                _decision(first_id),
                _decision(second_id, final_extra={"asset": "drifted_asset"}),
            ]
        )
        session.commit()

    result = certifier.certify(apply=True, ledger_path=ledger_path)

    assert result["applied"] is False
    assert result["changed"] is False
    assert any("decision state drift" in error for error in result["errors"])
    with get_session() as session:
        assert session.get(MessageDecision, first_id).chosen_source == "rules"
        assert "review_status" not in _json(session.get(MessageDecision, first_id).final_json)


def test_mid_apply_failure_rolls_back_every_provenance_change(client, monkeypatch, tmp_path):
    first_id = "2" * 64
    second_id = "3" * 64
    reviews = [
        _review(first_id, "Reviewed first.", 1),
        _review(second_id, "Reviewed second.", 2),
    ]
    ledger_path = _write_ledger(tmp_path, reviews)
    _set_scope(monkeypatch, unchanged=2, deferred=0)
    audit_calls = []
    monkeypatch.setattr(certifier, "append_audit_event", lambda *args: audit_calls.append(args))
    monkeypatch.setattr(certifier, "daily_hash_chain", lambda: audit_calls.append(("chain",)))
    with get_session() as session:
        session.add_all(
            [
                _raw(first_id, "Reviewed first.", 1),
                _raw(second_id, "Reviewed second.", 2),
                _decision(first_id),
                _decision(second_id),
            ]
        )
        session.commit()

    original_apply = certifier._apply_provenance
    calls = 0

    def fail_after_first(*args, **kwargs):
        nonlocal calls
        calls += 1
        original_apply(*args, **kwargs)
        if calls == 1:
            raise RuntimeError("injected failure")

    monkeypatch.setattr(certifier, "_apply_provenance", fail_after_first)
    result = certifier.certify(apply=True, ledger_path=ledger_path)

    assert result["applied"] is False
    assert result["changed"] is False
    assert result["errors"] == [
        "certification transaction failed: RuntimeError: injected failure"
    ]
    assert audit_calls == []
    with get_session() as session:
        for message_id in (first_id, second_id):
            decision = session.get(MessageDecision, message_id)
            assert decision.chosen_source == "rules"
            assert decision.needs_review is False
            assert "review_status" not in _json(decision.final_json)
