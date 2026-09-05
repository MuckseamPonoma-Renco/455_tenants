import hashlib
import json

from packages.db import (
    FilingJob,
    Incident,
    MessageDecision,
    RawMessage,
    ServiceRequestCase,
    get_session,
)
import scripts.repair_20260905_full_archive_semantics as repair_module


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _incident(incident_id: str, *, category: str = "elevator", asset: str | None = None) -> Incident:
    return Incident(
        incident_id=incident_id,
        category=category,
        asset=asset,
        severity=2,
        status="open",
        title=f"Incident {incident_id}",
        summary="",
        proof_refs="",
        report_count=0,
        witness_count=0,
        confidence=80,
        needs_review=False,
    )


def _raw(message_id: str, text: str, ts_epoch: int, sender_hash: str) -> RawMessage:
    return RawMessage(
        message_id=message_id,
        chat_name="455 Tenants",
        sender="Tenant",
        sender_hash=sender_hash,
        ts_iso=f"2026-01-01T00:{ts_epoch % 60:02d}:00Z",
        ts_epoch=ts_epoch,
        text=text,
        source="test",
    )


def _decision(
    message_id: str,
    *,
    incident_id: str | None,
    is_issue: bool,
    category: str | None,
    event_type: str | None,
) -> MessageDecision:
    return MessageDecision(
        message_id=message_id,
        incident_id=incident_id,
        chosen_source="legacy",
        is_issue=is_issue,
        category=category,
        event_type=event_type,
        confidence=70,
        needs_review=False,
        auto_file_candidate=True,
        final_json=json.dumps(
            {
                "is_issue": is_issue,
                "category": category,
                "event_type": event_type,
            }
        ),
    )


def _patch_plan(
    monkeypatch,
    corrections: tuple[repair_module.DecisionCorrection, ...],
    *,
    migrations: dict[str, str] | None = None,
    service_request_migrations: tuple[repair_module.ServiceRequestMigration, ...] | None = None,
    new_incidents: dict[str, repair_module.NewIncidentSpec] | None = None,
    overrides: dict[str, repair_module.IncidentOverride] | None = None,
):
    monkeypatch.setattr(repair_module, "CORRECTIONS", corrections)
    monkeypatch.setattr(repair_module, "INCIDENT_MIGRATIONS", migrations or {})
    monkeypatch.setattr(
        repair_module,
        "SERVICE_REQUEST_MIGRATIONS",
        service_request_migrations or (),
    )
    monkeypatch.setattr(repair_module, "NEW_INCIDENTS", new_incidents or {})
    monkeypatch.setattr(repair_module, "INCIDENT_OVERRIDES", overrides or {})
    monkeypatch.setattr(repair_module, "ARCHIVE_OCCURRENCE_ALIGNMENT", {})
    monkeypatch.setattr(repair_module, "DEFERRED_REVIEW", {})


def test_production_plan_is_exact_and_excludes_deferred_multisignal_rows():
    corrections = repair_module.CORRECTIONS
    ids = [row.message_id for row in corrections]

    assert len(corrections) == 47
    assert len(ids) == len(set(ids))
    assert all(len(row.message_id) == 64 for row in corrections)
    assert all(len(row.text_sha256) == 64 for row in corrections)
    assert all(row.incident_id is not None for row in corrections if row.is_issue)
    assert all(
        row.incident_id is None and row.category is None and row.event_type is None
        for row in corrections
        if not row.is_issue
    )
    assert set(repair_module.DEFERRED_REVIEW).isdisjoint(ids)
    assert set(repair_module.ARCHIVE_OCCURRENCE_ALIGNMENT.values()).issubset(ids)
    assert all(row.evidence_message_id in ids for row in repair_module.SERVICE_REQUEST_MIGRATIONS)


def test_dry_run_is_read_only_and_reports_exact_change(client, monkeypatch):
    message_id = "a" * 64
    text = "Both elevators are still out."
    correction = repair_module._correction(
        message_id,
        _sha(text),
        (True, "old", "elevator", "outage"),
        (True, "old", "elevator", "elevator_both", "still_out"),
        "Continuation, not a new outage.",
    )
    _patch_plan(monkeypatch, (correction,))
    audit_events = []
    monkeypatch.setattr(repair_module, "append_audit_event", lambda *args: audit_events.append(args))
    monkeypatch.setattr(repair_module, "daily_hash_chain", lambda: audit_events.append(("chain",)))

    with get_session() as session:
        session.add(_incident("old"))
        session.add(_raw(message_id, text, 100, "tenant-a"))
        session.add(
            _decision(
                message_id,
                incident_id="old",
                is_issue=True,
                category="elevator",
                event_type="outage",
            )
        )
        session.commit()

    result = repair_module.repair(apply=False)

    assert result["errors"] == []
    assert result["applied"] is False
    assert result["message_ids_to_change"] == [message_id]
    assert audit_events == []
    with get_session() as session:
        decision = session.get(MessageDecision, message_id)
        assert decision.event_type == "outage"
        assert decision.auto_file_candidate is True


def test_hash_mismatch_fails_closed_without_partial_mutation(client, monkeypatch):
    first_id = "b" * 64
    second_id = "c" * 64
    corrections = (
        repair_module._correction(
            first_id,
            _sha("reviewed first"),
            (False, None, None, None),
            (True, "target", "elevator", "elevator_both", "status_update"),
            "Verified status.",
        ),
        repair_module._correction(
            second_id,
            _sha("reviewed second"),
            (False, None, None, None),
            (True, "target", "elevator", "elevator_both", "status_update"),
            "Verified status.",
        ),
    )
    _patch_plan(monkeypatch, corrections)
    monkeypatch.setattr(repair_module, "append_audit_event", lambda *_args: None)
    monkeypatch.setattr(repair_module, "daily_hash_chain", lambda: None)

    with get_session() as session:
        session.add(_incident("target"))
        session.add_all(
            [
                _raw(first_id, "reviewed first", 100, "tenant-a"),
                _raw(second_id, "text changed after review", 101, "tenant-b"),
                _decision(first_id, incident_id=None, is_issue=False, category=None, event_type=None),
                _decision(second_id, incident_id=None, is_issue=False, category=None, event_type=None),
            ]
        )
        session.commit()

    result = repair_module.repair(apply=True)

    assert result["applied"] is False
    assert any("raw-text SHA256 mismatch" in error for error in result["errors"])
    with get_session() as session:
        assert session.get(MessageDecision, first_id).is_issue is False
        assert session.get(MessageDecision, second_id).is_issue is False
        assert session.query(FilingJob).count() == 0


def test_apply_migrates_protected_refs_prunes_false_positive_and_recomputes(client, monkeypatch):
    canonical_id = "d" * 64
    relink_id = "e" * 64
    false_positive_id = "f" * 64
    target_incident_id = "target"
    duplicate_incident_id = "duplicate"
    false_incident_id = "false-positive"
    corrections = (
        repair_module._correction(
            relink_id,
            _sha("Both are still out."),
            (True, duplicate_incident_id, "elevator", "outage"),
            (True, target_incident_id, "elevator", "elevator_both", "still_out"),
            "Continuation belongs to canonical outage.",
        ),
        repair_module._correction(
            false_positive_id,
            _sha("General advice only."),
            (True, false_incident_id, "other", "new_issue"),
            (False, None, None, None, None),
            "Advice is not a condition report.",
        ),
    )
    _patch_plan(
        monkeypatch,
        corrections,
        migrations={duplicate_incident_id: target_incident_id},
        overrides={
            target_incident_id: repair_module.IncidentOverride(asset="elevator_both")
        },
    )
    audit_events = []
    monkeypatch.setattr(repair_module, "append_audit_event", lambda *args: audit_events.append(args))
    monkeypatch.setattr(repair_module, "daily_hash_chain", lambda: audit_events.append(("chain",)))

    with get_session() as session:
        session.add_all(
            [
                _incident(target_incident_id, asset="elevator_north"),
                _incident(duplicate_incident_id),
                _incident(false_incident_id, category="other"),
                _raw(canonical_id, "Both elevators are out.", 90, "tenant-a"),
                _raw(relink_id, "Both are still out.", 100, "tenant-b"),
                _raw(false_positive_id, "General advice only.", 110, "tenant-c"),
                _decision(
                    canonical_id,
                    incident_id=target_incident_id,
                    is_issue=True,
                    category="elevator",
                    event_type="outage",
                ),
                _decision(
                    relink_id,
                    incident_id=duplicate_incident_id,
                    is_issue=True,
                    category="elevator",
                    event_type="outage",
                ),
                _decision(
                    false_positive_id,
                    incident_id=false_incident_id,
                    is_issue=True,
                    category="other",
                    event_type="new_issue",
                ),
            ]
        )
        session.flush()
        claimed = FilingJob(
            dedupe_key="claimed-job",
            incident_id=duplicate_incident_id,
            state="claimed",
            job_type="nyc311_file",
        )
        pending = FilingJob(
            dedupe_key="pending-false-positive",
            incident_id=false_incident_id,
            state="pending",
            job_type="nyc311_file",
        )
        session.add_all([claimed, pending])
        session.flush()
        session.add(
            ServiceRequestCase(
                service_request_number="SR-TEST-1",
                incident_id=duplicate_incident_id,
                filing_job_id=claimed.job_id,
                status="submitted",
            )
        )
        session.commit()
        claimed_job_id = claimed.job_id
        pending_job_id = pending.job_id

    result = repair_module.repair(apply=True)

    assert result["errors"] == []
    assert result["applied"] is True
    assert result["filing_jobs_before"] == result["filing_jobs_after"] == 2
    assert audit_events[0][0] == "FULL_ARCHIVE_SEMANTIC_REPAIR"
    assert audit_events[-1] == ("chain",)
    with get_session() as session:
        relinked = session.get(MessageDecision, relink_id)
        removed = session.get(MessageDecision, false_positive_id)
        target = session.get(Incident, target_incident_id)
        claimed = session.get(FilingJob, claimed_job_id)
        pending = session.get(FilingJob, pending_job_id)
        case = session.query(ServiceRequestCase).filter_by(service_request_number="SR-TEST-1").one()

        assert relinked.incident_id == target_incident_id
        assert relinked.event_type == "still_out"
        assert relinked.auto_file_candidate is False
        assert relinked.chosen_source == repair_module.CHOSEN_SOURCE
        assert _json(relinked.final_json)["repair_id"] == repair_module.REPAIR_ID
        assert removed.is_issue is False
        assert removed.incident_id is None
        assert session.get(Incident, duplicate_incident_id) is None
        assert session.get(Incident, false_incident_id) is None
        assert claimed.incident_id == target_incident_id
        assert claimed.state == "claimed"
        assert claimed.dedupe_key == "claimed-job"
        assert case.incident_id == target_incident_id
        assert pending.incident_id is None
        assert pending.state == "skipped"
        assert target.asset == "elevator_both"
        assert target.status == "open"
        assert target.report_count == 2
        assert target.witness_count == 2
        assert set(target.proof_refs.split(",")) == {canonical_id, relink_id}


def _json(value: str | None) -> dict:
    return json.loads(value or "{}")


def test_unmapped_service_request_blocks_false_positive_removal(client, monkeypatch):
    message_id = "1" * 64
    incident_id = "protected-false-positive"
    text = "Reference material only."
    correction = repair_module._correction(
        message_id,
        _sha(text),
        (True, incident_id, "other", "new_issue"),
        (False, None, None, None, None),
        "Not a condition report.",
    )
    _patch_plan(monkeypatch, (correction,))
    monkeypatch.setattr(repair_module, "append_audit_event", lambda *_args: None)
    monkeypatch.setattr(repair_module, "daily_hash_chain", lambda: None)

    with get_session() as session:
        session.add(_incident(incident_id, category="other"))
        session.add(_raw(message_id, text, 100, "tenant"))
        session.add(
            _decision(
                message_id,
                incident_id=incident_id,
                is_issue=True,
                category="other",
                event_type="new_issue",
            )
        )
        session.add(
            ServiceRequestCase(
                service_request_number="SR-PROTECTED",
                incident_id=incident_id,
                status="submitted",
            )
        )
        session.commit()

    result = repair_module.repair(apply=True)

    assert result["applied"] is False
    assert any("protected references" in error for error in result["errors"])
    with get_session() as session:
        assert session.get(MessageDecision, message_id).is_issue is True
        assert session.get(Incident, incident_id) is not None
        assert session.query(ServiceRequestCase).filter_by(service_request_number="SR-PROTECTED").count() == 1


def test_apply_creates_only_the_declared_incident_without_filing(client, monkeypatch):
    message_id = "2" * 64
    incident_id = "declared-washer"
    text = "Washer 16 is covered in dog hair."
    correction = repair_module._correction(
        message_id,
        _sha(text),
        (False, None, None, None),
        (True, incident_id, "laundry", "washer_16", "new_issue"),
        "Verified washer condition.",
    )
    _patch_plan(
        monkeypatch,
        (correction,),
        new_incidents={
            incident_id: repair_module.NewIncidentSpec(
                category="laundry",
                asset="washer_16",
                severity=2,
                title="Washer 16 cleanliness issue",
                summary="Verified washer condition.",
            )
        },
        overrides={
            incident_id: repair_module.IncidentOverride(category="laundry", asset="washer_16")
        },
    )
    monkeypatch.setattr(repair_module, "append_audit_event", lambda *_args: None)
    monkeypatch.setattr(repair_module, "daily_hash_chain", lambda: None)

    with get_session() as session:
        session.add(_raw(message_id, text, 123, "tenant"))
        session.add(
            _decision(
                message_id,
                incident_id=None,
                is_issue=False,
                category=None,
                event_type=None,
            )
        )
        session.commit()

    result = repair_module.repair(apply=True)

    assert result["applied"] is True
    assert result["created_incident_ids"] == [incident_id]
    assert result["filing_jobs_before"] == result["filing_jobs_after"] == 0
    with get_session() as session:
        decision = session.get(MessageDecision, message_id)
        incident = session.get(Incident, incident_id)
        assert decision.is_issue is True
        assert decision.category == "laundry"
        assert decision.event_type == "new_issue"
        assert decision.auto_file_candidate is False
        assert incident.category == "laundry"
        assert incident.asset == "washer_16"
        assert incident.status == "open"
        assert incident.report_count == 1
        assert incident.witness_count == 1
        assert session.query(FilingJob).count() == 0
