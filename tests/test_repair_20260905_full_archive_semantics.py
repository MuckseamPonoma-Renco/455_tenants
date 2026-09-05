import hashlib
import json

from packages.audit import compute_message_id
from packages.db import (
    FilingJob,
    Incident,
    MessageDecision,
    RawMessage,
    ServiceRequestCase,
    WatchdogAction,
    get_session,
)
import scripts.repair_20260905_full_archive_semantics as repair_module
from scripts.audit_whatsapp_export_decisions import llm_review_details


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

    assert len(corrections) == 52
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
    assert len(repair_module.DEFERRED_REVIEW) == 7
    assert set(repair_module.ARCHIVE_OCCURRENCE_ALIGNMENT.values()).issubset(ids)
    assert all(row.evidence_message_id in ids for row in repair_module.SERVICE_REQUEST_MIGRATIONS)


def test_followup_review_entries_are_exactly_hash_locked_and_deferred():
    corrections = {row.message_id: row for row in repair_module.CORRECTIONS}
    expected = {
        "3740c3acfdb84a42dbb89ca1ec1aa9891d21fb952c828204c21e24cd4d609658": {
            "text": "Since leaving my flat it has taken me 40' to board a Manhatten bound train. Most of it spent in 455OP. <This message was edited>",
            "incident_id": "c0dd69323702288da3d2d3a604acee95",
            "category": "elevator",
            "asset": "elevator_south",
            "event_type": "status_update",
            "evidence_refs": (),
        },
        "4ddabad9aedeb1e362ad048e14fb6978f38cf71d7d2755535aad59b58ba793cc": {
            "text": "Our flr. I haven't clean it up yet, cuz a little challenging with a dog with me. <This message was edited>",
            "incident_id": repair_module.STAIRWELL_LITTER_INCIDENT_ID,
            "category": "other",
            "asset": "stairwell_common_area",
            "event_type": "new_issue",
            "evidence_refs": (
                "00001977-PHOTO-2026-04-24-19-43-48.jpg#sha256=2f961f4cfb3fd9f910526a129afee86f038880d8fc347346fbddbf0e90307974",
                "00001978-PHOTO-2026-04-24-19-43-49.jpg#sha256=2d31060b93be4e08c69849a01beeda7230c2aba51bf64448d518bafaf66e384a",
            ),
        },
        "9a29f78e3fab730079ae60903db277fa3d59f138fbb14845be2149db2e385f03": {
            "text": "this building.",
            "incident_id": repair_module.COMMON_AREA_MOUSE_INCIDENT_ID,
            "category": "pests",
            "asset": "common_area",
            "event_type": "new_issue",
            "evidence_refs": (
                "00002140-PHOTO-2026-05-05-19-42-10.jpg#sha256=be8ff59a5bacab36d2263fc6a11534da14fbd9d2d06a7f830e81d98bb71f82fc",
                "00002141-PHOTO-2026-05-05-19-42-11.jpg#sha256=b065f8f785e3b96d9d519e63d55ab1e946232bee61bd5894d74e6695e59fe406",
            ),
        },
        "99171b20bda6d1950e831e6aaf487ccf86baf59c19b73f03e5696fc5c0af8cb4": {
            "text": "Ya, during 1⁄2 time NBA final!",
            "incident_id": "d0b9b945e4ad2911924ca2d192a98499",
            "category": "elevator",
            "asset": "elevator_north",
            "event_type": "status_update",
            "evidence_refs": (),
        },
        "69cdcbb3a4746e2b19112973d16e88476623110e5a4c1f206513a76e992fedea": {
            "text": "If you were out celebrating the Knicks win, you are SOL. <This message was edited>",
            "incident_id": "0ac578e2893619c38387e31bfe8c1cfc",
            "category": "elevator",
            "asset": "elevator_both",
            "event_type": "status_update",
            "evidence_refs": (),
        },
    }
    for message_id, spec in expected.items():
        correction = corrections[message_id]
        assert correction.text_sha256 == _sha(spec["text"])
        assert (
            correction.before_is_issue,
            correction.before_incident_id,
            correction.before_category,
            correction.before_event_type,
        ) == (False, None, None, None)
        assert correction.is_issue is True
        assert correction.incident_id == spec["incident_id"]
        assert correction.category == spec["category"]
        assert correction.asset == spec["asset"]
        assert correction.event_type == spec["event_type"]
        assert correction.evidence_refs == spec["evidence_refs"]

    expected_deferred_ids = {
        "a6f828e0f6650e7ce164651beafdefb0e765b98a9aba96fd42979fc4d8c80c01",
        "073350b00c89f9d7312422500b45dfaeeae9dbf6350b9fbec902cefeb351a25d",
    }
    assert expected_deferred_ids.issubset(repair_module.DEFERRED_REVIEW)
    assert all(repair_module.DEFERRED_REVIEW[message_id].strip() for message_id in expected_deferred_ids)
    assert repair_module.STAIRWELL_LITTER_INCIDENT_ID == compute_message_id(
        "other",
        "stairwell_common_area",
        "4/24/26 10:14:12 PM",
        "Stairwell/common-area litter and debris",
    )[:32]
    assert repair_module.COMMON_AREA_MOUSE_INCIDENT_ID == compute_message_id(
        "pests",
        "common_area",
        "5/5/26 7:42:15 PM",
        "Dead mouse at common-area threshold",
    )[:32]


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
        assert llm_review_details(relinked, text="Both are still out.")[0] == "completed"
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


def test_followup_corrections_dry_run_apply_and_reapply_preserve_history(client, monkeypatch):
    correction_ids = {
        "3740c3acfdb84a42dbb89ca1ec1aa9891d21fb952c828204c21e24cd4d609658",
        "4ddabad9aedeb1e362ad048e14fb6978f38cf71d7d2755535aad59b58ba793cc",
        "9a29f78e3fab730079ae60903db277fa3d59f138fbb14845be2149db2e385f03",
        "99171b20bda6d1950e831e6aaf487ccf86baf59c19b73f03e5696fc5c0af8cb4",
        "69cdcbb3a4746e2b19112973d16e88476623110e5a4c1f206513a76e992fedea",
    }
    corrections = tuple(
        row for row in repair_module.CORRECTIONS if row.message_id in correction_ids
    )
    assert len(corrections) == 5
    new_incident_ids = {
        repair_module.STAIRWELL_LITTER_INCIDENT_ID,
        repair_module.COMMON_AREA_MOUSE_INCIDENT_ID,
    }
    _patch_plan(
        monkeypatch,
        corrections,
        new_incidents={
            incident_id: repair_module.NEW_INCIDENTS[incident_id]
            for incident_id in new_incident_ids
        },
        overrides={
            incident_id: repair_module.INCIDENT_OVERRIDES[incident_id]
            for incident_id in new_incident_ids
        },
    )
    audit_events = []
    monkeypatch.setattr(
        repair_module,
        "append_audit_event",
        lambda *args: audit_events.append(args),
    )
    monkeypatch.setattr(repair_module, "daily_hash_chain", lambda: audit_events.append(("chain",)))

    south_id = "c0dd69323702288da3d2d3a604acee95"
    north_id = "d0b9b945e4ad2911924ca2d192a98499"
    both_id = "0ac578e2893619c38387e31bfe8c1cfc"
    south_outage_id = "7" * 64
    south_restore_id = "8" * 64
    north_outage_id = "9" * 64
    both_outage_id = "0" * 64
    texts = {
        "3740c3acfdb84a42dbb89ca1ec1aa9891d21fb952c828204c21e24cd4d609658":
            "Since leaving my flat it has taken me 40' to board a Manhatten bound train. Most of it spent in 455OP. <This message was edited>",
        "4ddabad9aedeb1e362ad048e14fb6978f38cf71d7d2755535aad59b58ba793cc":
            "Our flr. I haven't clean it up yet, cuz a little challenging with a dog with me. <This message was edited>",
        "9a29f78e3fab730079ae60903db277fa3d59f138fbb14845be2149db2e385f03":
            "this building.",
        "99171b20bda6d1950e831e6aaf487ccf86baf59c19b73f03e5696fc5c0af8cb4":
            "Ya, during 1⁄2 time NBA final!",
        "69cdcbb3a4746e2b19112973d16e88476623110e5a4c1f206513a76e992fedea":
            "If you were out celebrating the Knicks win, you are SOL. <This message was edited>",
    }
    timestamps = {
        "3740c3acfdb84a42dbb89ca1ec1aa9891d21fb952c828204c21e24cd4d609658": 110,
        "4ddabad9aedeb1e362ad048e14fb6978f38cf71d7d2755535aad59b58ba793cc": 310,
        "9a29f78e3fab730079ae60903db277fa3d59f138fbb14845be2149db2e385f03": 610,
        "99171b20bda6d1950e831e6aaf487ccf86baf59c19b73f03e5696fc5c0af8cb4": 210,
        "69cdcbb3a4746e2b19112973d16e88476623110e5a4c1f206513a76e992fedea": 410,
    }

    with get_session() as session:
        south = _incident(south_id, asset="elevator_south")
        north = _incident(north_id, asset="elevator_north")
        both = _incident(both_id, asset="elevator_both")
        north.status = "closed"
        north.end_ts = "2026-01-01T00:05:00Z"
        north.end_ts_epoch = 300
        north.last_ts_epoch = 300
        both.status = "closed"
        both.end_ts = "2026-01-01T00:08:20Z"
        both.end_ts_epoch = 500
        both.last_ts_epoch = 500
        session.add_all([south, north, both])
        session.add_all(
            [
                _raw(south_outage_id, "South elevator is out.", 100, "south-a"),
                _raw(south_restore_id, "South elevator is working again.", 120, "south-b"),
                _raw(north_outage_id, "North elevator is out.", 200, "north-a"),
                _raw(both_outage_id, "Both elevators are out.", 400, "both-a"),
                *[
                    _raw(message_id, texts[message_id], timestamps[message_id], f"review-{index}")
                    for index, message_id in enumerate(texts, start=1)
                ],
            ]
        )
        session.add_all(
            [
                _decision(
                    south_outage_id,
                    incident_id=south_id,
                    is_issue=True,
                    category="elevator",
                    event_type="outage",
                ),
                _decision(
                    south_restore_id,
                    incident_id=south_id,
                    is_issue=True,
                    category="elevator",
                    event_type="restore",
                ),
                _decision(
                    north_outage_id,
                    incident_id=north_id,
                    is_issue=True,
                    category="elevator",
                    event_type="outage",
                ),
                _decision(
                    both_outage_id,
                    incident_id=both_id,
                    is_issue=True,
                    category="elevator",
                    event_type="outage",
                ),
                *[
                    _decision(
                        message_id,
                        incident_id=None,
                        is_issue=False,
                        category=None,
                        event_type=None,
                    )
                    for message_id in texts
                ],
            ]
        )
        session.add_all(
            [
                FilingJob(
                    dedupe_key=f"existing-{incident_id}",
                    incident_id=incident_id,
                    state="submitted",
                    job_type="nyc311_file",
                )
                for incident_id in (south_id, north_id, both_id)
            ]
        )
        session.add(
            WatchdogAction(
                action_type="existing_history",
                severity="info",
                title="Existing historical action",
                status="completed",
                related_incident_id=both_id,
            )
        )
        session.commit()

    dry_run = repair_module.repair(apply=False)

    assert dry_run["errors"] == []
    assert dry_run["applied"] is False
    assert set(dry_run["message_ids_to_change"]) == correction_ids
    assert set(dry_run["would_create_incident_ids"]) == new_incident_ids
    assert audit_events == []
    with get_session() as session:
        assert all(session.get(MessageDecision, message_id).is_issue is False for message_id in correction_ids)
        assert session.query(FilingJob).count() == 3
        assert session.query(WatchdogAction).count() == 1
        assert session.get(Incident, north_id).end_ts_epoch == 300
        assert session.get(Incident, both_id).end_ts_epoch == 500

    applied = repair_module.repair(apply=True)

    assert applied["errors"] == []
    assert applied["applied"] is True
    assert applied["changed"] is True
    assert set(applied["message_ids_to_change"]) == correction_ids
    assert set(applied["created_incident_ids"]) == new_incident_ids
    assert applied["filing_jobs_before"] == applied["filing_jobs_after"] == 3
    assert applied["watchdog_actions_before"] == applied["watchdog_actions_after"] == 1
    assert audit_events[0][0] == "FULL_ARCHIVE_SEMANTIC_REPAIR"
    assert audit_events[-1] == ("chain",)
    audit_payload = audit_events[0][2]
    assert set(audit_payload["correction_review"]) == correction_ids
    assert len(
        audit_payload["correction_review"][
            "4ddabad9aedeb1e362ad048e14fb6978f38cf71d7d2755535aad59b58ba793cc"
        ]["evidence_refs"]
    ) == 2
    assert len(
        audit_payload["correction_review"][
            "9a29f78e3fab730079ae60903db277fa3d59f138fbb14845be2149db2e385f03"
        ]["evidence_refs"]
    ) == 2
    with get_session() as session:
        decisions = {message_id: session.get(MessageDecision, message_id) for message_id in correction_ids}
        expectations = {
            "3740c3acfdb84a42dbb89ca1ec1aa9891d21fb952c828204c21e24cd4d609658":
                (south_id, "elevator", "elevator_south", "status_update"),
            "4ddabad9aedeb1e362ad048e14fb6978f38cf71d7d2755535aad59b58ba793cc":
                (repair_module.STAIRWELL_LITTER_INCIDENT_ID, "other", "stairwell_common_area", "new_issue"),
            "9a29f78e3fab730079ae60903db277fa3d59f138fbb14845be2149db2e385f03":
                (repair_module.COMMON_AREA_MOUSE_INCIDENT_ID, "pests", "common_area", "new_issue"),
            "99171b20bda6d1950e831e6aaf487ccf86baf59c19b73f03e5696fc5c0af8cb4":
                (north_id, "elevator", "elevator_north", "status_update"),
            "69cdcbb3a4746e2b19112973d16e88476623110e5a4c1f206513a76e992fedea":
                (both_id, "elevator", "elevator_both", "status_update"),
        }
        for message_id, (incident_id, category, asset, event_type) in expectations.items():
            decision = decisions[message_id]
            assert decision.is_issue is True
            assert decision.incident_id == incident_id
            assert decision.category == category
            assert decision.event_type == event_type
            assert _json(decision.final_json)["asset"] == asset
        assert all(decision.auto_file_candidate is False for decision in decisions.values())
        assert all(decision.chosen_source == repair_module.CHOSEN_SOURCE for decision in decisions.values())
        assert len(
            _json(
                decisions[
                    "4ddabad9aedeb1e362ad048e14fb6978f38cf71d7d2755535aad59b58ba793cc"
                ].final_json
            )["repair_evidence_refs"]
        ) == 2
        assert len(
            _json(
                decisions[
                    "9a29f78e3fab730079ae60903db277fa3d59f138fbb14845be2149db2e385f03"
                ].final_json
            )["repair_evidence_refs"]
        ) == 2

        south = session.get(Incident, south_id)
        north = session.get(Incident, north_id)
        both = session.get(Incident, both_id)
        litter = session.get(Incident, repair_module.STAIRWELL_LITTER_INCIDENT_ID)
        mouse = session.get(Incident, repair_module.COMMON_AREA_MOUSE_INCIDENT_ID)
        assert (south.status, south.end_ts_epoch, south.last_ts_epoch, south.report_count) == (
            "closed",
            120,
            120,
            3,
        )
        assert (north.status, north.end_ts_epoch, north.last_ts_epoch, north.report_count) == (
            "closed",
            300,
            300,
            2,
        )
        assert (both.status, both.end_ts_epoch, both.last_ts_epoch, both.report_count) == (
            "closed",
            500,
            500,
            2,
        )
        assert (litter.category, litter.asset, litter.status, litter.end_ts_epoch, litter.report_count) == (
            "other",
            "stairwell_common_area",
            "open",
            None,
            1,
        )
        assert litter.title == "Stairwell/common-area litter and debris"
        assert litter.summary == "Stair landing/common-area litter and debris were reported left uncleaned."
        assert (mouse.category, mouse.asset, mouse.status, mouse.end_ts_epoch, mouse.report_count) == (
            "pests",
            "common_area",
            "open",
            None,
            1,
        )
        assert mouse.title == "Dead mouse at common-area threshold"
        assert mouse.summary == "A dead mouse was reported at a common-area building threshold."
        assert session.query(Incident).count() == 5
        updated_at = {
            south_id: south.updated_at,
            north_id: north.updated_at,
            both_id: both.updated_at,
            repair_module.STAIRWELL_LITTER_INCIDENT_ID: litter.updated_at,
            repair_module.COMMON_AREA_MOUSE_INCIDENT_ID: mouse.updated_at,
        }
        job_rows = [(row.job_id, row.dedupe_key, row.incident_id, row.state) for row in session.query(FilingJob).order_by(FilingJob.job_id)]
        action_rows = [(row.id, row.action_type, row.related_incident_id, row.status) for row in session.query(WatchdogAction).order_by(WatchdogAction.id)]

    audit_event_count = len(audit_events)
    reapplied = repair_module.repair(apply=True)

    assert reapplied["errors"] == []
    assert reapplied["applied"] is True
    assert reapplied["changed"] is False
    assert reapplied["message_ids_to_change"] == []
    assert reapplied["materialized_incident_ids"] == []
    assert reapplied["filing_jobs_before"] == reapplied["filing_jobs_after"] == 3
    assert reapplied["watchdog_actions_before"] == reapplied["watchdog_actions_after"] == 1
    assert len(audit_events) == audit_event_count
    with get_session() as session:
        assert {
            incident_id: session.get(Incident, incident_id).updated_at
            for incident_id in (
                south_id,
                north_id,
                both_id,
                repair_module.STAIRWELL_LITTER_INCIDENT_ID,
                repair_module.COMMON_AREA_MOUSE_INCIDENT_ID,
            )
        } == updated_at
        assert [(row.job_id, row.dedupe_key, row.incident_id, row.state) for row in session.query(FilingJob).order_by(FilingJob.job_id)] == job_rows
        assert [(row.id, row.action_type, row.related_incident_id, row.status) for row in session.query(WatchdogAction).order_by(WatchdogAction.id)] == action_rows
