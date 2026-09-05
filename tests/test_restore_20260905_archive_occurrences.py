from __future__ import annotations

import csv
import hashlib
import json
import zipfile
from pathlib import Path

from packages.audit import sender_hash
from packages.db import (
    FilingJob,
    Incident,
    MessageDecision,
    RawMessage,
    ServiceRequestCase,
    WatchdogAction,
    get_session,
)
from scripts.audit_whatsapp_export_decisions import iter_export_messages, run_audit
import scripts.restore_20260905_archive_occurrences as restore_module


def _sha(value: str | bytes) -> str:
    if isinstance(value, str):
        value = value.encode("utf-8")
    return hashlib.sha256(value).hexdigest()


def _synthetic_archive(tmp_path: Path) -> tuple[Path, bytes]:
    chat = (
        "[4/24/26, 7:43:48 PM] Tenant: image omitted\n"
        "[4/24/26, 7:43:48 PM] Tenant: image omitted\n"
        "[4/24/26, 7:44:48 PM] Tenant: This message was deleted.\n"
        "[4/24/26, 7:44:48 PM] Tenant: This message was deleted.\n"
        "[4/24/26, 7:45:48 PM] Tenant: And I took a pic 🤭 image omitted\n"
        "[4/24/26, 7:45:48 PM] Tenant: And I took a pic 🤭 image omitted\n"
    ).encode("utf-8")
    path = tmp_path / "locked-export.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("_chat.txt", chat)
    return path, chat


def _patch_synthetic_manifest(monkeypatch, archive_path: Path, chat: bytes):
    messages = iter_export_messages(archive_path, default_chat_name="455 Tenants")
    followups = [row for row in messages if row.physical_occurrence > 1]
    specs = tuple(
        restore_module.RestoreSpec(
            export_ordinal=row.export_ordinal,
            message_id=row.message_id,
            physical_occurrence=row.physical_occurrence,
            text=row.text,
            text_sha256=_sha(row.text),
            decision_kind=(
                "media_placeholder" if row.text == "image omitted" else "manual_nonissue"
            ),
            rationale="synthetic reviewed nonissue",
            allow_media_enrichment=row.text != "This message was deleted.",
        )
        for row in followups
    )
    monkeypatch.setattr(restore_module, "EXPECTED_ARCHIVE_SHA256", _sha(archive_path.read_bytes()))
    monkeypatch.setattr(restore_module, "EXPECTED_CHAT_TEXT_SHA256", _sha(chat))
    monkeypatch.setattr(restore_module, "EXPECTED_PHYSICAL_MESSAGES", len(messages))
    monkeypatch.setattr(restore_module, "EXPECTED_RESTORE_OCCURRENCES", len(specs))
    monkeypatch.setattr(restore_module, "EXPECTED_OCCURRENCES", specs)
    return messages, specs


def _ordinary_nonissue(message_id: str) -> MessageDecision:
    return MessageDecision(
        message_id=message_id,
        chosen_source="test_nonissue",
        is_issue=False,
        category=None,
        event_type="non_issue",
        confidence=100,
        needs_review=False,
        auto_file_candidate=False,
        final_json=json.dumps({"is_issue": False, "event_type": "non_issue"}),
    )


def test_production_restore_manifest_locks_all_20_followups_and_review_kinds():
    specs = restore_module.EXPECTED_OCCURRENCES

    assert len(specs) == restore_module.EXPECTED_RESTORE_OCCURRENCES == 20
    assert len({row.message_id for row in specs}) == 20
    assert len({row.export_ordinal for row in specs}) == 20
    assert sum(row.decision_kind == "media_placeholder" for row in specs) == 18
    assert {
        row.export_ordinal for row in specs if row.decision_kind == "manual_nonissue"
    } == {250, 1713}
    assert all(len(row.message_id) == 64 for row in specs)
    assert all(row.message_id.endswith(f"~{row.physical_occurrence}") for row in specs)
    assert all(_sha(row.text) == row.text_sha256 for row in specs)
    assert restore_module.EXPECTED_ARCHIVE_SHA256 == (
        "7baad13600e3226105ca79773b0961d00cb3bc87c17eff2b7bd5a0e6644c3471"
    )
    assert restore_module.EXPECTED_CHAT_TEXT_SHA256 == (
        "761e219b66245afdd8c0d9a38da97efe5a94d4883e6c35c519f8b7f0d0696b4e"
    )


def test_restore_dry_run_apply_and_rerun_are_exact_atomic_and_auditable(
    client,
    tmp_path: Path,
    monkeypatch,
):
    archive_path, chat = _synthetic_archive(tmp_path)
    messages, specs = _patch_synthetic_manifest(monkeypatch, archive_path, chat)
    audit_events: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        restore_module,
        "append_audit_event",
        lambda *args: audit_events.append(args),
    )
    monkeypatch.setattr(
        restore_module,
        "daily_hash_chain",
        lambda: audit_events.append(("hash_chain",)),
    )

    with get_session() as session:
        for message in messages:
            if message.physical_occurrence > 1:
                continue
            session.add(
                RawMessage(
                    message_id=message.message_id,
                    chat_name=message.chat_name,
                    sender=message.sender,
                    sender_hash=sender_hash(message.sender),
                    ts_iso=message.ts_iso,
                    ts_epoch=message.ts_epoch,
                    text=message.text,
                    attachments=message.attachments,
                    source="zip_import",
                )
            )
            session.flush()
            session.add(_ordinary_nonissue(message.message_id))
        session.commit()

    dry_run = restore_module.restore(archive_path=archive_path, apply=False)
    assert dry_run["ok"] is True
    assert dry_run["applied"] is False
    assert dry_run["to_restore"] == [row.message_id for row in specs]
    with get_session() as session:
        assert all(session.get(RawMessage, row.message_id) is None for row in specs)

    applied = restore_module.restore(archive_path=archive_path, apply=True)
    assert applied["ok"] is True
    assert applied["applied"] is True
    assert applied["changed"] is True
    assert applied["restored_message_ids"] == [row.message_id for row in specs]
    before = applied["database_counts_before"]
    after = applied["database_counts_after"]
    assert after["raw_messages"] - before["raw_messages"] == len(specs)
    assert after["message_decisions"] - before["message_decisions"] == len(specs)
    assert after["incidents"] == before["incidents"]
    assert after["filing_jobs"] == before["filing_jobs"]
    assert after["service_request_cases"] == before["service_request_cases"]
    assert after["watchdog_actions"] == before["watchdog_actions"]
    assert applied["sheet_sync_invoked"] is False
    assert applied["classifier_invoked"] is False
    with get_session() as session:
        assert session.query(Incident).count() == 0
        assert session.query(FilingJob).count() == 0
        assert session.query(ServiceRequestCase).count() == 0
        assert session.query(WatchdogAction).count() == 0
        for spec in specs:
            raw = session.get(RawMessage, spec.message_id)
            decision = session.get(MessageDecision, spec.message_id)
            assert raw is not None and raw.text == spec.text
            assert decision is not None and decision.is_issue is False
            assert decision.incident_id is None
            if spec.decision_kind == "manual_nonissue":
                assert decision.chosen_source == "review_codex_archive_occurrence_restore"

    audit = run_audit(
        archive_path,
        since="2000-01-01",
        out_dir=tmp_path / "audit",
        default_chat_name="455 Tenants",
    )
    assert audit["collision_followup_occurrences"] == len(specs)
    assert audit["missing_db_messages"] == 0
    assert audit["missing_decisions"] == 0
    assert audit["ok"] is True
    with (tmp_path / "audit" / "all_messages.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    restored_rows = [row for row in rows if int(row["physical_occurrence"]) > 1]
    assert [row["matched_message_id"] for row in restored_rows] == [
        row.message_id for row in specs
    ]
    assert all(row["match_method"] == "exact" for row in restored_rows)
    manual_ordinals = {
        str(row.export_ordinal) for row in specs if row.decision_kind == "manual_nonissue"
    }
    assert manual_ordinals == {"4", "6"}
    assert all(
        row["llm_review_status"] == "completed"
        for row in restored_rows
        if row["export_ordinal"] in manual_ordinals
    )

    rerun = restore_module.restore(archive_path=archive_path, apply=True)
    assert rerun["ok"] is True
    assert rerun["applied"] is True
    assert rerun["changed"] is False
    assert rerun["to_restore"] == []
    assert rerun["already_restored"] == [row.message_id for row in specs]
    assert len(audit_events) == 2


def test_restore_rejects_archive_hash_drift_before_database_access(
    client,
    tmp_path: Path,
    monkeypatch,
):
    archive_path, chat = _synthetic_archive(tmp_path)
    _, specs = _patch_synthetic_manifest(monkeypatch, archive_path, chat)
    monkeypatch.setattr(restore_module, "EXPECTED_ARCHIVE_SHA256", "0" * 64)

    result = restore_module.restore(archive_path=archive_path, apply=True)

    assert result["ok"] is False
    assert result["applied"] is False
    assert result["errors"] == ["archive SHA-256 mismatch"]
    with get_session() as session:
        assert all(session.get(RawMessage, row.message_id) is None for row in specs)


def test_restore_rolls_back_every_insert_when_one_existing_occurrence_drifted(
    client,
    tmp_path: Path,
    monkeypatch,
):
    archive_path, chat = _synthetic_archive(tmp_path)
    messages, specs = _patch_synthetic_manifest(monkeypatch, archive_path, chat)
    first = specs[0]
    source = next(row for row in messages if row.export_ordinal == first.export_ordinal)
    with get_session() as session:
        session.add(
            RawMessage(
                message_id=first.message_id,
                chat_name=source.chat_name,
                sender=source.sender,
                sender_hash=sender_hash(source.sender),
                ts_iso=source.ts_iso,
                ts_epoch=source.ts_epoch,
                text="wrong content",
                attachments=source.attachments,
                source="zip_import",
            )
        )
        session.flush()
        session.add(restore_module._decision_for(first, created_at="2026-09-05T12:00:00Z"))
        session.commit()

    result = restore_module.restore(archive_path=archive_path, apply=True)

    assert result["ok"] is False
    assert result["applied"] is False
    assert any("stored raw identity/content differs" in error for error in result["errors"])
    with get_session() as session:
        assert session.get(RawMessage, first.message_id).text == "wrong content"
        assert all(session.get(RawMessage, row.message_id) is None for row in specs[1:])
