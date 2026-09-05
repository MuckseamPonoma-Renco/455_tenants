from __future__ import annotations

import json
import zipfile
from pathlib import Path

from packages.audit import sender_hash
from packages.db import Incident, MessageDecision, RawMessage, get_session
from packages.timeutil import parse_ts_to_epoch
from packages.whatsapp.attachments import (
    attachment_items,
    build_attachment_manifest,
    make_attachment_item,
    merge_attachment_manifests,
)
from packages.whatsapp.parser import ParsedMessage
from scripts.audit_whatsapp_export_decisions import iter_export_messages, run_audit
from scripts.import_whatsapp_media_zip import (
    MediaMessage,
    _compatible_placeholder_kinds,
    _exact_placeholder_targets,
    _media_aliases_by_export_filename,
    _merge_placeholder_media,
    import_media_zip,
)


SENDER = "Tenant One"
TS_EPOCH = 1_777_000_000


def _raw(
    message_id: str,
    *,
    text: str,
    source: str,
    attachments: str | None = None,
    sender: str = SENDER,
    ts_epoch: int = TS_EPOCH,
    chat_name: str = "Tenants WhatsApp",
) -> RawMessage:
    return RawMessage(
        message_id=message_id,
        chat_name=chat_name,
        sender=sender,
        sender_hash=sender_hash(sender),
        ts_iso="2026-04-24T23:43:48Z",
        ts_epoch=ts_epoch,
        text=text,
        attachments=attachments,
        source=source,
    )


def _manifest(*filenames: str) -> str:
    manifest = build_attachment_manifest(
        items=[
            make_attachment_item(
                kind="image",
                status="downloaded",
                path=f"/private/media/{filename}",
                filename=filename,
                extra={"export_filename": filename},
            )
            for filename in filenames
        ],
        source="whatsapp_export",
    )
    assert manifest is not None
    return manifest


def _synthetic_nonissue_decision(message_id: str) -> MessageDecision:
    return MessageDecision(
        message_id=message_id,
        chosen_source="media_attachment",
        is_issue=False,
        category=None,
        event_type="non_issue",
        confidence=95,
        needs_review=False,
        auto_file_candidate=False,
        rules_json=json.dumps({"kind": "media_attachment"}),
        llm_json=json.dumps({}),
        final_json=json.dumps({"is_issue": False, "event_type": "non_issue"}),
    )


def _target_decision(message_id: str) -> MessageDecision:
    return MessageDecision(
        message_id=message_id,
        chosen_source="rules",
        is_issue=False,
        event_type="non_issue",
    )


def _stats() -> dict[str, int]:
    return {
        "merged_existing": 0,
        "matched_placeholder_media_messages": 0,
        "ambiguous_placeholder_media_messages": 0,
        "reconciled_media_alias_rows": 0,
        "retained_media_alias_rows": 0,
        "normalized_media_alias_rows": 0,
        "blocked_media_alias_rows": 0,
    }


def _media_message(*, message_id: str, occurrence: int, filename: str) -> MediaMessage:
    manifest = _manifest(filename)
    return MediaMessage(
        index=0,
        message_id=message_id,
        physical_occurrence=occurrence,
        parsed=ParsedMessage(
            chat_name="455 Tenants",
            sender=SENDER,
            ts_iso="4/24/26 7:43:48 PM",
            text="image omitted",
            attachments=filename,
        ),
        ts_epoch=TS_EPOCH,
        ts_iso="2026-04-24T23:43:48Z",
        attachment_names=[filename],
        manifest=manifest,
        text_for_storage="Photo attached",
    )


def test_placeholder_target_uses_media_kind_and_physical_occurrence(client):
    first_id = "a" * 64
    second_id = "a" * 62 + "~2"
    video_id = "b" * 64
    with get_session() as session:
        session.add_all(
            [
                _raw(first_id, text="image omitted", source="zip_import"),
                _raw(second_id, text="image omitted", source="export"),
                _raw(video_id, text="video omitted", source="zip_import"),
            ]
        )
        session.commit()

    message = _media_message(message_id="c" * 62 + "~2", occurrence=2, filename="0001-PHOTO.jpg")
    with get_session() as session:
        targets = _exact_placeholder_targets(session, message)

    assert [row.message_id for row in targets] == [second_id]


def test_gif_file_matches_text_only_gif_placeholder(client):
    placeholder_id = "1" * 64
    with get_session() as session:
        session.add(_raw(placeholder_id, text="gif omitted", source="zip_import"))
        session.commit()

    message = _media_message(message_id="2" * 64, occurrence=1, filename="0001-animation.gif")
    assert "gif" in _compatible_placeholder_kinds(message)
    with get_session() as session:
        assert [row.message_id for row in _exact_placeholder_targets(session, message)] == [
            placeholder_id
        ]


def test_media_merge_retains_multiple_aliases_and_transfers_their_full_manifests(client):
    placeholder_id = "d" * 64
    alias_id = "e" * 64
    second_alias_id = "8" * 64
    filename = "0002-PHOTO.jpg"
    earlier_filename = "0001-PHOTO.jpg"
    message = _media_message(message_id=alias_id, occurrence=1, filename=filename)
    second_alias_manifest = merge_attachment_manifests(
        message.manifest,
        _manifest(earlier_filename),
    )
    with get_session() as session:
        session.add(_raw(placeholder_id, text="image omitted", source="zip_import", attachments="omitted:image"))
        session.add(_raw(alias_id, text="Photo attached", source="export_media", attachments=message.manifest))
        session.add(
            _raw(
                second_alias_id,
                text="Photo attached",
                source="export_media",
                attachments=second_alias_manifest,
            )
        )
        session.add_all(
            [
                _target_decision(placeholder_id),
                _synthetic_nonissue_decision(alias_id),
                _synthetic_nonissue_decision(second_alias_id),
            ]
        )
        session.commit()

    stats = _stats()
    with get_session() as session:
        merged = _merge_placeholder_media(
            session,
            message,
            stats=stats,
            aliases_by_export_filename=_media_aliases_by_export_filename(session),
            accounted_alias_ids=set(),
        )
        session.commit()

    assert merged is True
    assert stats == {
        "merged_existing": 1,
        "matched_placeholder_media_messages": 1,
        "ambiguous_placeholder_media_messages": 0,
        "reconciled_media_alias_rows": 2,
        "retained_media_alias_rows": 2,
        "normalized_media_alias_rows": 2,
        "blocked_media_alias_rows": 0,
    }
    with get_session() as session:
        canonical = session.get(RawMessage, placeholder_id)
        assert canonical is not None
        assert session.get(RawMessage, alias_id).text == "image omitted"
        assert session.get(RawMessage, second_alias_id).text == "image omitted"
        assert session.get(MessageDecision, alias_id) is not None
        assert session.get(MessageDecision, second_alias_id) is not None
        assert {
            item.get("export_filename")
            for item in attachment_items(canonical.attachments)
            if item.get("export_filename")
        } == {filename, earlier_filename}


def test_media_merge_refuses_to_delete_issue_alias(client):
    placeholder_id = "f" * 64
    alias_id = "9" * 64
    filename = "0003-PHOTO.jpg"
    message = _media_message(message_id=alias_id, occurrence=1, filename=filename)
    with get_session() as session:
        session.add(_raw(placeholder_id, text="image omitted", source="zip_import"))
        session.add(_raw(alias_id, text="Photo attached", source="export_media", attachments=message.manifest))
        session.add_all(
            [
                _target_decision(placeholder_id),
                MessageDecision(
                    message_id=alias_id,
                    chosen_source="manual",
                    is_issue=True,
                    category="other",
                    event_type="new_issue",
                ),
            ]
        )
        session.commit()

    stats = _stats()
    with get_session() as session:
        assert _merge_placeholder_media(
            session,
            message,
            stats=stats,
            aliases_by_export_filename=_media_aliases_by_export_filename(session),
            accounted_alias_ids=set(),
        )
        session.commit()

    assert stats["blocked_media_alias_rows"] == 1
    with get_session() as session:
        assert session.get(RawMessage, alias_id) is not None
        assert session.get(MessageDecision, alias_id) is not None


def test_media_merge_does_not_delete_same_filename_from_another_physical_message(client):
    placeholder_id = "3" * 64
    alias_id = "4" * 64
    filename = "0004-PHOTO.jpg"
    message = _media_message(message_id="5" * 64, occurrence=1, filename=filename)
    with get_session() as session:
        session.add(_raw(placeholder_id, text="image omitted", source="zip_import"))
        session.add(
            _raw(
                alias_id,
                text="Photo attached",
                source="export_media",
                attachments=message.manifest,
                ts_epoch=TS_EPOCH + 60,
            )
        )
        session.add_all(
            [_target_decision(placeholder_id), _synthetic_nonissue_decision(alias_id)]
        )
        session.commit()

    stats = _stats()
    with get_session() as session:
        assert _merge_placeholder_media(
            session,
            message,
            stats=stats,
            aliases_by_export_filename=_media_aliases_by_export_filename(session),
            accounted_alias_ids=set(),
        )
        session.commit()

    assert stats["matched_placeholder_media_messages"] == 1
    assert stats["merged_existing"] == 1
    assert stats["reconciled_media_alias_rows"] == 0
    assert stats["blocked_media_alias_rows"] == 0
    with get_session() as session:
        assert session.get(RawMessage, alias_id) is not None


def test_media_merge_blocks_cross_chat_or_kind_placeholder_ambiguity(client):
    first_target_id = "6" * 64
    second_target_id = "7" * 64
    alias_id = "a" * 64
    filename = "0005-PHOTO.jpg"
    message = _media_message(message_id=alias_id, occurrence=1, filename=filename)
    with get_session() as session:
        session.add_all(
            [
                _raw(first_target_id, text="image omitted", source="export", chat_name="Chat One"),
                _raw(second_target_id, text="image omitted", source="zip_import", chat_name="Chat Two"),
                _raw(alias_id, text="Photo attached", source="export_media", attachments=message.manifest),
            ]
        )
        session.add_all(
            [
                _target_decision(first_target_id),
                _target_decision(second_target_id),
                _synthetic_nonissue_decision(alias_id),
            ]
        )
        session.commit()

    stats = _stats()
    with get_session() as session:
        assert _merge_placeholder_media(
            session,
            message,
            stats=stats,
            aliases_by_export_filename=_media_aliases_by_export_filename(session),
            accounted_alias_ids=set(),
        )
        session.commit()

    assert stats["matched_placeholder_media_messages"] == 0
    assert stats["ambiguous_placeholder_media_messages"] == 1
    assert stats["merged_existing"] == 0
    assert stats["reconciled_media_alias_rows"] == 0
    with get_session() as session:
        assert session.get(RawMessage, alias_id) is not None
        assert session.get(RawMessage, first_target_id).attachments is None
        assert session.get(RawMessage, second_target_id).attachments is None


def test_media_merge_preserves_proof_referenced_or_manually_reviewed_aliases(client):
    placeholder_id = "b" * 64
    proof_alias_id = "c" * 64
    manual_alias_id = "d" * 64
    filename = "0006-PHOTO.jpg"
    message = _media_message(message_id="e" * 64, occurrence=1, filename=filename)
    with get_session() as session:
        session.add_all(
            [
                _raw(placeholder_id, text="image omitted", source="zip_import"),
                _raw(proof_alias_id, text="Photo attached", source="export_media", attachments=message.manifest),
                _raw(manual_alias_id, text="Photo attached", source="export_media", attachments=message.manifest),
                Incident(
                    incident_id="protected-media-incident",
                    category="other",
                    title="Protected media evidence",
                    proof_refs=proof_alias_id,
                ),
            ]
        )
        session.add_all(
            [
                _target_decision(placeholder_id),
                _synthetic_nonissue_decision(proof_alias_id),
                MessageDecision(
                    message_id=manual_alias_id,
                    chosen_source="manual",
                    is_issue=False,
                    event_type="non_issue",
                    needs_review=False,
                    auto_file_candidate=False,
                ),
            ]
        )
        session.commit()

    stats = _stats()
    with get_session() as session:
        assert _merge_placeholder_media(
            session,
            message,
            stats=stats,
            aliases_by_export_filename=_media_aliases_by_export_filename(session),
            accounted_alias_ids=set(),
        )
        session.commit()

    assert stats["blocked_media_alias_rows"] == 2
    assert stats["reconciled_media_alias_rows"] == 0
    with get_session() as session:
        assert session.get(RawMessage, proof_alias_id) is not None
        assert session.get(RawMessage, manual_alias_id) is not None


def test_blocked_alias_is_counted_once_per_import_run(client):
    placeholder_id = "f" * 64
    alias_id = "0" * 64
    filename = "0007-PHOTO.jpg"
    message = _media_message(message_id=alias_id, occurrence=1, filename=filename)
    with get_session() as session:
        session.add_all(
            [
                _raw(placeholder_id, text="image omitted", source="zip_import"),
                _raw(alias_id, text="Photo attached", source="export_media", attachments=message.manifest),
            ]
        )
        session.add_all(
            [
                _target_decision(placeholder_id),
                MessageDecision(
                    message_id=alias_id,
                    chosen_source="manual",
                    is_issue=False,
                    event_type="non_issue",
                ),
            ]
        )
        session.commit()

    stats = _stats()
    accounted: set[str] = set()
    with get_session() as session:
        aliases = _media_aliases_by_export_filename(session)
        for _ in range(2):
            assert _merge_placeholder_media(
                session,
                message,
                stats=stats,
                aliases_by_export_filename=aliases,
                accounted_alias_ids=accounted,
            )
        session.commit()

    assert stats["matched_placeholder_media_messages"] == 2
    assert stats["blocked_media_alias_rows"] == 1
    assert stats["reconciled_media_alias_rows"] == 0


def test_import_media_zip_alias_reconciliation_is_idempotent(client, tmp_path: Path, monkeypatch):
    filename = "0008-PHOTO-2026-04-24-19-43-48.jpg"
    zip_path = tmp_path / "media-export.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(
            "_chat.txt",
            f"[4/24/26, 7:43:48 PM] {SENDER}: <attached: {filename}>\n",
        )
        zf.writestr(filename, b"not-a-real-jpeg")
    epoch = parse_ts_to_epoch("4/24/26 7:43:48 PM")
    assert epoch is not None
    placeholder_id = "1" * 63 + "a"
    first_alias_id = "2" * 63 + "a"
    second_alias_id = "3" * 63 + "a"
    manifest = _manifest(filename)
    with get_session() as session:
        session.add_all(
            [
                _raw(
                    placeholder_id,
                    text="image omitted",
                    source="zip_import",
                    ts_epoch=epoch,
                ),
                _raw(
                    first_alias_id,
                    text="Photo attached",
                    source="export_media",
                    attachments=manifest,
                    ts_epoch=epoch,
                ),
                _raw(
                    second_alias_id,
                    text="Photo attached",
                    source="export_media",
                    attachments=manifest,
                    ts_epoch=epoch,
                ),
            ]
        )
        session.add_all(
            [
                _target_decision(placeholder_id),
                _synthetic_nonissue_decision(first_alias_id),
                _synthetic_nonissue_decision(second_alias_id),
            ]
        )
        session.commit()

    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(tmp_path / "media"))
    first = import_media_zip(
        zip_path,
        chat_name="455 Tenants",
        repair_reply_context=False,
        sync_sheets=False,
    )
    second = import_media_zip(
        zip_path,
        chat_name="455 Tenants",
        repair_reply_context=False,
        sync_sheets=False,
    )

    assert first["media_messages"] == 1
    assert first["matched_placeholder_media_messages"] == 1
    assert first["merged_existing"] == 1
    assert first["reconciled_media_alias_rows"] == 2
    assert first["retained_media_alias_rows"] == 2
    assert first["normalized_media_alias_rows"] == 2
    assert first["inserted_media_rows"] == 0
    assert second["matched_placeholder_media_messages"] == 1
    assert second["merged_existing"] == 0
    assert second["reconciled_media_alias_rows"] == 2
    assert second["retained_media_alias_rows"] == 2
    assert second["normalized_media_alias_rows"] == 0
    assert second["inserted_media_rows"] == 0
    with get_session() as session:
        canonical = session.get(RawMessage, placeholder_id)
        assert canonical is not None
        assert session.get(RawMessage, first_alias_id).text == "image omitted"
        assert session.get(RawMessage, second_alias_id).text == "image omitted"
        assert session.get(MessageDecision, first_alias_id) is not None
        assert session.get(MessageDecision, second_alias_id) is not None
        assert {item.get("export_filename") for item in attachment_items(canonical.attachments)} == {
            filename
        }


def test_reconciliation_preserves_every_exact_repeated_media_occurrence_for_full_audit(
    client,
    tmp_path: Path,
    monkeypatch,
):
    timestamp = "4/24/26, 7:43:48 PM"
    filenames = [f"000{i}-PHOTO.jpg" for i in range(1, 4)]
    text_export = tmp_path / "WhatsApp Chat - 455 Tenants.txt"
    text_export.write_text(
        "".join(
            f"[{timestamp}] {SENDER}: image omitted\n"
            for _ in filenames
        ),
        encoding="utf-8",
    )
    occurrences = iter_export_messages(text_export, default_chat_name="455 Tenants")
    assert [row.physical_occurrence for row in occurrences] == [1, 2, 3]

    media_zip = tmp_path / "repeated-media.zip"
    with zipfile.ZipFile(media_zip, "w") as archive:
        archive.writestr(
            "_chat.txt",
            "".join(
                f"[{timestamp}] {SENDER}: <attached: {filename}>\n"
                for filename in filenames
            ),
        )
        for filename in filenames:
            archive.writestr(filename, b"image-bytes")

    target_ids = ["7" * 64, "8" * 62 + "~2", "9" * 62 + "~3"]
    with get_session() as session:
        for target_id, occurrence, filename in zip(target_ids, occurrences, filenames):
            session.add(
                _raw(
                    target_id,
                    text="image omitted",
                    source="zip_import",
                    attachments="omitted:image",
                    ts_epoch=int(occurrence.ts_epoch or 0),
                    chat_name="455 Tenants",
                )
            )
            session.add(_target_decision(target_id))
            session.add(
                _raw(
                    occurrence.message_id,
                    text="Photo attached",
                    source="export_media",
                    attachments=_manifest(filename),
                    ts_epoch=int(occurrence.ts_epoch or 0),
                    chat_name="455 Tenants",
                )
            )
            session.add(_synthetic_nonissue_decision(occurrence.message_id))
        session.commit()

    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(tmp_path / "media"))
    first = import_media_zip(
        media_zip,
        chat_name="455 Tenants",
        repair_reply_context=False,
        sync_sheets=False,
    )
    second = import_media_zip(
        media_zip,
        chat_name="455 Tenants",
        repair_reply_context=False,
        sync_sheets=False,
    )

    assert first["inserted_media_rows"] == 0
    assert first["merged_existing"] == 3
    assert first["retained_media_alias_rows"] == 3
    assert first["normalized_media_alias_rows"] == 3
    assert second["inserted_media_rows"] == 0
    assert second["merged_existing"] == 0
    assert second["retained_media_alias_rows"] == 3
    assert second["normalized_media_alias_rows"] == 0
    with get_session() as session:
        assert all(session.get(RawMessage, row.message_id) is not None for row in occurrences)
        assert all(session.get(MessageDecision, row.message_id) is not None for row in occurrences)
        assert all(session.get(RawMessage, row.message_id).text == "image omitted" for row in occurrences)

    summary = run_audit(
        text_export,
        since="2000-01-01",
        out_dir=tmp_path / "full-audit",
        default_chat_name="455 Tenants",
    )
    assert summary["collision_followup_occurrences"] == 2
    assert summary["missing_db_messages"] == 0
    assert summary["missing_decisions"] == 0
    assert summary["llm_review_not_applicable"] == 3
    assert summary["ok"] is True
