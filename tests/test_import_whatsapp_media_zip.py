from __future__ import annotations

from packages.audit import sender_hash
from packages.db import MessageDecision, RawMessage, get_session
from packages.whatsapp.attachments import attachment_items, build_attachment_manifest, make_attachment_item
from packages.whatsapp.parser import ParsedMessage
from scripts.import_whatsapp_media_zip import (
    MediaMessage,
    _exact_placeholder_targets,
    _generic_aliases_by_export_filename,
    _merge_placeholder_media,
)


SENDER = "Tenant One"
TS_EPOCH = 1_777_000_000


def _raw(message_id: str, *, text: str, source: str, attachments: str | None = None) -> RawMessage:
    return RawMessage(
        message_id=message_id,
        chat_name="Tenants WhatsApp",
        sender=SENDER,
        sender_hash=sender_hash(SENDER),
        ts_iso="2026-04-24T23:43:48Z",
        ts_epoch=TS_EPOCH,
        text=text,
        attachments=attachments,
        source=source,
    )


def _media_message(*, message_id: str, occurrence: int, filename: str) -> MediaMessage:
    manifest = build_attachment_manifest(
        items=[
            make_attachment_item(
                kind="image",
                status="downloaded",
                path=f"/private/media/{filename}",
                filename=filename,
                extra={"export_filename": filename},
            )
        ],
        source="whatsapp_export",
    )
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


def test_media_merge_removes_safe_generic_alias_and_preserves_canonical_row(client):
    placeholder_id = "d" * 64
    alias_id = "e" * 64
    filename = "0002-PHOTO.jpg"
    message = _media_message(message_id=alias_id, occurrence=1, filename=filename)
    with get_session() as session:
        session.add(_raw(placeholder_id, text="image omitted", source="zip_import", attachments="omitted:image"))
        session.add(_raw(alias_id, text="Photo attached", source="export_media", attachments=message.manifest))
        session.add(
            MessageDecision(
                message_id=alias_id,
                chosen_source="media_attachment",
                is_issue=False,
                event_type="non_issue",
                auto_file_candidate=False,
            )
        )
        session.commit()

    stats = {
        "merged_existing": 0,
        "reconciled_media_alias_rows": 0,
        "blocked_media_alias_rows": 0,
    }
    with get_session() as session:
        merged = _merge_placeholder_media(
            session,
            message,
            stats=stats,
            aliases_by_export_filename=_generic_aliases_by_export_filename(session),
            reconciled_alias_ids=set(),
        )
        session.commit()

    assert merged is True
    assert stats == {
        "merged_existing": 1,
        "reconciled_media_alias_rows": 1,
        "blocked_media_alias_rows": 0,
    }
    with get_session() as session:
        canonical = session.get(RawMessage, placeholder_id)
        assert canonical is not None
        assert session.get(RawMessage, alias_id) is None
        assert session.get(MessageDecision, alias_id) is None
        assert {
            item.get("export_filename")
            for item in attachment_items(canonical.attachments)
            if item.get("export_filename")
        } == {filename}


def test_media_merge_refuses_to_delete_issue_alias(client):
    placeholder_id = "f" * 64
    alias_id = "9" * 64
    filename = "0003-PHOTO.jpg"
    message = _media_message(message_id=alias_id, occurrence=1, filename=filename)
    with get_session() as session:
        session.add(_raw(placeholder_id, text="image omitted", source="zip_import"))
        session.add(_raw(alias_id, text="Photo attached", source="export_media", attachments=message.manifest))
        session.add(
            MessageDecision(
                message_id=alias_id,
                chosen_source="manual",
                is_issue=True,
                category="other",
                event_type="new_issue",
            )
        )
        session.commit()

    stats = {
        "merged_existing": 0,
        "reconciled_media_alias_rows": 0,
        "blocked_media_alias_rows": 0,
    }
    with get_session() as session:
        assert _merge_placeholder_media(
            session,
            message,
            stats=stats,
            aliases_by_export_filename=_generic_aliases_by_export_filename(session),
            reconciled_alias_ids=set(),
        )
        session.commit()

    assert stats["blocked_media_alias_rows"] == 1
    with get_session() as session:
        assert session.get(RawMessage, alias_id) is not None
        assert session.get(MessageDecision, alias_id) is not None
