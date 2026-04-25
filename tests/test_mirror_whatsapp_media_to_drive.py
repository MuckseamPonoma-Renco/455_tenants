from packages.db import RawMessage, get_session
from packages.whatsapp.attachments import build_attachment_manifest
from scripts.mirror_whatsapp_media_to_drive import mirror_media


def test_mirror_media_only_uploads_real_image_attachments(client, monkeypatch, tmp_path):
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    bubble = media_dir / "bubble.png"
    photo = media_dir / "photo.png"
    bubble.write_bytes(b"\x89PNG\r\n\x1a\n")
    photo.write_bytes(b"\x89PNG\r\n\x1a\n")

    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))
    manifest = build_attachment_manifest(
        items=[
            {"kind": "message_screenshot", "status": "captured", "path": str(bubble), "filename": "bubble.png"},
            {"kind": "image", "status": "downloaded", "path": str(photo), "filename": "photo.png"},
        ],
        source="whatsapp_web",
    )
    with get_session() as session:
        session.add(
            RawMessage(
                message_id="msg-real-photo",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-real-photo",
                ts_iso="2026-04-21T18:07:00Z",
                ts_epoch=1776804420,
                text="Photo attached",
                attachments=manifest,
                source="whatsapp_web",
            )
        )
        session.commit()

    result = mirror_media(dry_run=True)

    assert result["upload_candidates"] == 1
    assert result["details"] == [
        {
            "message_id": "msg-real-photo",
            "attachment_index": 1,
            "path": str(photo.resolve()),
            "kind": "image",
        }
    ]
