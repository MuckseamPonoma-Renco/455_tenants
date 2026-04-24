from packages.db import RawMessage, get_session
from packages.whatsapp.attachments import build_attachment_manifest
from packages.whatsapp.media import public_attachment_entries


def auth_headers():
    return {"Authorization": "Bearer test-token"}


def test_public_whatsapp_media_route_serves_allowed_file(client, monkeypatch, tmp_path):
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    evidence = media_dir / "evidence.txt"
    evidence.write_text("proof", encoding="utf-8")
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))

    manifest = build_attachment_manifest(
        items=[{"kind": "document", "status": "downloaded", "path": str(evidence), "filename": "evidence.txt"}],
        source="whatsapp_web",
    )

    with get_session() as session:
        session.add(
            RawMessage(
                message_id="msg-1",
                chat_name="455 Tenants",
                sender="Karen",
                sender_hash="hash-1",
                ts_iso="2026-04-21T14:00:00Z",
                ts_epoch=1776780000,
                text="See attached proof",
                attachments=manifest,
                source="whatsapp_web",
            )
        )
        session.commit()

    response = client.get("/media/whatsapp/msg-1/0")
    assert response.status_code == 200
    assert response.content == b"proof"
    assert response.headers["content-disposition"].startswith("attachment;")


def test_public_whatsapp_media_route_serves_images_inline(client, monkeypatch, tmp_path):
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    evidence = media_dir / "evidence.png"
    evidence.write_bytes(b"png")
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))

    manifest = build_attachment_manifest(
        items=[{"kind": "image", "status": "downloaded", "path": str(evidence), "filename": "evidence.png"}],
        source="whatsapp_web",
    )

    with get_session() as session:
        session.add(
            RawMessage(
                message_id="msg-image",
                chat_name="455 Tenants",
                sender="Karen",
                sender_hash="hash-1",
                ts_iso="2026-04-21T14:00:00Z",
                ts_epoch=1776780000,
                text="See attached proof",
                attachments=manifest,
                source="whatsapp_web",
            )
        )
        session.commit()

    response = client.get("/media/whatsapp/msg-image/0")
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"
    assert response.headers["content-disposition"].startswith("inline;")


def test_attachment_api_returns_public_urls(client, monkeypatch, tmp_path):
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    evidence = media_dir / "evidence.txt"
    evidence.write_text("proof", encoding="utf-8")
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")

    manifest = build_attachment_manifest(
        items=[{"kind": "document", "status": "downloaded", "path": str(evidence), "filename": "evidence.txt"}],
        message_context={"reply_text": "document"},
        links=["https://example.com/more"],
        source="whatsapp_web",
    )

    with get_session() as session:
        session.add(
            RawMessage(
                message_id="msg-2",
                chat_name="455 Tenants",
                sender="Karen",
                sender_hash="hash-1",
                ts_iso="2026-04-21T14:00:00Z",
                ts_epoch=1776780000,
                text="See attached proof",
                attachments=manifest,
                source="whatsapp_web",
            )
        )
        session.commit()

    response = client.get("/api/messages/msg-2/attachments", headers=auth_headers())
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"][0]["public_url"].startswith("https://tenant.example/media/whatsapp/msg-2/0?v=")
    assert payload["message_context"]["reply_text"] == "document"
    assert payload["links"] == ["https://example.com/more"]


def test_public_attachment_entries_marks_tiny_message_screenshots_not_previewable(monkeypatch, tmp_path):
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    tiny = media_dir / "tiny.png"
    tiny.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\rIHDR" + (508).to_bytes(4, "big") + (20).to_bytes(4, "big"))
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")

    manifest = build_attachment_manifest(
        items=[{"kind": "message_screenshot", "status": "captured", "path": str(tiny), "filename": "tiny.png"}],
        source="whatsapp_web",
    )

    item = public_attachment_entries("msg-tiny", manifest)[0]
    assert item["width"] == 508
    assert item["height"] == 20
    assert item["preview_eligible"] is False


def test_public_whatsapp_media_route_blocks_paths_outside_capture_root(client, monkeypatch, tmp_path):
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("secret", encoding="utf-8")
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))

    manifest = build_attachment_manifest(
        items=[{"kind": "document", "status": "downloaded", "path": str(outside), "filename": "outside.txt"}],
        source="whatsapp_web",
    )

    with get_session() as session:
        session.add(
            RawMessage(
                message_id="msg-3",
                chat_name="455 Tenants",
                sender="Karen",
                sender_hash="hash-1",
                ts_iso="2026-04-21T14:00:00Z",
                ts_epoch=1776780000,
                text="outside path",
                attachments=manifest,
                source="whatsapp_web",
            )
        )
        session.commit()

    response = client.get("/media/whatsapp/msg-3/0")
    assert response.status_code == 404


def test_attachment_api_accepts_staged_runtime_media_when_env_path_is_blank(client, monkeypatch, tmp_path):
    runtime_root = tmp_path / "runtime"
    media_dir = runtime_root / ".local" / "whatsapp_media"
    media_dir.mkdir(parents=True)
    evidence = media_dir / "runtime.txt"
    evidence.write_text("proof", encoding="utf-8")

    monkeypatch.delenv("WHATSAPP_CAPTURE_MEDIA_DIR", raising=False)
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")
    monkeypatch.setattr("packages.whatsapp.media.STAGED_RUNTIME_ROOT", runtime_root)

    manifest = build_attachment_manifest(
        items=[{"kind": "document", "status": "downloaded", "path": str(evidence), "filename": "runtime.txt"}],
        source="whatsapp_web",
    )

    with get_session() as session:
        session.add(
            RawMessage(
                message_id="msg-4",
                chat_name="455 Tenants",
                sender="Karen",
                sender_hash="hash-1",
                ts_iso="2026-04-21T14:00:00Z",
                ts_epoch=1776780000,
                text="runtime path",
                attachments=manifest,
                source="whatsapp_web",
            )
        )
        session.commit()

    response = client.get("/api/messages/msg-4/attachments", headers=auth_headers())
    assert response.status_code == 200
    assert response.json()["items"][0]["public_url"].startswith("https://tenant.example/media/whatsapp/msg-4/0?v=")
