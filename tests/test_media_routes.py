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


def test_public_attachment_entries_skips_message_screenshots(monkeypatch, tmp_path):
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

    assert public_attachment_entries("msg-tiny", manifest) == []


def test_public_attachment_entries_skips_whatsapp_ui_images_and_dedupes_paths(monkeypatch, tmp_path):
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    static_asset = media_dir / "static.webp"
    real_photo = media_dir / "photo.jpg"
    static_asset.write_bytes(b"RIFFxxxxWEBP")
    real_photo.write_bytes(b"\xff\xd8\xff\xd9")
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")

    manifest = build_attachment_manifest(
        items=[
            {
                "kind": "image",
                "status": "downloaded",
                "label": "inline_image",
                "capture_method": "inline_image",
                "path": str(static_asset),
                "filename": "static.webp",
                "source_url": "https://static.whatsapp.net/rsrc.php/yp/r/OA5z0a81CZk.webp",
            },
            {
                "kind": "image",
                "status": "downloaded",
                "label": "inline_image",
                "capture_method": "inline_image",
                "path": str(real_photo),
                "filename": "photo.jpg",
                "source_url": "blob:https://web.whatsapp.com/photo-1",
            },
            {
                "kind": "image",
                "status": "downloaded",
                "label": "inline_image",
                "capture_method": "inline_image",
                "path": str(real_photo),
                "filename": "photo.jpg",
                "source_url": "blob:https://web.whatsapp.com/photo-2",
            },
        ],
        source="whatsapp_web",
    )

    entries = public_attachment_entries("msg-photo", manifest)

    assert len(entries) == 1
    assert entries[0]["path"] == str(real_photo.resolve())
    assert entries[0]["source_url"] == "blob:https://web.whatsapp.com/photo-1"


def test_media_routes_do_not_expose_whatsapp_ui_images(client, monkeypatch, tmp_path):
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    static_asset = media_dir / "static.webp"
    static_asset.write_bytes(b"RIFFxxxxWEBP")
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")

    manifest = build_attachment_manifest(
        items=[
            {
                "kind": "image",
                "status": "downloaded",
                "label": "inline_image",
                "capture_method": "inline_image",
                "path": str(static_asset),
                "filename": "static.webp",
                "source_url": "https://static.whatsapp.net/rsrc.php/yp/r/OA5z0a81CZk.webp",
            }
        ],
        source="whatsapp_web",
    )
    with get_session() as session:
        session.add(
            RawMessage(
                message_id="msg-static-ui",
                chat_name="455 Tenants",
                sender="Karen",
                sender_hash="hash-static-ui",
                ts_iso="2026-04-21T14:00:00Z",
                ts_epoch=1776780000,
                text="Static image should not be exposed",
                attachments=manifest,
                source="whatsapp_web",
            )
        )
        session.commit()

    payload = client.get("/api/messages/msg-static-ui/attachments", headers=auth_headers()).json()

    assert payload["items"] == []
    assert client.get("/media/whatsapp/msg-static-ui/0").status_code == 404


def test_media_routes_do_not_expose_message_screenshots(client, monkeypatch, tmp_path):
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    bubble = media_dir / "bubble.png"
    photo = media_dir / "photo.png"
    bubble.write_bytes(b"\x89PNG\r\n\x1a\n")
    photo.write_bytes(b"\x89PNG\r\n\x1a\n")
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")

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
                message_id="msg-no-bubble",
                chat_name="455 Tenants",
                sender="Karen",
                sender_hash="hash-bubble",
                ts_iso="2026-04-21T14:00:00Z",
                ts_epoch=1776780000,
                text="See attached photo",
                attachments=manifest,
                source="whatsapp_web",
            )
        )
        session.commit()

    payload = client.get("/api/messages/msg-no-bubble/attachments", headers=auth_headers()).json()
    assert [item["kind"] for item in payload["items"]] == ["image"]
    assert payload["items"][0]["attachment_index"] == 1
    assert "/media/whatsapp/msg-no-bubble/1?v=" in payload["items"][0]["public_url"]

    assert client.get("/media/whatsapp/msg-no-bubble/0").status_code == 404
    assert client.get("/media/whatsapp/msg-no-bubble/1").status_code == 200


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
