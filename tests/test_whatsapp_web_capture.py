from packages.db import RawMessage, get_session
from packages.timeutil import parse_ts_to_epoch
from packages.whatsapp.attachments import attachment_items, build_attachment_manifest, parse_attachment_manifest
from packages.whatsapp import web_capture
from packages.whatsapp.web_capture import (
    WhatsAppCaptureCandidate,
    WhatsAppCaptureConfig,
    _attachment_manifest_for_candidate,
    _try_download_message_media,
    parse_whatsapp_message_meta,
)


def auth_headers():
    return {"Authorization": "Bearer test-token"}


def test_parse_whatsapp_message_meta_american_format():
    sender, ts_iso, ts_epoch = parse_whatsapp_message_meta("[6:21 PM, 4/17/2026] Karen: ")

    assert sender == "Karen"
    assert ts_epoch == parse_ts_to_epoch("4/17/2026 6:21 PM")
    assert ts_iso == "2026-04-17T22:21:00Z"


def test_parse_whatsapp_message_meta_day_first_format():
    sender, ts_iso, ts_epoch = parse_whatsapp_message_meta("[18:21, 17/04/2026] Karen: ")

    assert sender == "Karen"
    assert ts_epoch == parse_ts_to_epoch("4/17/2026 18:21")
    assert ts_iso == "2026-04-17T22:21:00Z"


def test_attachment_manifest_for_candidate_uses_real_media_not_bubble_screenshot(monkeypatch, tmp_path):
    config = WhatsAppCaptureConfig(
        chat_names=("455 Tenants",),
        ingest_token="test-token",
        api_bases=("http://127.0.0.1:8000",),
        headless=True,
        poll_seconds=30,
        message_limit=5,
        max_scroll_pages=1,
        user_data_dir=tmp_path / "profile",
        state_path=tmp_path / "state.json",
        status_path=tmp_path / "status.json",
        media_dir=tmp_path / "media",
        browser_channel="chrome",
        login_timeout_seconds=0,
        prime_visible_messages=False,
    )
    candidate = WhatsAppCaptureCandidate(
        chat_name="455 Tenants",
        sender="Karen",
        text="elevator photo attached",
        ts_iso="2026-04-21T18:07:00Z",
        ts_epoch=1776804420,
        fingerprint="abcdef123456",
        row={
            "media_kinds": ["image"],
            "links": ["https://example.com/details"],
            "reply_text": "old elevator message",
        },
    )

    def fake_download(_page, _locator, target_dir, _prefix, kind):
        real = target_dir / "real-photo.png"
        real.write_bytes(b"\x89PNG\r\n\x1a\n")
        return {"kind": kind, "status": "downloaded", "path": str(real), "filename": real.name}

    monkeypatch.setattr(web_capture, "_message_locator", lambda _page, _row: object())
    monkeypatch.setattr(web_capture, "_try_download_message_media", fake_download)

    manifest = _attachment_manifest_for_candidate(object(), candidate, config)

    parsed = parse_attachment_manifest(manifest)
    items = attachment_items(manifest)
    assert parsed["links"] == ["https://example.com/details"]
    assert parsed["message_context"]["reply_text"] == "old elevator message"
    assert [item["kind"] for item in items] == ["image"]
    assert items[0]["filename"] == "real-photo.png"


def test_image_media_download_falls_back_to_inline_bytes(tmp_path):
    class EmptyLocator:
        @property
        def first(self):
            return self

        def count(self):
            return 0

    class InlineImageLocator:
        def locator(self, _selector):
            return EmptyLocator()

        def evaluate(self, _script):
            return {
                "data_base64": "iVBORw0KGgo=",
                "content_type": "image/png",
                "width": 900,
                "height": 700,
                "source_url": "blob:https://web.whatsapp.com/photo",
            }

    item = _try_download_message_media(object(), InlineImageLocator(), tmp_path, "msg", "image")

    assert item["kind"] == "image"
    assert item["label"] == "inline_image"
    assert item["status"] == "downloaded"
    assert item["content_type"] == "image/png"
    assert item["filename"] == "msg_inline_image.png"
    assert item["capture_method"] == "inline_image"
    assert item["width"] == 900
    assert item["height"] == 700
    assert item["source_url"] == "blob:https://web.whatsapp.com/photo"
    assert (tmp_path / "msg_inline_image.png").read_bytes() == b"\x89PNG\r\n\x1a\n"


def test_ingest_whatsapp_web_stores_distinct_source(client):
    response = client.post(
        "/ingest/whatsapp_web",
        headers=auth_headers(),
        json={
            "chat_name": "455 Tenants Test",
            "text": "north elevator dead again",
            "sender": "Karen",
            "ts_epoch": 1771000500,
        },
    )

    assert response.status_code == 200
    assert response.json()["deduped"] is False
    with get_session() as session:
        row = session.query(RawMessage).one()
        assert row.source == "whatsapp_web"


def test_ingest_whatsapp_web_stores_attachment_manifest(client):
    manifest = build_attachment_manifest(
        items=[{"kind": "image", "status": "downloaded", "path": "/tmp/test.png"}],
        message_context={"reply_text": "old elevator photo"},
        links=["https://example.com"],
        source="whatsapp_web",
    )
    response = client.post(
        "/ingest/whatsapp_web",
        headers=auth_headers(),
        json={
            "chat_name": "455 Tenants Test",
            "text": "north elevator dead again",
            "sender": "Karen",
            "ts_epoch": 1771000500,
            "attachments": manifest,
        },
    )

    assert response.status_code == 200
    with get_session() as session:
        row = session.query(RawMessage).one()
        parsed = parse_attachment_manifest(row.attachments)
        assert parsed["source"] == "whatsapp_web"
        assert parsed["message_context"]["reply_text"] == "old elevator photo"
        assert parsed["links"] == ["https://example.com"]
        assert attachment_items(row.attachments)[0]["kind"] == "image"


def test_ingest_whatsapp_web_dedupes_against_tasker_capture(client):
    first = client.post(
        "/ingest/tasker",
        headers=auth_headers(),
        json={
            "chat_name": "455 Tenants Test",
            "text": "north elevator dead again",
            "sender": "Karen",
            "ts_epoch": 1771000500,
        },
    )
    second = client.post(
        "/ingest/whatsapp_web",
        headers=auth_headers(),
        json={
            "chat_name": "455 Tenants Test",
            "text": "north elevator dead again",
            "sender": "Karen",
            "ts_epoch": 1771000500,
        },
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["deduped"] is True
    with get_session() as session:
        rows = session.query(RawMessage).all()
        assert len(rows) == 1
        assert rows[0].source == "tasker"


def test_ingest_whatsapp_web_duplicate_can_fill_missing_attachments(client):
    first = client.post(
        "/ingest/whatsapp_web",
        headers=auth_headers(),
        json={
            "chat_name": "455 Tenants Test",
            "text": "stair A handrail broken",
            "sender": "Karen",
            "ts_epoch": 1771000500,
        },
    )
    manifest = build_attachment_manifest(
        items=[{"kind": "image", "status": "captured", "path": "/tmp/handrail.png", "filename": "handrail.png"}],
        source="whatsapp_web",
    )
    second = client.post(
        "/ingest/whatsapp_web",
        headers=auth_headers(),
        json={
            "chat_name": "455 Tenants Test",
            "text": "stair A handrail broken",
            "sender": "Karen",
            "ts_epoch": 1771000500,
            "attachments": manifest,
        },
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["deduped"] is True
    with get_session() as session:
        row = session.query(RawMessage).one()
        items = attachment_items(row.attachments)
        assert len(items) == 1
        assert items[0]["filename"] == "handrail.png"


def test_ingest_whatsapp_web_duplicate_dedupes_same_attachment_path(client, tmp_path):
    photo = tmp_path / "handrail.png"
    photo.write_bytes(b"\x89PNG\r\n\x1a\n")
    first_manifest = build_attachment_manifest(
        items=[
            {
                "kind": "image",
                "status": "downloaded",
                "path": str(photo),
                "filename": "handrail.png",
                "source_url": "blob:https://web.whatsapp.com/first",
            }
        ],
        source="whatsapp_web",
    )
    second_manifest = build_attachment_manifest(
        items=[
            {
                "kind": "image",
                "status": "downloaded",
                "path": str(photo),
                "filename": "handrail.png",
                "source_url": "blob:https://web.whatsapp.com/second",
            }
        ],
        source="whatsapp_web",
    )
    first = client.post(
        "/ingest/whatsapp_web",
        headers=auth_headers(),
        json={
            "chat_name": "455 Tenants Test",
            "text": "stair A handrail broken",
            "sender": "Karen",
            "ts_epoch": 1771000500,
            "attachments": first_manifest,
        },
    )
    second = client.post(
        "/ingest/whatsapp_web",
        headers=auth_headers(),
        json={
            "chat_name": "455 Tenants Test",
            "text": "stair A handrail broken",
            "sender": "Karen",
            "ts_epoch": 1771000500,
            "attachments": second_manifest,
        },
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["deduped"] is True
    with get_session() as session:
        row = session.query(RawMessage).one()
        items = attachment_items(row.attachments)
        assert len(items) == 1
        assert items[0]["source_url"] == "blob:https://web.whatsapp.com/first"


def test_parse_attachment_manifest_supports_legacy_placeholder():
    parsed = parse_attachment_manifest("omitted:image")

    assert parsed["source"] == "legacy_string"
    assert parsed["items"][0]["kind"] == "image"
    assert parsed["items"][0]["status"] == "placeholder"
