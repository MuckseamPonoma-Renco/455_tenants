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
from packages.whatsapp.parser import parse_export_text


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


def test_parse_export_text_removes_attached_token_but_keeps_filename():
    parsed = parse_export_text("[4/21/26, 2:07:03 PM] Nic: South lift out <attached: 00001961-PHOTO.jpg>\n")

    assert len(parsed) == 1
    assert parsed[0].text == "South lift out"
    assert parsed[0].attachments == "00001961-PHOTO.jpg"


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


def test_ingest_whatsapp_web_strips_quoted_reply_from_message_text(client):
    quoted = "Maureen VanTrease\n+1 (917) 693-7436\nI am getting tired of walking down 12 flights."
    manifest = build_attachment_manifest(
        message_context={"reply_text": f"{quoted}\n{quoted}"},
        source="whatsapp_web",
    )
    response = client.post(
        "/ingest/whatsapp_web",
        headers=auth_headers(),
        json={
            "chat_name": "455 Tenants Test",
            "text": f"{quoted}\nYeap, I do it every morning!!!",
            "sender": "Nic",
            "ts_epoch": 1771000500,
            "attachments": manifest,
        },
    )

    assert response.status_code == 200
    with get_session() as session:
        row = session.query(RawMessage).one()
        assert row.text == "Yeap, I do it every morning!!!"


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


def test_ingest_whatsapp_web_promotes_recent_export_alias_and_reprocesses(client, monkeypatch):
    text = "A resident reports the lobby door is too heavy to open when the doorman is away."
    with get_session() as session:
        session.add(
            RawMessage(
                message_id="export-alias",
                chat_name="Tenants WhatsApp",
                sender="~ Millie",
                sender_hash="export-sender",
                ts_iso="2026-07-13T14:04:37Z",
                ts_epoch=1783951477,
                text=text,
                attachments=None,
                source="zip_import",
            )
        )
        session.commit()

    process_calls = []

    def fake_enqueue(message_id, *, sync_sheets=True):
        process_calls.append((message_id, sync_sheets))
        return "reprocess-job"

    monkeypatch.setattr("apps.api.routers.ingest.enqueue_process_message", fake_enqueue)

    response = client.post(
        "/ingest/whatsapp_web",
        headers=auth_headers(),
        json={
            "chat_name": "455 Tenants",
            "text": text,
            "sender": "+1 (917) 400-8504",
            "ts_epoch": 1783951440,
        },
    )

    assert response.status_code == 200
    assert response.json()["deduped"] is True
    assert response.json()["reprocessed"] is True
    assert response.json()["message_id"] == "export-alias"
    assert process_calls == [("export-alias", True)]
    with get_session() as session:
        row = session.query(RawMessage).one()
        assert row.source == "whatsapp_web"
        assert row.chat_name == "455 Tenants"
        assert row.sender == "+1 (917) 400-8504"
        assert row.ts_epoch == 1783951440
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


def test_capture_config_uses_a_bounded_default_login_wait(monkeypatch):
    monkeypatch.setenv("INGEST_TOKEN", "test-token")
    monkeypatch.setenv("WHATSAPP_CAPTURE_CHAT_NAMES", "455 Tenants")
    monkeypatch.delenv("WHATSAPP_CAPTURE_LOGIN_TIMEOUT_SECONDS", raising=False)

    config = web_capture.capture_config_from_env()

    assert config.login_timeout_seconds == 120
    assert config.disk_cache_size_bytes == 268_435_456
    assert config.media_cache_size_bytes == 67_108_864


def test_launch_context_sets_bounded_chrome_cache_args(monkeypatch, tmp_path):
    launched = {}

    class Context:
        pass

    class Chromium:
        def launch_persistent_context(self, **kwargs):
            launched.update(kwargs)
            return Context()

    class Playwright:
        chromium = Chromium()

        def stop(self):
            pass

    class SyncPlaywright:
        def start(self):
            return Playwright()

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
        login_timeout_seconds=120,
        prime_visible_messages=False,
    )
    monkeypatch.setattr(web_capture, "sync_playwright", lambda: SyncPlaywright())

    runtime = web_capture._launch_context(config)

    assert runtime.context.__class__ is Context
    assert launched["args"] == ["--disk-cache-size=268435456", "--media-cache-size=67108864"]


def test_is_ready_accepts_modern_whatsapp_chat_list_without_an_open_chat():
    class Locator:
        def __init__(self, count):
            self._count = count

        def count(self):
            return self._count

    class Page:
        selector_counts = {
            "#pane-side": 1,
            "#side": 1,
            "#main": 0,
            "[data-testid='wa-web-main-screen']": 1,
        }

        def locator(self, selector):
            return Locator(self.selector_counts.get(selector, 0))

    assert web_capture._is_ready(Page()) is True


def test_startup_login_requirement_is_reported_without_waiting(monkeypatch, tmp_path):
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
        login_timeout_seconds=120,
        prime_visible_messages=False,
    )
    statuses = []

    monkeypatch.setattr(web_capture, "_is_ready", lambda _page: False)
    monkeypatch.setattr(web_capture, "_is_login_required", lambda _page: True)
    monkeypatch.setattr(web_capture, "read_capture_status", lambda _path: {})
    monkeypatch.setattr(web_capture, "_capture_status", lambda _config, **updates: statuses.append(updates))
    monkeypatch.setattr(web_capture, "append_audit_event", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        web_capture,
        "wait_for_whatsapp_ready",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not wait for a QR login")),
    )

    assert web_capture._ensure_session_ready(object(), config, startup=True) is False
    assert statuses[-1]["state"] == "login_required"
    assert statuses[-1]["login_required"] is True


def test_login_prompt_appearing_after_navigation_is_not_reported_as_a_timeout(monkeypatch, tmp_path):
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
        login_timeout_seconds=120,
        prime_visible_messages=False,
    )
    statuses = []
    login_states = iter((False, False, True))

    class Page:
        def __init__(self):
            self.gotos = 0

        def goto(self, *_args, **_kwargs):
            self.gotos += 1

    page = Page()
    monkeypatch.setattr(web_capture, "_is_ready", lambda _page: False)
    monkeypatch.setattr(web_capture, "_is_login_required", lambda _page: next(login_states))
    monkeypatch.setattr(web_capture, "read_capture_status", lambda _path: {})
    monkeypatch.setattr(web_capture, "_capture_status", lambda _config, **updates: statuses.append(updates))
    monkeypatch.setattr(web_capture, "append_audit_event", lambda *_args, **_kwargs: None)

    assert web_capture._ensure_session_ready(page, config) is False
    assert page.gotos == 1
    assert statuses[-1]["state"] == "login_required"
    assert statuses[-1]["login_required"] is True
