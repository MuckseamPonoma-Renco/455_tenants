from packages.db import RawMessage, get_session
from packages.timeutil import parse_ts_to_epoch
from packages.whatsapp.attachments import attachment_items, build_attachment_manifest, parse_attachment_manifest
from packages.whatsapp.web_capture import parse_whatsapp_message_meta


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


def test_parse_attachment_manifest_supports_legacy_placeholder():
    parsed = parse_attachment_manifest("omitted:image")

    assert parsed["source"] == "legacy_string"
    assert parsed["items"][0]["kind"] == "image"
    assert parsed["items"][0]["status"] == "placeholder"
