import json

from packages.whatsapp.status import write_capture_status


def test_health_includes_safe_whatsapp_and_chat_sync_status(client, monkeypatch, tmp_path):
    status_path = tmp_path / "whatsapp_status.json"
    sync_path = tmp_path / "chat-export-sync.json"
    monkeypatch.setenv("WHATSAPP_CAPTURE_STATUS_PATH", str(status_path))
    monkeypatch.setenv("CHAT_EXPORT_SYNC_STATE_PATH", str(sync_path))
    write_capture_status(status_path, state="login_required", login_required=True, chat_names=["455 Tenants"])
    sync_path.write_text(
        json.dumps(
            {
                "last_checked_at": "2026-07-20T01:54:18Z",
                "last_processed_at": "2026-07-20T01:11:36Z",
                "last_processed_fingerprint": {"name": "WhatsApp Chat - 455 Tenants.zip"},
            }
        ),
        encoding="utf-8",
    )

    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["whatsapp_capture"]["state"] == "login_required"
    assert payload["whatsapp_capture"]["login_required"] is True
    assert "chat_names" not in payload["whatsapp_capture"]
    assert payload["chat_export_sync"] == {
        "state": "ready",
        "last_checked_at": "2026-07-20T01:54:18Z",
        "last_processed_at": "2026-07-20T01:11:36Z",
        "has_error": False,
    }
