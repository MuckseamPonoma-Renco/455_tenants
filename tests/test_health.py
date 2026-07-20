import json
from types import SimpleNamespace

import apps.api.routers.health as health_router
from packages.automation_status import write_automation_status
from packages.whatsapp.status import write_capture_status


def test_health_includes_safe_whatsapp_and_chat_sync_status(client, monkeypatch, tmp_path):
    status_path = tmp_path / "whatsapp_status.json"
    sync_path = tmp_path / "chat-export-sync.json"
    automation_status_path = tmp_path / "automation_status.json"
    monkeypatch.setenv("WHATSAPP_CAPTURE_STATUS_PATH", str(status_path))
    monkeypatch.setenv("CHAT_EXPORT_SYNC_STATE_PATH", str(sync_path))
    monkeypatch.setenv("AUTOMATION_STATUS_PATH", str(automation_status_path))
    monkeypatch.setattr(health_router.shutil, "disk_usage", lambda _path: SimpleNamespace(free=20 * 1024 * 1024 * 1024))
    monkeypatch.setattr(health_router, "database_is_ready", lambda: True)
    write_capture_status(status_path, state="login_required", login_required=True, chat_names=["455 Tenants"])
    write_automation_status(
        automation_status_path,
        state="ready",
        poll_seconds=60,
        last_cycle_at="2026-07-20T01:55:00Z",
    )
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
    assert payload["storage"] == {"state": "ready", "low_disk": False}
    assert payload["database_ready"] is True
    assert payload["automation"] == {
        "state": "ready",
        "last_cycle_at": "2026-07-20T01:55:00Z",
        "poll_seconds": 60,
        "updated_at": payload["automation"]["updated_at"],
        "has_error": False,
    }


def test_health_reports_low_storage_without_public_capacity_details(client, monkeypatch):
    monkeypatch.setattr(health_router.shutil, "disk_usage", lambda _path: SimpleNamespace(free=5 * 1024 * 1024 * 1024))
    monkeypatch.setattr(health_router, "database_is_ready", lambda: True)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["storage"] == {"state": "low_disk", "low_disk": True}
    assert "free_bytes" not in response.json()["storage"]


def test_health_reports_unreachable_database_without_error_detail(client, monkeypatch):
    monkeypatch.setattr(health_router, "database_is_ready", lambda: False)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["database_ready"] is False
