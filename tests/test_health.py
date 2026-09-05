import datetime as dt
import json
from types import SimpleNamespace

import pytest

import apps.api.routers.health as health_router
from packages.automation_status import write_automation_status
from packages.whatsapp.status import write_capture_status


@pytest.fixture(autouse=True)
def _isolate_cloud_receiver_configuration(monkeypatch):
    monkeypatch.delenv("CLOUD_EXPORT_RECEIVER_URL", raising=False)
    monkeypatch.delenv("CLOUD_EXPORT_RECEIVER_PULL_TOKEN", raising=False)


def test_health_includes_safe_whatsapp_and_chat_sync_status(client, monkeypatch, tmp_path):
    status_path = tmp_path / "whatsapp_status.json"
    sync_path = tmp_path / "chat-export-sync.json"
    automation_status_path = tmp_path / "automation_status.json"
    monkeypatch.setenv("WHATSAPP_CAPTURE_STATUS_PATH", str(status_path))
    monkeypatch.setenv("CHAT_EXPORT_SYNC_STATE_PATH", str(sync_path))
    monkeypatch.setenv("AUTOMATION_STATUS_PATH", str(automation_status_path))
    monkeypatch.delenv("CLOUD_EXPORT_RECEIVER_URL", raising=False)
    monkeypatch.delenv("CLOUD_EXPORT_RECEIVER_PULL_TOKEN", raising=False)
    monkeypatch.setattr(health_router, "_utcnow", lambda: dt.datetime(2026, 7, 20, 1, 55, tzinfo=dt.UTC))
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
    assert payload["cloud_export_receiver"] == {"state": "not_configured", "configured": False}
    assert payload["database_ready"] is True
    assert payload["automation"] == {
        "state": "ready",
        "last_cycle_at": "2026-07-20T01:55:00Z",
        "poll_seconds": 60,
        "updated_at": payload["automation"]["updated_at"],
        "has_error": False,
    }


def test_health_reports_cloud_export_receiver_configured_without_secret_values(client, monkeypatch):
    monkeypatch.setenv("CLOUD_EXPORT_RECEIVER_URL", "https://uploads.example.test")
    monkeypatch.setenv("CLOUD_EXPORT_RECEIVER_PULL_TOKEN", "secret-pull-token")

    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["cloud_export_receiver"] == {"state": "configured", "configured": True}
    assert "uploads.example.test" not in json.dumps(payload)
    assert "secret-pull-token" not in json.dumps(payload)


def test_newer_cloud_failure_cannot_be_masked_by_older_icloud_success(client, monkeypatch, tmp_path):
    icloud_path = tmp_path / "chat-export-sync.json"
    cloud_path = tmp_path / "cloud-chat-export-sync.json"
    monkeypatch.setenv("CHAT_EXPORT_SYNC_STATE_PATH", str(icloud_path))
    monkeypatch.setenv("CLOUD_CHAT_EXPORT_SYNC_STATE_PATH", str(cloud_path))
    monkeypatch.setenv("CLOUD_EXPORT_RECEIVER_URL", "https://uploads.example.test")
    monkeypatch.setenv("CLOUD_EXPORT_RECEIVER_PULL_TOKEN", "secret-pull-token")
    monkeypatch.setattr(health_router, "_utcnow", lambda: dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC))
    icloud_path.write_text(
        json.dumps(
            {
                "last_checked_at": "2026-07-20T01:59:00Z",
                # Processing time is later than the cloud upload, but the source
                # fingerprint proves this was an older physical export.
                "last_processed_at": "2026-07-20T01:45:00Z",
                "last_processed_fingerprint": {
                    "name": "WhatsApp Chat - 455 Tenants.zip",
                    "mtime_ns": int(dt.datetime(2026, 7, 20, 1, 0, tzinfo=dt.UTC).timestamp() * 1_000_000_000),
                },
            }
        ),
        encoding="utf-8",
    )
    key = "pending/newer-export"
    cloud_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "last_checked_at": "2026-07-20T01:59:30Z",
                "latest_receipt_key": key,
                "pending_export_keys": [key],
                "receipts": {
                    key: {
                        "receipt_version": 1,
                        "key": key,
                        "uploaded_at": "2026-07-20T01:30:00Z",
                        "discovered_at": "2026-07-20T01:31:00Z",
                        "status": "blocked_model_review",
                        "stages": {
                            "upload": {"state": "complete"},
                            "discovery": {"state": "complete"},
                            "download": {"state": "complete"},
                            "processing": {"state": "complete", "at": "2026-07-20T01:32:00Z"},
                            "audit": {"state": "blocked_model_review"},
                            "sheet_sync": {"state": "pending"},
                            "sheet_readback": {"state": "not_reported"},
                            "acknowledgement": {"state": "pending"},
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    response = client.get("/health")

    assert response.status_code == 200
    sync = response.json()["chat_export_sync"]
    assert sync["state"] == "blocked_model_review"
    assert sync["has_error"] is True
    assert sync["pending_cloud_exports"] == 1
    assert sync["blocked_cloud_exports"] == 1
    assert sync["latest_export"] == {
        "source": "cloud_receiver",
        "uploaded_at": "2026-07-20T01:30:00Z",
        "discovered_at": "2026-07-20T01:31:00Z",
        "receipt_version": 1,
        "status": "blocked_model_review",
        "stages": {
            "upload": "complete",
            "discovery": "complete",
            "download": "complete",
            "processing": "complete",
            "audit": "blocked_model_review",
            "sheet_sync": "pending",
            "sheet_readback": "not_reported",
            "acknowledgement": "pending",
        },
    }
    assert key not in json.dumps(response.json())


def test_missing_cloud_state_cannot_be_masked_by_legacy_icloud_success(client, monkeypatch, tmp_path):
    icloud_path = tmp_path / "chat-export-sync.json"
    monkeypatch.setenv("CHAT_EXPORT_SYNC_STATE_PATH", str(icloud_path))
    monkeypatch.setenv("CLOUD_CHAT_EXPORT_SYNC_STATE_PATH", str(tmp_path / "missing-cloud-state.json"))
    monkeypatch.setenv("CLOUD_EXPORT_RECEIVER_URL", "https://uploads.example.test")
    monkeypatch.setenv("CLOUD_EXPORT_RECEIVER_PULL_TOKEN", "secret-pull-token")
    icloud_path.write_text(
        json.dumps(
            {
                "last_checked_at": "2026-07-20T01:59:00Z",
                "last_processed_at": "2026-07-20T01:00:00Z",
                "last_processed_fingerprint": {"name": "WhatsApp Chat - 455 Tenants.zip"},
            }
        ),
        encoding="utf-8",
    )

    sync = client.get("/health").json()["chat_export_sync"]

    assert sync["state"] == "cloud_state_missing"
    assert sync["has_error"] is True


def test_cloud_receipt_distinguishes_sheet_sync_from_missing_readback(client, monkeypatch, tmp_path):
    icloud_path = tmp_path / "chat-export-sync.json"
    cloud_path = tmp_path / "cloud-chat-export-sync.json"
    monkeypatch.setenv("CHAT_EXPORT_SYNC_STATE_PATH", str(icloud_path))
    monkeypatch.setenv("CLOUD_CHAT_EXPORT_SYNC_STATE_PATH", str(cloud_path))
    monkeypatch.setenv("CLOUD_EXPORT_RECEIVER_URL", "https://uploads.example.test")
    monkeypatch.setenv("CLOUD_EXPORT_RECEIVER_PULL_TOKEN", "secret-pull-token")
    monkeypatch.setattr(health_router, "_utcnow", lambda: dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC))
    icloud_path.write_text("{}", encoding="utf-8")
    key = "pending/readback-unverified"
    stages = {
        name: {"state": "complete"}
        for name in (
            "upload",
            "discovery",
            "download",
            "processing",
            "audit",
            "sheet_sync",
            "acknowledgement",
        )
    }
    stages["sheet_readback"] = {"state": "not_reported"}
    cloud_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "last_checked_at": "2026-07-20T01:59:30Z",
                "latest_receipt_key": key,
                "pending_export_keys": [],
                "receipts": {
                    key: {
                        "receipt_version": 1,
                        "key": key,
                        "uploaded_at": "2026-07-20T01:30:00Z",
                        "discovered_at": "2026-07-20T01:31:00Z",
                        "status": "sheet_readback_unverified",
                        "stages": stages,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    sync = client.get("/health").json()["chat_export_sync"]

    assert sync["state"] == "sheet_readback_unverified"
    assert sync["has_error"] is True
    assert sync["latest_export"]["stages"]["sheet_sync"] == "complete"
    assert sync["latest_export"]["stages"]["sheet_readback"] == "not_reported"


def test_cloud_receipt_reconciles_legacy_staged_export_as_unverified(client, monkeypatch, tmp_path):
    cloud_path = tmp_path / "cloud-chat-export-sync.json"
    monkeypatch.setenv("CHAT_EXPORT_SYNC_STATE_PATH", str(tmp_path / "missing-icloud.json"))
    monkeypatch.setenv("CLOUD_CHAT_EXPORT_SYNC_STATE_PATH", str(cloud_path))
    monkeypatch.setenv("CLOUD_EXPORT_RECEIVER_URL", "https://uploads.example.test")
    monkeypatch.setenv("CLOUD_EXPORT_RECEIVER_PULL_TOKEN", "secret-pull-token")
    monkeypatch.setattr(health_router, "_utcnow", lambda: dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC))
    key = "legacy-local/" + "a" * 64
    cloud_path.write_text(
        json.dumps(
            {
                "last_checked_at": "2026-07-20T01:59:30Z",
                "latest_receipt_key": key,
                "pending_export_keys": [],
                "receipts": {
                    key: {
                        "receipt_version": 1,
                        "key": key,
                        "legacy_reconstructed": True,
                        "discovered_at": "2026-07-20T01:30:00Z",
                        "status": "ready",
                        "stages": {},
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    sync = client.get("/health").json()["chat_export_sync"]

    assert sync["state"] == "legacy_unverified"
    assert sync["has_error"] is True
    assert sync["latest_export"]["status"] == "legacy_unverified"


def test_pending_hard_failure_precedes_newer_quota_block(client, monkeypatch, tmp_path):
    cloud_path = tmp_path / "cloud-chat-export-sync.json"
    monkeypatch.setenv("CHAT_EXPORT_SYNC_STATE_PATH", str(tmp_path / "missing-icloud.json"))
    monkeypatch.setenv("CLOUD_CHAT_EXPORT_SYNC_STATE_PATH", str(cloud_path))
    monkeypatch.setenv("CLOUD_EXPORT_RECEIVER_URL", "https://uploads.example.test")
    monkeypatch.setenv("CLOUD_EXPORT_RECEIVER_PULL_TOKEN", "secret-pull-token")
    monkeypatch.setattr(health_router, "_utcnow", lambda: dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC))
    failed_key = "pending/failed"
    blocked_key = "pending/blocked"
    completed_stages = {
        name: {"state": "complete"}
        for name in health_router.CLOUD_RECEIPT_STAGE_NAMES
    }
    failed_stages = {name: dict(stage) for name, stage in completed_stages.items()}
    failed_stages["sheet_readback"] = {"state": "error"}
    failed_stages["acknowledgement"] = {"state": "pending"}
    blocked_stages = {name: dict(stage) for name, stage in completed_stages.items()}
    blocked_stages["audit"] = {"state": "blocked_model_review"}
    blocked_stages["sheet_sync"] = {"state": "pending"}
    blocked_stages["sheet_readback"] = {"state": "not_reported"}
    blocked_stages["acknowledgement"] = {"state": "pending"}
    cloud_path.write_text(
        json.dumps(
            {
                "last_checked_at": "2026-07-20T01:59:30Z",
                "last_error": "one pending export failed",
                # Deliberately stale: health must determine latest by export time.
                "latest_receipt_key": failed_key,
                "pending_export_keys": [failed_key, blocked_key],
                "receipts": {
                    failed_key: {
                        "receipt_version": 1,
                        "key": failed_key,
                        "uploaded_at": "2026-07-20T01:30:00Z",
                        "discovered_at": "2026-07-20T01:31:00Z",
                        "stages": failed_stages,
                    },
                    blocked_key: {
                        "receipt_version": 1,
                        "key": blocked_key,
                        "uploaded_at": "2026-07-20T01:40:00Z",
                        "discovered_at": "2026-07-20T01:41:00Z",
                        "stages": blocked_stages,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    sync = client.get("/health").json()["chat_export_sync"]

    assert sync["state"] == "sheet_readback_failed"
    assert sync["has_error"] is True
    assert sync["blocked_cloud_exports"] == 1
    assert sync["latest_export"]["status"] == "blocked_model_review"


def test_health_marks_stale_chat_export_sync_as_an_error(client, monkeypatch, tmp_path):
    sync_path = tmp_path / "chat-export-sync.json"
    monkeypatch.setenv("CHAT_EXPORT_SYNC_STATE_PATH", str(sync_path))
    monkeypatch.setattr(health_router, "_utcnow", lambda: dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC))
    sync_path.write_text(
        json.dumps(
            {
                "last_checked_at": "2026-07-20T00:00:00Z",
                "last_processed_at": "2026-07-20T00:00:00Z",
                "last_processed_fingerprint": {"name": "WhatsApp Chat - 455 Tenants.zip"},
            }
        ),
        encoding="utf-8",
    )

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["chat_export_sync"] == {
        "state": "stale",
        "last_checked_at": "2026-07-20T00:00:00Z",
        "last_processed_at": "2026-07-20T00:00:00Z",
        "has_error": True,
    }


def test_health_names_incomplete_model_review_as_blocked(client, monkeypatch, tmp_path):
    sync_path = tmp_path / "chat-export-sync.json"
    monkeypatch.setenv("CHAT_EXPORT_SYNC_STATE_PATH", str(sync_path))
    monkeypatch.setattr(health_router, "_utcnow", lambda: dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC))
    sync_path.write_text(
        json.dumps(
            {
                "last_checked_at": "2026-07-20T01:59:00Z",
                "last_processed_at": "2026-07-20T00:00:00Z",
                "last_error": "model review incomplete: 104 missing, 38 failed",
            }
        ),
        encoding="utf-8",
    )

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["chat_export_sync"] == {
        "state": "blocked_model_review",
        "last_checked_at": "2026-07-20T01:59:00Z",
        "last_processed_at": "2026-07-20T00:00:00Z",
        "has_error": True,
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
