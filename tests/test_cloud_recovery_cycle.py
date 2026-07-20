import datetime as dt
import json

import scripts.run_cloud_recovery_cycle as recovery
from scripts.run_cloud_recovery_cycle import CloudRecoveryOperations, _compact_cloud_result, config_errors, run_cycle


def test_config_errors_requires_all_cloud_recovery_inputs(tmp_path):
    credentials_path = tmp_path / "gcp.json"
    credentials_path.write_text("{}", encoding="utf-8")
    configured = {
        "DATABASE_URL": "postgresql://example",
        "CLOUD_EXPORT_RECEIVER_URL": "https://uploads.example.test",
        "CLOUD_EXPORT_RECEIVER_PULL_TOKEN": "token",
        "GOOGLE_APPLICATION_CREDENTIALS": str(credentials_path),
        "GOOGLE_SHEETS_SPREADSHEET_ID": "sheet-id",
    }

    assert config_errors(configured) == []
    configured["GOOGLE_APPLICATION_CREDENTIALS"] = str(tmp_path / "missing.json")
    assert config_errors(configured) == ["GOOGLE_APPLICATION_CREDENTIALS file is missing"]


def test_full_cycle_runs_exports_and_maintenance_without_portal_filing():
    calls = []
    receiver = object()
    operations = CloudRecoveryOperations(
        receiver_config=lambda: receiver,
        sync_cloud_exports=lambda config: calls.append(("exports", config)) or {
            "action": "processed",
            "processed": [{"key": "pending/one"}],
            "pending_exports": 2,
            "recovered_acknowledgements": 1,
        },
        sync_311_statuses=lambda: calls.append(("status", None)) or {"ok": True, "updated": 2},
        sync_replacement_watchdog=lambda: calls.append(("watchdog", None)) or {"ok": True, "actions_open": 1},
        audit_public_tenant_log=lambda: calls.append(("audit", None)) or {"ok": True, "live_recent_rows": 14},
    )

    result = run_cycle("full", operations=operations, primary_healthy=lambda: False)

    assert calls == [("exports", receiver), ("status", None), ("watchdog", None), ("audit", None)]
    assert result["cloud_exports"] == {
        "action": "processed",
        "processed_exports": 1,
        "pending_exports": 2,
        "recovered_acknowledgements": 1,
    }
    assert result["status_sync"]["updated"] == 2
    assert result["replacement_watchdog"]["actions_open"] == 1
    assert result["public_tenant_log_qa"]["live_recent_rows"] == 14


def test_status_cycle_does_not_download_or_file_exports():
    calls = []
    operations = CloudRecoveryOperations(
        receiver_config=lambda: (_ for _ in ()).throw(AssertionError("receiver should not be used")),
        sync_cloud_exports=lambda _config: (_ for _ in ()).throw(AssertionError("exports should not be synced")),
        sync_311_statuses=lambda: calls.append("status") or {"ok": True},
        sync_replacement_watchdog=lambda: (_ for _ in ()).throw(AssertionError("watchdog should not run")),
        audit_public_tenant_log=lambda: calls.append("audit") or {"ok": True},
    )

    assert run_cycle("status", operations=operations, primary_healthy=lambda: False)["ok"] is True
    assert calls == ["status", "audit"]


def test_cycle_skips_without_loading_runtime_operations_when_primary_is_healthy():
    result = run_cycle("full", primary_healthy=lambda: True)

    assert result == {"ok": True, "mode": "full", "action": "skipped_primary_healthy"}


def test_primary_automation_health_requires_a_fresh_working_heartbeat(monkeypatch):
    now = dt.datetime(2026, 7, 20, 4, 30, tzinfo=dt.UTC)

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    payload = {
        "ok": True,
        "automation": {
            "state": "ready",
            "has_error": False,
            "last_cycle_at": "2026-07-20T04:20:00Z",
        },
    }
    monkeypatch.setattr(recovery.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())
    monkeypatch.setattr(recovery.json, "load", lambda _response: payload)

    assert recovery.primary_automation_healthy(now=now) is True

    payload["automation"]["last_cycle_at"] = "2026-07-20T04:00:00Z"
    assert recovery.primary_automation_healthy(now=now) is False

    payload["automation"]["last_cycle_at"] = "2026-07-20T04:31:00Z"
    assert recovery.primary_automation_healthy(now=now) is False


def test_primary_automation_health_tolerates_invalid_maximum_age(monkeypatch):
    monkeypatch.setenv("CLOUD_RECOVERY_PRIMARY_MAX_AGE_SECONDS", "not-a-number")

    assert recovery._primary_maximum_age_seconds() == 1200


def test_compact_cloud_result_excludes_local_paths_and_audit_content():
    result = _compact_cloud_result(
        {
            "action": "processed",
            "processed": [{"staged_export": "/private/chat.zip", "audit": {"raw": "message"}}],
            "pending_exports": 0,
            "recovered_acknowledgements": 0,
        }
    )

    assert result == {
        "action": "processed",
        "processed_exports": 1,
        "pending_exports": 0,
        "recovered_acknowledgements": 0,
    }
