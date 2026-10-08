import datetime as dt
import json
from types import SimpleNamespace

import pytest

import scripts.run_cloud_recovery_cycle as recovery
from scripts.run_cloud_recovery_cycle import CloudRecoveryOperations, _compact_cloud_result, config_errors, run_cycle
from packages.public_records.config import source_configs


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
            "ok": True,
            "action": "processed",
            "processed": [{"key": "pending/one"}],
            "pending_exports": 2,
            "recovered_acknowledgements": 1,
            "recovered_cloud_receipts": 1,
        },
        sync_311_statuses=lambda: calls.append(("status", None)) or {"ok": True, "updated": 2},
        sync_replacement_watchdog=lambda: calls.append(("watchdog", None)) or {
            "ok": True, "source_errors": 0, "sheet_errors": 0, "sheet_readback_ok": True, "actions_open": 1,
            "source_health": {"ok": True, "sources": [
                {"source_key": source.key, "state": "ready", "source_errors": 0, "last_success_at": dt.datetime.now(dt.UTC).isoformat()}
                for source in source_configs()
            ]},
        },
        audit_public_tenant_log=lambda: calls.append(("audit", None)) or {"ok": True, "live_recent_rows": 14},
    )

    result = run_cycle("full", operations=operations, primary_healthy=lambda: False, primary_watchdog_healthy=lambda: False)

    assert calls == [("exports", receiver), ("status", None), ("watchdog", None), ("audit", None)]
    assert result["ok"] is True
    assert result["cloud_exports"] == {
        "action": "processed",
        "processed_exports": 1,
        "blocked_model_review_exports": 0,
        "pending_exports": 2,
        "recovered_acknowledgements": 1,
        "recovered_cloud_receipts": 1,
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


def test_status_cycle_skips_when_core_is_healthy_despite_export_degradation():
    operations = CloudRecoveryOperations(
        receiver_config=lambda: (_ for _ in ()).throw(AssertionError("receiver should not be used")),
        sync_cloud_exports=lambda _config: (_ for _ in ()).throw(AssertionError("exports should not be synced")),
        sync_311_statuses=lambda: (_ for _ in ()).throw(AssertionError("status should not be synced")),
        sync_replacement_watchdog=lambda: (_ for _ in ()).throw(AssertionError("watchdog should not run")),
        audit_public_tenant_log=lambda: (_ for _ in ()).throw(AssertionError("sheet should not be audited")),
    )

    result = run_cycle(
        "status",
        operations=operations,
        primary_healthy=lambda: False,
        primary_core_healthy=lambda: True,
    )

    assert result == {"ok": True, "mode": "status", "action": "skipped_primary_healthy"}


def test_watchdog_cycle_skips_when_watchdog_is_healthy_despite_export_degradation():
    operations = CloudRecoveryOperations(
        receiver_config=lambda: (_ for _ in ()).throw(AssertionError("receiver should not be used")),
        sync_cloud_exports=lambda _config: (_ for _ in ()).throw(AssertionError("exports should not be synced")),
        sync_311_statuses=lambda: (_ for _ in ()).throw(AssertionError("status should not be synced")),
        sync_replacement_watchdog=lambda: (_ for _ in ()).throw(AssertionError("watchdog should not run")),
        audit_public_tenant_log=lambda: (_ for _ in ()).throw(AssertionError("sheet should not be audited")),
    )

    result = run_cycle(
        "watchdog",
        operations=operations,
        primary_healthy=lambda: False,
        primary_core_healthy=lambda: True,
        primary_watchdog_healthy=lambda: True,
    )

    assert result == {"ok": True, "mode": "watchdog", "action": "skipped_primary_healthy"}


def test_full_cycle_runs_only_exports_when_core_is_healthy():
    calls = []
    receiver = object()
    operations = CloudRecoveryOperations(
        receiver_config=lambda: receiver,
        sync_cloud_exports=lambda config: calls.append(("exports", config)) or {
            "ok": True,
            "action": "unchanged_skip",
            "processed": [],
            "pending_exports": 0,
        },
        sync_311_statuses=lambda: (_ for _ in ()).throw(AssertionError("status should not be synced")),
        sync_replacement_watchdog=lambda: (_ for _ in ()).throw(AssertionError("watchdog should not run")),
        audit_public_tenant_log=lambda: (_ for _ in ()).throw(AssertionError("sheet should not be audited")),
    )

    result = run_cycle(
        "full",
        operations=operations,
        primary_healthy=lambda: False,
        primary_core_healthy=lambda: True,
        primary_watchdog_healthy=lambda: True,
    )

    assert calls == [("exports", receiver)]
    assert result["action"] == "partial_recovery_run"
    assert "status_sync" not in result
    assert "replacement_watchdog" not in result
    assert "public_tenant_log_qa" not in result


def test_cycle_skips_without_loading_runtime_operations_when_primary_is_healthy():
    result = run_cycle("full", primary_healthy=lambda: True, primary_watchdog_healthy=lambda: True)

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
        "database_ready": True,
        "storage": {
            "state": "ready",
            "low_disk": False,
        },
        "automation": {
            "state": "ready",
            "has_error": False,
            "last_cycle_at": "2026-07-20T04:20:00Z",
        },
        "chat_export_sync": {
            "state": "ready",
            "has_error": False,
        },
    }
    monkeypatch.setattr(recovery.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())
    monkeypatch.setattr(recovery.json, "load", lambda _response: payload)

    assert recovery.primary_automation_healthy(now=now) is True

    payload["chat_export_sync"]["state"] = "sheet_readback_pending"
    assert recovery.primary_automation_healthy(now=now) is True
    payload["chat_export_sync"]["state"] = "ready"

    payload["automation"]["last_cycle_at"] = "2026-07-20T04:00:00Z"
    assert recovery.primary_automation_healthy(now=now) is False

    payload["automation"]["last_cycle_at"] = "2026-07-20T04:31:00Z"
    assert recovery.primary_automation_healthy(now=now) is False

    payload["automation"]["last_cycle_at"] = "2026-07-20T04:20:00Z"
    payload["chat_export_sync"] = {"state": "legacy_unverified", "has_error": True}
    assert recovery.primary_automation_healthy(now=now) is False
    assert recovery.primary_automation_healthy(now=now, require_chat_export_sync=False) is True


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
        "blocked_model_review_exports": 0,
        "pending_exports": 0,
        "recovered_acknowledgements": 0,
        "recovered_cloud_receipts": 0,
    }


def _successful_watchdog_result():
    return {
        "ok": True, "source_errors": 0, "sheet_errors": 0, "sheet_readback_ok": True,
        "source_health": {"ok": True, "sources": [
            {"source_key": source.key, "state": "ready", "source_errors": 0,
             "last_success_at": dt.datetime.now(dt.UTC).isoformat()}
            for source in source_configs()
        ]},
    }


@pytest.mark.parametrize("failed_step", ["cloud_exports", "status_sync", "replacement_watchdog", "public_tenant_log_qa"])
def test_one_recovery_exception_does_not_skip_independent_steps_or_expose_details(failed_step):
    calls = []

    def operation(name, result):
        def run(*_args):
            calls.append(name)
            if name == failed_step:
                raise RuntimeError("private fixture path and message content must not be emitted")
            return result
        return run

    operations = CloudRecoveryOperations(
        receiver_config=lambda: object(),
        sync_cloud_exports=operation("cloud_exports", {"ok": True, "action": "unchanged_skip"}),
        sync_311_statuses=operation("status_sync", {"ok": True}),
        sync_replacement_watchdog=operation("replacement_watchdog", _successful_watchdog_result()),
        audit_public_tenant_log=operation("public_tenant_log_qa", {"ok": True}),
    )
    result = run_cycle("full", operations=operations, force=True)
    assert calls == ["cloud_exports", "status_sync", "replacement_watchdog", "public_tenant_log_qa"]
    assert result["ok"] is False
    assert result["failed_steps"] == [failed_step]
    assert result[failed_step] == {"ok": False, "error": "recovery_step_failed"}
    assert "private fixture" not in json.dumps(result)


def test_missing_receiver_does_not_block_full_cycle_maintenance():
    calls = []
    operations = CloudRecoveryOperations(
        receiver_config=lambda: None,
        sync_cloud_exports=lambda _config: pytest.fail("not configured"),
        sync_311_statuses=lambda: calls.append("status") or {"ok": True},
        sync_replacement_watchdog=lambda: calls.append("watchdog") or _successful_watchdog_result(),
        audit_public_tenant_log=lambda: calls.append("audit") or {"ok": True},
    )
    result = run_cycle("full", operations=operations, force=True)
    assert calls == ["status", "watchdog", "audit"]
    assert result["ok"] is False
    assert result["failed_steps"] == ["cloud_exports"]


@pytest.mark.parametrize("export_result", [{}, {"ok": False}, None])
def test_export_result_requires_explicit_success(export_result):
    operations = CloudRecoveryOperations(
        receiver_config=lambda: object(), sync_cloud_exports=lambda _config: export_result,
        sync_311_statuses=lambda: pytest.fail("wrong mode"),
        sync_replacement_watchdog=lambda: pytest.fail("wrong mode"),
        audit_public_tenant_log=lambda: pytest.fail("wrong mode"),
    )
    result = run_cycle("exports", operations=operations, force=True)
    assert result["ok"] is False
    assert result["failed_steps"] == ["cloud_exports"]


def test_primary_health_exception_does_not_abort_other_capabilities():
    calls = []

    def failed_primary_health():
        raise ValueError("invalid health endpoint fixture")

    operations = CloudRecoveryOperations(
        receiver_config=lambda: object(),
        sync_cloud_exports=lambda _config: calls.append("exports") or {"ok": True, "action": "unchanged_skip"},
        sync_311_statuses=lambda: pytest.fail("healthy status capability must remain primary-owned"),
        sync_replacement_watchdog=lambda: calls.append("watchdog") or _successful_watchdog_result(),
        audit_public_tenant_log=lambda: calls.append("audit") or {"ok": True},
    )
    result = run_cycle("full", operations=operations, primary_healthy=failed_primary_health,
                       primary_core_healthy=lambda: True, primary_watchdog_healthy=failed_primary_health)
    assert calls == ["exports", "watchdog", "audit"]
    assert result["ok"] is True
    assert result["action"] == "partial_recovery_run"


@pytest.mark.parametrize("mode,check_config,expected_exit", [
    ("status", False, 0), ("watchdog", False, 0), ("full", False, 0),
    ("status", True, 0), ("watchdog", True, 0), ("full", True, 2), ("exports", False, 2),
])
def test_entrypoint_validates_only_required_capabilities(tmp_path, monkeypatch, mode, check_config, expected_exit):
    # main intentionally sets these for its own process. Register their prior
    # state with monkeypatch so that entrypoint calls cannot alter later tests.
    monkeypatch.setenv("AUTO_FILE_ENABLED", "1")
    monkeypatch.setenv("PROCESS_INLINE", "1")
    monkeypatch.setenv("DISABLE_SHEETS_SYNC", "0")
    credentials = tmp_path / "fixture-credentials.json"
    credentials.write_text("{}")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(credentials))
    monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "fixture-sheet")
    monkeypatch.delenv("CLOUD_EXPORT_RECEIVER_URL", raising=False)
    monkeypatch.delenv("CLOUD_EXPORT_RECEIVER_PULL_TOKEN", raising=False)
    monkeypatch.setattr(recovery, "parse_args", lambda: SimpleNamespace(
        env_file=None, mode=mode, check_config=check_config, force=False,
    ))
    monkeypatch.setattr(recovery, "load_local_env_file", lambda _path: None)
    calls = []
    monkeypatch.setattr(recovery, "run_cycle", lambda selected_mode, **_kwargs: calls.append(selected_mode) or {"ok": True})
    assert recovery.main() == expected_exit
    assert calls == ([mode] if not check_config and expected_exit == 0 else [])
