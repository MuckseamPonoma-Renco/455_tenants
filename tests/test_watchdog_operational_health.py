import datetime as dt
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import apps.api.routers.health as health
import packages.automation_status as status
import scripts.audit_public_watchdog_tabs as audit
import scripts.run_automation_daemon as daemon
import scripts.run_cloud_recovery_cycle as recovery
from packages.public_records.config import source_configs


NOW = dt.datetime(2026, 9, 6, 14, 0, tzinfo=dt.UTC)


def _success_result():
    return {
        "ok": True, "source_errors": 0, "sheet_errors": 0, "sheet_readback_ok": True,
        "source_health": {"ok": True, "sources": [
            {"source_key": source.key, "state": "ready", "source_errors": 0, "last_success_at": status._now_iso()}
            for source in source_configs()
        ]},
    }


@pytest.fixture
def isolated_status(monkeypatch, tmp_path):
    path = tmp_path / "automation.json"
    monkeypatch.setenv("AUTOMATION_STATUS_PATH", str(path))
    monkeypatch.setenv("AUTOMATION_PUBLIC_RECORD_SYNC_SECONDS", "21600")
    monkeypatch.delenv("AUTOMATION_PUBLIC_RECORD_RETRY_SECONDS", raising=False)
    monkeypatch.delenv("PUBLIC_RECORD_SYNC_GRACE_SECONDS", raising=False)
    monkeypatch.setattr(status, "_now_iso", lambda: "2026-09-06T14:00:00Z")
    monkeypatch.setattr(health, "_utcnow", lambda: NOW)
    return path


def test_partial_sync_stays_degraded_after_heartbeat_and_retries_in_five_minutes(monkeypatch, isolated_status):
    status.update_watchdog_status(state="ready", interval_seconds=21600, result=_success_result())
    monkeypatch.setattr(status, "_now_iso", lambda: "2026-09-06T14:05:00Z")
    monkeypatch.setattr(daemon, "resync_replacement_watchdog", lambda: {"ok": True, "source_errors": 1})
    events = []
    monkeypatch.setattr(daemon, "_record_audit_event", lambda *args: events.append(args))

    result = daemon._run_watchdog_step(poll_seconds=60, interval_seconds=21600)
    daemon._write_automation_status(state="ready", poll_seconds=60)

    saved = status.read_automation_status()
    assert saved["state"] == "ready"
    assert saved["watchdog"]["state"] == "degraded"
    assert saved["watchdog"]["last_success_at"] == "2026-09-06T14:00:00Z"
    assert saved["watchdog"]["last_completed_at"] == "2026-09-06T14:05:00Z"
    assert health._public_watchdog_status(saved)["has_error"] is True
    assert daemon._watchdog_next_delay(result, 21600) == 300
    assert events[0][0] == "AUTOMATION_STEP_ERROR"


def test_failed_sync_exception_preserves_success_and_recovery_clears_failure(monkeypatch, isolated_status):
    status.update_watchdog_status(state="ready", interval_seconds=21600, result=_success_result())
    monkeypatch.setattr(daemon, "resync_replacement_watchdog", lambda: (_ for _ in ()).throw(RuntimeError("private connection detail")))
    monkeypatch.setattr(daemon, "_record_audit_event", lambda *_args: None)
    failed = daemon._run_watchdog_step(poll_seconds=60, interval_seconds=21600)
    public = health._public_watchdog_status(status.read_automation_status())
    assert public["state"] == "degraded"
    assert "private connection detail" not in json.dumps(public)
    assert daemon._watchdog_next_delay(failed, 21600) == 300

    monkeypatch.setattr(status, "_now_iso", lambda: "2026-09-06T14:05:00Z")
    monkeypatch.setattr(daemon, "resync_replacement_watchdog", _success_result)
    recovered = daemon._run_watchdog_step(poll_seconds=60, interval_seconds=21600)
    assert status.read_automation_status()["watchdog"]["last_success_at"] == "2026-09-06T14:05:00Z"
    assert status.read_automation_status()["watchdog"]["has_error"] is False
    assert daemon._watchdog_next_delay(recovered, 21600) == 21600


@pytest.mark.parametrize("change", [
    {"last_success_at": "2026-09-06T07:00:00Z"},
    {"last_success_at": "2026-09-06T14:01:00Z"},
    {"source_errors": 1},
    {"sheet_errors": 1},
    {"sheet_readback_ok": False},
    {"last_success_at": None},
    {"interval_seconds": 0},
    {"sources": []},
    {"sources": None},
])
def test_cloud_requires_successful_fresh_watchdog_even_with_healthy_loop(monkeypatch, isolated_status, change):
    payload = {
        "ok": True, "database_ready": True,
        "storage": {"state": "ready", "low_disk": False},
        "automation": {"state": "ready", "has_error": False, "last_cycle_at": "2026-09-06T13:59:30Z"},
        "watchdog": {
            "state": "ready", "has_error": False, "last_success_at": "2026-09-06T09:00:00Z",
            "source_errors": 0, "sheet_errors": 0, "sheet_readback_ok": True, "interval_seconds": 21600,
            "sources": _success_result()["source_health"]["sources"],
        },
    }

    class Response:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *_args): return False

    monkeypatch.setattr(recovery.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())
    monkeypatch.setattr(recovery.json, "load", lambda _response: payload)
    assert recovery.primary_automation_healthy(now=NOW, require_chat_export_sync=False, require_watchdog_sync=True)
    payload["watchdog"].update(change)
    assert recovery.primary_automation_healthy(now=NOW, require_chat_export_sync=False)
    assert not recovery.primary_automation_healthy(now=NOW, require_chat_export_sync=False, require_watchdog_sync=True)


def test_cloud_runs_stale_watchdog_despite_healthy_core_and_fails_on_partial_source_sync():
    calls = []
    operations = recovery.CloudRecoveryOperations(
        receiver_config=lambda: None,
        sync_cloud_exports=lambda _config: pytest.fail("unexpected export"),
        sync_311_statuses=lambda: pytest.fail("unexpected status"),
        sync_replacement_watchdog=lambda: calls.append("watchdog") or {"ok": True, "source_errors": 1},
        audit_public_tenant_log=lambda: {"ok": True},
    )
    result = recovery.run_cycle(
        "watchdog", operations=operations,
        primary_healthy=lambda: True, primary_core_healthy=lambda: True, primary_watchdog_healthy=lambda: False,
    )
    assert calls == ["watchdog"]
    assert result["ok"] is False
    assert result["failed_steps"] == ["replacement_watchdog"]


def test_cloud_cli_returns_failure_for_incomplete_step(monkeypatch):
    monkeypatch.setenv("AUTO_FILE_ENABLED", "1")
    monkeypatch.setenv("PROCESS_INLINE", "1")
    monkeypatch.setenv("DISABLE_SHEETS_SYNC", "0")
    monkeypatch.setattr(recovery, "parse_args", lambda: SimpleNamespace(env_file=None, check_config=False, mode="watchdog", force=False))
    monkeypatch.setattr(recovery, "load_local_env_file", lambda *_args: None)
    monkeypatch.setattr(recovery, "config_errors", lambda **_kwargs: [])
    monkeypatch.setattr(recovery, "run_cycle", lambda *_args, **_kwargs: {"ok": False, "failed_steps": ["replacement_watchdog"]})
    assert recovery.main() == 1


def test_public_source_freshness_uses_six_hour_cadence_but_policy_still_uses_90_minutes(isolated_status):
    spec = audit.TabSpec(
        logical_name="ElevatorWatch", title="ElevatorWatch", headers=("Topic", "Last checked"),
        date_columns=frozenset({1}), volatile_timestamp_column=1,
        system_freshness_topic=audit.SYSTEM_WATCHDOG_FRESHNESS_TOPIC,
    )
    values = [list(spec.headers), ["DOB filing", "2026-09-06T09:00:00Z"], [audit.SYSTEM_WATCHDOG_FRESHNESS_TOPIC, "2026-09-06T13:30:00Z"]]

    def check(rows):
        return audit._audit_tab(
            spec, audit.ExpectedTab(rows, "USER_ENTERED"), audit.LiveTab(rows, rows),
            metadata={}, workbook_tz=ZoneInfo("America/New_York"), now=NOW,
            max_age_seconds=status.watchdog_max_age_seconds(), max_drift_seconds=5400, limit=20,
        )

    result = check(values)
    assert result["ok"] is True
    assert result["volatile_last_checked"][0]["max_age_seconds"] == 23400
    values[2][1] = "2026-09-06T12:00:00Z"
    stale_policy = check(values)
    assert stale_policy["ok"] is False
    assert stale_policy["system_watchdog_freshness"]["within_freshness_bound"] is False


def test_health_shows_degradation_without_leaking_source_errors(client, monkeypatch, isolated_status):
    status.update_watchdog_status(state="degraded", interval_seconds=21600, result={
        "ok": False, "source_errors": 1,
        "source_health": {"ok": False, "sources": [{"source_key": "dob_complaints", "state": "error", "source_errors": 1, "last_error": "secret query"}]},
    })
    payload = client.get("/health").json()
    assert payload["watchdog_operational_state"] == "degraded"
    assert payload["watchdog"]["state"] == "degraded"
    assert payload["watchdog"]["sources"][0]["source_key"] == "dob_complaints"
    assert "secret query" not in json.dumps(payload)


@pytest.mark.parametrize("sources", [None, [], [{"source_key": "unknown", "state": "ready", "source_errors": 0, "last_success_at": "2026-09-06T09:00:00Z"}]])
def test_missing_source_evidence_cannot_be_promoted_to_success(isolated_status, sources):
    result = _success_result()
    result["source_health"]["sources"] = sources
    assert status.watchdog_result_ok(result) is False
    status.update_watchdog_status(state="ready", interval_seconds=21600, result=result)
    assert health._public_watchdog_status(status.read_automation_status())["state"] == "degraded"


@pytest.mark.parametrize("field", ["sheet_readback_ok", "sheet_errors"])
def test_result_requires_explicit_sheet_completion_evidence(isolated_status, field):
    result = _success_result()
    result.pop(field)
    assert status.watchdog_result_ok(result) is False


def test_stale_receipts_cannot_renew_success_or_wait_six_hours_for_retry(isolated_status):
    result = _success_result()
    result["source_health"]["sources"][0]["last_success_at"] = "2026-09-05T00:00:00Z"
    assert status.watchdog_result_ok(result) is False
    assert daemon._watchdog_next_delay(result, 21600) == 300
