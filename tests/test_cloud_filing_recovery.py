import copy
import datetime as dt

import pytest

from scripts import run_cloud_recovery_cycle as recovery


def complete_receipt():
    now = dt.datetime.now(dt.UTC).isoformat()
    return {
        "receipt_version": 1,
        "uploaded_at": now,
        "acknowledged_at": now,
        "sha256": "a" * 64,
        "stages": {name: {"state": "complete"} for name in (
            "upload", "discovery", "download", "processing", "audit", "sheet_sync", "sheet_readback", "acknowledgement"
        )},
        "audit": {"audited_messages": 10, "matched_messages": 10, "missing_db_messages": 0,
                  "missing_decisions": 0, "llm_review_required": 9, "llm_review_completed": 9,
                  "llm_review_missing": 0, "llm_review_failed": 0},
    }


def test_verified_receipt_requires_every_stage_and_count():
    good = complete_receipt()
    assert recovery._verified_filing_receipt(good)
    for stage in good["stages"]:
        bad = copy.deepcopy(good)
        bad["stages"][stage]["state"] = "pending"
        assert not recovery._verified_filing_receipt(bad)
    for field in good["audit"]:
        bad = copy.deepcopy(good)
        bad["audit"].pop(field)
        assert not recovery._verified_filing_receipt(bad)
    for field in ("missing_db_messages", "missing_decisions", "llm_review_missing", "llm_review_failed"):
        bad = copy.deepcopy(good)
        bad["audit"][field] = 1
        assert not recovery._verified_filing_receipt(bad)
    for field in ("uploaded_at", "acknowledged_at"):
        for hours in (-25, 1):
            bad = copy.deepcopy(good)
            bad[field] = (dt.datetime.now(dt.UTC) + dt.timedelta(hours=hours)).isoformat()
            assert not recovery._verified_filing_receipt(bad)


def operations(export_result, calls):
    return recovery.CloudRecoveryOperations(
        receiver_config=lambda: object(),
        sync_cloud_exports=lambda _config: export_result,
        sync_311_statuses=lambda: {"ok": True},
        sync_replacement_watchdog=lambda: {"ok": True},
        audit_public_tenant_log=lambda: {"ok": True},
        file_current_incidents=lambda: calls.append("filing") or {"ok": True, "empty": True},
    )


@pytest.mark.parametrize("action,pending,ok", [
    ("blocked_model_review", 1, False), ("skipped_concurrent_run", 0, True),
    ("unknown", 0, True), ("processed", 1, True), ("retry_deferred", 1, False),
])
def test_incomplete_or_unverified_exports_cannot_file(monkeypatch, action, pending, ok):
    monkeypatch.setenv("CLOUD_311_FILING_ENABLED", "true")
    calls = []
    result = recovery.run_cycle("exports", force=True, operations=operations(
        {"ok": ok, "action": action, "pending_exports": pending}, calls))
    assert calls == []
    assert result["current_filings"]["blocked"] == "export_audit_incomplete"
    assert result["ok"] is False


def test_force_does_not_enable_cloud_filing(monkeypatch):
    monkeypatch.delenv("CLOUD_311_FILING_ENABLED", raising=False)
    calls = []
    result = recovery.run_cycle("exports", force=True, operations=operations(
        {"ok": True, "action": "processed", "pending_exports": 0}, calls))
    assert result["ok"] is True
    assert calls == []
    assert "current_filings" not in result


def test_export_recovery_invokes_separate_opted_in_operation(monkeypatch):
    monkeypatch.setenv("CLOUD_311_FILING_ENABLED", "true")
    calls = []
    result = recovery.run_cycle("exports", force=True, operations=operations(
        {"ok": True, "action": "processed", "pending_exports": 0}, calls))
    assert result["current_filings"]["ok"] is True
    assert calls == ["filing"]


def test_mac_heartbeat_does_not_suppress_enabled_cloud_filing(monkeypatch):
    monkeypatch.setenv("CLOUD_311_FILING_ENABLED", "true")
    calls = []
    result = recovery.run_cycle("exports", primary_healthy=lambda: True, operations=operations(
        {"ok": True, "action": "unchanged_skip", "pending_exports": 0}, calls))
    assert result["current_filings"]["ok"] is True
    assert calls == ["filing"]


def test_runtime_worker_not_imported_when_durable_audit_fails(monkeypatch):
    monkeypatch.setenv("CLOUD_311_FILING_ENABLED", "true")
    monkeypatch.setattr(recovery, "cloud_filing_audit_ready", lambda: False)
    assert recovery.file_current_incidents() == {"ok": False, "blocked": "export_audit_unverified"}


def test_browser_failure_blocks_filing_before_audit_or_job_access(monkeypatch):
    monkeypatch.setenv("CLOUD_311_FILING_ENABLED", "true")
    monkeypatch.setenv("CLOUD_311_BROWSER_READY", "false")
    monkeypatch.setattr(recovery, "cloud_filing_audit_ready", lambda: pytest.fail("must not reach audit"))
    assert recovery.file_current_incidents() == {"ok": False, "blocked": "browser_unavailable"}


def test_browser_installed_and_launched_before_recovery():
    from pathlib import Path
    workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/cloud-recovery.yml").read_text()
    assert workflow.index("playwright install --with-deps chromium") < workflow.index("Run configuration check or recovery cycle")
    assert workflow.index("python scripts/check_portal_browser.py") < workflow.index("Run configuration check or recovery cycle")


def test_execution_receipt_avoids_boolean_secret_masking_and_private_fields():
    import json
    receipt = recovery.filing_execution_receipt({"ok": True, "current_filings": {
        "ok": True, "submitted": 1, "lookup_verified": 1, "empty": True, "blocked": 0,
        "private_payload": "never emit", "service_request_number": "private receipt",
    }})
    assert receipt == {"schema": 1, "ok": 1, "cycle_ok": 1, "submitted": 1, "lookup_verified": 1, "empty": 1, "blocked": 0}
    assert "true" not in json.dumps(receipt)
    assert "private" not in json.dumps(receipt)
