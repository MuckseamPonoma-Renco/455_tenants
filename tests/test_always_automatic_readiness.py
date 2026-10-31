import argparse
import base64
import datetime as dt
import json

import scripts.check_always_automatic_readiness as readiness
from scripts.check_always_automatic_readiness import CommandResult, build_report, compact_report


def _args(**overrides):
    values = {
        "repo": "owner/repo",
        "include_public_sheet": False,
        "include_watchdog_sync": False,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


HEAD_SHA = "a" * 40


def _runner(*, cloud_ready: bool, github_ready: bool, filing_supported=False, filing_enabled=False, worker_receipt=False):
    def fake(args, _timeout):
        command = " ".join(args)
        if args[0].endswith("check_mac_services.sh"):
            return CommandResult(
                0,
                json.dumps(
                    {
                        "outcome": "healthy",
                        "services": [
                            {"name": "api", "state": "healthy"},
                            {"name": "cloud_export_receiver", "state": "healthy" if cloud_ready else "not_configured"},
                        ],
                    }
                ),
                "",
            )
        if len(args) > 1 and args[1].endswith("check_public_health.py"):
            strict = "--require-cloud-export-receiver" in args
            if strict and not cloud_ready:
                return CommandResult(
                    1,
                    json.dumps({"ok": False, "failures": ["cloud export receiver is not_configured"]}),
                    "",
                )
            return CommandResult(0, json.dumps({"ok": True, "failures": []}), "")
        if args[:3] == ["gh", "secret", "list"]:
            stdout = (
                "CLOUD_RECOVERY_ENABLED\t2026-07-20T00:00:00Z\n"
                "CLOUD_RECOVERY_ENV\t2026-07-20T00:00:00Z\n"
                "CLOUD_RECOVERY_GOOGLE_SERVICE_ACCOUNT_JSON\t2026-07-20T00:00:00Z\n"
                if github_ready
                else ""
            )
            return CommandResult(0, stdout, "")
        if args[:3] == ["gh", "variable", "get"]:
            if args[3] == "CLOUD_311_FILING_ENABLED":
                return CommandResult(0, "true\n" if filing_enabled else "false\n", "")
            return CommandResult(0 if github_ready else 1, "true\n" if github_ready else "", "")
        if args[:2] == ["gh", "api"]:
            if "/commits/main" in args[2]:
                return CommandResult(0, HEAD_SHA, "")
            source = "CLOUD_RECOVERY_CAPABILITIES = " + repr({
                "automatic_311_filing": filing_supported, "live_whatsapp_capture": False,
            })
            return CommandResult(0, json.dumps({"sha": "blob", "content": base64.b64encode(source.encode()).decode()}), "")
        if args[:3] == ["gh", "run", "list"]:
            return CommandResult(0, json.dumps([{
                "databaseId": 123, "headSha": HEAD_SHA, "conclusion": "success",
                "updatedAt": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat(),
            }]), "")
        if args[:3] == ["gh", "run", "view"]:
            lines = ['recover\tRun cycle\t2026-10-31T00:00:00.123Z {"ok": ***}']
            if worker_receipt:
                receipt = {"schema": 1, "ok": 1, "cycle_ok": 1, "submitted": 0, "lookup_verified": 0, "empty": 1, "blocked": 0}
                lines.append("recover\tRun cycle\t2026-10-31T00:00:00.123Z CLOUD_311_RECEIPT_V1 " + json.dumps(receipt))
            return CommandResult(0, "\n".join(lines), "")
        if args[:2] == ["fdesetup", "status"]:
            return CommandResult(0, "FileVault is On.\n", "")
        if len(args) > 2 and args[1] == "-c" and "sync_replacement_watchdog" in args[2]:
            assert "sync_replacement_watchdog_to_sheets" in args[2]
            return CommandResult(0, json.dumps({"sheets_synced": True}), "")
        if len(args) > 1 and args[1].endswith("audit_public_watchdog_tabs.py"):
            return CommandResult(0, json.dumps({"ok": True}), "")
        raise AssertionError(f"unexpected command: {command}")

    return fake


def test_readiness_report_fails_when_mac_off_receiver_and_github_recovery_are_missing(monkeypatch):
    monkeypatch.setattr(readiness.platform, "system", lambda: "Darwin")

    report = build_report(_args(), runner=_runner(cloud_ready=False, github_ready=False))

    assert report["ok"] is False
    by_name = {row["name"]: row for row in report["checks"]}
    assert by_name["mac_services"]["ok"] is True
    assert by_name["cloud_receiver_configuration"]["ok"] is False
    assert by_name["github_cloud_recovery_gate"]["ok"] is False
    assert by_name["filevault_boundary"]["status"] == "warn"
    assert "Cloudflare receiver" in report["next_required_action"]


def test_configured_export_recovery_does_not_prove_mac_off_filing(monkeypatch):
    monkeypatch.setattr(readiness.platform, "system", lambda: "Darwin")

    report = build_report(_args(), runner=_runner(cloud_ready=True, github_ready=True))

    assert report["ok"] is False
    assert report["capabilities"]["mac_off_311_filing_verified"] is False
    assert report["capabilities"]["cloud_live_whatsapp_capture_supported"] is False
    assert "cloud export intake and status updates do not prove" in report["next_required_action"]


def test_compact_report_keeps_verdict_without_raw_command_evidence(monkeypatch):
    monkeypatch.setattr(readiness.platform, "system", lambda: "Darwin")
    report = build_report(_args(), runner=_runner(cloud_ready=False, github_ready=False))

    compact = compact_report(report)

    assert compact["ok"] is False
    assert compact["status_counts"] == {"pass": 2, "fail": 4, "warn": 1}
    assert [row["name"] for row in compact["failed_checks"]] == [
        "cloud_receiver_configuration",
        "github_cloud_recovery_gate",
        "cloud_311_filing_configuration",
        "mac_off_311_filing",
    ]
    assert compact["warnings"][0]["name"] == "filevault_boundary"
    assert "checks" not in compact
    assert "command" not in json.dumps(compact)


def test_watchdog_readiness_refreshes_then_audits_public_tabs(monkeypatch):
    monkeypatch.setattr(readiness.platform, "system", lambda: "Darwin")

    report = build_report(
        _args(include_watchdog_sync=True),
        runner=_runner(cloud_ready=True, github_ready=True),
    )

    assert report["ok"] is False
    assert [row["name"] for row in report["checks"][-2:]] == [
        "replacement_watchdog",
        "public_watchdog_tabs",
    ]


def test_next_action_names_model_quota_and_cloud_recovery_gaps():
    action = readiness._next_required_action(
        [
            {
                "name": "mac_services",
                "evidence": {
                    "payload": {
                        "services": [
                            {
                                "name": "chat_export_sync",
                                "state": "blocked",
                                "reason": "required OpenAI model review is incomplete",
                            }
                        ]
                    }
                },
            },
            {
                "name": "github_cloud_recovery_gate",
                "detail": "missing recovery secrets",
            },
        ]
    )

    assert "OpenAI API project's quota" in action
    assert "three GitHub cloud-recovery secrets" in action


def test_cloud_filing_requires_capability_flag_and_worker_receipt(monkeypatch):
    monkeypatch.setattr(readiness.platform, "system", lambda: "Darwin")
    for supported, enabled, receipt in [(False, True, True), (True, False, True), (True, True, False)]:
        report = build_report(_args(), runner=_runner(
            cloud_ready=True, github_ready=True, filing_supported=supported,
            filing_enabled=enabled, worker_receipt=receipt,
        ))
        assert report["ok"] is False
        assert report["capabilities"]["mac_off_311_filing_verified"] is False


def test_cloud_filing_passes_with_recent_exact_source_safe_batch(monkeypatch):
    monkeypatch.setattr(readiness.platform, "system", lambda: "Darwin")
    report = build_report(_args(), runner=_runner(
        cloud_ready=True, github_ready=True, filing_supported=True,
        filing_enabled=True, worker_receipt=True,
    ))
    assert report["ok"] is True
    assert report["capabilities"]["mac_off_311_filing_verified"] is True
    assert report["capabilities"]["mac_services_require_login"] is True
    assert report["capabilities"]["cloud_live_whatsapp_capture_supported"] is False


def test_legacy_cloud_auto_file_disable_is_negative_capability_evidence():
    capabilities = readiness._cloud_capabilities('import os\ndef main():\n    os.environ["AUTO_FILE_ENABLED"] = "0"\n')
    assert capabilities["automatic_311_filing"] is False
    assert readiness._cloud_capabilities("pass")["automatic_311_filing"] is None


def test_cloud_source_cannot_execute_code():
    source = 'CLOUD_RECOVERY_CAPABILITIES = dict(automatic_311_filing=True)'
    try:
        readiness._cloud_capabilities(source)
    except ValueError:
        pass
    else:
        raise AssertionError("nonliteral declaration was accepted")


def test_filevault_off_still_reports_login_dependency(monkeypatch):
    monkeypatch.setattr(readiness.platform, "system", lambda: "Darwin")
    result = readiness.check_filevault_boundary(lambda *_: CommandResult(0, "FileVault is Off.", ""))
    assert result["status"] == "warn"
    assert result["evidence"]["filevault_enabled"] is False
    assert result["evidence"]["mac_after_reboot_without_login_ready"] is False


def test_receipt_parser_accepts_integer_marker_amid_github_masked_json():
    receipt = {"schema": 1, "ok": 1, "cycle_ok": 1, "submitted": 0, "lookup_verified": 0, "empty": 1, "blocked": 0}
    prefix = "recover\tRun configuration check or recovery cycle\t2026-10-31T00:00:00.1234567Z "
    log = "\n".join([
        prefix + '{"ok": ***, "current_filings": {"ok": ***, "empty": ***}}',
        prefix + "CLOUD_311_RECEIPT_V1 " + json.dumps(receipt, separators=(",", ":")),
        prefix + '{"configuration_ready": ***}',
    ])
    assert readiness._filing_receipt_from_log(log) == receipt


def test_receipt_parser_rejects_unmarked_private_and_invalid_receipts():
    receipt = {"schema": 1, "ok": 1, "cycle_ok": 1, "submitted": 0, "lookup_verified": 0, "empty": 1, "blocked": 0}
    assert readiness._filing_receipt_from_log(json.dumps(receipt)) is None
    assert readiness._filing_receipt_from_log(json.dumps({"ok": True, "current_filings": {"ok": True}})) is None
    for changes in (
        {"schema": 2}, {"ok": 0}, {"cycle_ok": 0}, {"blocked": 1},
        {"submitted": -1}, {"ok": True}, {"empty": "1"}, {"lookup_verified": 1.0},
        {"tenant_text": "must not escape"},
    ):
        payload = {**receipt, **changes}
        assert readiness._filing_receipt_from_log("CLOUD_311_RECEIPT_V1 " + json.dumps(payload)) is None
    missing = dict(receipt)
    missing.pop("empty")
    assert readiness._filing_receipt_from_log("CLOUD_311_RECEIPT_V1 " + json.dumps(missing)) is None


def test_stale_or_wrong_source_runs_do_not_prove_cloud_filing():
    now = dt.datetime(2026, 10, 31, tzinfo=dt.UTC)
    configuration = {"ok": True, "evidence": {"head_sha": HEAD_SHA}}
    calls = []
    def runner(args, timeout):
        calls.append(args)
        return CommandResult(0, json.dumps([
            {"databaseId": 1, "headSha": HEAD_SHA, "conclusion": "success", "updatedAt": "2026-10-30T20:00:00Z"},
            {"databaseId": 2, "headSha": "b" * 40, "conclusion": "success", "updatedAt": "2026-10-30T23:59:00Z"},
        ]), "")
    result = readiness.check_cloud_filing_execution("owner/repo", configuration, runner, now=now)
    assert result["ok"] is False
    assert len(calls) == 1


def test_cloud_source_lookup_failure_is_not_reported_ready():
    result = readiness.check_cloud_filing_capability("owner/repo", lambda *_: CommandResult(1, "", "not found"))
    assert result["ok"] is False
    assert "Could not inspect" in result["detail"]
