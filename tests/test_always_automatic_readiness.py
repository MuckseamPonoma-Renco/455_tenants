import argparse
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


def _runner(*, cloud_ready: bool, github_ready: bool):
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
            return CommandResult(0 if github_ready else 1, "true\n" if github_ready else "", "")
        if args[:2] == ["fdesetup", "status"]:
            return CommandResult(0, "FileVault is On.\n", "")
        raise AssertionError(f"unexpected command: {command}")

    return fake


def test_readiness_report_fails_when_mac_off_receiver_and_github_recovery_are_missing(monkeypatch):
    monkeypatch.setattr(readiness.platform, "system", lambda: "Darwin")

    report = build_report(_args(), runner=_runner(cloud_ready=False, github_ready=False))

    assert report["ok"] is False
    by_name = {row["name"]: row for row in report["checks"]}
    assert by_name["mac_services"]["ok"] is True
    assert by_name["mac_off_intake_health"]["ok"] is False
    assert by_name["github_cloud_recovery_gate"]["ok"] is False
    assert by_name["filevault_boundary"]["status"] == "warn"
    assert "Cloudflare receiver" in report["next_required_action"]


def test_readiness_report_passes_when_cloud_receiver_and_github_gate_are_configured(monkeypatch):
    monkeypatch.setattr(readiness.platform, "system", lambda: "Darwin")

    report = build_report(_args(), runner=_runner(cloud_ready=True, github_ready=True))

    assert report["ok"] is True
    assert report["next_required_action"] == ""
    assert {row["status"] for row in report["checks"]} <= {"pass", "warn"}


def test_compact_report_keeps_verdict_without_raw_command_evidence(monkeypatch):
    monkeypatch.setattr(readiness.platform, "system", lambda: "Darwin")
    report = build_report(_args(), runner=_runner(cloud_ready=False, github_ready=False))

    compact = compact_report(report)

    assert compact["ok"] is False
    assert compact["status_counts"] == {"pass": 2, "fail": 2, "warn": 1}
    assert [row["name"] for row in compact["failed_checks"]] == [
        "mac_off_intake_health",
        "github_cloud_recovery_gate",
    ]
    assert compact["warnings"][0]["name"] == "filevault_boundary"
    assert "checks" not in compact
    assert "command" not in json.dumps(compact)
