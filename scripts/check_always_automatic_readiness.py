#!/usr/bin/env python3
"""Audit whether Tenant Issue OS is actually ready for unattended operation."""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_CLOUD_RECOVERY_SECRETS = {
    "CLOUD_RECOVERY_ENABLED",
    "CLOUD_RECOVERY_ENV",
    "CLOUD_RECOVERY_GOOGLE_SERVICE_ACCOUNT_JSON",
}


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


Runner = Callable[[list[str], int], CommandResult]


def run_command(args: list[str], timeout_seconds: int) -> CommandResult:
    completed = subprocess.run(
        args,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_seconds,
    )
    return CommandResult(completed.returncode, completed.stdout, completed.stderr)


def _check(name: str, *, status: str, ok: bool, detail: str, evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "ok": ok,
        "detail": detail,
        "evidence": evidence or {},
    }


def _command_check(
    name: str,
    args: list[str],
    *,
    runner: Runner,
    timeout_seconds: int,
    success_detail: str,
    failure_detail: str,
) -> dict[str, Any]:
    try:
        result = runner(args, timeout_seconds)
    except Exception as exc:
        return _check(
            name,
            status="fail",
            ok=False,
            detail=f"{failure_detail}: {exc}",
            evidence={"command": args},
        )
    evidence: dict[str, Any] = {"command": args, "returncode": result.returncode}
    try:
        payload = json.loads(result.stdout)
    except Exception:
        payload = None
    if isinstance(payload, dict):
        evidence["payload"] = payload
    elif result.stdout.strip():
        evidence["stdout_tail"] = result.stdout.strip()[-1000:]
    if result.stderr.strip():
        evidence["stderr_tail"] = result.stderr.strip()[-1000:]
    ok = result.returncode == 0
    return _check(
        name,
        status="pass" if ok else "fail",
        ok=ok,
        detail=success_detail if ok else failure_detail,
        evidence=evidence,
    )


def check_mac_services(runner: Runner) -> dict[str, Any]:
    result = _command_check(
        "mac_services",
        [str(ROOT / "scripts" / "check_mac_services.sh"), "--json"],
        runner=runner,
        timeout_seconds=60,
        success_detail="Local LaunchAgents, API, tunnel, storage, WhatsApp capture, and iCloud importer are healthy.",
        failure_detail="One or more Mac-side services are unhealthy.",
    )
    payload = result["evidence"].get("payload")
    if isinstance(payload, dict):
        services = {
            row.get("name"): row
            for row in payload.get("services", [])
            if isinstance(row, dict) and row.get("name")
        }
        cloud = services.get("cloud_export_receiver")
        if isinstance(cloud, dict):
            result["evidence"]["cloud_export_receiver_state"] = cloud.get("state")
    return result


def check_public_health(runner: Runner, *, require_cloud_receiver: bool) -> dict[str, Any]:
    args = [
        sys.executable,
        str(ROOT / "scripts" / "check_public_health.py"),
        "--url",
        "https://api.455tenants.com/health",
        "--max-capture-age-seconds",
        "600",
        "--max-automation-age-seconds",
        "1200",
        "--max-import-age-seconds",
        "3600",
    ]
    name = "mac_off_intake_health" if require_cloud_receiver else "public_health"
    if require_cloud_receiver:
        args.append("--require-cloud-export-receiver")
    return _command_check(
        name,
        args,
        runner=runner,
        timeout_seconds=30,
        success_detail=(
            "Public health is fresh and the private cloud receiver is configured."
            if require_cloud_receiver
            else "Public API, automation heartbeat, WhatsApp capture, storage, database, Sheets, and iCloud importer are fresh."
        ),
        failure_detail=(
            "Mac-off intake is not ready because the private cloud receiver is missing or unhealthy."
            if require_cloud_receiver
            else "Public health is stale or unhealthy."
        ),
    )


def check_public_tenant_log(runner: Runner) -> dict[str, Any]:
    return _command_check(
        "public_tenant_log",
        [sys.executable, str(ROOT / "scripts" / "audit_public_tenant_log.py"), "--days", "30", "--limit", "20"],
        runner=runner,
        timeout_seconds=45,
        success_detail="Published Tenant Log matches the source renderer for the recent window.",
        failure_detail="Published Tenant Log does not match the source renderer.",
    )


def check_replacement_watchdog(runner: Runner) -> dict[str, Any]:
    code = """
import os
from pathlib import Path
for line in Path('.env').read_text(encoding='utf-8').splitlines():
    if not line or line.lstrip().startswith('#') or '=' not in line:
        continue
    key, value = line.split('=', 1)
    os.environ.setdefault(key.strip(), value.strip())
from packages.db import get_session
from packages.public_records.sync import sync_replacement_watchdog
with get_session() as session:
    result = sync_replacement_watchdog(session)
    session.commit()
print(__import__('json').dumps(result, sort_keys=True))
"""
    return _command_check(
        "replacement_watchdog",
        [sys.executable, "-c", code],
        runner=runner,
        timeout_seconds=90,
        success_detail="Elevator replacement watchdog fetched official records and applied rules without source errors.",
        failure_detail="Elevator replacement watchdog did not complete cleanly.",
    )


def check_filevault_boundary(runner: Runner) -> dict[str, Any]:
    if platform.system() != "Darwin":
        return _check(
            "filevault_boundary",
            status="info",
            ok=True,
            detail="FileVault boundary check is macOS-only.",
        )
    try:
        result = runner(["fdesetup", "status"], 15)
    except Exception as exc:
        return _check(
            "filevault_boundary",
            status="warn",
            ok=True,
            detail=f"Could not inspect FileVault status: {exc}",
        )
    status_text = f"{result.stdout} {result.stderr}".strip()
    return _check(
        "filevault_boundary",
        status="warn" if "FileVault is On" in status_text else "pass",
        ok=True,
        detail=(
            "FileVault is on, so Mac LaunchAgents cannot run after a reboot until the user unlocks macOS; Mac-off readiness depends on cloud intake/recovery."
            if "FileVault is On" in status_text
            else "FileVault does not appear to be blocking post-reboot LaunchAgents."
        ),
        evidence={"status": status_text},
    )


def check_github_cloud_recovery(repo: str, runner: Runner) -> dict[str, Any]:
    evidence: dict[str, Any] = {"repo": repo}
    failures: list[str] = []

    try:
        secrets = runner(["gh", "secret", "list", "--repo", repo], 30)
    except Exception as exc:
        return _check(
            "github_cloud_recovery_gate",
            status="fail",
            ok=False,
            detail=f"Could not inspect GitHub recovery secrets: {exc}",
            evidence=evidence,
        )
    evidence["secret_list_returncode"] = secrets.returncode
    if secrets.returncode:
        failures.append("GitHub recovery secrets could not be listed")
        evidence["secret_list_stderr"] = secrets.stderr.strip()[-1000:]
    else:
        configured = {line.split()[0] for line in secrets.stdout.splitlines() if line.strip()}
        missing = sorted(REQUIRED_CLOUD_RECOVERY_SECRETS - configured)
        evidence["configured_recovery_secret_names"] = sorted(configured & REQUIRED_CLOUD_RECOVERY_SECRETS)
        if missing:
            failures.append(f"missing recovery secrets: {', '.join(missing)}")

    try:
        variable = runner(["gh", "variable", "get", "REQUIRE_CLOUD_EXPORT_RECEIVER", "--repo", repo], 30)
    except Exception as exc:
        failures.append(f"could not inspect REQUIRE_CLOUD_EXPORT_RECEIVER variable: {exc}")
    else:
        value = variable.stdout.strip().lower() if variable.returncode == 0 else ""
        evidence["require_cloud_export_receiver"] = value or None
        if value != "true":
            failures.append("GitHub variable REQUIRE_CLOUD_EXPORT_RECEIVER is not true")

    return _check(
        "github_cloud_recovery_gate",
        status="pass" if not failures else "fail",
        ok=not failures,
        detail=(
            "GitHub recovery secrets and strict cloud-receiver health gate are configured."
            if not failures
            else "; ".join(failures)
        ),
        evidence=evidence,
    )


def build_report(args: argparse.Namespace, *, runner: Runner = run_command) -> dict[str, Any]:
    checks = [
        check_mac_services(runner),
        check_public_health(runner, require_cloud_receiver=False),
        check_public_health(runner, require_cloud_receiver=True),
        check_filevault_boundary(runner),
        check_github_cloud_recovery(args.repo, runner),
    ]
    if args.include_public_sheet:
        checks.append(check_public_tenant_log(runner))
    if args.include_watchdog_sync:
        checks.append(check_replacement_watchdog(runner))

    hard_failures = [row for row in checks if row["ok"] is not True and row["status"] != "warn"]
    return {
        "ok": not hard_failures,
        "checks": checks,
        "next_required_action": (
            ""
            if not hard_failures
            else "Authorize and deploy the private Cloudflare receiver, configure GitHub recovery secrets, then set REQUIRE_CLOUD_EXPORT_RECEIVER=true."
        ),
    }


def compact_report(report: dict[str, Any]) -> dict[str, Any]:
    checks = [row for row in report.get("checks", []) if isinstance(row, dict)]
    status_counts: dict[str, int] = {}
    for row in checks:
        status = str(row.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    def summarize(row: dict[str, Any]) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "name": row.get("name"),
            "status": row.get("status"),
            "detail": row.get("detail"),
        }
        evidence = row.get("evidence")
        if isinstance(evidence, dict):
            payload = evidence.get("payload")
            if isinstance(payload, dict) and payload.get("failures"):
                summary["failures"] = payload.get("failures")
            if evidence.get("cloud_export_receiver_state"):
                summary["cloud_export_receiver_state"] = evidence.get("cloud_export_receiver_state")
            if evidence.get("require_cloud_export_receiver") is not None:
                summary["require_cloud_export_receiver"] = evidence.get("require_cloud_export_receiver")
            configured = evidence.get("configured_recovery_secret_names")
            if configured is not None:
                summary["configured_recovery_secret_names"] = configured
        return summary

    failures = [summarize(row) for row in checks if row.get("ok") is not True and row.get("status") != "warn"]
    warnings = [summarize(row) for row in checks if row.get("status") == "warn"]
    return {
        "ok": bool(report.get("ok")),
        "status_counts": status_counts,
        "failed_checks": failures,
        "warnings": warnings,
        "next_required_action": report.get("next_required_action") or "",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default="MuckseamPonoma-Renco/455_tenants", help="GitHub repo for recovery secret/variable checks.")
    parser.add_argument("--include-public-sheet", action="store_true", help="Run the live published Tenant Log source-vs-sheet audit.")
    parser.add_argument("--include-watchdog-sync", action="store_true", help="Run the live elevator replacement watchdog sync.")
    parser.add_argument("--compact", action="store_true", help="Print a compact verdict without raw command evidence.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(args)
    output = compact_report(report) if args.compact else report
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
