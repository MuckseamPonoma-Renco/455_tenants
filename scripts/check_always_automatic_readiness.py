#!/usr/bin/env python3
"""Audit whether Tenant Issue OS is actually ready for unattended operation."""

from __future__ import annotations

import argparse
import ast
import base64
import datetime as dt
import json
import platform
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
INSTALLED_RUNTIME_ROOT = Path.home() / ".local" / "share" / "tenant-issue-os" / "runtime"
OPERATION_ROOT = (
    INSTALLED_RUNTIME_ROOT
    if (INSTALLED_RUNTIME_ROOT / ".env").is_file()
    and (INSTALLED_RUNTIME_ROOT / "scripts" / "check_mac_services.sh").is_file()
    else ROOT
)
OPERATION_PYTHON = (
    INSTALLED_RUNTIME_ROOT / ".venv" / "bin" / "python"
    if (INSTALLED_RUNTIME_ROOT / ".venv" / "bin" / "python").is_file()
    else Path(sys.executable)
)
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
        cwd=OPERATION_ROOT,
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
        [str(OPERATION_ROOT / "scripts" / "check_mac_services.sh"), "--json"],
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
        str(OPERATION_PYTHON),
        str(OPERATION_ROOT / "scripts" / "check_public_health.py"),
        "--url",
        "https://api.455tenants.com/health",
        "--max-capture-age-seconds",
        "600",
        "--max-automation-age-seconds",
        "1200",
        "--max-import-age-seconds",
        "3600",
    ]
    name = "cloud_receiver_configuration" if require_cloud_receiver else "public_health"
    if require_cloud_receiver:
        args.append("--require-cloud-export-receiver")
    return _command_check(
        name,
        args,
        runner=runner,
        timeout_seconds=30,
        success_detail=(
            "The Mac's public health reports a configured private cloud receiver; this is not a Mac-off execution test."
            if require_cloud_receiver
            else "Public API, automation, WhatsApp capture, storage, and iCloud importer are fresh; database and Sheets configuration is present."
        ),
        failure_detail=(
            "The Mac's public health cannot confirm the private cloud receiver configuration."
            if require_cloud_receiver
            else "Public health is stale or unhealthy."
        ),
    )


def check_public_tenant_log(runner: Runner) -> dict[str, Any]:
    return _command_check(
        "public_tenant_log",
        [str(OPERATION_PYTHON), str(OPERATION_ROOT / "scripts" / "audit_public_tenant_log.py"), "--days", "30", "--limit", "20"],
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
from packages.sheets.sync import sync_replacement_watchdog_to_sheets
with get_session() as session:
    result = sync_replacement_watchdog(session)
    session.commit()
sync_replacement_watchdog_to_sheets()
result["sheets_synced"] = True
print(__import__('json').dumps(result, sort_keys=True))
"""
    return _command_check(
        "replacement_watchdog",
        [str(OPERATION_PYTHON), "-c", code],
        runner=runner,
        timeout_seconds=120,
        success_detail="Elevator replacement watchdog fetched official records, applied rules, and refreshed its public tabs.",
        failure_detail="Elevator replacement watchdog did not complete cleanly.",
    )


def check_public_watchdog_tabs(runner: Runner) -> dict[str, Any]:
    return _command_check(
        "public_watchdog_tabs",
        [
            str(OPERATION_PYTHON),
            str(OPERATION_ROOT / "scripts" / "audit_public_watchdog_tabs.py"),
            "--limit",
            "20",
        ],
        runner=runner,
        timeout_seconds=60,
        success_detail="Published watchdog tabs match the freshly synchronized official-record source.",
        failure_detail="Published watchdog tabs do not match the freshly synchronized official-record source.",
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
            detail=f"Mac LaunchAgents require a logged-in user. Could not inspect FileVault status: {exc}",
            evidence={
                "filevault_enabled": None,
                "mac_services_require_login": True,
                "mac_after_reboot_without_login_ready": False,
            },
        )
    status_text = f"{result.stdout} {result.stderr}".strip()
    enabled = (
        True if result.returncode == 0 and "FileVault is On" in status_text
        else False if result.returncode == 0 and "FileVault is Off" in status_text
        else None
    )
    return _check(
        "filevault_boundary",
        status="warn",
        ok=True,
        detail=(
            "Mac services are per-user LaunchAgents and require a logged-in user, even when idle sleep is disabled. "
            + (
                "FileVault also requires an authorized unlock after reboot."
                if enabled is True
                else "FileVault is off; the LaunchAgent login dependency remains."
                if enabled is False
                else "FileVault status could not be confirmed."
            )
        ),
        evidence={
            "filevault_enabled": enabled,
            "mac_services_require_login": True,
            "mac_after_reboot_without_login_ready": False,
        },
    )


def _cloud_capabilities(source: str) -> dict[str, bool | None]:
    """Read declarations without executing downloaded repository code."""
    tree = ast.parse(source)
    capabilities: dict[str, bool | None] = {
        "automatic_311_filing": None,
        "live_whatsapp_capture": None,
    }
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not any(
            isinstance(target, ast.Name) and target.id == "CLOUD_RECOVERY_CAPABILITIES"
            for target in node.targets
        ):
            continue
        declared = ast.literal_eval(node.value)
        if not isinstance(declared, dict):
            raise ValueError("cloud capability declaration is not an object")
        for name, value in declared.items():
            if not isinstance(name, str) or not isinstance(value, bool):
                raise ValueError("cloud capabilities must be named booleans")
            capabilities[name] = value
        return capabilities

    # Existing deployments predate capability declarations and explicitly turn
    # off new filing. This is negative evidence only, never proof of readiness.
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Constant):
            continue
        for target in node.targets:
            if (
                isinstance(target, ast.Subscript)
                and isinstance(target.value, ast.Attribute)
                and isinstance(target.value.value, ast.Name)
                and target.value.value.id == "os"
                and target.value.attr == "environ"
                and isinstance(target.slice, ast.Constant)
                and target.slice.value == "AUTO_FILE_ENABLED"
                and node.value.value == "0"
            ):
                capabilities["automatic_311_filing"] = False
    return capabilities


def check_cloud_filing_capability(repo: str, runner: Runner) -> dict[str, Any]:
    evidence: dict[str, Any] = {"repo": repo, "reference": "main"}
    failures: list[str] = []
    try:
        head = runner(["gh", "api", f"repos/{repo}/commits/main", "--jq", ".sha"], 30)
        head_sha = head.stdout.strip()
        if head.returncode != 0 or not re.fullmatch(r"[0-9a-f]{40}", head_sha):
            raise ValueError("current main commit is unavailable")
        evidence["head_sha"] = head_sha
        result = runner([
            "gh", "api",
            f"repos/{repo}/contents/scripts/run_cloud_recovery_cycle.py?ref={head_sha}",
        ], 30)
        if result.returncode != 0:
            raise ValueError("current cloud recovery source is unavailable")
        payload = json.loads(result.stdout)
        source = base64.b64decode(payload["content"]).decode("utf-8")
        capabilities = _cloud_capabilities(source)
        evidence.update({"source_blob_sha": payload.get("sha"), "capabilities": capabilities})
        if capabilities.get("automatic_311_filing") is not True:
            failures.append(
                "Current cloud recovery cannot file new 311 cases while the Mac is unavailable."
                if capabilities.get("automatic_311_filing") is False
                else "Current cloud recovery has no verified declaration of automatic 311 filing support."
            )
    except (OSError, ValueError, KeyError, TypeError, SyntaxError, subprocess.SubprocessError):
        failures.append("Could not inspect current cloud 311 filing capability.")

    try:
        variable = runner(["gh", "variable", "get", "CLOUD_311_FILING_ENABLED", "--repo", repo], 30)
        enabled = variable.returncode == 0 and variable.stdout.strip().lower() == "true"
        evidence["cloud_311_filing_enabled"] = enabled
        if not enabled:
            failures.append("CLOUD_311_FILING_ENABLED is not true.")
    except Exception:
        evidence["cloud_311_filing_enabled"] = None
        failures.append("Could not inspect the cloud 311 filing enablement gate.")

    return _check(
        "cloud_311_filing_configuration",
        status="pass" if not failures else "fail",
        ok=not failures,
        detail=(
            "Current cloud code supports automatic 311 filing and its separate enablement gate is on; execution still requires a recent worker receipt."
            if not failures else " ".join(failures)
        ),
        evidence=evidence,
    )


def _filing_receipt_from_log(log: str) -> dict[str, Any] | None:
    # GitHub masks the literal secret value "true", even inside JSON. The
    # separately marked receipt uses integers and contains no private fields.
    marker = "CLOUD_311_RECEIPT_V1 "
    fields = {"schema", "ok", "cycle_ok", "submitted", "lookup_verified", "empty", "blocked"}
    for line in log.splitlines():
        cleaned = re.sub(
            r"^.*?\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z ?", "", line,
        ).strip()
        if not cleaned.startswith(marker):
            continue
        try:
            payload = json.loads(cleaned[len(marker):])
        except ValueError:
            continue
        if not isinstance(payload, dict) or set(payload) != fields:
            continue
        if any(type(value) is not int or value < 0 for value in payload.values()):
            continue
        if (payload["schema"], payload["ok"], payload["cycle_ok"], payload["blocked"]) != (1, 1, 1, 0):
            continue
        return payload
    return None


def check_cloud_filing_execution(
    repo: str, configuration: dict[str, Any], runner: Runner,
    *, now: dt.datetime | None = None,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {"maximum_age_seconds": 7200}
    detail = "No successful cloud 311 worker receipt at the current main commit was verified within the last two hours."
    if configuration.get("ok") is not True:
        return _check(
            "mac_off_311_filing", status="fail", ok=False,
            detail="Mac-off 311 filing is unavailable until its code capability and enablement gate are ready.",
            evidence=evidence,
        )
    head_sha = configuration["evidence"]["head_sha"]
    evidence["head_sha"] = head_sha
    current_time = now or dt.datetime.now(dt.UTC)
    try:
        result = runner([
            "gh", "run", "list", "--repo", repo, "--workflow", "cloud-recovery.yml",
            "--branch", "main", "--status", "completed", "--limit", "20",
            "--json", "databaseId,headSha,conclusion,updatedAt",
        ], 30)
        if result.returncode != 0:
            raise ValueError("cloud run list is unavailable")
        runs = json.loads(result.stdout)
        if not isinstance(runs, list):
            raise ValueError("cloud run list is not a list")
        inspected = 0
        for run in runs:
            if not isinstance(run, dict) or run.get("headSha") != head_sha or run.get("conclusion") != "success":
                continue
            try:
                stamp = dt.datetime.fromisoformat(str(run.get("updatedAt") or "").replace("Z", "+00:00"))
                age = (current_time - stamp).total_seconds()
            except (ValueError, TypeError):
                continue
            if not 0 <= age <= evidence["maximum_age_seconds"]:
                continue
            run_id = str(run.get("databaseId") or "")
            if not run_id.isdigit():
                continue
            logs = runner(["gh", "run", "view", run_id, "--repo", repo, "--log"], 30)
            inspected += 1
            receipt = _filing_receipt_from_log(logs.stdout) if logs.returncode == 0 else None
            if receipt is not None:
                evidence.update({"run_id": int(run_id), "updated_at": run["updatedAt"], "current_filings": receipt})
                return _check(
                    "mac_off_311_filing", status="pass", ok=True,
                    detail="The enabled cloud 311 worker completed a safe batch at the current main commit within two hours; individual cases still require their own receipts.",
                    evidence=evidence,
                )
            if inspected >= 6:
                break
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
        detail = "Could not verify a recent cloud 311 worker receipt."
    return _check("mac_off_311_filing", status="fail", ok=False, detail=detail, evidence=evidence)


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


def _next_required_action(hard_failures: list[dict[str, Any]]) -> str:
    if not hard_failures:
        return ""
    serialized = json.dumps(hard_failures, sort_keys=True).casefold()
    actions: list[str] = []
    if "blocked_model_review" in serialized or "required openai model review is incomplete" in serialized:
        actions.append("Restore the configured OpenAI API project's quota so the staged chat audit can finish.")
    if (
        any(row.get("name") == "github_cloud_recovery_gate" for row in hard_failures)
        or "cloud export receiver is not_configured" in serialized
    ):
        actions.append(
            "Configure the three GitHub cloud-recovery secrets for the private Cloudflare receiver and set "
            "REQUIRE_CLOUD_EXPORT_RECEIVER=true."
        )
    if any(row.get("name") in {"cloud_311_filing_configuration", "mac_off_311_filing"} for row in hard_failures):
        actions.append(
            "Verify the separate cloud 311 worker's support, enablement gate, and a recent successful worker receipt; "
            "cloud export intake and status updates do not prove new cases can be filed while the Mac is unavailable."
        )
    if not actions:
        actions.append("Resolve the failed readiness checks shown in this report, then rerun the audit.")
    return " ".join(actions)


def build_report(args: argparse.Namespace, *, runner: Runner = run_command) -> dict[str, Any]:
    checks = [
        check_mac_services(runner),
        check_public_health(runner, require_cloud_receiver=False),
        check_public_health(runner, require_cloud_receiver=True),
        check_filevault_boundary(runner),
        check_github_cloud_recovery(args.repo, runner),
        check_cloud_filing_capability(args.repo, runner),
    ]
    cloud_configuration = checks[-1]
    checks.append(check_cloud_filing_execution(args.repo, cloud_configuration, runner))
    if args.include_public_sheet:
        checks.append(check_public_tenant_log(runner))
    if args.include_watchdog_sync:
        checks.append(check_replacement_watchdog(runner))
        checks.append(check_public_watchdog_tabs(runner))

    hard_failures = [row for row in checks if row["ok"] is not True and row["status"] != "warn"]
    return {
        "ok": not hard_failures,
        "checks": checks,
        "capabilities": {
            "mac_services_require_login": platform.system() == "Darwin",
            "cloud_311_filing_configured": cloud_configuration["ok"],
            "mac_off_311_filing_verified": next(row["ok"] for row in checks if row["name"] == "mac_off_311_filing"),
            "cloud_live_whatsapp_capture_supported": cloud_configuration["evidence"].get("capabilities", {}).get("live_whatsapp_capture"),
        },
        "next_required_action": _next_required_action(hard_failures),
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
            for name in ("capabilities", "cloud_311_filing_enabled", "mac_services_require_login", "filevault_enabled", "run_id", "updated_at", "current_filings"):
                if name in evidence:
                    summary[name] = evidence[name]
        return summary

    failures = [summarize(row) for row in checks if row.get("ok") is not True and row.get("status") != "warn"]
    warnings = [summarize(row) for row in checks if row.get("status") == "warn"]
    return {
        "ok": bool(report.get("ok")),
        "capabilities": report.get("capabilities") or {},
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
