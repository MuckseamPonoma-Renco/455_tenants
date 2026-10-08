#!/usr/bin/env python3
"""Run the Mac-independent recovery work after a private cloud export arrives."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file
from packages.automation_status import watchdog_max_age_seconds, watchdog_result_ok, watchdog_source_receipts_complete

MODES = ("exports", "status", "watchdog", "full")
PRIMARY_ACTIVE_CHAT_EXPORT_STATES = {
    "ready",
    "no_export",
    "waiting_for_download",
    "discovered",
    "processing",
    "pending_cloud_exports",
    "sheet_sync_pending",
    "sheet_readback_pending",
    "pending_acknowledgement",
}
DEFAULT_PRIMARY_HEALTH_URL = "https://api.455tenants.com/health"
REQUIRED_ENVIRONMENT = (
    "DATABASE_URL",
    "CLOUD_EXPORT_RECEIVER_URL",
    "CLOUD_EXPORT_RECEIVER_PULL_TOKEN",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GOOGLE_SHEETS_SPREADSHEET_ID",
)


@dataclass(frozen=True)
class CloudRecoveryOperations:
    receiver_config: Callable[[], Any]
    sync_cloud_exports: Callable[[Any], dict[str, Any]]
    sync_311_statuses: Callable[[], dict[str, Any]]
    sync_replacement_watchdog: Callable[[], dict[str, Any]]
    audit_public_tenant_log: Callable[[], dict[str, Any]]


def config_errors(
    environ: dict[str, str] | None = None, *, include_export_receiver: bool = True
) -> list[str]:
    values = os.environ if environ is None else environ
    required = REQUIRED_ENVIRONMENT if include_export_receiver else tuple(
        name for name in REQUIRED_ENVIRONMENT if not name.startswith("CLOUD_EXPORT_RECEIVER_")
    )
    errors = [name for name in required if not str(values.get(name) or "").strip()]
    credentials_path = str(values.get("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if credentials_path and not Path(credentials_path).expanduser().is_file():
        errors.append("GOOGLE_APPLICATION_CREDENTIALS file is missing")
    from packages.sheets.public_semantic_overrides import (
        PublicSemanticOverrideError, require_private_overrides_for_publication,
    )
    try:
        require_private_overrides_for_publication({**values, "DISABLE_SHEETS_SYNC": "0"})
    except (PublicSemanticOverrideError, OSError, ValueError):
        errors.append("valid private TENANT_PUBLIC_SEMANTIC_OVERRIDES_PATH is required for public Sheet recovery")
    return errors


def _parse_timestamp(value: Any) -> dt.datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(dt.UTC)


def _primary_maximum_age_seconds() -> int:
    try:
        configured = int(os.environ.get("CLOUD_RECOVERY_PRIMARY_MAX_AGE_SECONDS") or "1200")
    except ValueError:
        configured = 1200
    return max(300, configured)


def primary_automation_healthy(
    url: str | None = None,
    *,
    now: dt.datetime | None = None,
    require_chat_export_sync: bool = True,
    require_watchdog_sync: bool = False,
) -> bool:
    """Return true when the Mac can own maintenance for this capability.

    Each capability requires its own completion evidence. A fresh loop heartbeat
    cannot establish that public-record queries actually completed successfully.
    """
    endpoint = (url or os.environ.get("CLOUD_RECOVERY_PRIMARY_HEALTH_URL") or DEFAULT_PRIMARY_HEALTH_URL).strip()
    maximum_age = _primary_maximum_age_seconds()
    current_time = now or dt.datetime.now(dt.UTC)
    request = urllib.request.Request(
        endpoint,
        headers={"Accept": "application/json", "User-Agent": "tenant-issue-os-cloud-recovery/1"},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            if response.status != 200:
                return False
            payload = json.load(response)
    except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
        return False

    if not isinstance(payload, dict) or payload.get("ok") is not True:
        return False
    if payload.get("database_ready") is not True:
        return False
    storage = payload.get("storage")
    if not isinstance(storage, dict) or storage.get("state") != "ready" or storage.get("low_disk") is not False:
        return False
    automation = payload.get("automation")
    if not isinstance(automation, dict):
        return False
    if automation.get("state") not in {"ready", "starting", "working"} or automation.get("has_error") is True:
        return False
    last_cycle = _parse_timestamp(automation.get("last_cycle_at"))
    if last_cycle is None:
        return False
    age_seconds = int((current_time - last_cycle).total_seconds())
    if not 0 <= age_seconds <= maximum_age:
        return False
    if require_chat_export_sync:
        chat_export_sync = payload.get("chat_export_sync")
        if not isinstance(chat_export_sync, dict):
            return False
        if chat_export_sync.get("state") not in PRIMARY_ACTIVE_CHAT_EXPORT_STATES:
            return False
        if chat_export_sync.get("has_error") is True:
            return False
    if require_watchdog_sync:
        watchdog = payload.get("watchdog")
        if not isinstance(watchdog, dict) or watchdog.get("state") not in {"ready", "working"}:
            return False
        if watchdog.get("has_error") is not False or watchdog.get("source_errors") != 0:
            return False
        if watchdog.get("sheet_errors") != 0 or watchdog.get("sheet_readback_ok") is not True:
            return False
        last_success = _parse_timestamp(watchdog.get("last_success_at"))
        if last_success is None:
            return False
        interval = watchdog.get("interval_seconds")
        if not isinstance(interval, int) or interval <= 0:
            return False
        # Do not let an accidentally inflated primary setting disable recovery.
        maximum_watchdog_age = min(watchdog_max_age_seconds(interval), watchdog_max_age_seconds())
        if not 0 <= (current_time - last_success).total_seconds() <= maximum_watchdog_age:
            return False
        if not watchdog_source_receipts_complete(watchdog.get("sources"), now=current_time, max_age_seconds=maximum_watchdog_age):
            return False
    return True


def _runtime_operations() -> CloudRecoveryOperations:
    from packages.worker_jobs import resync_replacement_watchdog, sync_311_statuses
    from scripts.run_automation_daemon import _public_tenant_log_qa
    from scripts.sync_cloud_chat_export_inbox import receiver_config, run_once

    return CloudRecoveryOperations(
        receiver_config=receiver_config,
        sync_cloud_exports=run_once,
        sync_311_statuses=sync_311_statuses,
        sync_replacement_watchdog=resync_replacement_watchdog,
        audit_public_tenant_log=_public_tenant_log_qa,
    )


def _compact_cloud_result(result: dict[str, Any]) -> dict[str, int | str]:
    processed = result.get("processed")
    blocked = result.get("blocked_exports")
    return {
        "action": str(result.get("action") or "unknown"),
        "processed_exports": len(processed) if isinstance(processed, list) else 0,
        "blocked_model_review_exports": len(blocked) if isinstance(blocked, list) else 0,
        "pending_exports": int(result.get("pending_exports") or 0),
        "recovered_acknowledgements": int(result.get("recovered_acknowledgements") or 0),
        "recovered_cloud_receipts": int(result.get("recovered_cloud_receipts") or 0),
    }


def run_cycle(
    mode: str,
    *,
    operations: CloudRecoveryOperations | None = None,
    primary_healthy: Callable[[], bool] | None = None,
    primary_core_healthy: Callable[[], bool] | None = None,
    primary_watchdog_healthy: Callable[[], bool] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    if mode not in MODES:
        raise ValueError(f"mode must be one of: {', '.join(MODES)}")
    export_health_check = primary_healthy or primary_automation_healthy
    core_health_check = primary_core_healthy or primary_healthy or (
        lambda: primary_automation_healthy(require_chat_export_sync=False)
    )
    watchdog_health_check = primary_watchdog_healthy or (
        lambda: primary_automation_healthy(require_chat_export_sync=False, require_watchdog_sync=True)
    )
    run_exports = mode in {"exports", "full"}
    run_status = mode in {"status", "full"}
    run_watchdog = mode in {"watchdog", "full"}
    if not force:
        def primary_owns(check: Callable[[], bool]) -> bool:
            try:
                return check() is True
            except Exception:
                # An unavailable or malformed primary-health response must not
                # prevent independent recovery work from being attempted.
                return False

        core_healthy = primary_owns(core_health_check) if run_status else False
        watchdog_healthy = primary_owns(watchdog_health_check) if run_watchdog else False
        export_healthy = primary_owns(export_health_check) if run_exports else False
        run_status = run_status and not core_healthy
        run_watchdog = run_watchdog and not watchdog_healthy
        run_exports = run_exports and not export_healthy
        if not run_exports and not run_status and not run_watchdog:
            return {"ok": True, "mode": mode, "action": "skipped_primary_healthy"}

    operations = operations or _runtime_operations()
    result: dict[str, Any] = {
        "ok": True,
        "mode": mode,
        "action": "recovery_run",
    }
    if not force and mode == "full" and not (run_exports and run_status and run_watchdog):
        result["action"] = "partial_recovery_run"

    def run_step(name: str, operation: Callable[[], dict[str, Any]], valid: Callable[[dict[str, Any]], bool]) -> None:
        try:
            step_result = operation()
            if not isinstance(step_result, dict):
                raise ValueError("invalid recovery result")
            result[name] = step_result
            succeeded = valid(step_result)
        except Exception:
            # Export errors can contain private archive paths. Report only the
            # failed capability here, and continue independently useful work.
            result[name] = {"ok": False, "error": "recovery_step_failed"}
            succeeded = False
        if not succeeded:
            result["ok"] = False
            result.setdefault("failed_steps", []).append(name)

    if run_exports:
        def export_step() -> dict[str, Any]:
            config = operations.receiver_config()
            if config is None:
                raise RuntimeError("private cloud export receiver is not configured")
            exported = operations.sync_cloud_exports(config)
            if not isinstance(exported, dict) or exported.get("ok") is not True:
                raise RuntimeError("cloud export sync did not complete")
            return _compact_cloud_result(exported)

        run_step("cloud_exports", export_step, lambda _value: True)

    if run_status:
        run_step("status_sync", operations.sync_311_statuses, lambda value: value.get("ok") is True)

    if run_watchdog:
        run_step("replacement_watchdog", operations.sync_replacement_watchdog, watchdog_result_ok)

    if run_status or run_watchdog:
        run_step("public_tenant_log_qa", operations.audit_public_tenant_log,
                 lambda value: (value.get("repair_ok") if "repair_ok" in value else value.get("ok")) is True)

    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", help="Optional private environment file to load before starting work.")
    parser.add_argument("--mode", choices=MODES, default="full")
    parser.add_argument("--check-config", action="store_true", help="Validate required configuration without reading or writing remote data.")
    parser.add_argument("--force", action="store_true", help="Run even when the primary Mac automation health is fresh.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.env_file:
        load_local_env_file(Path(args.env_file).expanduser())
    else:
        load_local_env_file(ROOT / ".env")

    # GitHub recovery intentionally does not run the portal worker. It can keep
    # facts, Sheet state, and export decisions current without filing new cases.
    os.environ["AUTO_FILE_ENABLED"] = "0"
    os.environ.setdefault("PROCESS_INLINE", "1")
    os.environ.setdefault("DISABLE_SHEETS_SYNC", "0")

    # Full recovery isolates a missing export receiver in its export step, so
    # status/watchdog work can still run. Explicit configuration checks remain
    # strict for every capability selected by the caller.
    errors = config_errors(include_export_receiver=(
        args.mode == "exports" or (args.check_config and args.mode == "full")
    ))
    if errors:
        print(json.dumps({"ok": False, "configuration_errors": errors}, sort_keys=True))
        return 2
    if args.check_config:
        print(json.dumps({"ok": True, "action": "configuration_ready"}, sort_keys=True))
        return 0

    try:
        result = run_cycle(args.mode, force=args.force)
    except Exception:
        # This runner can handle private chat archives. Do not expose exception
        # details through GitHub Actions logs; local error details stay in the
        # Cloudflare receiver and database audit records.
        print(json.dumps({"ok": False, "error": "cloud_recovery_failed"}, sort_keys=True))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0 if result.get("ok") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
