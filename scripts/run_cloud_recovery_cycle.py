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

MODES = ("exports", "status", "watchdog", "full")
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


def config_errors(environ: dict[str, str] | None = None) -> list[str]:
    values = os.environ if environ is None else environ
    errors = [name for name in REQUIRED_ENVIRONMENT if not str(values.get(name) or "").strip()]
    credentials_path = str(values.get("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if credentials_path and not Path(credentials_path).expanduser().is_file():
        errors.append("GOOGLE_APPLICATION_CREDENTIALS file is missing")
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
) -> bool:
    """Return true only when the Mac's public automation heartbeat is fresh."""
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
    automation = payload.get("automation")
    if not isinstance(automation, dict):
        return False
    if automation.get("state") not in {"ready", "starting", "working"} or automation.get("has_error") is True:
        return False
    last_cycle = _parse_timestamp(automation.get("last_cycle_at"))
    if last_cycle is None:
        return False
    age_seconds = int((current_time - last_cycle).total_seconds())
    return 0 <= age_seconds <= maximum_age


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
    return {
        "action": str(result.get("action") or "unknown"),
        "processed_exports": len(processed) if isinstance(processed, list) else 0,
        "pending_exports": int(result.get("pending_exports") or 0),
        "recovered_acknowledgements": int(result.get("recovered_acknowledgements") or 0),
    }


def run_cycle(
    mode: str,
    *,
    operations: CloudRecoveryOperations | None = None,
    primary_healthy: Callable[[], bool] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    if mode not in MODES:
        raise ValueError(f"mode must be one of: {', '.join(MODES)}")
    is_primary_healthy = primary_healthy or primary_automation_healthy
    if not force and is_primary_healthy():
        return {"ok": True, "mode": mode, "action": "skipped_primary_healthy"}

    operations = operations or _runtime_operations()
    result: dict[str, Any] = {"ok": True, "mode": mode, "action": "recovery_run"}

    if mode in {"exports", "full"}:
        config = operations.receiver_config()
        if config is None:
            raise RuntimeError("private cloud export receiver is not configured")
        result["cloud_exports"] = _compact_cloud_result(operations.sync_cloud_exports(config))

    if mode in {"status", "full"}:
        result["status_sync"] = operations.sync_311_statuses()

    if mode in {"watchdog", "full"}:
        result["replacement_watchdog"] = operations.sync_replacement_watchdog()

    if mode in {"status", "watchdog", "full"}:
        result["public_tenant_log_qa"] = operations.audit_public_tenant_log()

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

    errors = config_errors()
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
