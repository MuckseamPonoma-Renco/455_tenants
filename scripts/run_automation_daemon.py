from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file()

from packages.audit import append_audit_event, daily_hash_chain
from packages.nyc311.portal_worker import run_portal_filing_once
from packages.worker_jobs import process_pending_messages, resync_replacement_watchdog, sync_311_statuses
from scripts.audit_public_tenant_log import run_audit as run_public_tenant_log_audit


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return default


def _log(message: str) -> None:
    print(message, flush=True)


def _run_step(label: str, func):
    try:
        return func()
    except Exception as exc:
        append_audit_event("AUTOMATION_STEP_ERROR", None, {"step": label, "error": str(exc)[:500]})
        daily_hash_chain()
        _log(f"{label} error: {exc}")
        return None


def _public_tenant_log_qa() -> dict[str, object]:
    days = max(1, _env_int("AUTOMATION_PUBLIC_TENANT_LOG_AUDIT_DAYS", 7))
    result = run_public_tenant_log_audit(days=days, resync=False, retries=3, retry_sleep=5.0, limit=5)
    missing_count = len(result.get("missing_recent_rows") or [])
    unexpected_count = len(result.get("unexpected_recent_rows") or [])
    missing_source_count = len(result.get("missing_source_rows") or [])
    meta: dict[str, object] = {
        "ok": bool(result.get("ok")),
        "days": days,
        "expected_recent_rows": result.get("expected_recent_rows"),
        "live_recent_rows": result.get("live_recent_rows"),
        "source_recent_rows": result.get("source_recent_rows"),
        "missing_recent_rows": missing_count,
        "unexpected_recent_rows": unexpected_count,
        "missing_source_rows": missing_source_count,
        "expected_latest_update": result.get("expected_latest_update"),
        "live_latest_update": result.get("live_latest_update"),
    }
    if result.get("ok"):
        append_audit_event("PUBLIC_TENANT_LOG_QA_OK", None, meta)
        daily_hash_chain()
        return meta

    append_audit_event("PUBLIC_TENANT_LOG_QA_MISMATCH", None, meta)
    repaired = run_public_tenant_log_audit(days=days, resync=True, retries=5, retry_sleep=8.0, limit=5)
    repair_meta = {
        **meta,
        "repair_ok": bool(repaired.get("ok")),
        "repair_missing_recent_rows": len(repaired.get("missing_recent_rows") or []),
        "repair_unexpected_recent_rows": len(repaired.get("unexpected_recent_rows") or []),
        "repair_missing_source_rows": len(repaired.get("missing_source_rows") or []),
        "repair_source_recent_rows": repaired.get("source_recent_rows"),
        "repair_live_latest_update": repaired.get("live_latest_update"),
    }
    append_audit_event(
        "PUBLIC_TENANT_LOG_QA_REPAIRED" if repaired.get("ok") else "PUBLIC_TENANT_LOG_QA_REPAIR_FAILED",
        None,
        repair_meta,
    )
    daily_hash_chain()
    return repair_meta


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Tenant Issue OS automation loop.")
    parser.add_argument("--headful", action="store_true", help="Run Playwright with a visible browser window.")
    parser.add_argument("--skip-lookup", action="store_true", help="Skip portal status lookup after successful submissions.")
    parser.add_argument("--poll-seconds", type=int, help="Idle sleep between automation cycles.")
    parser.add_argument("--error-sleep-seconds", type=int, help="Sleep after an unexpected automation error.")
    parser.add_argument("--status-sync-seconds", type=int, help="How often to run NYC311 case-status sync. Use 0 to disable.")
    parser.add_argument("--public-record-sync-seconds", type=int, help="How often to run replacement-watchdog public-record sync. Use 0 to disable.")
    parser.add_argument("--public-tenant-log-audit-seconds", type=int, help="How often to QA the public Tenant Log. Use 0 to disable.")
    parser.add_argument("--burst-size", type=int, help="Maximum filing jobs to process per cycle.")
    parser.add_argument("--between-jobs-seconds", type=int, help="Pause between successful filing jobs in the same cycle.")
    parser.add_argument("--startup-catchup-limit", type=int, help="Process up to this many undecided raw messages before the main loop starts.")
    return parser.parse_args()


def main() -> None:
    load_local_env_file()
    args = parse_args()

    headless = not args.headful
    verify_lookup = not args.skip_lookup
    poll_seconds = max(10, args.poll_seconds or _env_int("AUTOMATION_POLL_SECONDS", 60))
    error_sleep_seconds = max(10, args.error_sleep_seconds or _env_int("AUTOMATION_ERROR_SLEEP_SECONDS", 30))
    status_sync_seconds = max(0, args.status_sync_seconds if args.status_sync_seconds is not None else _env_int("AUTOMATION_STATUS_SYNC_SECONDS", 3600))
    public_record_sync_seconds = max(
        0,
        args.public_record_sync_seconds
        if args.public_record_sync_seconds is not None
        else _env_int("AUTOMATION_PUBLIC_RECORD_SYNC_SECONDS", 21600),
    )
    public_tenant_log_audit_seconds = max(
        0,
        args.public_tenant_log_audit_seconds
        if args.public_tenant_log_audit_seconds is not None
        else _env_int("AUTOMATION_PUBLIC_TENANT_LOG_AUDIT_SECONDS", 1800),
    )
    burst_size = max(1, args.burst_size or _env_int("AUTOMATION_BURST_SIZE", 2))
    between_jobs_seconds = max(0, args.between_jobs_seconds or _env_int("AUTOMATION_BETWEEN_JOBS_SECONDS", 5))
    startup_catchup_limit = max(0, args.startup_catchup_limit if args.startup_catchup_limit is not None else _env_int("AUTOMATION_STARTUP_CATCHUP_LIMIT", 200))

    _log(
        "automation loop starting "
        f"headless={headless} verify_lookup={verify_lookup} poll_seconds={poll_seconds} "
        f"status_sync_seconds={status_sync_seconds} public_record_sync_seconds={public_record_sync_seconds} "
        f"public_tenant_log_audit_seconds={public_tenant_log_audit_seconds} burst_size={burst_size} "
        f"startup_catchup_limit={startup_catchup_limit}"
    )
    append_audit_event(
        "AUTOMATION_LOOP_STARTED",
        None,
        {
            "headless": headless,
            "verify_lookup": verify_lookup,
            "poll_seconds": poll_seconds,
            "status_sync_seconds": status_sync_seconds,
            "public_record_sync_seconds": public_record_sync_seconds,
            "public_tenant_log_audit_seconds": public_tenant_log_audit_seconds,
            "burst_size": burst_size,
            "startup_catchup_limit": startup_catchup_limit,
        },
    )
    daily_hash_chain()

    if startup_catchup_limit > 0:
        catchup = process_pending_messages(limit=startup_catchup_limit, resync_sheets=True)
        if catchup.get("pending_selected") or catchup.get("errors_total"):
            _log(f"startup catch-up result: {catchup}")

    next_status_sync_at = time.monotonic() if status_sync_seconds > 0 else None
    next_public_record_sync_at = time.monotonic() if public_record_sync_seconds > 0 else None
    next_public_tenant_log_audit_at = time.monotonic() if public_tenant_log_audit_seconds > 0 else None

    while True:
        did_work = False
        try:
            now = time.monotonic()
            if next_public_tenant_log_audit_at is not None and now >= next_public_tenant_log_audit_at:
                result = _run_step("public Tenant Log QA", _public_tenant_log_qa)
                if result is not None:
                    _log(f"public Tenant Log QA result: {result}")
                    did_work = True
                next_public_tenant_log_audit_at = time.monotonic() + public_tenant_log_audit_seconds

            if next_public_record_sync_at is not None and now >= next_public_record_sync_at:
                result = _run_step("replacement watchdog sync", resync_replacement_watchdog)
                if result is not None:
                    _log(f"replacement watchdog sync result: {result}")
                    did_work = True
                next_public_record_sync_at = time.monotonic() + public_record_sync_seconds

            if next_status_sync_at is not None and now >= next_status_sync_at:
                result = _run_step("status sync", sync_311_statuses)
                if result is not None:
                    _log(f"status sync result: {result}")
                    did_work = True
                next_status_sync_at = time.monotonic() + status_sync_seconds

            processed = 0
            while processed < burst_size:
                result = _run_step(
                    "portal filing",
                    lambda: run_portal_filing_once(headless=headless, verify_lookup=verify_lookup),
                )
                if result is None:
                    break
                job_meta = result.get("job")
                if job_meta is None and result.get("job_id") is None:
                    break
                processed += 1
                did_work = True
                _log(f"portal filing result: {result}")
                if not result.get("ok"):
                    break
                if processed < burst_size and between_jobs_seconds:
                    time.sleep(between_jobs_seconds)
        except KeyboardInterrupt:
            append_audit_event("AUTOMATION_LOOP_STOPPED", None, {"reason": "keyboard_interrupt"})
            daily_hash_chain()
            raise
        except Exception as exc:
            append_audit_event("AUTOMATION_LOOP_ERROR", None, {"error": str(exc)[:500]})
            daily_hash_chain()
            _log(f"automation error: {exc}")
            time.sleep(error_sleep_seconds)
            continue

        time.sleep(poll_seconds if not did_work else max(5, min(poll_seconds, between_jobs_seconds or poll_seconds)))


if __name__ == "__main__":
    main()
