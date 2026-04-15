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
from packages.worker_jobs import process_pending_messages, sync_311_statuses


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Tenant Issue OS automation loop.")
    parser.add_argument("--headful", action="store_true", help="Run Playwright with a visible browser window.")
    parser.add_argument("--skip-lookup", action="store_true", help="Skip portal status lookup after successful submissions.")
    parser.add_argument("--poll-seconds", type=int, help="Idle sleep between automation cycles.")
    parser.add_argument("--error-sleep-seconds", type=int, help="Sleep after an unexpected automation error.")
    parser.add_argument("--status-sync-seconds", type=int, help="How often to run NYC311 case-status sync. Use 0 to disable.")
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
    burst_size = max(1, args.burst_size or _env_int("AUTOMATION_BURST_SIZE", 2))
    between_jobs_seconds = max(0, args.between_jobs_seconds or _env_int("AUTOMATION_BETWEEN_JOBS_SECONDS", 5))
    startup_catchup_limit = max(0, args.startup_catchup_limit if args.startup_catchup_limit is not None else _env_int("AUTOMATION_STARTUP_CATCHUP_LIMIT", 200))

    _log(
        "automation loop starting "
        f"headless={headless} verify_lookup={verify_lookup} poll_seconds={poll_seconds} "
        f"status_sync_seconds={status_sync_seconds} burst_size={burst_size} "
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

    while True:
        did_work = False
        try:
            now = time.monotonic()
            if next_status_sync_at is not None and now >= next_status_sync_at:
                result = sync_311_statuses()
                _log(f"status sync result: {result}")
                next_status_sync_at = time.monotonic() + status_sync_seconds
                did_work = True

            processed = 0
            while processed < burst_size:
                result = run_portal_filing_once(headless=headless, verify_lookup=verify_lookup)
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
