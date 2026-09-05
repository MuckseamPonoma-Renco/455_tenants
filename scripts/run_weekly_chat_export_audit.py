from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.timeutil import parse_ts_to_epoch
from scripts.audit_whatsapp_export_decisions import (
    DEFAULT_SINCE,
    EXPORT_EXTENSIONS,
    _match_export_message,
    iter_export_messages,
    llm_review_details,
    run_audit,
)
from scripts.reconcile_cross_source_duplicates import run_reconciliation


def newest_export(inbox: Path) -> Path:
    inbox = Path(inbox).expanduser().resolve()
    inbox.mkdir(parents=True, exist_ok=True)
    candidates = [
        path
        for path in inbox.iterdir()
        if path.is_file() and path.suffix.casefold() in EXPORT_EXTENSIONS and not path.name.startswith(".")
    ]
    if not candidates:
        raise SystemExit(f"No .zip or .txt exports found in {inbox}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def import_export(export_path: Path, *, llm_mode: str) -> None:
    if export_path.suffix.casefold() == ".zip":
        cmd = [
            sys.executable,
            str(ROOT / "scripts" / "import_whatsapp_zip.py"),
            "--zip",
            str(export_path),
            "--llm-mode",
            llm_mode,
        ]
    else:
        cmd = [
            sys.executable,
            str(ROOT / "scripts" / "import_whatsapp_export.py"),
            str(export_path),
            "--llm-mode",
            llm_mode,
        ]
    env = os.environ.copy()
    env["AUTO_FILE_ENABLED"] = "0"
    env["DISABLE_SHEETS_SYNC"] = "1"
    subprocess.run(cmd, cwd=ROOT, check=True, env=env)


def sync_sheets_after_success() -> None:
    from packages.worker_jobs import sync_all_sheets

    previous = os.environ.get("DISABLE_SHEETS_SYNC")
    os.environ["DISABLE_SHEETS_SYNC"] = "0"
    try:
        sync_all_sheets()
    finally:
        if previous is None:
            os.environ.pop("DISABLE_SHEETS_SYNC", None)
        else:
            os.environ["DISABLE_SHEETS_SYNC"] = previous


def retry_incomplete_llm_reviews(export_path: Path, *, since: str, llm_mode: str) -> dict[str, object]:
    since_epoch = parse_ts_to_epoch(since)
    if since_epoch is None:
        raise ValueError(f"Unable to parse audit cutoff: {since}")

    from packages.db import MessageDecision, RawMessage, get_session
    from packages.incident import extractor
    from packages.incident.cross_source_reconciliation import _recompute_incident_materialization

    os.environ["LLM_MODE"] = llm_mode
    extractor.LLM_MODE = llm_mode

    messages = [
        message
        for message in iter_export_messages(export_path)
        if message.ts_epoch is None or int(message.ts_epoch) >= int(since_epoch)
    ]
    pending_ids: list[str] = []
    seen_ids: set[str] = set()
    with get_session() as session:
        for message in messages:
            raw, _match_method, _alternate = _match_export_message(session, message)
            if raw is None or raw.message_id in seen_ids:
                continue
            seen_ids.add(raw.message_id)
            decision = session.get(MessageDecision, raw.message_id)
            status, _error = llm_review_details(decision, text=raw.text or "")
            if status in {"missing", "failed"}:
                pending_ids.append(raw.message_id)

    result: dict[str, object] = {
        "required": len(seen_ids),
        "pending_before": len(pending_ids),
        "attempted": 0,
        "completed": 0,
        "failed": 0,
        "error": "",
    }
    if not pending_ids or llm_mode not in {"all", "supervised"}:
        return result

    touched_incident_ids: set[str] = set()
    with get_session() as session:
        try:
            for message_id in pending_ids:
                raw = session.get(RawMessage, message_id)
                if raw is None:
                    continue
                previous = session.get(MessageDecision, message_id)
                if previous and previous.incident_id:
                    touched_incident_ids.add(previous.incident_id)
                result["attempted"] = int(result["attempted"]) + 1
                extractor.classify_and_upsert_incident(session, raw, allow_filing_job=False)
                decision = session.get(MessageDecision, message_id)
                status, error = llm_review_details(decision, text=raw.text or "")
                if status not in {"completed", "not_applicable"}:
                    result["failed"] = 1
                    result["error"] = error or status
                    session.rollback()
                    return result
                if decision and decision.incident_id:
                    touched_incident_ids.add(decision.incident_id)
                result["completed"] = int(result["completed"]) + 1

            for incident_id in touched_incident_ids:
                incident = session.get(extractor.Incident, incident_id)
                if incident is not None:
                    _recompute_incident_materialization(session, incident)
            session.commit()
        except Exception as exc:
            session.rollback()
            result["failed"] = 1
            result["error"] = str(exc)[:300]
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Import the newest chat export from an inbox, then create a decision audit roster.")
    parser.add_argument("--inbox", default=str(ROOT / "incoming" / "chat_exports"), help="Folder containing weekly .zip/.txt exports")
    parser.add_argument("--export", help="Specific export file to process instead of the newest inbox file")
    parser.add_argument("--since", default=DEFAULT_SINCE, help=f"Audit cutoff timestamp. Default: {DEFAULT_SINCE}")
    parser.add_argument(
        "--llm-mode",
        default="all",
        choices=("off", "uncertain", "assist", "all", "supervised"),
        help="Model-review mode for newly imported messages. Default: all.",
    )
    parser.add_argument("--skip-import", action="store_true", help="Only create audit artifacts; do not import/reprocess the export first")
    parser.add_argument(
        "--allow-incomplete-llm-review",
        action="store_true",
        help="Write the roster without failing when one or more required model reviews are unavailable.",
    )
    parser.add_argument(
        "--sync-sheets-after-success",
        action="store_true",
        help="Publish Sheets only after every required model review succeeds.",
    )
    parser.add_argument("--out-dir", help="Output directory for audit artifacts")
    args = parser.parse_args()

    export_path = Path(args.export).expanduser().resolve() if args.export else newest_export(Path(args.inbox))
    out_dir = (
        Path(args.out_dir).expanduser().resolve()
        if args.out_dir
        else ROOT / "exports" / "message_decision_audits" / dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    )
    if not args.skip_import:
        import_export(export_path, llm_mode=args.llm_mode)
    retry = (
        retry_incomplete_llm_reviews(export_path, since=args.since, llm_mode=args.llm_mode)
        if not args.skip_import
        else {
            "required": 0,
            "pending_before": 0,
            "attempted": 0,
            "completed": 0,
            "failed": 0,
            "error": "",
        }
    )
    reconciliation = run_reconciliation(out_dir=out_dir)
    require_llm_review = args.llm_mode in {"all", "supervised"}
    summary = run_audit(
        export_path,
        since=args.since,
        out_dir=out_dir,
        require_llm_review=require_llm_review,
    )
    summary["llm_review_retry"] = retry
    summary["cross_source_reconciliation"] = reconciliation
    review_complete = bool(summary.get("llm_review_complete"))
    data_complete = not int(summary.get("missing_db_messages") or 0) and not int(
        summary.get("missing_decisions") or 0
    )
    if not data_complete:
        summary["ok"] = False
        summary["error"] = (
            f"data audit incomplete: {summary.get('missing_db_messages', 0)} messages missing from DB, "
            f"{summary.get('missing_decisions', 0)} decisions missing"
        )
    if require_llm_review and not review_complete:
        summary["ok"] = False
        summary["error"] = (
            f"model review incomplete: {summary.get('llm_review_missing', 0)} missing, "
            f"{summary.get('llm_review_failed', 0)} failed"
        )
    sheet_sync_complete = not args.sync_sheets_after_success
    if args.sync_sheets_after_success and review_complete and data_complete:
        try:
            sync_sheets_after_success()
            sheet_sync_complete = True
        except Exception as exc:
            summary["ok"] = False
            summary["error"] = f"post-audit sheet sync failed: {str(exc)[:300]}"
    summary["sheet_sync_requested"] = bool(args.sync_sheets_after_success)
    summary["sheet_sync_complete"] = bool(sheet_sync_complete)
    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    if require_llm_review and not review_complete and not args.allow_incomplete_llm_review:
        raise SystemExit(2)
    if not data_complete:
        raise SystemExit(4)
    if args.sync_sheets_after_success and not sheet_sync_complete:
        raise SystemExit(3)


if __name__ == "__main__":
    main()
