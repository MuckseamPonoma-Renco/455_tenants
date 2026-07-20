from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.audit import append_audit_event, daily_hash_chain
from packages.db import get_session
from packages.incident.cross_source_reconciliation import ReconciliationSummary, reconcile_exact_cross_source_duplicates
from packages.local_env import load_local_env_file

load_local_env_file(ROOT / ".env")

ROSTER_FIELDS = (
    "archive_message_id",
    "live_message_id",
    "delta_seconds",
    "canonical_message_id",
    "duplicate_message_id",
    "canonical_incident_id",
    "duplicate_incident_id",
    "moved_service_cases",
    "moved_filing_jobs",
    "moved_watchdog_actions",
    "action",
)


def _default_out_dir() -> Path:
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    return ROOT / "exports" / "message_decision_audits" / stamp


def _write_artifacts(out_dir: Path, summary: ReconciliationSummary, *, dry_run: bool) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    roster_path = out_dir / "cross_source_duplicate_reconciliation.csv"
    with roster_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ROSTER_FIELDS)
        writer.writeheader()
        writer.writerows(row.as_dict() for row in summary.rows)
    summary_path = out_dir / "cross_source_duplicate_reconciliation.json"
    payload = {**summary.as_dict(), "dry_run": dry_run, "roster_csv": str(roster_path)}
    summary_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"roster_csv": str(roster_path), "summary_json": str(summary_path)}


def run_reconciliation(*, out_dir: Path | None = None, dry_run: bool = False) -> dict[str, object]:
    with get_session() as session:
        summary = reconcile_exact_cross_source_duplicates(session, dry_run=dry_run)
        if not dry_run:
            session.commit()

    if not dry_run:
        for row in summary.rows:
            if row.action != "reconciled":
                continue
            append_audit_event(
                "CROSS_SOURCE_DUPLICATE_RECONCILED",
                row.canonical_message_id,
                {
                    "duplicate_message_id": row.duplicate_message_id,
                    "delta_seconds": row.delta_seconds,
                    "moved_service_cases": row.moved_service_cases,
                    "moved_filing_jobs": row.moved_filing_jobs,
                    "moved_watchdog_actions": row.moved_watchdog_actions,
                },
            )
        daily_hash_chain()

    outputs = _write_artifacts(out_dir or _default_out_dir(), summary, dry_run=dry_run)
    return {**summary.as_dict(), **outputs, "dry_run": dry_run}


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile exact archive/live WhatsApp aliases without changing submitted 311 cases.")
    parser.add_argument("--dry-run", action="store_true", help="Write a roster without changing the database")
    parser.add_argument("--out-dir", help="Directory for the private reconciliation roster")
    args = parser.parse_args()
    result = run_reconciliation(
        out_dir=Path(args.out_dir).expanduser().resolve() if args.out_dir else None,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
