from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file()

from packages.db import get_session
from packages.incident.dedupe import DEFAULT_DEDUPE_GAP_SECONDS, dedupe_open_incidents
from packages.worker_jobs import full_resync_sheets


def main() -> None:
    parser = argparse.ArgumentParser(description="Conservatively merge duplicate open incidents.")
    parser.add_argument("--apply", action="store_true", help="Apply changes. Default is dry-run only.")
    parser.add_argument("--gap-seconds", type=int, default=DEFAULT_DEDUPE_GAP_SECONDS, help="Maximum time gap for clustering duplicates.")
    parser.add_argument("--resync-sheets", action="store_true", help="Resync Google Sheets after applying changes.")
    args = parser.parse_args()

    with get_session() as session:
        summary = dedupe_open_incidents(session, gap_seconds=args.gap_seconds, dry_run=not args.apply)
        if args.apply:
            session.commit()
        else:
            session.rollback()

    print(
        {
            "apply": bool(args.apply),
            "gap_seconds": int(args.gap_seconds),
            "merged_incidents": summary.merged_incidents,
            "deleted_jobs": summary.deleted_jobs,
            "moved_jobs": summary.moved_jobs,
            "updated_decisions": summary.updated_decisions,
            "updated_witnesses": summary.updated_witnesses,
            "clusters_merged": summary.clusters_merged,
            "clusters_skipped_multi_case": summary.clusters_skipped_multi_case,
        }
    )

    if args.apply and args.resync_sheets:
        full_resync_sheets()


if __name__ == "__main__":
    main()
