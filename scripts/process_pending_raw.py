import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / ".env", override=True)

from packages.db import Incident, MessageDecision, RawMessage, get_session
from packages.incident.extractor import classify_and_upsert_incident
from packages.nyc311.planner import ensure_filing_jobs
from packages.worker_jobs import full_resync_sheets


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process raw messages that do not yet have a decision row."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of pending raw messages to process in this run.",
    )
    parser.add_argument(
        "--commit-every",
        type=int,
        default=25,
        help="Commit progress every N processed messages.",
    )
    parser.add_argument(
        "--latest-first",
        action="store_true",
        help="Process newest pending messages first instead of oldest first.",
    )
    parser.add_argument(
        "--resync-sheets",
        action="store_true",
        help="Run a full Sheets resync after processing completes.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.resync_sheets:
        os.environ["DISABLE_SHEETS_SYNC"] = "0"

    order = RawMessage.ts_epoch.desc().nullslast() if args.latest_first else RawMessage.ts_epoch.asc().nullsfirst()

    with get_session() as session:
        pending_ids = [
            mid
            for (mid,) in session.execute(
                select(RawMessage.message_id)
                .outerjoin(MessageDecision, MessageDecision.message_id == RawMessage.message_id)
                .where(MessageDecision.message_id.is_(None))
                .order_by(order)
                .limit(args.limit)
            ).all()
        ]

    print(f"pending_selected={len(pending_ids)} latest_first={args.latest_first}")

    processed = 0
    errors = 0
    for idx, message_id in enumerate(pending_ids, start=1):
        try:
            with get_session() as session:
                raw = session.get(RawMessage, message_id)
                if raw is None or session.get(MessageDecision, message_id) is not None:
                    continue
                classify_and_upsert_incident(session, raw)
                session.commit()
        except Exception as exc:
            errors += 1
            print(f"error message_id={message_id[:12]} err={exc}")
            continue

        processed += 1
        if processed % args.commit_every == 0:
            print(f"processed={processed}/{len(pending_ids)}")

    with get_session() as session:
        ensure_filing_jobs(session)
        session.commit()

        remaining = session.query(RawMessage).outerjoin(
            MessageDecision, MessageDecision.message_id == RawMessage.message_id
        ).filter(MessageDecision.message_id.is_(None)).count()
        incidents = session.query(Incident).count()
        decisions = session.query(MessageDecision).count()

    print(f"processed_total={processed}")
    print(f"errors_total={errors}")
    print(f"remaining_pending={remaining}")
    print(f"incidents_total={incidents}")
    print(f"decisions_total={decisions}")

    if args.resync_sheets:
        print("resync_sheets=1")
        full_resync_sheets()
        print("resync_sheets=0")


if __name__ == "__main__":
    main()
