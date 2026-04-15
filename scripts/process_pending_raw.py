import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file
from packages.worker_jobs import process_pending_messages

load_local_env_file(ROOT / ".env")


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
        help="Reserved for backward compatibility.",
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

    result = process_pending_messages(
        limit=args.limit,
        latest_first=args.latest_first,
        resync_sheets=args.resync_sheets,
    )
    print(f"pending_selected={result['pending_selected']} latest_first={args.latest_first}")
    print(f"processed_total={result['processed_total']}")
    print(f"errors_total={result['errors_total']}")
    print(f"remaining_pending={result['remaining_pending']}")
    print(f"incidents_total={result['incidents_total']}")
    print(f"decisions_total={result['decisions_total']}")
    if args.resync_sheets:
        print("resync_sheets=1")
        print("resync_sheets=0")


if __name__ == "__main__":
    main()
