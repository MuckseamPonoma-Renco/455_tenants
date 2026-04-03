from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.nyc311.portal import load_local_env_file, lookup_service_request_status
from packages.nyc311.portal_worker import run_portal_filing_once


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the NYC311 portal Playwright worker once or look up an SR.")
    parser.add_argument("--headful", action="store_true", help="Show the browser instead of running headless.")
    parser.add_argument("--lookup", metavar="SR_NUMBER", help="Look up a single NYC311 service request in the portal.")
    parser.add_argument("--skip-lookup", action="store_true", help="Skip portal status lookup after a new submission.")
    args = parser.parse_args()

    load_local_env_file()
    if args.lookup:
        result = lookup_service_request_status(args.lookup, headless=not args.headful)
        print(json.dumps(result.__dict__, indent=2, ensure_ascii=False))
        return

    result = run_portal_filing_once(headless=not args.headful, verify_lookup=not args.skip_lookup)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
