from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file()

from packages.nyc311.portal import lookup_service_request_status, sign_in_interactively
from packages.nyc311.portal_worker import run_portal_filing_once, release_verified_sign_in_holds
from packages.nyc311.session_health import check_saved_session


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the NYC311 portal Playwright worker once or look up an SR.")
    parser.add_argument("--headful", action="store_true", help="Show the browser instead of running headless.")
    parser.add_argument("--lookup", metavar="SR_NUMBER", help="Look up a single NYC311 service request in the portal.")
    parser.add_argument("--skip-lookup", action="store_true", help="Skip portal status lookup after a new submission.")
    parser.add_argument("--sign-in", action="store_true", help="Open a visible NYC.ID sign-in and save an owner-only local app session; never files a complaint.")
    parser.add_argument("--check-session", action="store_true", help="Verify and retain a valid saved login without opening a complaint.")
    parser.add_argument("--resume-human-job", type=int, help="Resume exactly one job held for human verification; uncertain submissions cannot be resumed.")
    parser.add_argument("--human-verification-wait-seconds", type=int, default=0, help="Wait up to 900 seconds for the user to solve a visible challenge; requires --headful.")
    args = parser.parse_args()

    load_local_env_file()
    if args.check_session:
        if args.sign_in or args.lookup or args.resume_human_job is not None:
            parser.error("--check-session cannot be combined with other actions")
        result = check_saved_session()
        print(json.dumps(result, indent=2))
        if result.get("has_error"):
            raise SystemExit(1)
        return
    if args.sign_in:
        if args.lookup or args.resume_human_job is not None:
            parser.error("--sign-in cannot be combined with filing or lookup")
        result = sign_in_interactively(wait_seconds=args.human_verification_wait_seconds or 300)
        if result.get("authenticated"):
            result["sign_in_holds_released"] = release_verified_sign_in_holds()
        print(json.dumps(result, indent=2))
        return
    if args.lookup:
        result = lookup_service_request_status(args.lookup, headless=not args.headful)
        print(json.dumps(result.__dict__, indent=2, ensure_ascii=False))
        return

    if (args.resume_human_job is not None or args.human_verification_wait_seconds) and not args.headful:
        parser.error("Human verification requires --headful")
    result = run_portal_filing_once(headless=not args.headful, verify_lookup=not args.skip_lookup,
                                   resume_human_job_id=args.resume_human_job,
                                   human_verification_timeout_seconds=args.human_verification_wait_seconds)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
