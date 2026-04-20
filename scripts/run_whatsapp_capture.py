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

from packages.whatsapp.web_capture import capture_config_from_env, run_capture_loop


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the WhatsApp Web Playwright capture worker against Chrome.")
    parser.add_argument("--chat", action="append", dest="chat_names", help="Exact WhatsApp chat name to watch. Repeat for more than one.")
    parser.add_argument("--api-base", help="Override the API base URL. Defaults to local API first, then PUBLIC_BASE_URL.")
    parser.add_argument("--poll-seconds", type=int, help="Seconds between capture passes.")
    parser.add_argument("--message-limit", type=int, help="How many recent visible messages to inspect inside each chat.")
    parser.add_argument("--media-dir", help="Directory where downloaded WhatsApp media evidence should be stored.")
    parser.add_argument("--user-data-dir", help="Persistent Chrome profile directory for WhatsApp Web login/session state.")
    parser.add_argument("--state-path", help="Path to the local capture state file.")
    parser.add_argument("--browser-channel", help='Playwright browser channel. Defaults to "chrome".')
    parser.add_argument("--login-timeout-seconds", type=int, help="How long to wait for WhatsApp Web login before failing. 0 waits forever.")
    parser.add_argument("--once", action="store_true", help="Run one capture pass and exit.")
    parser.add_argument("--headless", action="store_true", help="Run Chrome headlessly.")
    parser.add_argument("--headful", action="store_true", help="Force a visible Chrome window.")
    parser.add_argument("--no-prime", action="store_true", help="Capture currently visible messages immediately instead of priming them as already seen.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.headful and args.headless:
        raise SystemExit("Use only one of --headful or --headless")

    config = capture_config_from_env(
        chat_names=args.chat_names,
        api_base=args.api_base,
        headless=(False if args.headful else True) if args.headful or args.headless else None,
        poll_seconds=args.poll_seconds,
        message_limit=args.message_limit,
        media_dir=args.media_dir,
        user_data_dir=args.user_data_dir,
        state_path=args.state_path,
        browser_channel=args.browser_channel,
        login_timeout_seconds=args.login_timeout_seconds,
        prime_visible_messages=False if args.no_prime else None,
    )
    result = run_capture_loop(config, once=args.once)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
