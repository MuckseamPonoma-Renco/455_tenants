#!/usr/bin/env python3
"""Check the actual browser launch before touching any 311 job or case."""

import json
from playwright.sync_api import sync_playwright


def main() -> int:
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            page.set_content("<title>311 browser readiness</title>")
            if page.title() != "311 browser readiness":
                raise RuntimeError("browser page verification failed")
            browser.close()
    except Exception:
        print(json.dumps({"ok": False, "browser": "chromium", "error": "browser_launch_failed"}))
        return 1
    print(json.dumps({"ok": True, "browser": "chromium", "action": "browser_ready"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
