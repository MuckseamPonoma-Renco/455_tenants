"""Local, sanitized authentication receipts; never exposes cookie contents."""
from __future__ import annotations

import hashlib
import fcntl
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


def session_state_path() -> Path:
    return Path(os.environ.get("NYC311_SESSION_STATE_PATH") or
                str(Path.home() / ".local/state/tenant-issue-os/private/nyc311-session.json")).expanduser()


def status_path() -> Path:
    return Path(os.environ.get("NYC311_SESSION_HEALTH_PATH") or
                str(Path.home() / ".local/state/tenant-issue-os/nyc311-session-health.json")).expanduser()


def check_interval_seconds() -> int:
    try:
        return min(3600, max(300, int(os.environ.get("NYC311_SESSION_CHECK_SECONDS", "1800"))))
    except ValueError:
        return 1800


def configured() -> bool:
    return session_state_path().is_file() or bool(
        os.environ.get("311_EMAIL") or os.environ.get("NYC311_EMAIL")
    )


def state_fingerprint(state: dict) -> str:
    return hashlib.sha256(json.dumps(state, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def current_fingerprint() -> str | None:
    try:
        state = json.loads(session_state_path().read_text())
        return state_fingerprint(state) if isinstance(state, dict) else None
    except (OSError, ValueError):
        return None


@contextmanager
def session_lock():
    path = session_state_path().with_suffix(".lock")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open("a+") as lock:
        path.chmod(0o600)
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        yield


def record_for_session(state: str, expected_fingerprint: str | None) -> dict:
    with session_lock():
        if current_fingerprint() == expected_fingerprint:
            return record(state)
        return public_status()


def _read() -> dict:
    try:
        value = json.loads(status_path().read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def record(state: str, *, fingerprint: str | None = None) -> dict:
    if state not in {"ready", "login_required", "unavailable", "not_configured"}:
        raise ValueError("Invalid authentication status")
    previous = _read()
    now = datetime.now(timezone.utc).isoformat()
    value = {"state": state, "last_checked_at": now,
             "last_success_at": now if state == "ready" else previous.get("last_success_at"),
             "session_fingerprint": fingerprint if state == "ready" else None}
    path = status_path(); path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value)); temporary.chmod(0o600); os.replace(temporary, path)
    return public_status()


def public_status() -> dict:
    if not configured():
        return {"state": "not_configured", "configured": False, "has_error": False}
    receipt = _read(); state = receipt.get("state", "unverified")
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(receipt["last_checked_at"].replace("Z", "+00:00"))).total_seconds()
    except (KeyError, TypeError, ValueError):
        age = float("inf")
    if state == "ready":
        if not session_state_path().is_file() or receipt.get("session_fingerprint") != current_fingerprint():
            state = "unverified"
        elif not 0 <= age <= check_interval_seconds() + 600:
            state = "stale"
    return {"state": state, "configured": True, "has_error": state != "ready",
            "login_required": state == "login_required",
            "last_checked_at": receipt.get("last_checked_at"),
            "last_success_at": receipt.get("last_success_at"),
            "check_interval_seconds": check_interval_seconds()}


def check_saved_session() -> dict:
    """Renew the portal session via normal NYC.ID SSO; never open a complaint."""
    if not configured():
        return record("not_configured")
    from packages.nyc311 import portal
    initial = current_fingerprint()
    if initial is None:
        return record("login_required")
    try:
        options = portal._session_context_options()
        initial = state_fingerprint(options["storage_state"])
        with portal.sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                context = browser.new_context(**options)
                page = context.new_page()
                response = page.goto(portal.PORTAL_HOME_URL, wait_until="domcontentloaded", timeout=45000)
                if response is None or response.status >= 400:
                    raise RuntimeError("Portal unavailable")
                if not portal._is_portal_authenticated(page):
                    # The portal cookie can expire while NYC.ID is still
                    # authenticated. Home alone cannot establish login expiry.
                    response = page.goto(portal.PORTAL_SIGN_IN_URL, wait_until="domcontentloaded", timeout=45000)
                    if response is None or response.status >= 400:
                        raise RuntimeError("Sign-in service unavailable")
                    portal._wait_for_sign_in_redirect(page)
                if portal._is_portal_authenticated(page):
                    portal._save_verified_session(context, page, expected_fingerprint=initial)
                    return public_status()
                state = "login_required"
            finally:
                browser.close()
    except portal.PortalHumanVerificationRequired:
        state = "login_required"
    except Exception:
        state = "unavailable"
    # A slow old probe must not overwrite a newer interactive login receipt.
    return record_for_session(state, initial)
