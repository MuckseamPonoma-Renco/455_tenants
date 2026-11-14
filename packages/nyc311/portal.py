from __future__ import annotations

import json
import os
import re
import time
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page, sync_playwright

from packages.local_env import load_local_env_file
from packages.nyc311.address import canonicalize_building_address
from packages.nyc311.tracker import normalize_sr_number
from packages.nyc311 import session_health

PORTAL_BASE_URL = "https://portal.311.nyc.gov"
PORTAL_HOME_URL = f"{PORTAL_BASE_URL}/"
PORTAL_SIGN_IN_URL = f"{PORTAL_BASE_URL}/SignIn?returnUrl=%2F"
ELEVATOR_ARTICLE_URL = f"{PORTAL_BASE_URL}/article/?kanumber=KA-02015"
CHECK_STATUS_URL = f"{PORTAL_BASE_URL}/check-status/"
NY = ZoneInfo("America/New_York")
SR_RE = re.compile(r"\b311[-\s]?(\d{8,})\b")


class PortalSubmissionCancelled(RuntimeError):
    pass


class PortalSubmissionUncertain(RuntimeError):
    """Final submit may have reached NYC311; never retry without reconciliation."""

    def __init__(self, message: str, *, evidence_path: str | None = None):
        super().__init__(message)
        self.evidence_path = evidence_path


class PortalHumanVerificationRequired(RuntimeError):
    """A visible unsolved challenge blocks submission; no final click occurred."""

    def __init__(self, *, evidence_path: str | None = None, reason: str = "captcha"):
        super().__init__("NYC311 sign-in requires user verification" if reason == "sign_in"
                         else "NYC311 requires human CAPTCHA verification before submission")
        self.evidence_path = evidence_path
        self.reason = reason


def _session_state_path() -> Path:
    return session_health.session_state_path()


def _session_context_options() -> dict[str, Any]:
    path = _session_state_path()
    if not path.is_file():
        return {}
    try:
        state = json.loads(path.read_text())
        if not isinstance(state, dict) or not isinstance(state.get("cookies"), list):
            raise ValueError("invalid session state")
        return {"storage_state": state}
    except (OSError, ValueError):
        raise PortalHumanVerificationRequired(reason="sign_in") from None


def _is_portal_authenticated(page: Page) -> bool:
    if urlparse(page.url).netloc != urlparse(PORTAL_BASE_URL).netloc:
        return False
    # Merely returning to the portal origin (including /SignIn) is not proof.
    return page.locator('a[href*="LogOff"], a[href*="SignOut"], a[href*="signout"], a[href*="logout"]').count() > 0


def _wait_for_sign_in_redirect(page: Page, *, timeout_seconds: int = 45) -> bool:
    """Allow NYC.ID SSO to restore the portal session without entering secrets.

    A visible login form or challenge requires interaction; an unfinished
    redirect is unavailable, not evidence that the saved identity expired.
    """
    for _ in range(timeout_seconds + 1):
        try:
            if _is_portal_authenticated(page):
                return True
            if _human_verification_required(page):
                raise PortalHumanVerificationRequired(reason="sign_in")
            if page.locator('input[type="password"]:visible, #gigya-passwordless-login-form:visible').count():
                return False
        except PlaywrightError as exc:
            # NYC.ID can replace the document between the two DOM checks.
            # Retry only this navigation race, never a closed browser or an
            # arbitrary browser error.
            if "Execution context was destroyed" not in str(exc):
                raise
        page.wait_for_timeout(1000)
    raise TimeoutError("NYC.ID sign-in redirect did not finish")


def _save_verified_session(context, page: Page, *, expected_fingerprint: str | None = None) -> str | None:
    if not _is_portal_authenticated(page):
        raise PortalHumanVerificationRequired(reason="sign_in")
    path = _session_state_path()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    state = context.storage_state()
    fingerprint = session_health.state_fingerprint(state)
    with session_health.session_lock():
        if expected_fingerprint is not None and session_health.current_fingerprint() != expected_fingerprint:
            return None
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=".nyc311-session-", delete=False) as handle:
                temporary = Path(handle.name)
                json.dump(state, handle)
            temporary.chmod(0o600)
            os.replace(temporary, path)
            session_health.record("ready", fingerprint=fingerprint)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    return fingerprint


def sign_in_interactively(*, wait_seconds: int = 300) -> dict[str, Any]:
    """Explicit operator flow: sign in and remember only this app's session."""
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        context = browser.new_context(viewport={"width": 1440, "height": 1100}, timezone_id="America/New_York")
        page = context.new_page()
        try:
            page.goto(PORTAL_SIGN_IN_URL, wait_until="domcontentloaded", timeout=120000)
            print("Complete NYC.ID sign-in in the visible browser. No complaint will be submitted.", flush=True)
            deadline = time.monotonic() + min(900, max(1, wait_seconds))
            while time.monotonic() < deadline:
                if _is_portal_authenticated(page):
                    _save_verified_session(context, page)
                    return {"ok": True, "authenticated": True, "session_saved": True}
                page.wait_for_timeout(1000)
            return {"ok": False, "error": "sign_in_not_completed", "session_saved": False}
        except PlaywrightError:
            return {"ok": False, "error": "sign_in_interrupted", "session_saved": False}
        finally:
            browser.close()


def _human_verification_required(page: Page) -> bool:
    """Inspect visible challenge UI without clicking or solving it."""
    return bool(page.evaluate("""() => {
        const visible = el => {
            const r = el.getBoundingClientRect();
            const s = getComputedStyle(el);
            return r.width > 0 && r.height > 0 && s.display !== 'none' && s.visibility !== 'hidden';
        };
        const token = [...document.querySelectorAll('textarea[name="g-recaptcha-response"], textarea[name="h-captcha-response"]')]
            .some(el => el.value.trim());
        const challenge = [...document.querySelectorAll('iframe')].some(el =>
            visible(el) && /recaptcha|hcaptcha/i.test(el.src + ' ' + el.title) &&
            !/invisible/i.test(el.title) && !/[?&]size=invisible/i.test(el.src));
        return challenge && !token;
    }"""))


@dataclass
class PortalAddressMatch:
    address_id: str
    full_address: str
    raw_record: dict[str, Any]


@dataclass
class PortalSubmissionResult:
    service_request_number: str | None
    confirmation_text: str
    final_url: str
    address_id: str
    address_text: str
    login_used: bool
    review_screenshot_path: str | None = None
    confirmation_screenshot_path: str | None = None


@dataclass
class PortalStatusLookup:
    service_request_number: str
    found: bool
    status: str | None
    page_text: str
    final_url: str


def _env(*names: str, default: str = "") -> str:
    for name in names:
        value = os.environ.get(name)
        if value is not None and value != "":
            return value
    return default


def _safe_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _set_value(page: Page, selector: str, value: str, *, checked: bool | None = None) -> None:
    if checked is None:
        script = """
        (el, val) => {
          el.value = val;
          el.dispatchEvent(new Event('input', { bubbles: true }));
          el.dispatchEvent(new Event('change', { bubbles: true }));
        }
        """
        page.eval_on_selector(selector, script, value)
        return
    script = """
    (el, shouldCheck) => {
      el.checked = shouldCheck;
      el.dispatchEvent(new Event('input', { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
      el.dispatchEvent(new Event('click', { bubbles: true }));
    }
    """
    page.eval_on_selector(selector, script, checked)


def _wait_for_url_change(page: Page, previous_url: str, timeout_ms: int = 120_000) -> None:
    page.wait_for_function(
        "prevUrl => window.location.href !== prevUrl",
        arg=previous_url,
        timeout=timeout_ms,
    )


def _trim_description(value: str) -> str:
    clean = (value or "").strip()
    return clean[:1900]


def _wait_for_submit_confirmation(page: Page, previous_url: str, timeout_ms: int = 30_000) -> None:
    page.wait_for_function(
        """
        prevUrl => {
          const text = (document.body && document.body.innerText) || "";
          return window.location.href !== prevUrl
            || /service request|311[-\\s]?\\d{8,}|thank you/i.test(text);
        }
        """,
        arg=previous_url,
        timeout=timeout_ms,
    )


def _observed_at_text(payload: dict[str, Any]) -> str:
    latest_allowed = datetime.now(NY) - timedelta(minutes=5)
    incident = payload.get("incident") or {}
    for key in ("start_ts", "last_ts"):
        raw = incident.get(key)
        if not raw:
            continue
        try:
            parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=NY)
            observed = parsed.astimezone(NY)
            if observed > latest_allowed:
                observed = latest_allowed
            return observed.strftime("%-m/%-d/%Y %-I:%M %p")
        except Exception:
            continue
    return latest_allowed.strftime("%-m/%-d/%Y %-I:%M %p")


def _address_queries(payload: dict[str, Any]) -> tuple[list[str], str]:
    building = canonicalize_building_address(payload.get("building") or {})
    street = building.get("street_address", "")
    city = building.get("city", "")
    zip_code = building.get("zip", "")
    full_address = building.get("full_address", "")
    queries: list[str] = []
    for candidate in (
        full_address,
        f"{street} {city}".strip(),
        f"{street} {city} {zip_code}".strip(),
        street,
        street.replace(" Pkwy", " Parkway").replace(" PKWY", " PARKWAY").replace(" Pky", " Parkway"),
    ):
        clean = " ".join(candidate.split()).strip()
        if clean and clean.upper() not in {q.upper() for q in queries}:
            queries.append(clean)
    return queries, zip_code


def _maybe_sign_in(page: Page) -> bool:
    email = _env("311_EMAIL", "NYC311_EMAIL")
    password = _env("311_PASSWORD", "NYC311_PASSWORD")
    if (not email or not password) and not _session_state_path().is_file():
        return False

    try:
        page.goto(PORTAL_SIGN_IN_URL, wait_until="domcontentloaded", timeout=120_000)
        if _wait_for_sign_in_redirect(page):
            return True

        if page.locator('#gigya-passwordless-login-form:visible').count():
            raise PortalHumanVerificationRequired(reason="sign_in")
        if urlparse(page.url).hostname not in {"www1.nyc.gov", "nyc.gov", "prd-login.nyc.gov"} or not email or not password:
            raise PortalHumanVerificationRequired(reason="sign_in")

        email_input = page.locator('#gigya-loginID, input[name="username"]').first
        password_input = page.locator('#gigya-password, input[name="password"]').first
        submit_button = page.locator('input[type="submit"][value="Login"]').first

        email_input.wait_for(state="visible", timeout=20_000)
        password_input.wait_for(state="visible", timeout=20_000)
        email_input.fill(email)
        password_input.fill(password)
        submit_button.click(force=True, no_wait_after=True)
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if _is_portal_authenticated(page):
                return True
            page.wait_for_timeout(1000)
        raise PortalHumanVerificationRequired(reason="sign_in")
    except PortalHumanVerificationRequired:
        raise
    except Exception:
        # Configured authentication must not silently become anonymous filing.
        raise PortalHumanVerificationRequired(reason="sign_in") from None


def _open_elevator_flow(page: Page) -> None:
    page.goto(ELEVATOR_ARTICLE_URL, wait_until="domcontentloaded", timeout=120_000)
    page.wait_for_timeout(1500)
    js = page.locator("a.contentaction").first.get_attribute("onclick")
    if not js:
        raise RuntimeError("NYC311 elevator complaint launcher not found")
    previous_url = page.url
    page.evaluate(js.replace("javascript:", ""))
    _wait_for_url_change(page, previous_url)
    page.wait_for_timeout(1200)


def _pick_best_address_match(records: list[dict[str, Any]], query: str, preferred_zip: str = "") -> PortalAddressMatch:
    if not records:
        raise RuntimeError(f"No portal address results matched {query!r}")

    query_upper = query.upper()
    preferred_zip = preferred_zip.strip()

    scored: list[tuple[tuple[int, int, int], PortalAddressMatch]] = []
    for record in records:
        attrs = {item.get("Name"): item.get("DisplayValue") or item.get("Value") for item in record.get("Attributes") or []}
        address_id = str(attrs.get("n311_addressid") or record.get("Id") or "").strip()
        full_address = str(
            attrs.get("n311_fulladdress")
            or attrs.get("n311_holderfulladdress")
            or attrs.get("n311_address")
            or attrs.get("n311_name")
            or ""
        ).strip()
        if not address_id:
            continue
        upper = full_address.upper()
        zip_score = 1 if preferred_zip and preferred_zip in upper else 0
        exact_score = 1 if upper.startswith(query_upper) else 0
        token_score = sum(1 for token in query_upper.split() if token in upper)
        scored.append(
            (
                (zip_score, exact_score, token_score),
                PortalAddressMatch(address_id=address_id, full_address=full_address or query, raw_record=record),
            )
        )
    if not scored:
        raise RuntimeError(f"Portal address lookup returned records without ids for {query!r}")
    scored.sort(key=lambda item: item[0], reverse=True)
    return scored[0][1]


def _resolve_address(page: Page, payload: dict[str, Any]) -> PortalAddressMatch:
    queries, preferred_zip = _address_queries(payload)
    data_url = page.locator("#n311_addressid_lookupmodal .entity-lookup").get_attribute("data-url")
    if not data_url:
        raise RuntimeError("Address lookup endpoint not found in NYC311 portal")
    lookup_url = urljoin(PORTAL_BASE_URL, data_url)

    page.eval_on_selector('button.launchentitylookup[aria-label="Address Launch lookup modal"]', "el => el.click()")
    page.wait_for_timeout(1200)

    for query in queries:
        search_box = page.locator('#n311_addressid_lookupmodal input[placeholder="Search"]')
        search_button = page.locator('#n311_addressid_lookupmodal button[aria-label="Search Results"]')
        search_box.fill(query.upper(), force=True)
        page.wait_for_timeout(250)
        with page.expect_response(
            lambda resp: resp.url.startswith(lookup_url) and resp.request.method == "POST",
            timeout=20_000,
        ) as response_info:
            page.eval_on_selector(
                '#n311_addressid_lookupmodal button[aria-label="Search Results"]',
                "el => el.click()",
            )
        payload_json = response_info.value.json()
        match = _pick_best_address_match(payload_json.get("Records") or [], query, preferred_zip=preferred_zip)
        page.eval_on_selector("#n311_addressid_lookupmodal button.form-close", "el => el.click()")
        page.wait_for_timeout(300)
        return match
    raise RuntimeError("NYC311 portal address lookup did not return a usable match")


def _apply_address(page: Page, match: PortalAddressMatch, *, anonymous: bool) -> None:
    if anonymous:
        _set_value(page, "#n311_portalsubmitanonymous_1", "", checked=True)
    _set_value(page, "#n311_portaladdresstype_0", "", checked=True)
    _set_value(page, "#n311_addresstype", "614110000")
    _set_value(page, "#n311_addressid", match.address_id)
    _set_value(page, "#n311_addressid_name", match.full_address)
    _set_value(page, "#n311_addressid_entityname", "n311_address")


def _select_visible_where_address(page: Page, payload: dict[str, Any]) -> PortalAddressMatch | None:
    building = canonicalize_building_address(payload.get("building") or {})
    full_address = building.get("full_address", "")
    if not full_address:
        raise RuntimeError("Missing building address")

    page.wait_for_timeout(1000)
    try:
        visible_picker = page.locator("#SelectAddressWhere")
        if visible_picker.count() == 0:
            return None
    except Exception:
        return None

    page.eval_on_selector("#SelectAddressWhere", "el => el.click()")
    page.wait_for_timeout(2000)

    search_box = page.locator("#address-search-box-input")
    if search_box.count() == 0 or not search_box.is_visible():
        raise RuntimeError("Visible address picker search box did not appear")
    search_box.fill(full_address, force=True)
    page.wait_for_timeout(1000)
    search_box.press("ArrowDown")
    page.wait_for_timeout(500)
    search_box.press("Enter")
    page.wait_for_timeout(5000)

    select_button = page.locator("#SelectAddressMap")
    if select_button.count() == 0:
        raise RuntimeError("Visible address picker select button did not appear")
    page.eval_on_selector("#SelectAddressMap", "el => el.click()")
    page.wait_for_timeout(3000)

    address_id = str(page.locator("#n311_addressid").evaluate("el => el.value || ''")).strip()
    if not address_id:
        raise RuntimeError(f"Visible address picker did not populate an address id for {full_address}")

    return PortalAddressMatch(
        address_id=address_id,
        full_address=full_address,
        raw_record={"Id": address_id, "Source": "visible_address_picker"},
    )


def _review_has_address(review_text: str, payload: dict[str, Any]) -> bool:
    street = canonicalize_building_address(payload.get("building") or {}).get("street_address", "")
    if not street:
        return False
    return street.upper() in (review_text or "").upper()


def _save_screenshot(page: Page, screenshot_dir: Path | None, label: str) -> str | None:
    if screenshot_dir is None:
        return None
    screenshot_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_safe_name(label)}.png"
    path = screenshot_dir / filename
    page.screenshot(path=str(path), full_page=True)
    path.chmod(0o600)
    return str(path)


def _extract_confirmation_sr_number(page: Page, confirmation_text: str) -> str | None:
    from_text = normalize_sr_number(confirmation_text)
    if from_text:
        return from_text
    try:
        details_link = page.locator('a[href*="sr-details/?srnum="]').first
        href = details_link.get_attribute("href")
        return normalize_sr_number(href or "")
    except Exception:
        return None


def _extract_lookup_status(page_text: str) -> str | None:
    invalid = {"sign in | sign up", "subscribe", "service request status"}
    lines = [
        line.replace("\u202f", " ").replace("\u200e", "").replace("\u200f", "").strip()
        for line in (page_text or "").splitlines()
    ]
    for label in ("SR Status", "Status"):
        for idx, line in enumerate(lines):
            if line.casefold() != label.casefold():
                continue
            for candidate in lines[idx + 1 :]:
                if not candidate:
                    continue
                if candidate.casefold() in invalid:
                    continue
                if candidate.casefold() in {"in progress", "open", "closed", "pending", "assigned", "started", "completed", "cancelled", "canceled"}:
                    return candidate
                break
    # Closed DOB pages omit SR Status. Infer only when an explicit status was
    # absent so contradictory open-status/closed-date evidence remains visible.
    for idx, line in enumerate(lines):
        if line.casefold() == "date closed" and idx + 1 < len(lines):
            if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}(?:,?\s+.*)?", lines[idx + 1]):
                return "Closed"
    if re.search(r"\b(?:reviewed this complaint and closed it|service request (?:has been|is) closed)\b", page_text, re.I):
        return "Closed"
    return None


def _lookup_matches(page_text: str, sr_number: str) -> bool:
    return bool(re.search(r"\bSR Number\s+" + re.escape(sr_number) + r"\b", page_text, re.I))


def submit_elevator_complaint(
    payload: dict[str, Any],
    *,
    headless: bool = True,
    submit_live: bool = False,
    screenshot_dir: str | Path | None = None,
    before_submit: Callable[[], bool] | None = None,
    human_verification_timeout_seconds: int = 0,
) -> PortalSubmissionResult:
    if human_verification_timeout_seconds and headless:
        raise ValueError("Human verification requires a visible browser")
    load_local_env_file()
    screenshot_path = Path(screenshot_dir) if screenshot_dir else Path(".local/nyc311_portal")
    description = _trim_description(str(payload.get("description") or payload.get("payload", {}).get("description") or ""))
    if not description:
        raise RuntimeError("Missing complaint description")

    session_version = session_health.current_fingerprint()
    try:
        session_options = _session_context_options()
    except PortalHumanVerificationRequired:
        session_health.record_for_session("login_required", session_version)
        raise
    session_version = session_health.state_fingerprint(session_options["storage_state"]) if "storage_state" in session_options else None
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        context = browser.new_context(viewport={"width": 1440, "height": 2200}, timezone_id="America/New_York", **session_options)
        page = context.new_page()
        try:
            login_used = _maybe_sign_in(page)
        except PortalHumanVerificationRequired as exc:
            evidence = _save_screenshot(page, screenshot_path, "sign_in_required")
            browser.close()
            session_health.record_for_session("login_required", session_version)
            raise PortalHumanVerificationRequired(reason=exc.reason, evidence_path=evidence) from None

        if login_used and session_version is not None:
            session_version = _save_verified_session(context, page, expected_fingerprint=session_version)

        _open_elevator_flow(page)
        _set_value(page, "#n311_additionaldetailsid_select", "0023fcab-0291-e811-a964-000d3a199997")
        page.locator("#n311_description").fill(description, force=True)
        page.locator("#n311_datetimeobserved_datepicker_description").fill(_observed_at_text(payload), force=True)
        _set_value(page, "#n311_priority", "614110001")

        previous_url = page.url
        page.locator("#NextButton").click(force=True)
        _wait_for_url_change(page, previous_url)

        address_match = _select_visible_where_address(page, payload)
        if address_match is None:
            address_match = _resolve_address(page, payload)
            _apply_address(page, address_match, anonymous=True)

        previous_url = page.url
        page.locator("#NextButton").click(force=True)
        _wait_for_url_change(page, previous_url)

        page.wait_for_timeout(1200)
        _set_value(page, "#n311_portalsubmitanonymous", "", checked=True)

        previous_url = page.url
        page.locator("#NextButton").click(force=True, no_wait_after=True)
        _wait_for_url_change(page, previous_url)
        page.wait_for_timeout(1500)

        review_screenshot_path = _save_screenshot(page, screenshot_path, "review")
        review_text = page.locator("body").inner_text()
        if login_used and not _is_portal_authenticated(page):
            browser.close()
            session_health.record_for_session("login_required", session_version)
            raise PortalHumanVerificationRequired(reason="sign_in", evidence_path=review_screenshot_path)
        if login_used and session_version is not None:
            _save_verified_session(context, page, expected_fingerprint=session_version)
        if submit_live and not _review_has_address(review_text, payload):
            browser.close()
            raise RuntimeError(f"NYC311 review screen did not show the complaint address for {address_match.full_address}")
        if not submit_live:
            browser.close()
            return PortalSubmissionResult(
                service_request_number=None,
                confirmation_text=review_text,
                final_url=page.url,
                address_id=address_match.address_id,
                address_text=address_match.full_address,
                login_used=login_used,
                review_screenshot_path=review_screenshot_path,
                confirmation_screenshot_path=None,
            )
        if human_verification_timeout_seconds and _human_verification_required(page):
            deadline = time.monotonic() + min(900, max(0, human_verification_timeout_seconds))
            print("NYC311 requires human verification in the visible browser; waiting before submission.", flush=True)
            while _human_verification_required(page) and time.monotonic() < deadline:
                page.wait_for_timeout(1000)
        if _human_verification_required(page):
            browser.close()
            raise PortalHumanVerificationRequired(evidence_path=review_screenshot_path)
        if before_submit is not None and not before_submit():
            browser.close()
            raise PortalSubmissionCancelled("Filing payload or incident state changed before final submission")

        previous_url = page.url
        try:
            page.locator("#NextButton").click(force=True, no_wait_after=True)
            # A generic 'Service Request' heading exists before submission too.
            # Wait for an actual numbered receipt instead of that heading.
            page.wait_for_function(r"""() => {
                const text = document.body?.innerText || '';
                return /\b311[-\s]?\d{8}\b/.test(text)
                    || !!document.querySelector('a[href*="sr-details/?srnum=311-"]');
            }""", timeout=60000)
            confirmation_text = page.locator("body").inner_text()
            service_request_number = _extract_confirmation_sr_number(page, confirmation_text)
            if not service_request_number:
                raise RuntimeError("No numbered NYC311 receipt after final submit")
            # Screenshots are evidence aids, not a reason to lose a real receipt.
            try:
                confirmation_screenshot_path = _save_screenshot(page, screenshot_path, "confirmation")
            except PlaywrightError:
                confirmation_screenshot_path = None
        except Exception as exc:
            evidence_path = None
            try:
                evidence_path = _save_screenshot(page, screenshot_path, "submission_unknown")
                if evidence_path:
                    Path(evidence_path).chmod(0o600)
            except Exception:
                pass
            browser.close()
            raise PortalSubmissionUncertain(
                "NYC311 final submission outcome needs receipt reconciliation",
                evidence_path=evidence_path,
            ) from exc
        browser.close()
        return PortalSubmissionResult(
            service_request_number=service_request_number,
            confirmation_text=confirmation_text,
            final_url=page.url,
            address_id=address_match.address_id,
            address_text=address_match.full_address,
            login_used=login_used,
            review_screenshot_path=review_screenshot_path,
            confirmation_screenshot_path=confirmation_screenshot_path,
        )


def lookup_service_request_status(sr_number: str, *, headless: bool = True, timeout_seconds: float = 120) -> PortalStatusLookup:
    load_local_env_file()
    normalized = normalize_sr_number(sr_number)
    if not normalized:
        raise RuntimeError(f"Invalid service request number: {sr_number}")

    deadline = time.monotonic() + timeout_seconds
    def remaining_ms():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("NYC311 lookup deadline exceeded")
        return max(1, int(remaining * 1000))

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless, timeout=remaining_ms())
        try:
            context = browser.new_context(viewport={"width": 1440, "height": 1600}, timezone_id="America/New_York")
            page = context.new_page()
            page.goto(CHECK_STATUS_URL, wait_until="domcontentloaded", timeout=remaining_ms())
            page.locator("#ReferenceNumberInput").fill(normalized, force=True, timeout=remaining_ms())
            page.locator("#SubmitBtn").click(force=True, no_wait_after=True, timeout=remaining_ms())
            page.wait_for_function(r"""sr => {
            const text = document.body?.innerText || '';
            return /invalid service request|service request (?:was )?not found/i.test(text)
                || (text.includes(sr) && /SR Number/.test(text)
                    && (/SR Status/.test(text) || /Date Closed\s+\d/.test(text)
                        || /reviewed this complaint and closed it/i.test(text)));
            }""", arg=normalized, timeout=remaining_ms())
            text = page.locator("body").inner_text(timeout=remaining_ms())
            final_url = page.url
        finally:
            browser.close()

    status = _extract_lookup_status(text)
    found = _lookup_matches(text, normalized) and status is not None
    return PortalStatusLookup(
        service_request_number=normalized,
        found=found,
        status=status,
        page_text=text,
        final_url=final_url,
    )
