from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page, sync_playwright

from packages.local_env import load_local_env_file
from packages.nyc311.address import canonicalize_building_address
from packages.nyc311.tracker import normalize_sr_number

PORTAL_BASE_URL = "https://portal.311.nyc.gov"
PORTAL_HOME_URL = f"{PORTAL_BASE_URL}/"
PORTAL_SIGN_IN_URL = f"{PORTAL_BASE_URL}/SignIn?returnUrl=%2F"
ELEVATOR_ARTICLE_URL = f"{PORTAL_BASE_URL}/article/?kanumber=KA-02015"
CHECK_STATUS_URL = f"{PORTAL_BASE_URL}/check-status/"
NY = ZoneInfo("America/New_York")
SR_RE = re.compile(r"\b311[-\s]?(\d{8,})\b")


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


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, default=str(default)))
    except Exception:
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
    max_age_hours = _env_int("PORTAL_OBSERVED_MAX_AGE_HOURS", 24)
    oldest_allowed = latest_allowed - timedelta(hours=max_age_hours) if max_age_hours > 0 else None
    incident = payload.get("incident") or {}
    for key in ("last_ts", "start_ts"):
        raw = incident.get(key)
        if not raw:
            continue
        try:
            parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=NY)
            observed = parsed.astimezone(NY)
            if observed > latest_allowed or (oldest_allowed is not None and observed < oldest_allowed):
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
    if not email or not password:
        return False

    try:
        page.goto(PORTAL_SIGN_IN_URL, wait_until="domcontentloaded", timeout=120_000)
        page.wait_for_timeout(5000)
        if page.url.startswith(PORTAL_BASE_URL):
            return True

        email_input = page.locator('#gigya-loginID, input[name="username"]').first
        password_input = page.locator('#gigya-password, input[name="password"]').first
        submit_button = page.locator('input[type="submit"][value="Login"]').first

        email_input.wait_for(state="visible", timeout=20_000)
        password_input.wait_for(state="visible", timeout=20_000)
        email_input.fill(email)
        password_input.fill(password)
        submit_button.click(force=True, no_wait_after=True)
        page.wait_for_function(
            "base => window.location.href.startsWith(base)",
            arg=PORTAL_BASE_URL,
            timeout=45_000,
        )
        page.wait_for_timeout(2000)
        return page.url.startswith(PORTAL_BASE_URL)
    except Exception:
        return False


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
    for pattern in (r"\bSR Status\s+([^\n]+)", r"\bStatus\s+([^\n]+)"):
        match = re.search(pattern, page_text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None


def submit_elevator_complaint(
    payload: dict[str, Any],
    *,
    headless: bool = True,
    submit_live: bool = True,
    screenshot_dir: str | Path | None = None,
) -> PortalSubmissionResult:
    load_local_env_file()
    screenshot_path = Path(screenshot_dir) if screenshot_dir else Path(".local/nyc311_portal")
    description = _trim_description(str(payload.get("description") or payload.get("payload", {}).get("description") or ""))
    if not description:
        raise RuntimeError("Missing complaint description")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        context = browser.new_context(viewport={"width": 1440, "height": 2200}, timezone_id="America/New_York")
        page = context.new_page()
        login_used = _maybe_sign_in(page)

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

        previous_url = page.url
        page.locator("#NextButton").click(force=True, no_wait_after=True)
        try:
            _wait_for_submit_confirmation(page, previous_url)
        except PlaywrightError:
            page.wait_for_timeout(6000)
        page.wait_for_timeout(2500)
        confirmation_text = page.locator("body").inner_text()
        service_request_number = _extract_confirmation_sr_number(page, confirmation_text)
        confirmation_screenshot_path = _save_screenshot(page, screenshot_path, "confirmation")
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


def lookup_service_request_status(sr_number: str, *, headless: bool = True) -> PortalStatusLookup:
    load_local_env_file()
    normalized = normalize_sr_number(sr_number)
    if not normalized:
        raise RuntimeError(f"Invalid service request number: {sr_number}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        context = browser.new_context(viewport={"width": 1440, "height": 1600}, timezone_id="America/New_York")
        page = context.new_page()
        page.goto(CHECK_STATUS_URL, wait_until="domcontentloaded", timeout=120_000)
        page.locator("#ReferenceNumberInput").fill(normalized, force=True)
        page.locator("#SubmitBtn").click(force=True, no_wait_after=True)
        page.wait_for_timeout(4000)
        text = page.locator("body").inner_text()
        browser.close()

    found = "Invalid Service Request number" not in text
    status = _extract_lookup_status(text)
    return PortalStatusLookup(
        service_request_number=normalized,
        found=found,
        status=status,
        page_text=text,
        final_url=CHECK_STATUS_URL,
    )
