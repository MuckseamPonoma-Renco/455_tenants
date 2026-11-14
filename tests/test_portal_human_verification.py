import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from playwright.sync_api import sync_playwright

from packages.nyc311 import portal


@pytest.fixture(scope="module")
def local_browser():
    with sync_playwright() as p:
        if not Path(p.chromium.executable_path).exists():
            pytest.skip("Chromium required for local challenge-DOM verification")
        browser = p.chromium.launch(headless=True)
        yield browser
        browser.close()


@pytest.mark.parametrize("style,token,size,expected", [
    ("width:300px;height:100px", "", "normal", True),
    ("width:300px;height:100px", "fixture-token", "normal", False),
    ("display:none", "", "normal", False),
    ("width:300px;height:100px", "", "invisible", False),
])
def test_detects_unsolved_visible_challenge_without_contacting_provider(local_browser, style, token, size, expected):
    page = local_browser.new_page()
    page.route("**/*", lambda route: route.abort())
    page.set_content(f'<iframe title="reCAPTCHA" src="https://captcha.invalid/anchor?size={size}" style="{style}"></iframe>'
                     f'<textarea name="g-recaptcha-response">{token}</textarea>')
    assert portal._human_verification_required(page) is expected
    page.close()


@pytest.mark.parametrize("url,logout_count,expected", [
    ("https://portal.311.nyc.gov/SignIn", 0, False),
    ("https://portal.311.nyc.gov/profile", 1, True),
    ("https://unrelated.invalid/profile", 1, False),
])
def test_authentication_requires_portal_signout_control(url, logout_count, expected):
    page = SimpleNamespace(url=url, locator=lambda _selector: SimpleNamespace(count=lambda: logout_count))
    assert portal._is_portal_authenticated(page) is expected


def test_sso_wait_allows_delayed_redirect_without_touching_login_controls(monkeypatch):
    checks = iter([False, False, True])
    waits = []
    page = SimpleNamespace(locator=lambda _: SimpleNamespace(count=lambda: 0), wait_for_timeout=waits.append)
    monkeypatch.setattr(portal, '_is_portal_authenticated', lambda _: next(checks))
    monkeypatch.setattr(portal, '_human_verification_required', lambda _: False)
    assert portal._wait_for_sign_in_redirect(page, timeout_seconds=3)
    assert waits == [1000, 1000]


@pytest.mark.parametrize('form,challenge,expected', [(True, False, 'form'), (False, True, 'challenge'), (False, False, 'timeout')])
def test_sso_wait_stops_at_authentication_boundary(monkeypatch, form, challenge, expected):
    page = SimpleNamespace(locator=lambda _: SimpleNamespace(count=lambda: int(form)), wait_for_timeout=lambda _: None)
    monkeypatch.setattr(portal, '_is_portal_authenticated', lambda _: False)
    monkeypatch.setattr(portal, '_human_verification_required', lambda _: challenge)
    if expected == 'form':
        assert portal._wait_for_sign_in_redirect(page, timeout_seconds=0) is False
    else:
        with pytest.raises(portal.PortalHumanVerificationRequired if challenge else TimeoutError):
            portal._wait_for_sign_in_redirect(page, timeout_seconds=0)


def test_changed_passwordless_login_does_not_silently_fall_back_to_anonymous(monkeypatch, tmp_path):
    monkeypatch.setenv("NYC311_EMAIL", "fixture@example.test")
    monkeypatch.setenv("NYC311_PASSWORD", "fixture-password")
    monkeypatch.setenv("NYC311_SESSION_STATE_PATH", str(tmp_path / "session.json"))
    page = SimpleNamespace(url="https://www1.nyc.gov/account/oplogin.htm",
        goto=lambda *a, **k: None, wait_for_timeout=lambda _ms: None,
        locator=lambda selector: SimpleNamespace(count=lambda: int(selector == "#gigya-passwordless-login-form:visible")))
    monkeypatch.setattr(portal, '_wait_for_sign_in_redirect', lambda _: False)
    with pytest.raises(portal.PortalHumanVerificationRequired) as exc:
        portal._maybe_sign_in(page)
    assert exc.value.reason == "sign_in"


def test_session_is_saved_only_after_verified_login_and_remains_private(monkeypatch, tmp_path):
    target = tmp_path / "private" / "session.json"
    monkeypatch.setenv("NYC311_SESSION_STATE_PATH", str(target))
    context = SimpleNamespace(storage_state=lambda: {"cookies": [], "origins": []})
    monkeypatch.setattr(portal, "_is_portal_authenticated", lambda page: False)
    with pytest.raises(portal.PortalHumanVerificationRequired):
        portal._save_verified_session(context, object())
    assert not target.exists()
    monkeypatch.setattr(portal, "_is_portal_authenticated", lambda page: True)
    portal._save_verified_session(context, object())
    assert target.stat().st_mode & 0o777 == 0o600
    assert target.parent.stat().st_mode & 0o777 == 0o700
    assert portal._session_context_options() == {"storage_state": {"cookies": [], "origins": []}}


def test_corrupt_saved_session_requires_signin_instead_of_anonymous_fallback(monkeypatch, tmp_path):
    target = tmp_path / "session.json"
    target.write_text("invalid")
    monkeypatch.setenv("NYC311_SESSION_STATE_PATH", str(target))
    with pytest.raises(portal.PortalHumanVerificationRequired) as exc:
        portal._session_context_options()
    assert exc.value.reason == "sign_in"


def test_visible_challenge_stops_before_final_click_fence(monkeypatch, tmp_path):
    calls = []
    class Page:
        url = "https://portal.311.nyc.gov/review"
        def locator(self, selector): return self
        def fill(self, *a, **k): pass
        def click(self, *a, **k): calls.append("next")
        def wait_for_timeout(self, *a): pass
        def inner_text(self): return "Review 455 OCEAN PARKWAY"
        def evaluate(self, script): return True
        def screenshot(self, *, path, full_page): Path(path).write_bytes(b"fixture")
    page = Page()
    browser = SimpleNamespace(new_context=lambda **k: SimpleNamespace(new_page=lambda: page), close=lambda: None)
    @contextmanager
    def manager():
        yield SimpleNamespace(chromium=SimpleNamespace(launch=lambda **k: browser))
    monkeypatch.setattr(portal, "sync_playwright", manager)
    monkeypatch.setattr(portal, "_maybe_sign_in", lambda page: False)
    monkeypatch.setattr(portal, "_open_elevator_flow", lambda page: None)
    monkeypatch.setattr(portal, "_set_value", lambda *a, **k: None)
    monkeypatch.setattr(portal, "_wait_for_url_change", lambda *a: None)
    monkeypatch.setattr(portal, "_select_visible_where_address", lambda *a: portal.PortalAddressMatch("fixture", "455 OCEAN PARKWAY", {}))
    with pytest.raises(portal.PortalHumanVerificationRequired):
        portal.submit_elevator_complaint({"description": "Elevator not working.", "building": {"street_address": "455 OCEAN PARKWAY"}},
            submit_live=True, screenshot_dir=tmp_path, before_submit=lambda: pytest.fail("Final click must not be armed"))
    assert calls == ["next", "next", "next"]
