import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from packages.nyc311 import portal, session_health as health
from packages.nyc311.portal_worker import release_verified_sign_in_holds
from packages.db import FilingJob, get_session


@pytest.fixture
def saved(monkeypatch, tmp_path):
    monkeypatch.setenv('NYC311_SESSION_STATE_PATH', str(tmp_path / 'private/session.json'))
    monkeypatch.setenv('NYC311_SESSION_HEALTH_PATH', str(tmp_path / 'health.json'))
    for name in ('NYC311_EMAIL', '311_EMAIL'):
        monkeypatch.delenv(name, raising=False)
    state = {'cookies': [{'name': 'session', 'value': 'private-fixture'}], 'origins': []}
    monkeypatch.setattr(portal, '_is_portal_authenticated', lambda _: True)
    portal._save_verified_session(SimpleNamespace(storage_state=lambda: state), object())
    return state


def fake_browser(monkeypatch, state, *, authenticated=True, status=200, error=None, on_visit=None):
    visits = []
    closed = []
    def goto(url, **_kwargs):
        visits.append(url)
        if on_visit:
            on_visit()
        if error:
            raise error
        return SimpleNamespace(status=status)
    page = SimpleNamespace(goto=goto)
    context = SimpleNamespace(new_page=lambda: page, storage_state=lambda: state)
    browser = SimpleNamespace(new_context=lambda **_: context, close=lambda: closed.append(True))
    @contextmanager
    def manager():
        yield SimpleNamespace(chromium=SimpleNamespace(launch=lambda **_: browser))
    monkeypatch.setattr(portal, 'sync_playwright', manager)
    monkeypatch.setattr(portal, '_is_portal_authenticated', lambda _: authenticated)
    monkeypatch.setattr(portal, '_wait_for_sign_in_redirect', lambda _: authenticated)
    return visits, closed


def test_probe_retains_real_renewal_and_only_visits_home(monkeypatch, saved):
    renewal = {'cookies': [{'name': 'session', 'value': 'renewed-fixture'}], 'origins': []}
    visits, closed = fake_browser(monkeypatch, renewal)
    status = health.check_saved_session()
    assert status['state'] == 'ready'
    assert visits == [portal.PORTAL_HOME_URL]
    assert closed == [True]
    assert json.loads(health.session_state_path().read_text()) == renewal
    assert health.session_state_path().stat().st_mode & 0o777 == 0o600
    assert health.status_path().stat().st_mode & 0o777 == 0o600
    assert not any(word in json.dumps(status) for word in ('cookie', 'fixture', 'fingerprint', '/private/'))


def test_logged_out_portal_recovers_through_saved_identity_without_credentials(monkeypatch, saved):
    renewal = {'cookies': [{'name': 'session', 'value': 'sso-renewal-fixture'}], 'origins': []}
    visits, closed = fake_browser(monkeypatch, renewal, authenticated=False)
    restored = False
    def redirect(_page):
        nonlocal restored
        restored = True
        return True
    monkeypatch.setattr(portal, '_wait_for_sign_in_redirect', redirect)
    monkeypatch.setattr(portal, '_is_portal_authenticated', lambda _: restored)
    assert health.check_saved_session()['state'] == 'ready'
    assert visits == [portal.PORTAL_HOME_URL, portal.PORTAL_SIGN_IN_URL]
    assert closed == [True]
    assert json.loads(health.session_state_path().read_text()) == renewal


@pytest.mark.parametrize('error,expected', [(TimeoutError(), 'unavailable'), (portal.PortalHumanVerificationRequired(reason='sign_in'), 'login_required')])
def test_redirect_failure_preserves_session_and_never_claims_ready(monkeypatch, saved, error, expected):
    fake_browser(monkeypatch, saved, authenticated=False)
    def failed(_page):
        raise error
    monkeypatch.setattr(portal, '_wait_for_sign_in_redirect', failed)
    assert health.check_saved_session()['state'] == expected
    assert json.loads(health.session_state_path().read_text()) == saved


@pytest.mark.parametrize('authenticated,status,error,expected', [
    (False, 200, None, 'login_required'),
    (True, 503, None, 'unavailable'),
    (True, 200, TimeoutError(), 'unavailable'),
])
def test_failed_probe_is_visible_and_preserves_last_good_session(monkeypatch, saved, authenticated, status, error, expected):
    last_success = health.public_status()['last_success_at']
    fake_browser(monkeypatch, saved, authenticated=authenticated, status=status, error=error)
    result = health.check_saved_session()
    assert result['state'] == expected and result['has_error']
    assert result['last_success_at'] == last_success
    assert json.loads(health.session_state_path().read_text()) == saved


def test_stale_or_changed_session_cannot_show_green(saved):
    receipt = json.loads(health.status_path().read_text())
    receipt['last_checked_at'] = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    health.status_path().write_text(json.dumps(receipt))
    assert health.public_status()['state'] == 'stale'
    health.record('ready', fingerprint=health.current_fingerprint())
    health.session_state_path().write_text('{}')
    assert health.public_status()['state'] == 'unverified'
    health.session_state_path().unlink()
    assert health.public_status()['state'] == 'not_configured'


@pytest.mark.parametrize('authenticated', [True, False])
def test_slow_probe_cannot_overwrite_newer_interactive_login(monkeypatch, saved, authenticated):
    newer = {'cookies': [{'name': 'session', 'value': 'new-interactive-login'}], 'origins': []}
    def new_login():
        health.session_state_path().write_text(json.dumps(newer))
        health.record('ready', fingerprint=health.state_fingerprint(newer))
    fake_browser(monkeypatch, saved, authenticated=authenticated, on_visit=new_login)
    assert health.check_saved_session()['state'] == 'ready'
    assert json.loads(health.session_state_path().read_text()) == newer


def test_missing_or_corrupt_saved_login_requires_signin(monkeypatch, saved):
    monkeypatch.setenv('NYC311_EMAIL', 'fixture@example.test')
    for content in ('bad json', None):
        if content is None:
            health.session_state_path().unlink()
        else:
            health.session_state_path().write_text(content)
        assert health.check_saved_session()['state'] == 'login_required'


def test_corrupt_session_stops_filing_before_browser_opens(monkeypatch, saved):
    health.session_state_path().write_text('invalid')
    monkeypatch.setattr(portal, 'sync_playwright', lambda: pytest.fail('No browser should open'))
    with pytest.raises(portal.PortalHumanVerificationRequired):
        portal.submit_elevator_complaint({'description': 'Fixture only'}, submit_live=True)
    assert health.public_status()['state'] == 'login_required'


def test_check_session_command_reports_failure_without_running_a_filing(monkeypatch):
    from scripts import run_311_portal_worker as cli
    monkeypatch.setattr(cli.sys, 'argv', ['worker', '--check-session'])
    monkeypatch.setattr(cli, 'load_local_env_file', lambda: None)
    monkeypatch.setattr(cli, 'check_saved_session', lambda: {'state': 'login_required', 'has_error': True})
    monkeypatch.setattr(cli, 'run_portal_filing_once', lambda **_: pytest.fail('No filing allowed'))
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 1


@pytest.mark.parametrize('age,state,expected', [(0, 'ready', 1), (90, 'ready', 0), (0, 'login_required', 0)])
def test_only_verified_presubmit_signin_holds_resume(client, monkeypatch, age, state, expected):
    status = {'state': state, 'last_checked_at': (datetime.now(timezone.utc) - timedelta(seconds=age)).isoformat()}
    monkeypatch.setattr(health, 'public_status', lambda: status)
    monkeypatch.setattr('packages.nyc311.portal_worker._safe_sync_sheets', lambda: None)
    specs = [
        ('awaiting_human_verification', 'NYC311 sign-in requires user verification', '', None),
        ('awaiting_human_verification', 'NYC311 CAPTCHA requires user verification', '', None),
        ('submission_unknown', 'NYC311 sign-in requires user verification', '', None),
        ('submitted', 'NYC311 sign-in requires user verification', '', None),
        ('awaiting_human_verification', 'NYC311 sign-in requires user verification', 'final submit armed', None),
        ('awaiting_human_verification', 'NYC311 sign-in requires user verification', '', '2026-11-02T10:00:00Z'),
    ]
    with get_session() as session:
        for i, (job_state, error, notes, completed) in enumerate(specs):
            session.add(FilingJob(dedupe_key=f'fixture:{i}', incident_id=f'fixture:{i}',
                state=job_state, last_error=error, notes=notes, completed_at=completed,
                attempts=2, complaint_type='fixture', form_target='fixture', payload_json='{}'))
        session.commit()
    assert release_verified_sign_in_holds() == expected
    with get_session() as session:
        jobs = session.query(FilingJob).order_by(FilingJob.job_id).all()
        assert jobs[0].state == ('pending' if expected else specs[0][0])
        assert all(job.state == spec[0] for job, spec in zip(jobs[1:], specs[1:]))
        assert all(job.attempts == 2 for job in jobs)
