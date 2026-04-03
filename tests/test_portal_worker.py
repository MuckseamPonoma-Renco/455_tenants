from datetime import datetime, timedelta, timezone

from packages.db import FilingJob, ServiceRequestCase, get_session
from packages.nyc311.planner import claim_next_job
from packages.nyc311.portal import (
    NY,
    PortalSubmissionResult,
    _extract_confirmation_sr_number,
    _extract_lookup_status,
    _observed_at_text,
    _pick_best_address_match,
    _review_has_address,
    _address_queries,
    _wait_for_url_change,
    submit_elevator_complaint,
)
from packages.nyc311.portal_worker import run_portal_filing_once


def auth_headers():
    return {'Authorization': 'Bearer test-token'}


def test_pick_best_address_match_prefers_exact_zip():
    records = [
        {
            'Id': 'older',
            'Attributes': [
                {'Name': 'n311_addressid', 'Value': 'older'},
                {'Name': 'n311_fulladdress', 'DisplayValue': '455 OCEAN PARKWAY BROOKLYN'},
            ],
        },
        {
            'Id': 'exact',
            'Attributes': [
                {'Name': 'n311_addressid', 'Value': 'exact'},
                {'Name': 'n311_fulladdress', 'DisplayValue': '455 OCEAN PARKWAY, BROOKLYN, NY, 11218'},
            ],
        },
    ]

    match = _pick_best_address_match(records, '455 OCEAN PARKWAY', preferred_zip='11218')
    assert match.address_id == 'exact'
    assert match.full_address.endswith('11218')


def test_address_queries_keep_full_address_and_street_lookup():
    queries, preferred_zip = _address_queries({
        'building': {
            'full_address': '455 OCEAN PARKWAY, BROOKLYN, NY, 11218',
        },
    })

    assert queries[0] == '455 OCEAN PARKWAY, BROOKLYN, NY, 11218'
    assert '455 OCEAN PARKWAY BROOKLYN 11218' in queries
    assert '455 OCEAN PARKWAY' in queries
    assert preferred_zip == '11218'


def test_review_has_address_checks_street_line():
    payload = {
        'building': {
            'full_address': '455 OCEAN PARKWAY, BROOKLYN, NY, 11218',
        },
    }

    assert _review_has_address('Where\nAddress\n455 OCEAN PARKWAY, BROOKLYN', payload) is True
    assert _review_has_address('Where\nAddress\n', payload) is False


def test_run_portal_filing_once_marks_job_submitted(client, monkeypatch):
    client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out again and people are stuck walking home.',
        'sender': 'Karen',
        'ts_epoch': 1770000500,
    })

    monkeypatch.setattr(
        'packages.nyc311.portal_worker.submit_elevator_complaint',
        lambda payload, **kwargs: PortalSubmissionResult(
            service_request_number='311-77778888',
            confirmation_text='Confirmation 311-77778888',
            final_url='https://portal.311.nyc.gov/confirmation',
            address_id='addr-123',
            address_text='455 OCEAN PARKWAY, BROOKLYN, NY, 11218',
            login_used=False,
            review_screenshot_path='/tmp/review.png',
            confirmation_screenshot_path='/tmp/confirmation.png',
        ),
    )
    monkeypatch.setattr(
        'packages.nyc311.portal_worker.lookup_service_request_status',
        lambda sr_number, **kwargs: {'service_request_number': sr_number, 'found': True, 'status': 'Submitted'},
    )

    result = run_portal_filing_once(headless=True, verify_lookup=True)

    assert result['ok'] is True
    assert result['service_request_number'] == '311-77778888'
    with get_session() as session:
        job = session.query(FilingJob).filter_by(job_id=result['job_id']).one()
        case = session.query(ServiceRequestCase).filter_by(service_request_number='311-77778888').one()
        assert job.state == 'submitted'
        assert job.filing_channel == 'portal_playwright'
        assert case.source == 'portal_playwright'


def test_run_portal_filing_once_returns_none_when_queue_empty():
    result = run_portal_filing_once(headless=True, verify_lookup=False)
    assert result == {'ok': True, 'job': None}


def test_run_portal_filing_once_skips_ineligible_jobs(client, monkeypatch):
    client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out again and people are stuck walking home.',
        'sender': 'Karen',
        'ts_epoch': 1770000500,
    })

    monkeypatch.setattr('packages.nyc311.planner.incident_is_auto_eligible', lambda inc: False)

    result = run_portal_filing_once(headless=True, verify_lookup=False)

    assert result == {'ok': True, 'job': None}
    with get_session() as session:
        job = session.query(FilingJob).one()
        assert job.state == 'skipped'
        assert 'no longer auto-eligible' in (job.notes or '')


def test_observed_at_text_clamps_future_timestamp():
    future = datetime.now(timezone.utc) + timedelta(days=1)
    observed = _observed_at_text({'incident': {'last_ts': future.isoformat()}})
    parsed = datetime.strptime(observed, '%m/%d/%Y %I:%M %p').replace(tzinfo=NY)
    assert parsed <= datetime.now(NY)


def test_observed_at_text_clamps_old_timestamp(monkeypatch):
    monkeypatch.setenv('PORTAL_OBSERVED_MAX_AGE_HOURS', '24')
    old = datetime.now(timezone.utc) - timedelta(days=3)
    observed = _observed_at_text({'incident': {'last_ts': old.isoformat()}})
    parsed = datetime.strptime(observed, '%m/%d/%Y %I:%M %p').replace(tzinfo=NY)
    assert parsed >= datetime.now(NY) - timedelta(hours=2)


def test_claim_next_job_requeues_stale_claims(client, monkeypatch):
    client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out again and people are stuck walking home.',
        'sender': 'Karen',
        'ts_epoch': 1770000500,
    })

    monkeypatch.setenv('CLAIM_STALE_MINUTES', '10')
    old_claimed_at = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()

    with get_session() as session:
        job = session.query(FilingJob).one()
        job.state = 'claimed'
        job.claimed_at = old_claimed_at
        job.updated_at = old_claimed_at
        session.commit()

    with get_session() as session:
        job, skipped = claim_next_job(session)
        assert skipped == 0
        assert job is not None
        assert job.state == 'claimed'
        assert job.claimed_at is not None
        assert job.claimed_at != old_claimed_at
        assert 'auto-requeued because a claimed job went stale' in (job.notes or '')


def test_wait_for_url_change_passes_previous_url_by_keyword():
    recorded = {}

    class FakePage:
        def wait_for_function(self, expression, *, arg=None, timeout=None):
            recorded['expression'] = expression
            recorded['arg'] = arg
            recorded['timeout'] = timeout

    _wait_for_url_change(FakePage(), 'https://portal.311.nyc.gov/article', timeout_ms=1234)

    assert recorded == {
        'expression': 'prevUrl => window.location.href !== prevUrl',
        'arg': 'https://portal.311.nyc.gov/article',
        'timeout': 1234,
    }


def test_extract_confirmation_sr_number_falls_back_to_details_link():
    class FakeLink:
        def get_attribute(self, name):
            assert name == 'href'
            return 'https://portal.311.nyc.gov/sr-details/?srnum=311-27014524'

    class FakeLocator:
        @property
        def first(self):
            return FakeLink()

    class FakePage:
        def locator(self, selector):
            assert selector == 'a[href*="sr-details/?srnum="]'
            return FakeLocator()

    assert _extract_confirmation_sr_number(FakePage(), 'submitted successfully') == '311-27014524'


def test_extract_lookup_status_prefers_sr_status_label():
    text = 'Service Request Status\nSign In | Sign Up\nSR Status\nIn Progress\nProblem\nElevator'
    assert _extract_lookup_status(text) == 'In Progress'


def test_submit_elevator_complaint_passes_viewport_to_new_context(monkeypatch, tmp_path):
    calls = []

    class FakePage:
        def __init__(self):
            self.url = 'https://portal.311.nyc.gov/review'

        def locator(self, selector):
            return self

        def inner_text(self):
            return 'Review screen'

        def screenshot(self, path, full_page):
            calls.append(('screenshot', path, full_page))

        def fill(self, value, force=False):
            calls.append(('fill', value, force))

        def click(self, force=False, no_wait_after=False):
            calls.append(('click', force, no_wait_after))

        def wait_for_timeout(self, timeout_ms):
            calls.append(('wait_for_timeout', timeout_ms))

    class FakeContext:
        def __init__(self):
            self.page = FakePage()

        def new_page(self, **kwargs):
            calls.append(('new_page', kwargs))
            return self.page

    class FakeBrowser:
        def new_context(self, **kwargs):
            calls.append(('new_context', kwargs))
            return FakeContext()

        def close(self):
            calls.append(('browser_close',))

    class FakeChromium:
        def launch(self, **kwargs):
            calls.append(('launch', kwargs))
            return FakeBrowser()

    class FakePlaywright:
        chromium = FakeChromium()

    class FakeManager:
        def __enter__(self):
            return FakePlaywright()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr('packages.nyc311.portal.sync_playwright', lambda: FakeManager())
    monkeypatch.setattr('packages.nyc311.portal._maybe_sign_in', lambda page: False)
    monkeypatch.setattr('packages.nyc311.portal._open_elevator_flow', lambda page: None)
    monkeypatch.setattr('packages.nyc311.portal._set_value', lambda *args, **kwargs: None)
    monkeypatch.setattr('packages.nyc311.portal._resolve_address', lambda page, payload: type('M', (), {
        'address_id': 'addr-123',
        'full_address': '455 OCEAN PARKWAY, BROOKLYN, NY, 11218',
    })())
    monkeypatch.setattr('packages.nyc311.portal._apply_address', lambda page, match, anonymous: None)
    monkeypatch.setattr('packages.nyc311.portal._wait_for_url_change', lambda *args, **kwargs: None)

    result = submit_elevator_complaint(
        {
            'description': 'Elevator is down.',
            'building': {
                'street_address': '455 OCEAN PARKWAY',
                'city': 'BROOKLYN',
                'zip': '11218',
            },
            'incident': {'last_ts': datetime.now(timezone.utc).isoformat()},
        },
        headless=True,
        submit_live=False,
        screenshot_dir=tmp_path,
    )

    assert ('new_context', {'viewport': {'width': 1440, 'height': 2200}, 'timezone_id': 'America/New_York'}) in calls
    assert ('new_page', {}) in calls
    assert result.address_id == 'addr-123'
