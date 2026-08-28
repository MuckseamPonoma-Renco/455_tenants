import json
from datetime import datetime, timedelta, timezone

from packages.db import FilingJob, Incident, ServiceRequestCase, get_session
from packages.nyc311.planner import (
    claim_next_job,
    claimed_filing_job_is_current,
    ensure_filing_job_for_incident,
)
from packages.nyc311.drafts import build_filing_draft
from packages.nyc311.portal import (
    NY,
    PortalSubmissionCancelled,
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


def _elevator_incident(incident_id, *, timestamp):
    return Incident(
        incident_id=incident_id,
        category="elevator",
        asset="elevator_south",
        severity=4,
        status="open",
        start_ts=datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
        start_ts_epoch=timestamp,
        last_ts_epoch=timestamp,
        title="South elevator outage",
        summary="South elevator not working.",
        proof_refs="",
        report_count=1,
        witness_count=1,
        confidence=80,
        needs_review=False,
        updated_at=datetime.now(timezone.utc).isoformat(),
    )


def test_equivalent_incident_alias_is_skipped_before_a_second_311_job(client):
    now_epoch = int(datetime.now(timezone.utc).timestamp()) - 24 * 60 * 60
    with get_session() as session:
        first_incident = _elevator_incident("south-live", timestamp=now_epoch)
        session.add(first_incident)
        session.flush()
        first_job = ensure_filing_job_for_incident(session, first_incident)
        assert first_job is not None
        assert first_job.state == "pending"
        session.flush()
        first_job.state = "submitted"
        first_job.completed_at = datetime.fromtimestamp(now_epoch, timezone.utc).isoformat()
        first_job.updated_at = first_job.completed_at

        alias_incident = _elevator_incident("south-export-alias", timestamp=now_epoch)
        session.add(alias_incident)
        skipped_job = ensure_filing_job_for_incident(session, alias_incident)
        assert skipped_job is not None
        assert skipped_job.state == "skipped"
        assert f"{first_job.job_id}" in (skipped_job.notes or "")
        session.commit()


def test_claim_next_job_skips_pending_alias_after_equivalent_submission(client, monkeypatch):
    now = datetime.now(timezone.utc).isoformat()
    timestamp = int(datetime.now(timezone.utc).timestamp()) - 60
    monkeypatch.setattr("packages.nyc311.planner.incident_is_auto_eligible", lambda _incident: True)
    with get_session() as session:
        submitted_incident = _elevator_incident("submitted-incident", timestamp=timestamp)
        pending_incident = _elevator_incident("pending-alias", timestamp=timestamp)
        session.add_all([submitted_incident, pending_incident])
        session.flush()
        submitted_draft = build_filing_draft(submitted_incident)
        pending_draft = build_filing_draft(pending_incident)
        assert submitted_draft is not None
        assert pending_draft is not None
        session.add_all([
            FilingJob(
                dedupe_key="311:submitted-incident",
                incident_id="submitted-incident",
                state="submitted",
                complaint_type="Elevator or Escalator Complaint",
                form_target="elevator_not_working",
                payload_json=submitted_draft.payload_json(),
                created_at=now,
                updated_at=now,
                completed_at=now,
            ),
            FilingJob(
                dedupe_key="311:pending-alias",
                incident_id="pending-alias",
                state="pending",
                complaint_type="Elevator or Escalator Complaint",
                form_target="elevator_not_working",
                payload_json=pending_draft.payload_json(),
                created_at=now,
                updated_at=now,
            ),
        ])
        session.commit()

    with get_session() as session:
        job, skipped = claim_next_job(session)
        assert job is None
        assert skipped == 1
        alias_job = session.query(FilingJob).filter_by(dedupe_key="311:pending-alias").one()
        assert alias_job.state == "skipped"
        assert "submitted-incident" in (alias_job.notes or "")


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
        lambda sr_number, **kwargs: {
            'service_request_number': sr_number,
            'found': True,
            'status': 'In Progress',
            'page_text': (
                'Your Service Request has been submitted to the Department of Buildings.\n'
                'SR Number\n311-77778888\n'
                'Updated On\n04/19/2026, 09:49 PM\n'
                'Date Reported\n04/19/2026, 09:37 PM\n'
                'Date Closed\n-\n'
                'SR Status\nIn Progress\n'
                'Problem\nElevator\n'
                'Problem Details\\nNot Working'
            ),
            'final_url': 'https://portal.311.nyc.gov/check-status/',
        },
    )

    result = run_portal_filing_once(headless=True, verify_lookup=True)

    assert result['ok'] is True
    assert result['job']['job_id'] == result['job_id']
    assert result['service_request_number'] == '311-77778888'
    with get_session() as session:
        job = session.query(FilingJob).filter_by(job_id=result['job_id']).one()
        case = session.query(ServiceRequestCase).filter_by(service_request_number='311-77778888').one()
        assert job.state == 'submitted'
        assert job.filing_channel == 'portal_playwright'
        assert case.source == 'portal_playwright'
        assert case.status == 'In Progress'
        assert case.agency == 'DOB'
        assert case.complaint_type == 'Elevator or Escalator Complaint'
        assert case.submitted_at == '2026-04-20T01:37:00Z'
        assert case.last_checked_at is not None
        assert '"source": "nyc311_portal"' in (case.raw_status_json or '')


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


def test_portal_worker_cancels_at_review_when_incident_state_changes(client, monkeypatch):
    client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out again and people are stuck walking home.',
        'sender': 'Karen',
        'ts_epoch': 1770000500,
    })
    def cancel_at_review(*_args, **_kwargs):
        raise PortalSubmissionCancelled('incident closed before final submit')

    monkeypatch.setattr('packages.nyc311.portal_worker.submit_elevator_complaint', cancel_at_review)
    result = run_portal_filing_once(headless=True, verify_lookup=False)

    assert result['ok'] is True
    assert result['skipped'] is True
    with get_session() as session:
        job = session.query(FilingJob).one()
        assert job.state == 'skipped'
        assert 'cancelled at portal review' in (job.notes or '')


def test_observed_at_text_clamps_future_timestamp():
    future = datetime.now(timezone.utc) + timedelta(days=1)
    observed = _observed_at_text({'incident': {'last_ts': future.isoformat()}})
    parsed = datetime.strptime(observed, '%m/%d/%Y %I:%M %p').replace(tzinfo=NY)
    assert parsed <= datetime.now(NY)


def test_observed_at_text_preserves_old_timestamp():
    old = datetime.now(timezone.utc) - timedelta(days=3)
    observed = _observed_at_text({'incident': {'start_ts': old.isoformat()}})
    parsed = datetime.strptime(observed, '%m/%d/%Y %I:%M %p').replace(tzinfo=NY)
    assert abs((parsed - old.astimezone(NY)).total_seconds()) < 60


def test_observed_at_text_prefers_first_report_over_latest_update():
    first = datetime.now(timezone.utc) - timedelta(days=2)
    latest = datetime.now(timezone.utc) - timedelta(hours=1)
    observed = _observed_at_text({
        'incident': {'start_ts': first.isoformat(), 'last_ts': latest.isoformat()},
    })
    parsed = datetime.strptime(observed, '%m/%d/%Y %I:%M %p').replace(tzinfo=NY)
    assert abs((parsed - first.astimezone(NY)).total_seconds()) < 60


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
        job, skipped = claim_next_job(session)
        assert skipped == 0
        assert job is not None
        assert job.state == 'claimed'
        job.claimed_at = old_claimed_at
        job.updated_at = old_claimed_at
        session.commit()

    with get_session() as session:
        job, skipped = claim_next_job(session)
        assert skipped == 0
        assert job is not None
        reset = session.query(FilingJob).one()
        assert reset.state == 'claimed'
        assert reset.claimed_at is not None
        assert reset.attempts == 2
        assert 'auto-requeued because a claimed job went stale' in (reset.notes or '')


def test_claim_next_job_retires_closed_pending_jobs(client):
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    with get_session() as session:
        incident = _elevator_incident("closed-before-approval", timestamp=now_epoch)
        incident.status = "closed"
        incident.end_ts = datetime.fromtimestamp(now_epoch, timezone.utc).isoformat()
        incident.end_ts_epoch = now_epoch
        session.add(incident)
        session.flush()
        session.add(
            FilingJob(
                dedupe_key="311:closed-before-approval",
                incident_id=incident.incident_id,
                    state="pending",
                complaint_type="Elevator or Escalator Complaint",
                form_target="elevator_not_working",
                payload_json=json.dumps({"description": "South elevator not working."}),
                created_at=datetime.now(timezone.utc).isoformat(),
                updated_at=datetime.now(timezone.utc).isoformat(),
            )
        )
        session.commit()

    with get_session() as session:
        job, skipped = claim_next_job(session)
        assert job is None
        assert skipped == 1
        retired = session.query(FilingJob).one()
        assert retired.state == "skipped"
        assert "no longer auto-eligible" in (retired.notes or "")


def test_claim_next_job_binds_payload_and_rejects_changed_incident(client):
    client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'South elevator is dead.',
        'sender': 'Karen',
        'ts_epoch': int(datetime.now(timezone.utc).timestamp()) - 60,
    })

    with get_session() as session:
        job, skipped = claim_next_job(session)
        assert skipped == 0
        assert job is not None
        assert job.state == 'claimed'
        assert 'claimed_payload_sha256=' in (job.notes or '')
        assert claimed_filing_job_is_current(session, job) is True

        incident = session.get(Incident, job.incident_id)
        incident.report_count += 1
        assert claimed_filing_job_is_current(session, job) is False


def test_claim_next_job_stops_after_portal_attempt_limit(client, monkeypatch):
    client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'South elevator is dead.',
        'sender': 'Karen',
        'ts_epoch': int(datetime.now(timezone.utc).timestamp()) - 60,
    })
    monkeypatch.setenv('AUTO_FILE_MAX_PORTAL_ATTEMPTS', '3')

    with get_session() as session:
        job = session.query(FilingJob).one()
        job.state = 'failed'
        job.attempts = 3
        session.commit()

    with get_session() as session:
        job, skipped = claim_next_job(session)
        assert job is None
        assert skipped == 0
        failed = session.query(FilingJob).one()
        assert failed.state == 'failed'
        assert failed.attempts == 3


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


def test_extract_lookup_status_ignores_navigation_status_text():
    text = 'Service Request Status\nSign In | Sign Up\nSubscribe\nProblem\nElevator'
    assert _extract_lookup_status(text) is None


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
