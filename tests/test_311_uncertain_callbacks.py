from datetime import datetime, timedelta, timezone

import pytest

from packages.db import FilingJob, ServiceRequestCase, get_session
from packages.sheets import sync as sheet_sync


MOBILE_HEADERS = {'Authorization': 'Bearer mobile-token'}
API_HEADERS = {'Authorization': 'Bearer test-token'}


def _job(state='submission_unknown', *, updated_at=None, dedupe_key='uncertain-callback'):
    with get_session() as session:
        job = FilingJob(
            dedupe_key=dedupe_key, state=state, payload_json='{}',
            updated_at=updated_at or datetime.now(timezone.utc).isoformat(),
            last_error='Final click result was not verified',
        )
        session.add(job)
        session.commit()
        return job.job_id


@pytest.mark.parametrize('state', ['submitting', 'submission_unknown', 'submitted', 'skipped'])
def test_failed_callback_cannot_make_protected_state_retryable(client, state):
    job_id = _job(state)
    response = client.post(f'/mobile/filings/{job_id}/failed', headers=MOBILE_HEADERS, json={'error': 'legacy worker error'})
    assert response.status_code == 409
    with get_session() as session:
        assert session.get(FilingJob, job_id).state == state


def test_unknown_receipt_reconciliation_requires_auth_and_exact_number(client, monkeypatch):
    monkeypatch.setattr('apps.api.routers.mobile._schedule_sheet_refresh', lambda: None)
    job_id = _job()
    path = f'/mobile/filings/{job_id}/submitted'
    assert client.post(path, json={'service_request_number': '311-12345678'}).status_code == 401
    assert client.post(path, headers=MOBILE_HEADERS, json={'service_request_number': 'receipt missing'}).status_code == 400
    assert client.post(path, headers=MOBILE_HEADERS, json={'service_request_number': '311-12345678 or 311-87654321'}).status_code == 400
    response = client.post(path, headers=MOBILE_HEADERS, json={'service_request_number': '311-12345678'})
    assert response.status_code == 200
    with get_session() as session:
        job = session.get(FilingJob, job_id)
        assert job.state == 'submitted'
        assert job.last_error is None
        assert session.query(ServiceRequestCase).one().filing_job_id == job_id


def test_receipt_reconciliation_does_not_rebind_another_job_receipt(client, monkeypatch):
    monkeypatch.setattr('apps.api.routers.mobile._schedule_sheet_refresh', lambda: None)
    first = _job(dedupe_key='first-unknown')
    second = _job(dedupe_key='second-unknown')
    payload = {'service_request_number': '311-12345678'}
    assert client.post(f'/mobile/filings/{first}/submitted', headers=MOBILE_HEADERS, json=payload).status_code == 200
    assert client.post(f'/mobile/filings/{second}/submitted', headers=MOBILE_HEADERS, json=payload).status_code == 409
    assert client.post(f'/mobile/filings/{first}/submitted', headers=MOBILE_HEADERS, json={'service_request_number': '311-87654321'}).status_code == 409
    with get_session() as session:
        assert session.get(FilingJob, second).state == 'submission_unknown'
        assert session.query(ServiceRequestCase).count() == 1


@pytest.mark.parametrize('state,age_minutes', [('submission_unknown', 0), ('submitting', 60)])
def test_summary_requires_reconciliation_even_without_an_open_incident(client, state, age_minutes):
    _job(state, updated_at=(datetime.now(timezone.utc) - timedelta(minutes=age_minutes)).isoformat())
    response = client.get('/api/summary', headers=API_HEADERS)
    assert response.status_code == 200
    payload = response.json()
    assert payload['stage'] == 'filing_attention_needed'
    assert payload['metrics']['filing_jobs_receipt_reconciliation_needed'] == 1
    assert any(alert['code'] == '311_submission_unknown' for alert in payload['alerts'])
    assert 'before retrying' in payload['next_step']


def test_summary_marks_a_fresh_submission_as_in_progress(client):
    _job('submitting')
    payload = client.get('/api/summary', headers=API_HEADERS).json()
    assert payload['stage'] == 'filing_submission_in_progress'
    assert payload['metrics']['filing_jobs_receipt_reconciliation_needed'] == 0
    assert payload['metrics']['filing_jobs_submitting'] == 1


def test_operator_queue_sheet_keeps_uncertain_jobs_visible(client, monkeypatch):
    _job('submitting', dedupe_key='submitting-sheet')
    _job('submission_unknown', dedupe_key='unknown-sheet')
    captured = []
    monkeypatch.setattr(sheet_sync, '_service', lambda: object())
    monkeypatch.setattr(sheet_sync, '_sheet_id', lambda: 'test-sheet')
    monkeypatch.setattr(sheet_sync, '_replace_tab_values', lambda _svc, _sid, _tab, values: captured.extend(values))
    monkeypatch.setattr(sheet_sync, '_apply_tab_layout', lambda *_args, **_kwargs: None)
    sheet_sync.sync_311_queue_to_sheets()
    assert {row[2] for row in captured[1:]} == {'submitting', 'submission_unknown'}
