from packages.db import ServiceRequestCase, get_session


def auth_headers():
    return {'Authorization': 'Bearer test-token'}


def mobile_headers():
    return {'Authorization': 'Bearer mobile-token'}


def test_summary_endpoint_shows_android_filer_stage(client):
    client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out again and I had to walk up to the 16th floor!',
        'sender': 'Tibor Simon',
        'ts_epoch': 1770000000,
    })

    response = client.get('/api/summary', headers=auth_headers())
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['stage'] == 'ready_for_android_filer'
    assert payload['metrics']['filing_jobs_pending'] >= 1
    assert any(action['title'] == 'Submit the first real complaint' for action in payload['actions'])


def test_briefing_endpoint_returns_fallback_without_llm(client):
    client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North lift dead',
        'sender': 'Karen',
        'ts_epoch': 1770000100,
    })
    response = client.get('/api/briefing', headers=auth_headers())
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['briefing']['used_llm'] is False
    assert payload['summary']['metrics']['incidents_open'] >= 1
    assert 'Next best action' in payload['briefing']['tenant_update_draft']


def test_summary_stage_moves_to_tracking_live_after_submission(client):
    client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North lift dead',
        'sender': 'Karen',
        'ts_epoch': 1770000100,
    })
    claim = client.post('/mobile/filings/claim_next', headers=mobile_headers()).json()['job']
    client.post(
        f"/mobile/filings/{claim['job_id']}/submitted",
        headers=mobile_headers(),
        json={'service_request_number': '311-11112222'},
    )

    response = client.get('/api/summary', headers=auth_headers())
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['stage'] == 'tracking_live'
    assert payload['metrics']['service_requests_total'] >= 1
    with get_session() as session:
        assert session.query(ServiceRequestCase).filter_by(service_request_number='311-11112222').count() == 1
