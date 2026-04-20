import json

from packages.db import FilingJob, Incident, ServiceRequestCase, get_session
from packages.timeutil import parse_ts_to_epoch


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


def test_api_surfaces_normalize_legacy_incident_timestamps(client):
    start_ts = '3/18/26 6:46:19 PM'
    updated_at = '3/18/26 7:01:00 PM'
    start_epoch = parse_ts_to_epoch(start_ts)
    with get_session() as session:
        session.add(Incident(
            incident_id='legacy-incident',
            category='elevator',
            asset='elevator_both',
            status='open',
            severity=4,
            start_ts=start_ts,
            start_ts_epoch=start_epoch,
            end_ts=None,
            end_ts_epoch=None,
            last_ts_epoch=start_epoch,
            title='Legacy incident',
            summary='Legacy timestamp data',
            proof_refs='legacy-proof',
            report_count=1,
            witness_count=1,
            confidence=80,
            needs_review=False,
            updated_at=updated_at,
        ))
        session.commit()

    incidents = client.get('/api/incidents', headers=auth_headers())
    assert incidents.status_code == 200, incidents.text
    incident_payload = incidents.json()['incidents'][0]
    assert incident_payload['start_ts'] == '2026-03-18T22:46:19Z'

    summary = client.get('/api/summary', headers=auth_headers())
    assert summary.status_code == 200, summary.text
    summary_incident = summary.json()['open_incidents'][0]
    assert summary_incident['start_ts'] == '2026-03-18T22:46:19Z'
    assert summary_incident['updated_at'] == '2026-03-18T23:01:00Z'


def test_summary_surfaces_replacement_bundle_action(client):
    with get_session() as session:
        for idx in range(3):
            session.add(Incident(
                incident_id=f'elevator-{idx}',
                category='elevator',
                asset='elevator_north',
                status='open',
                severity=4,
                start_ts='2026-04-18T00:00:00Z',
                start_ts_epoch=1776460800 + idx,
                end_ts=None,
                end_ts_epoch=None,
                last_ts_epoch=1776460800 + idx,
                title=f'Elevator outage {idx}',
                summary='Repeated elevator outage',
                proof_refs=f'proof-{idx}',
                report_count=1,
                witness_count=1,
                confidence=80,
                needs_review=False,
                updated_at='2026-04-18T00:10:00Z',
            ))
        session.commit()

    response = client.get('/api/summary', headers=auth_headers())
    assert response.status_code == 200, response.text
    payload = response.json()
    assert any(action['title'] == 'Export the elevator replacement bundle' for action in payload['actions'])


def test_queue_endpoint_normalizes_legacy_payload_timestamps(client):
    with get_session() as session:
        session.add(FilingJob(
            dedupe_key='legacy-job',
            incident_id='legacy-incident',
            state='pending',
            complaint_type='Elevator or Escalator Complaint',
            form_target='elevator_not_working',
            payload_json=json.dumps({
                'incident': {
                    'start_ts': '1770000300',
                    'end_ts': '3/17/26 8:01:51 AM',
                },
                'notes': {
                    'created_at': '3/18/26 7:01:00 PM',
                },
            }),
            created_at='2026-04-03T01:00:00Z',
            updated_at='2026-04-03T01:00:00Z',
        ))
        session.commit()

    response = client.get('/api/queue', headers=auth_headers())
    assert response.status_code == 200, response.text
    payload = response.json()['jobs'][0]['payload']
    assert payload['incident']['start_ts'] == '2026-02-02T02:45:00Z'
    assert payload['incident']['end_ts'] == '2026-03-17T12:01:51Z'
    assert payload['notes']['created_at'] == '2026-03-18T23:01:00Z'
