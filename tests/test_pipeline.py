from pathlib import Path
from packages.db import FilingJob, Incident, IncidentWitness, MessageDecision, ServiceRequestCase, get_session
from packages.nyc311.legal_export import export_legal_bundle


def auth_headers():
    return {'Authorization': 'Bearer test-token'}


def mobile_headers():
    return {'Authorization': 'Bearer mobile-token'}


def test_tasker_ingest_creates_incident_and_queue(client):
    response = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out again and I had to walk up to the 16th floor!',
        'sender': 'Tibor Simon',
        'ts_epoch': 1770000000,
    })
    assert response.status_code == 200, response.text
    with get_session() as session:
        incidents = session.query(Incident).all()
        jobs = session.query(FilingJob).all()
        assert len(incidents) == 1
        assert incidents[0].category == 'elevator'
        assert len(jobs) == 1
        assert jobs[0].state in {'pending', 'claimed', 'submitted', 'failed'}


def test_export_ingest_extracts_manual_sr_number(client, tmp_path):
    chat_text = '''[2/15/26, 8:56:59 AM] Karen KWA: North lift dead
[2/15/26, 4:15:18 PM] Karen KWA: Dead again.
[2/15/26, 5:15:18 PM] Diana: report number 311-25842195
[2/16/26, 9:25:27 AM] Karen KWA: 2 lifts working.
'''
    export_path = tmp_path / 'chat.txt'
    export_path.write_text(chat_text, encoding='utf-8')
    with export_path.open('rb') as f:
        response = client.post('/ingest/export', headers=auth_headers(), files={'file': ('chat.txt', f, 'text/plain')})
    assert response.status_code == 200, response.text
    with get_session() as session:
        cases = session.query(ServiceRequestCase).all()
        incidents = session.query(Incident).all()
        assert any(case.service_request_number == '311-25842195' for case in cases)
        assert len(incidents) >= 1


def test_export_ingest_dedupes_identical_messages_in_same_file(client, tmp_path):
    chat_text = '''[2/15/26, 8:56:59 AM] Karen KWA: North lift dead
[2/15/26, 8:56:59 AM] Karen KWA: North lift dead
'''
    export_path = tmp_path / 'dupe_chat.txt'
    export_path.write_text(chat_text, encoding='utf-8')
    with export_path.open('rb') as f:
        response = client.post('/ingest/export', headers=auth_headers(), files={'file': ('dupe_chat.txt', f, 'text/plain')})
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['inserted'] == 1
    with get_session() as session:
        assert session.query(Incident).count() == 1


def test_mobile_claim_submit_and_status_sync(client, monkeypatch):
    client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North lift dead',
        'sender': 'Karen',
        'ts_epoch': 1770000100,
    })
    claim = client.post('/mobile/filings/claim_next', headers=mobile_headers())
    assert claim.status_code == 200, claim.text
    payload = claim.json()['job']
    assert payload is not None
    job_id = payload['job_id']

    submitted = client.post(f'/mobile/filings/{job_id}/submitted', headers=mobile_headers(), json={
        'service_request_number': '311-99999999',
        'app_status': 'submitted',
        'notes': 'submitted from test',
    })
    assert submitted.status_code == 200, submitted.text

    def fake_sync(session):
        case = session.query(ServiceRequestCase).filter_by(service_request_number='311-99999999').one()
        case.status = 'Closed'
        case.agency = 'DOB'
        case.resolution_description = 'Test resolution'
        return [{'service_request_number': case.service_request_number, 'status': case.status}]

    monkeypatch.setattr('packages.worker_jobs.sync_all_case_statuses', fake_sync)
    sync_response = client.post('/admin/sync_311_statuses', headers=auth_headers())
    assert sync_response.status_code == 200, sync_response.text
    with get_session() as session:
        case = session.query(ServiceRequestCase).filter_by(service_request_number='311-99999999').one()
        assert case.status == 'Closed'
        assert case.agency == 'DOB'


def test_legal_export_bundle(client):
    client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out again',
        'sender': 'Tibor Simon',
        'ts_epoch': 1770000200,
    })
    claim = client.post('/mobile/filings/claim_next', headers=mobile_headers()).json()['job']
    client.post(f"/mobile/filings/{claim['job_id']}/submitted", headers=mobile_headers(), json={'service_request_number': '311-12345678'})
    with get_session() as session:
        result = export_legal_bundle(session)
    assert Path(result['csv']).exists()
    assert Path(result['markdown']).exists()


def test_tasker_epoch_is_normalized_to_iso(client):
    response = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out again',
        'sender': 'Tibor Simon',
        'ts_epoch': 1770000000,
    })
    assert response.status_code == 200, response.text
    with get_session() as session:
        incident = session.query(Incident).one()
        assert incident.start_ts.endswith('Z')


def test_report_form_submit_creates_incident(client):
    response = client.post('/report/submit', data={
        'reporter': '16F',
        'kind': 'elevator_out',
        'asset': 'elevator_both',
        'note': 'still broken',
    })
    assert response.status_code == 200, response.text
    with get_session() as session:
        incident = session.query(Incident).one()
        assert incident.category == 'elevator'
        assert incident.report_count >= 1


def test_llm_assist_can_promote_issue_and_logs_decision(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'assist')

    def fake_llm(*args, **kwargs):
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': 'elevator_north',
            'event_type': 'outage',
            'severity': 4,
            'confidence': 91,
            'title': 'Elevator outage',
            'summary': 'LLM recognized a fuzzy elevator outage.',
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)
    response = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'The north lift keeps skipping our floor and won’t open',
        'sender': 'Karen',
        'ts_epoch': 1770000300,
    })
    assert response.status_code == 200, response.text
    with get_session() as session:
        incident = session.query(Incident).one()
        decision = session.query(MessageDecision).one()
        assert incident.category == 'elevator'
        assert decision.chosen_source in {'llm', 'hybrid'}
        assert decision.is_issue is True


def test_reprocess_last_is_idempotent_for_existing_incidents(client):
    response = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North lift dead',
        'sender': 'Tibor Simon',
        'ts_epoch': 1770000200,
    })
    assert response.status_code == 200, response.text

    follow_up = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North lift still dead',
        'sender': 'Tibor Simon',
        'ts_epoch': 1770000260,
    })
    assert follow_up.status_code == 200, follow_up.text

    replay = client.post('/admin/reprocess_last/2', headers=auth_headers())
    assert replay.status_code == 200, replay.text

    with get_session() as session:
        incidents = session.query(Incident).all()
        witnesses = session.query(IncidentWitness).all()
        decisions = session.query(MessageDecision).all()
        assert len(incidents) == 1
        assert len(witnesses) == 1
        assert len(decisions) == 2


def test_older_elevator_message_does_not_merge_into_newer_incident(client):
    newer = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are broken right now',
        'sender': 'Molly',
        'ts_epoch': 1773873979,
    })
    assert newer.status_code == 200, newer.text

    older = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both lifts are out at this moment',
        'sender': 'Harry',
        'ts_epoch': 1759939693,
    })
    assert older.status_code == 200, older.text

    with get_session() as session:
        incidents = session.query(Incident).filter_by(category='elevator').all()
        assert len(incidents) == 2
        latest = max(incidents, key=lambda row: int(row.last_ts_epoch or 0))
        assert latest.start_ts_epoch == 1773873979
        assert latest.last_ts_epoch == 1773873979
