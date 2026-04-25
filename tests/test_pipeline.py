import json

from pathlib import Path
from packages.db import FilingJob, Incident, IncidentWitness, MessageDecision, RawMessage, ServiceRequestCase, get_session
from packages.nyc311.legal_export import export_legal_bundle
from packages.nyc311.tracker import find_sr_numbers, normalize_sr_number
from packages.timeutil import parse_ts_to_epoch


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


def test_tasker_batch_ingest_creates_rows_and_queue(client):
    response = client.post('/ingest/tasker_batch', headers=auth_headers(), json={
        'items': [
            {
                'chat_name': '455 Tenants',
                'text': 'Both elevators are out again',
                'sender': 'Karen',
                'ts_epoch': 1770000000,
            },
            {
                'chat_name': '455 Tenants',
                'text': 'North elevator still dead',
                'sender': 'Tibor Simon',
                'ts_epoch': 1770000300,
            },
        ]
    })
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['received'] == 2
    assert payload['inserted'] == 2
    assert payload['deduped'] == 0
    with get_session() as session:
        assert session.query(RawMessage).count() == 2
        assert session.query(MessageDecision).count() == 2
        assert session.query(Incident).count() >= 1
        assert session.query(FilingJob).count() == 1


def test_tasker_batch_dedupes_existing_and_same_batch_duplicates(client):
    first = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out again',
        'sender': 'Karen',
        'ts_epoch': 1770000000,
    })
    assert first.status_code == 200, first.text

    response = client.post('/ingest/tasker_batch', headers=auth_headers(), json={
        'items': [
            {
                'chat_name': '455 Tenants',
                'text': 'Both elevators are out again',
                'sender': 'Karen',
                'ts_epoch': 1770000000,
            },
            {
                'chat_name': '455 Tenants',
                'text': 'North elevator still dead',
                'sender': 'Tibor Simon',
                'ts_epoch': 1770000300,
            },
            {
                'chat_name': '455 Tenants',
                'text': 'North elevator still dead',
                'sender': 'Tibor Simon',
                'ts_epoch': 1770000310,
            },
        ]
    })
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['received'] == 3
    assert payload['inserted'] == 1
    assert payload['deduped'] == 2
    with get_session() as session:
        assert session.query(RawMessage).count() == 2
        assert session.query(MessageDecision).count() == 2


def test_filing_draft_description_is_short_and_casual(client):
    response = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North elevator trapped a passenger and is stuck again.',
        'sender': 'Tibor Simon',
        'ts_epoch': 1775064958,
    })
    assert response.status_code == 200, response.text
    with get_session() as session:
        job = session.query(FilingJob).one()
        payload = json.loads(job.payload_json)
        assert payload['description'] == 'North elevator stuck and trapped a passenger.'


def test_filing_draft_uses_canonical_full_building_address(client):
    response = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out again and people are stuck walking home.',
        'sender': 'Karen',
        'ts_epoch': 1770000500,
    })
    assert response.status_code == 200, response.text
    with get_session() as session:
        job = session.query(FilingJob).one()
        payload = json.loads(job.payload_json)
        assert payload['building']['full_address'] == '455 OCEAN PARKWAY, BROOKLYN, NY, 11218'
        assert payload['building']['street_address'] == '455 OCEAN PARKWAY'


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


def test_export_ingest_links_manual_sr_number_to_recent_incident(client, tmp_path):
    chat_text = '''[1/4/26, 4:45:25 PM] Diana: just spoke with the doorman. heat is not working.
[1/4/26, 5:09:40 PM] Diana: report number 311-25815998
'''
    export_path = tmp_path / 'manual_sr_link.txt'
    export_path.write_text(chat_text, encoding='utf-8')
    with export_path.open('rb') as f:
        response = client.post('/ingest/export', headers=auth_headers(), files={'file': ('manual_sr_link.txt', f, 'text/plain')})
    assert response.status_code == 200, response.text
    with get_session() as session:
        case = session.query(ServiceRequestCase).filter_by(service_request_number='311-25815998').one()
        assert case.incident_id is not None
        incident = session.get(Incident, case.incident_id)
        assert incident is not None
        assert incident.category == 'heat_hot_water'
        assert parse_ts_to_epoch(case.submitted_at) == parse_ts_to_epoch('1/4/26 5:09:40 PM')


def test_export_ingest_media_placeholder_is_not_classified_as_issue(client, tmp_path, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'all')
    llm_calls: list[str] = []

    def fake_llm(*args, **kwargs):
        llm_calls.append(args[0])
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': None,
            'event_type': 'still_out',
            'severity': 4,
            'confidence': 95,
            'title': 'Elevator issue',
            'summary': 'Placeholder should not reach the model.',
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)

    export_path = tmp_path / 'media_only.txt'
    export_path.write_text('[4/12/26, 9:45:00 AM] Karen KWA: image omitted\n', encoding='utf-8')
    with export_path.open('rb') as f:
        response = client.post('/ingest/export', headers=auth_headers(), files={'file': ('media_only.txt', f, 'text/plain')})

    assert response.status_code == 200, response.text
    with get_session() as session:
        decision = session.query(MessageDecision).one()
        raw = session.query(RawMessage).one()
        assert raw.attachments == 'omitted:image'
        assert decision.chosen_source == 'media_placeholder'
        assert decision.is_issue is False
        assert session.query(Incident).count() == 0
    assert llm_calls == []


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


def test_export_ingest_dedupes_matching_tasker_message_even_when_chat_name_differs(client, tmp_path):
    client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North lift dead',
        'sender': 'Karen KWA',
        'ts_epoch': parse_ts_to_epoch('2/15/26 8:56:59 AM'),
    })

    export_path = tmp_path / 'chat.txt'
    export_path.write_text('[2/15/26, 8:56:59 AM] Karen KWA: North lift dead\n', encoding='utf-8')
    with export_path.open('rb') as f:
        response = client.post('/ingest/export', headers=auth_headers(), files={'file': ('chat.txt', f, 'text/plain')})

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['parsed'] == 1
    assert payload['inserted'] == 0
    assert payload['deduped'] == 1

    with get_session() as session:
        raws = session.query(RawMessage).all()
        decisions = session.query(MessageDecision).all()

    assert len(raws) == 1
    assert len(decisions) == 1
    assert raws[0].source == 'tasker'


def test_tasker_batch_schedules_single_resync_after_bulk_processing(client, monkeypatch):
    process_calls: list[tuple[str, bool]] = []
    resync_calls: list[bool] = []

    def fake_enqueue_process_message(message_id: str, *, sync_sheets: bool = True):
        process_calls.append((message_id, sync_sheets))
        return f"job-{message_id[:8]}"

    def fake_enqueue_full_resync():
        resync_calls.append(True)
        return "resync-job"

    monkeypatch.setattr('apps.api.routers.ingest.enqueue_process_message', fake_enqueue_process_message)
    monkeypatch.setattr('apps.api.routers.ingest.enqueue_full_resync', fake_enqueue_full_resync)

    response = client.post('/ingest/tasker_batch', headers=auth_headers(), json={
        'items': [
            {
                'chat_name': '455 Tenants',
                'text': 'Both elevators are out again',
                'sender': 'Karen',
                'ts_epoch': 1770000000,
            },
            {
                'chat_name': '455 Tenants',
                'text': 'North elevator still dead',
                'sender': 'Tibor Simon',
                'ts_epoch': 1770000300,
            },
        ]
    })

    assert response.status_code == 200, response.text
    assert len(process_calls) == 2
    assert all(sync_sheets is False for _message_id, sync_sheets in process_calls)
    assert len(resync_calls) == 1


def test_operator_text_no_longer_false_matches_pests(client):
    response = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'The 311 operator took that complaint as well.',
        'sender': 'Molly',
        'ts_epoch': 1776800000,
    })

    assert response.status_code == 200, response.text

    with get_session() as session:
        decision = session.get(MessageDecision, response.json()['message_id'])
        assert decision is not None
        assert decision.is_issue is False
        assert decision.category is None
        assert session.query(Incident).count() == 0


def test_export_ingest_schedules_single_resync_after_bulk_processing(client, tmp_path, monkeypatch):
    process_calls: list[tuple[str, bool]] = []
    resync_calls: list[bool] = []

    def fake_enqueue_process_message(message_id: str, *, sync_sheets: bool = True):
        process_calls.append((message_id, sync_sheets))
        return f"job-{message_id[:8]}"

    def fake_enqueue_full_resync():
        resync_calls.append(True)
        return "resync-job"

    monkeypatch.setattr('apps.api.routers.ingest.enqueue_process_message', fake_enqueue_process_message)
    monkeypatch.setattr('apps.api.routers.ingest.enqueue_full_resync', fake_enqueue_full_resync)

    export_path = tmp_path / 'bulk_chat.txt'
    export_path.write_text(
        '[2/15/26, 8:56:59 AM] Karen KWA: North lift dead\n'
        '[2/15/26, 9:01:59 AM] Tibor Simon: South lift dead\n',
        encoding='utf-8',
    )
    with export_path.open('rb') as f:
        response = client.post('/ingest/export', headers=auth_headers(), files={'file': ('bulk_chat.txt', f, 'text/plain')})

    assert response.status_code == 200, response.text
    assert len(process_calls) == 2
    assert all(sync_sheets is False for _message_id, sync_sheets in process_calls)
    assert len(resync_calls) == 1


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


def test_sr_normalization_accepts_bare_eight_digit_value():
    assert normalize_sr_number('25815998') == '311-25815998'
    assert normalize_sr_number('311-25815998') == '311-25815998'
    assert find_sr_numbers('Chat shows 311-25815998 and 311 25815998 again') == ['311-25815998']


def test_mobile_submitted_accepts_bare_eight_digit_sr_number(client):
    client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out again',
        'sender': 'Karen',
        'ts_epoch': 1770000105,
    })
    claim = client.post('/mobile/filings/claim_next', headers=mobile_headers())
    assert claim.status_code == 200, claim.text
    job_id = claim.json()['job']['job_id']

    submitted = client.post(f'/mobile/filings/{job_id}/submitted', headers=mobile_headers(), json={
        'service_request_number': '25815998',
        'app_status': 'submitted',
    })
    assert submitted.status_code == 200, submitted.text
    payload = submitted.json()
    assert payload['service_request_number'] == '311-25815998'

    with get_session() as session:
        case = session.query(ServiceRequestCase).filter_by(service_request_number='311-25815998').one()
        assert case.filing_job_id == job_id
        assert case.status == 'submitted'


def test_mobile_submitted_is_idempotent_when_sr_case_already_exists(client):
    client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out again',
        'sender': 'Karen',
        'ts_epoch': 1770000110,
    })
    claim = client.post('/mobile/filings/claim_next', headers=mobile_headers())
    assert claim.status_code == 200, claim.text
    job_id = claim.json()['job']['job_id']

    update = client.post('/mobile/sr_updates', headers=mobile_headers(), json={
        'service_request_number': '311-88887777',
        'status': 'verification_complete',
        'agency': 'TEST',
        'complaint_type': 'Elevator or Escalator Complaint',
        'resolution_description': 'created before submitted',
    })
    assert update.status_code == 200, update.text

    submitted = client.post(f'/mobile/filings/{job_id}/submitted', headers=mobile_headers(), json={
        'service_request_number': '311-88887777',
        'app_status': 'submitted',
        'notes': 'submitted after sr update',
    })
    assert submitted.status_code == 200, submitted.text

    with get_session() as session:
        cases = session.query(ServiceRequestCase).filter_by(service_request_number='311-88887777').all()
        assert len(cases) == 1
        assert cases[0].filing_job_id == job_id
        assert cases[0].incident_id is not None
        job = session.get(FilingJob, job_id)
        assert job.state == 'submitted'


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


def test_issue_summary_uses_source_text_instead_of_llm_inference(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'all')

    def fake_llm(*args, **kwargs):
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': None,
            'event_type': 'still_out',
            'severity': 4,
            'confidence': 95,
            'title': 'Elevator outage',
            'summary': 'Elevator is broken again with no one trapped inside, indicating persistent elevator outage issue.',
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)

    response = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': "no one is in the elevator. it's just broken. again. for the millionth time.",
        'sender': 'Karen',
        'ts_epoch': 1776802000,
    })

    assert response.status_code == 200, response.text
    with get_session() as session:
        incident = session.query(Incident).one()
        decision = session.query(MessageDecision).one()
        final = json.loads(decision.final_json or '{}')

        assert incident.summary == "No one is in the elevator. It's just broken again for the millionth time."
        assert final['summary'] == incident.summary
        assert 'trapped' not in incident.summary
        assert 'indicating' not in incident.summary
        assert 'persistent' not in incident.summary


def test_issue_summary_strips_contact_lines_and_person_followup(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'all')

    def fake_llm(*args, **kwargs):
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': 'elevator_north',
            'event_type': 'outage',
            'severity': 4,
            'confidence': 92,
            'title': 'North elevator is no longer working',
            'summary': 'Previously reported as working, but now the north elevator is reported to be out of service again.',
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)

    response = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Karen KWA\n+1 (917) 257-4844\n14D\nNorth lift working!!!\nNo longer :(',
        'sender': 'Karen',
        'ts_epoch': 1776802100,
    })

    assert response.status_code == 200, response.text
    with get_session() as session:
        incident = session.query(Incident).one()

        assert incident.summary == 'North lift working. No longer.'
        assert 'Karen' not in incident.summary
        assert '917' not in incident.summary
        assert '14D' not in incident.summary
        assert 'Previously reported' not in incident.summary
        assert 'out of service' not in incident.summary


def test_ambiguous_only_lift_fragment_gets_clear_summary_and_review(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'all')

    def fake_llm(*args, **kwargs):
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': 'elevator_south',
            'event_type': 'status_update',
            'severity': 3,
            'confidence': 88,
            'title': 'South elevator status update',
            'summary': 'South elevator status update.',
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)

    response = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'I think south lift only now.',
        'sender': 'Karen',
        'ts_epoch': 1776802150,
    })

    assert response.status_code == 200, response.text
    with get_session() as session:
        incident = session.query(Incident).one()
        decision = session.query(MessageDecision).one()
        final = json.loads(decision.final_json or '{}')

        assert incident.summary == (
            'Status update mentions only the south lift now; '
            'unclear whether the south lift is working or affected.'
        )
        assert incident.needs_review is True
        assert final['summary'] == incident.summary
        assert final['needs_review'] is True
        assert 'I think' not in incident.summary
        assert 'Karen' not in incident.summary


def test_followup_duplicate_summary_collapses_after_person_phrase_removed(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'all')

    def fake_llm(*args, **kwargs):
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'security_access',
            'asset': None,
            'event_type': 'still_out',
            'severity': 3,
            'confidence': 90,
            'title': 'Handrail broken on 10th floor stair A',
            'summary': 'Tenant reports the stair A handrail on the 10th floor is broken again and has informed Jack.',
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)

    first = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'The stair A, 10th flr handrail is kaputt AGAIN.',
        'sender': 'Karen',
        'ts_epoch': 1776802200,
    })
    second = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'The stair A, 10th flr handrail is kaputt AGAIN. Reported to Jack.',
        'sender': 'Karen',
        'ts_epoch': 1776802300,
    })

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    with get_session() as session:
        incident = session.query(Incident).one()

        assert incident.summary == 'The stair A, 10th flr handrail is kaputt AGAIN.'
        assert 'Jack' not in incident.summary
        assert '|' not in incident.summary


def test_guardrail_blocks_unsupported_issue_when_llm_confidently_says_non_issue(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'assist')

    def fake_rules(_text):
        return {
            'is_issue': True,
            'category': 'pests',
            'asset': None,
            'severity': 3,
            'title': 'Pest issue',
            'summary': 'Tenant reports pests.',
            'kind': 'issue',
        }

    def fake_llm(message_text, open_incidents=None, recent_related=None, recent_chat=None):
        return {
            'is_issue': False,
            'signal_type': 'discussion',
            'category': 'other',
            'asset': None,
            'event_type': 'non_issue',
            'severity': 1,
            'confidence': 95,
            'title': 'Discussion only',
            'summary': 'This is not a building issue.',
            'refers_to_open_incident': False,
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.classify_rules', fake_rules)
    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)

    response = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'We should organize better next week.',
        'sender': 'Karen',
        'ts_epoch': 1776801000,
    })

    assert response.status_code == 200, response.text

    with get_session() as session:
        decision = session.get(MessageDecision, response.json()['message_id'])
        assert decision is not None
        assert decision.is_issue is False
        assert decision.chosen_source == 'guardrail_non_issue'
        assert decision.category == 'other'
        assert decision.needs_review is True
        assert session.query(Incident).count() == 0


def test_review_model_resolves_ambiguous_elevator_follow_up(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'assist')

    first = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North elevator is stuck again',
        'sender': 'Karen',
        'ts_epoch': 1770000400,
    })
    assert first.status_code == 200, first.text

    captured = {}

    def fake_llm(message_text, open_incidents=None, recent_related=None, recent_chat=None):
        captured['recent_chat'] = recent_chat or []
        captured['open_incidents'] = open_incidents or []
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': 'elevator_north',
            'event_type': 'still_out',
            'severity': 3,
            'confidence': 75,
            'title': 'Elevator briefly resumed',
            'summary': 'Tenant reports the elevator got stuck and only resumed after forcing the door.',
            'refers_to_open_incident': True,
            'close_incident': False,
            'needs_review': True,
        }

    def fake_review(message_text, rules_choice, llm_choice, open_incidents=None, recent_related=None, recent_chat=None):
        assert rules_choice['category'] == 'security_access'
        assert llm_choice['category'] == 'elevator'
        assert any('North elevator is stuck again' in row['text'] for row in (recent_chat or []))
        assert any(row['category'] == 'elevator' for row in (open_incidents or []))
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': 'elevator_north',
            'event_type': 'still_out',
            'severity': 4,
            'confidence': 92,
            'title': 'Elevator still malfunctioning',
            'summary': 'Follow-up confirms the north elevator remains unreliable after getting stuck.',
            'refers_to_open_incident': True,
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)
    monkeypatch.setattr('packages.incident.extractor.llm_review_decision', fake_review)

    follow_up = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'I think stuck. I shoved the door several times at it resumed its journey. But, meh.',
        'sender': 'Karen',
        'ts_epoch': 1770000460,
    })
    assert follow_up.status_code == 200, follow_up.text

    assert any('North elevator is stuck again' in row['text'] for row in captured['recent_chat'])

    with get_session() as session:
        incidents = session.query(Incident).all()
        assert len(incidents) == 1
        incident = incidents[0]
        assert incident.category == 'elevator'
        assert incident.asset == 'elevator_north'
        assert incident.report_count == 2
        assert session.query(Incident).filter_by(category='security_access').count() == 0

        decision = session.get(MessageDecision, follow_up.json()['message_id'])
        assert decision is not None
        assert decision.chosen_source == 'review'
        assert decision.category == 'elevator'
        assert decision.event_type == 'still_out'
        assert decision.needs_review is False


def test_still_out_follow_up_merges_into_existing_elevator_incident_after_silence_gap(client):
    first = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out right now',
        'sender': 'Karen',
        'ts_epoch': 1770040000,
    })
    assert first.status_code == 200, first.text

    follow_up = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are still down',
        'sender': 'Molly',
        'ts_epoch': 1770050800,
    })
    assert follow_up.status_code == 200, follow_up.text

    with get_session() as session:
        incidents = session.query(Incident).filter_by(category='elevator').all()
        assert len(incidents) == 1
        incident = incidents[0]
        assert incident.report_count == 2
        assert incident.status == 'open'

        decision = session.get(MessageDecision, follow_up.json()['message_id'])
        assert decision is not None
        assert decision.event_type == 'still_out'
        assert decision.incident_id == incident.incident_id


def test_context_can_promote_elevator_category_without_inflating_asset(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'assist')

    first = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North elevator is out again',
        'sender': 'Karen',
        'ts_epoch': 1770002000,
    })
    assert first.status_code == 200, first.text

    def fake_llm(message_text, open_incidents=None, recent_related=None, recent_chat=None):
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': 'elevator_both',
            'event_type': 'status_update',
            'severity': 3,
            'confidence': 85,
            'title': 'Elevator got stuck but moved again',
            'summary': 'Context suggests this is still the elevator issue.',
            'refers_to_open_incident': True,
            'close_incident': False,
            'needs_review': False,
        }

    def fake_review(message_text, rules_choice, llm_choice, open_incidents=None, recent_related=None, recent_chat=None):
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': 'elevator_both',
            'event_type': 'status_update',
            'severity': 3,
            'confidence': 92,
            'title': 'Elevator still acting up',
            'summary': 'Follow-up still refers to the elevator problem.',
            'refers_to_open_incident': True,
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)
    monkeypatch.setattr('packages.incident.extractor.llm_review_decision', fake_review)

    follow_up = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'I think stuck. I shoved the door several times and it resumed.',
        'sender': 'Karen',
        'ts_epoch': 1770002060,
    })
    assert follow_up.status_code == 200, follow_up.text

    with get_session() as session:
        incident = session.query(Incident).one()
        assert incident.asset == 'elevator_north'
        assert incident.report_count == 2

        decision = session.get(MessageDecision, follow_up.json()['message_id'])
        assert decision is not None
        final = json.loads(decision.final_json or '{}')
        assert final.get('category') == 'elevator'
        assert final.get('asset') is None


def test_cross_day_ambiguous_elevator_follow_up_becomes_new_unknown_asset_incident(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'assist')

    first = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North elevator is out again',
        'sender': 'Karen',
        'ts_epoch': 1775347200,
    })
    assert first.status_code == 200, first.text

    def fake_llm(message_text, open_incidents=None, recent_related=None, recent_chat=None):
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': None,
            'event_type': 'status_update',
            'severity': 3,
            'confidence': 84,
            'title': 'Elevator stuck again',
            'summary': 'This sounds like the elevator is acting up again today.',
            'refers_to_open_incident': True,
            'close_incident': False,
            'needs_review': False,
        }

    def fake_review(message_text, rules_choice, llm_choice, open_incidents=None, recent_related=None, recent_chat=None):
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': None,
            'event_type': 'status_update',
            'severity': 3,
            'confidence': 92,
            'title': 'Elevator briefly stuck',
            'summary': 'A new elevator issue happened today, but the specific elevator is unclear.',
            'refers_to_open_incident': True,
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)
    monkeypatch.setattr('packages.incident.extractor.llm_review_decision', fake_review)

    follow_up = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'I think stuck. I shoved the door several times at it resumed its journey. But, meh.',
        'sender': 'Karen',
        'ts_epoch': 1775519401,
    })
    assert follow_up.status_code == 200, follow_up.text

    with get_session() as session:
        incidents = session.query(Incident).filter_by(category='elevator').order_by(Incident.start_ts_epoch.asc()).all()
        assert len(incidents) == 2
        assert incidents[0].asset == 'elevator_north'
        assert incidents[1].asset is None
        assert incidents[1].start_ts_epoch == 1775519401

        decision = session.get(MessageDecision, follow_up.json()['message_id'])
        assert decision is not None
        assert decision.incident_id == incidents[1].incident_id
        assert decision.event_type == 'new_issue'


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
