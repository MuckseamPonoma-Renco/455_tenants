import json
import zipfile

from pathlib import Path
from packages.db import (
    FilingJob,
    Incident,
    IncidentWitness,
    MessageDecision,
    RawMessage,
    ServiceRequestCase,
    WatchdogAction,
    get_session,
)
from packages.incident.extractor import _normalize_choice
from packages.incident.rules import classify_rules, explicit_elevator_asset
from packages.nyc311.legal_export import export_legal_bundle
from packages.nyc311.tracker import find_sr_numbers, normalize_sr_number
from packages.timeutil import parse_ts_to_epoch


def auth_headers():
    return {'Authorization': 'Bearer test-token'}


def mobile_headers():
    return {'Authorization': 'Bearer mobile-token'}


def claim_next_filing(client):
    claim = client.post('/mobile/filings/claim_next', headers=mobile_headers())
    assert claim.status_code == 200, claim.text
    return claim


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
        assert jobs[0].state == 'pending'


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


def test_rough_north_elevator_ride_and_confirmation_stay_one_accurate_case(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'all')

    def fake_llm(message_text, *args, **kwargs):
        if 'Same' in message_text:
            return {
                'is_issue': True,
                'signal_type': 'report',
                'category': 'elevator',
                'asset': 'elevator_north',
                'event_type': 'status_update',
                'severity': 4,
                'confidence': 90,
                'title': 'North elevator issue confirmed',
                'summary': 'Second report confirms the same north elevator issue.',
                'refers_to_open_incident': True,
                'close_incident': False,
                'needs_review': False,
            }
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': 'elevator_north',
            'event_type': 'new_issue',
            'severity': 4,
            'confidence': 90,
            'title': 'North elevator made loud clunk and bounced',
            'summary': 'North elevator made a loud clunk, bounced, and opened slowly.',
            'refers_to_open_incident': False,
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)

    first = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North lift made that unpleasant loud clunk sound when it delivered me to my flr, the car bounced up and down slightly and the door opened in slo-mo at 10:30pm.',
        'sender': 'Karen',
        'ts_epoch': 1779850380,
    })
    second = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Yes. Same. North lift at 11 pm.',
        'sender': 'Molly',
        'ts_epoch': 1779851400,
    })

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text

    with get_session() as session:
        incidents = session.query(Incident).all()
        jobs = session.query(FilingJob).all()
        decisions = session.query(MessageDecision).order_by(MessageDecision.created_at.asc()).all()

        assert len(incidents) == 1
        assert incidents[0].asset == 'elevator_north'
        assert incidents[0].report_count == 2
        assert len(jobs) == 1
        payload = json.loads(jobs[0].payload_json)
        assert payload['description'] == 'North elevator made a loud clunk, bounced, and opened slowly.'
        assert jobs[0].notes == 'North elevator made a loud clunk, bounced, and opened slowly.'
        assert [decision.incident_id for decision in decisions] == [incidents[0].incident_id, incidents[0].incident_id]


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
        assert session.query(FilingJob).count() == 0


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


def test_all_mode_reviews_contextual_followups(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'all')
    monkeypatch.setattr(
        'packages.incident.extractor._contextual_elevator_followup_choice',
        lambda *_args, **_kwargs: {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': 'elevator_south',
            'event_type': 'still_out',
            'severity': 3,
            'confidence': 80,
            'title': 'South elevator outage',
            'summary': 'South elevator remains unavailable.',
            'refers_to_open_incident': True,
            'close_incident': False,
            'needs_review': False,
        },
    )
    llm_calls = []

    def fake_llm(message_text, *args, **kwargs):
        llm_calls.append(message_text)
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': 'elevator_south',
            'event_type': 'still_out',
            'severity': 3,
            'confidence': 95,
            'title': 'South elevator outage',
            'summary': 'South elevator remains unavailable.',
            'refers_to_open_incident': True,
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)
    response = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Still broken.',
        'sender': 'Karen',
        'ts_iso': '2026-06-06T12:00:00Z',
    })

    assert response.status_code == 200, response.text
    assert llm_calls == ['Still broken.']


def test_export_ingest_preserves_identical_physical_messages_and_is_idempotent(client, tmp_path):
    chat_text = '''[2/15/26, 8:56:59 AM] Karen KWA: North lift dead
[2/15/26, 8:56:59 AM] Karen KWA: North lift dead
'''
    export_path = tmp_path / 'dupe_chat.txt'
    export_path.write_text(chat_text, encoding='utf-8')
    with export_path.open('rb') as f:
        response = client.post('/ingest/export', headers=auth_headers(), files={'file': ('dupe_chat.txt', f, 'text/plain')})
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['parsed'] == 2
    assert payload['inserted'] == 2
    assert payload['deduped'] == 0
    with get_session() as session:
        message_ids = sorted(row.message_id for row in session.query(RawMessage).all())
        assert len(message_ids) == 2
        assert any(message_id.endswith('~2') for message_id in message_ids)
        assert session.query(Incident).count() == 1

    with export_path.open('rb') as f:
        repeated = client.post('/ingest/export', headers=auth_headers(), files={'file': ('dupe_chat.txt', f, 'text/plain')})
    assert repeated.status_code == 200, repeated.text
    assert repeated.json()['inserted'] == 0
    assert repeated.json()['deduped'] == 2
    with get_session() as session:
        assert session.query(RawMessage).count() == 2


def test_export_ingest_imports_all_chat_txt_files_in_zip(client, tmp_path):
    export_path = tmp_path / 'all_chats.zip'
    with zipfile.ZipFile(export_path, 'w') as archive:
        archive.writestr('WhatsApp Chat - 455 Tenants.txt', '[6/5/26, 9:00:00 AM] Karen: North lift dead\n')
        archive.writestr('WhatsApp Chat - Building Lobby.txt', '[6/5/26, 9:05:00 AM] Molly: lobby door lock broken\n')
        archive.writestr('notes.txt', 'not a chat export\n')

    with export_path.open('rb') as f:
        response = client.post('/ingest/export', headers=auth_headers(), files={'file': ('all_chats.zip', f, 'application/zip')})

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['parsed'] == 2
    assert payload['inserted'] == 2
    assert payload['chat_files'] == ['WhatsApp Chat - 455 Tenants.txt', 'WhatsApp Chat - Building Lobby.txt']

    with get_session() as session:
        chat_names = {row.chat_name for row in session.query(RawMessage).all()}
        assert chat_names == {'455 Tenants', 'Building Lobby'}


def test_export_ingest_rejects_uploads_over_the_configured_limit(client, monkeypatch):
    monkeypatch.setenv('INGEST_EXPORT_MAX_BYTES', '16')

    response = client.post(
        '/ingest/export',
        headers=auth_headers(),
        files={'file': ('too-large.txt', b'[6/5/26, 9:00:00 AM] Karen: North lift dead\n', 'text/plain')},
    )

    assert response.status_code == 413
    assert 'upload limit' in response.json()['detail']


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


def test_export_collision_preserves_later_occurrence_after_cross_source_match(client, tmp_path):
    captured = client.post('/ingest/tasker', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North lift dead',
        'sender': 'Karen KWA',
        'ts_epoch': parse_ts_to_epoch('2/15/26 8:56:59 AM'),
    })
    assert captured.status_code == 200, captured.text

    export_path = tmp_path / 'WhatsApp Chat - 455 Tenants.txt'
    export_path.write_text(
        '[2/15/26, 8:56:59 AM] Karen KWA: North lift dead\n'
        '[2/15/26, 8:56:59 AM] Karen KWA: North lift dead\n',
        encoding='utf-8',
    )
    with export_path.open('rb') as f:
        response = client.post(
            '/ingest/export',
            headers=auth_headers(),
            files={'file': (export_path.name, f, 'text/plain')},
        )

    assert response.status_code == 200, response.text
    assert response.json()['parsed'] == 2
    assert response.json()['inserted'] == 1
    assert response.json()['deduped'] == 1
    with get_session() as session:
        raws = session.query(RawMessage).order_by(RawMessage.source).all()
    assert len(raws) == 2
    assert {row.source for row in raws} == {'tasker', 'export'}
    assert next(row for row in raws if row.source == 'export').message_id.endswith('~2')


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


def test_elevator_zero_lifts_and_both_working_rules_are_classified():
    zero = classify_rules("Still no lifts & no mechanic as yet.")
    assert zero["is_issue"] is True
    assert zero["category"] == "elevator"
    assert zero["asset"] == "elevator_both"
    assert zero["event_type"] == "still_out"

    one = classify_rules("Hi all-currently one elevator in service")
    assert one["is_issue"] is True
    assert one["category"] == "elevator"

    restored = classify_rules("Both elevators working.")
    assert restored["is_issue"] is True
    assert restored["category"] == "elevator"
    assert restored["kind"] == "restore"

    guidance = classify_rules("NYC.gov Elevator Safety / 3 Rules if You Get Stuck")
    assert guidance["is_issue"] is False

    conditional = classify_rules(
        "It is worth noting that the location of a stuck lift is UNKNOWN. "
        "If both lifts are stuck, they can't see where the stuck lift is by looking up the adjacent shaft."
    )
    assert conditional["is_issue"] is False


def test_audited_laundry_electrical_and_entry_reports_have_stable_rules():
    restored = classify_rules("All lifts working this morning")
    assert restored["is_issue"] is True
    assert restored["category"] == "elevator"
    assert restored["asset"] == "elevator_both"
    assert restored["kind"] == "restore"

    cautious_restore = classify_rules("Seems both lifts working")
    assert cautious_restore["is_issue"] is True
    assert cautious_restore["category"] == "elevator"
    assert cautious_restore["asset"] == "elevator_both"
    assert cautious_restore["event_type"] == "status_update"
    assert cautious_restore["kind"] == "issue"

    laundry = classify_rules(
        "I bought a laundry card instead and it is giving me an error on every machine?"
    )
    assert laundry["is_issue"] is True
    assert laundry["category"] == "other"
    assert laundry["title"] == "Laundry facility issue"
    assert laundry["preserve_issue"] is True
    assert laundry["preserve_event_type"] is True

    unreadable_card = classify_rules(
        "Don't try to use washer number 15 - it won't read my Hercules card"
    )
    assert unreadable_card["is_issue"] is True
    assert unreadable_card["category"] == "other"
    assert unreadable_card["title"] == "Laundry facility issue"

    electrical = classify_rules(
        "A few of mine are painted over and the oven is wired to an outlet in the living room."
    )
    assert electrical["is_issue"] is True
    assert electrical["category"] == "other"
    assert electrical["title"] == "Electrical wiring concern"
    assert electrical["preserve_issue"] is True
    assert electrical["preserve_event_type"] is True

    entry = classify_rules(
        "The super came to my apartment trying to enter even though I had not requested a visit?"
    )
    assert entry["is_issue"] is True
    assert entry["category"] == "security_access"
    assert entry["preserve_issue"] is True
    assert entry["preserve_event_type"] is True


def test_external_news_and_general_advice_cannot_become_building_incidents(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'all')

    def false_positive_model(message_text, *args, **kwargs):
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator' if 'PIX11' in message_text else 'other',
            'asset': None,
            'event_type': 'outage' if 'PIX11' in message_text else 'status_update',
            'severity': 4,
            'confidence': 96,
            'title': 'Incorrect model incident',
            'summary': 'The model incorrectly treated reference material as a report.',
            'refers_to_open_incident': False,
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', false_positive_model)
    monkeypatch.setattr('packages.incident.extractor.llm_review_decision', false_positive_model)

    article_text = (
        'Our letter is timed with this announcement. '
        'MANHATTAN, N.Y. (PIX11) \u2014 The cases include buildings with broken elevators and no heat. '
        'https://pix11.com/news/morning/new-york-city-to-fast-track-urgent-housing-cases/'
    )
    advisory_text = (
        'Pet owners should use pet bounce. Everyone should clean the lint tray after each use. '
        'Lint acts like kindling. A stuffed tray = fire hazard.'
    )
    responses = [
        client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
            'chat_name': '455 Tenants',
            'text': text,
            'sender': 'Karen',
            'ts_epoch': 1787699567 + offset,
        })
        for offset, text in enumerate((article_text, advisory_text))
    ]
    assert all(response.status_code == 200 for response in responses)

    with get_session() as session:
        decisions = [session.get(MessageDecision, response.json()['message_id']) for response in responses]
        assert [decision.is_issue for decision in decisions] == [False, False]
        assert decisions[0].chosen_source == 'guardrail_external_reference'
        assert decisions[1].chosen_source == 'guardrail_general_advisory'
        assert session.query(Incident).count() == 0

    direct_report = classify_rules(
        'Our north elevator is down now. MANHATTAN, N.Y. (PIX11) \u2014 Housing update. '
        'https://pix11.com/news/morning/housing-update/'
    )
    assert direct_report['is_issue'] is True
    assert direct_report['category'] == 'elevator'


def test_nonissue_reclassification_retires_orphan_draft_and_watchdog(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'off')
    from packages.incident.extractor import classify_and_upsert_incident

    article_text = (
        'Our letter is timed with this announcement. '
        'MANHATTAN, N.Y. (PIX11) \u2014 Buildings with broken elevators were discussed. '
        'https://pix11.com/news/morning/housing-update/'
    )
    with get_session() as session:
        raw = RawMessage(
            message_id='article-message',
            chat_name='455 Tenants',
            sender='Karen',
            sender_hash='sender-a',
            ts_epoch=1787699567,
            text=article_text,
            source='zip_import',
        )
        incident = Incident(
            incident_id='false-article-incident',
            category='elevator',
            asset=None,
            severity=4,
            status='open',
            start_ts_epoch=1787699567,
            last_ts_epoch=1787699567,
            title='Elevator outage',
            summary='Incorrect article classification.',
            proof_refs=raw.message_id,
            report_count=1,
            witness_count=1,
            confidence=90,
            needs_review=True,
        )
        session.add_all([raw, incident])
        session.add(MessageDecision(
            message_id=raw.message_id,
            incident_id=incident.incident_id,
            is_issue=True,
            category='elevator',
            event_type='outage',
        ))
        session.add(FilingJob(
            dedupe_key='311:false-article-incident',
            incident_id=incident.incident_id,
            state='awaiting_approval',
        ))
        session.add(WatchdogAction(
            action_type='elevator_outage',
            severity='high',
            title='Track false article',
            status='open',
            related_incident_id=incident.incident_id,
        ))
        session.commit()

        classify_and_upsert_incident(session, raw, allow_filing_job=False)
        session.commit()

    with get_session() as session:
        decision = session.get(MessageDecision, 'article-message')
        job = session.query(FilingJob).one()
        action = session.query(WatchdogAction).one()
        assert decision.is_issue is False
        assert decision.incident_id is None
        assert session.get(Incident, 'false-article-incident') is None
        assert job.state == 'skipped'
        assert job.incident_id is None
        assert action.status == 'completed'
        assert action.related_incident_id is None


def test_elevator_continuations_merge_and_refresh_anonymous_311_draft(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'off')
    messages = (
        (1787763180, 'One elevator down'),
        (1787778180, 'ONE LIFT ONLY! STILL!'),
        (1787778300, 'Wojtek says no mechanic in bldg. He says he will put in a call....'),
        (1787792880, 'Mechanic is unable to repair tonight.'),
        (1787831880, 'North elevator still down'),
    )
    message_ids = []
    for ts_epoch, text in messages:
        response = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
            'chat_name': '455 Tenants',
            'text': text,
            'sender': 'Karen',
            'ts_epoch': ts_epoch,
        })
        assert response.status_code == 200, response.text
        message_ids.append(response.json()['message_id'])

    with get_session() as session:
        incidents = session.query(Incident).all()
        assert len(incidents) == 1
        assert incidents[0].asset == 'elevator_north'
        assert incidents[0].report_count == 5
        decisions = [session.get(MessageDecision, message_id) for message_id in message_ids]
        assert [decision.event_type for decision in decisions] == [
            'outage',
            'still_out',
            'status_update',
            'status_update',
            'still_out',
        ]
        assert len({decision.incident_id for decision in decisions}) == 1
        job = session.query(FilingJob).one()
        payload = json.loads(job.payload_json)
        assert job.state == 'pending'
        assert payload['incident']['asset'] == 'elevator_north'
        assert payload['incident']['report_count'] == 5
        assert payload['description'] == 'North elevator dead.'
        assert payload['submission'] == {'mode': 'anonymous'}
        assert 'contact' not in payload


def test_front_desk_phone_correction_closes_the_reported_issue(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'off')
    first = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'The front desk intercom is not working.',
        'sender': 'Kendall',
        'ts_epoch': 1787137860,
    })
    correction = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'I have the number Karen gave for the front desk and that works.',
        'sender': 'Molly',
        'ts_epoch': 1787139360,
    })
    assert first.status_code == 200, first.text
    assert correction.status_code == 200, correction.text

    with get_session() as session:
        incident = session.query(Incident).one()
        decision = session.get(MessageDecision, correction.json()['message_id'])
        assert decision.event_type == 'restore'
        assert decision.incident_id == incident.incident_id
        assert incident.status == 'closed'
        assert incident.end_ts_epoch == 1787139360


def test_pending_311_draft_refreshes_after_incident_changes(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'off')
    first = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'One elevator down',
        'sender': 'Karen',
        'ts_epoch': 1787763180,
    })
    assert first.status_code == 200, first.text
    with get_session() as session:
        job_id = session.query(FilingJob).one().job_id

    update = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North elevator still down',
        'sender': 'Molly',
        'ts_epoch': 1787778180,
    })
    assert update.status_code == 200, update.text

    with get_session() as session:
        job = session.get(FilingJob, job_id)
        payload = json.loads(job.payload_json)
        assert job.state == 'pending'
        assert 'claimed_payload_sha256=' not in (job.notes or '')
        assert payload['incident']['asset'] == 'elevator_north'
        assert payload['incident']['report_count'] == 2


def test_reviewed_export_guardrails_override_model_drift(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'all')

    reviewed_nonissues = (
        'should never enter a premises without 24 hours notice',
        'came with list in hand',
        'sticking something like a coat hanger',
        'Did you see 1 working',
        "I don't hear it moving now",
        "Fingers crossed that it doesn't go out of service",
    )

    def model_choice(message_text):
        if 'painted over' in message_text:
            return {
                'is_issue': False,
                'signal_type': 'discussion',
                'category': 'other',
                'asset': None,
                'event_type': 'non_issue',
                'severity': 1,
                'confidence': 96,
                'title': 'Discussion only',
                'summary': 'The model incorrectly dismissed the electrical report.',
                'refers_to_open_incident': False,
                'close_incident': False,
                'needs_review': False,
            }
        if 'come into my apartment' in message_text:
            category = 'security_access'
            event_type = 'status_update'
            asset = None
        elif any(fragment in message_text for fragment in reviewed_nonissues):
            category = 'elevator' if 'super' not in message_text and 'list in hand' not in message_text else 'security_access'
            event_type = 'status_update'
            asset = None
        else:
            category = 'elevator'
            event_type = 'restore' if 'working' in message_text.casefold() else 'outage'
            asset = 'elevator_both' if 'both' in message_text.casefold() else 'elevator_south'
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': category,
            'asset': asset,
            'event_type': event_type,
            'severity': 4,
            'confidence': 94,
            'title': 'Model choice',
            'summary': 'Deliberately drifting model choice.',
            'refers_to_open_incident': category == 'elevator',
            'close_incident': event_type == 'restore',
            'needs_review': False,
        }

    def fake_llm(message_text, open_incidents=None, recent_related=None, recent_chat=None):
        return model_choice(message_text)

    def fake_review(message_text, rules_choice, llm_choice, open_incidents=None, recent_related=None, recent_chat=None):
        return model_choice(message_text)

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)
    monkeypatch.setattr('packages.incident.extractor.llm_review_decision', fake_review)

    texts = [
        'South elevator is out.',
        'Jose said they are both out',
        'Seems both lifts working',
        'South is definitely not working, Val is going to go look for it',
        'South is back in service',
        'South is out now as of 9 pm. Just walked into lobby.',
        "It doesn't stop at 10 I have to call the doorman to send it up",
        "Right! Why would he try to come into my apartment in the middle of the day when presumably I'm not home?",
        'A few of mine are painted over and the oven is wired to an outlet in the living room.',
        'I do not trust this building super. he should never enter a premises without 24 hours notice.',
        'yeah super came with list in hand and boogied on it to my surprise lol',
        'Young David is down there sticking something like a coat hanger in the north one.',
        'Did you see 1 working, because I waited 5 minutes in dead silence?',
        "I don't hear it moving now",
        "Fingers crossed that it doesn't go out of service.",
    ]
    message_ids = []
    for offset, text in enumerate(texts):
        response = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
            'chat_name': '455 Tenants',
            'text': text,
            'sender': 'Karen',
            'ts_epoch': 1785700000 + offset * 60,
        })
        assert response.status_code == 200, response.text
        message_ids.append(response.json()['message_id'])

    with get_session() as session:
        decisions = [session.get(MessageDecision, message_id) for message_id in message_ids]

    assert decisions[1].event_type == 'status_update'
    assert decisions[2].event_type == 'status_update'
    assert json.loads(decisions[2].final_json or '{}')['title'] == 'Elevator working update'
    assert decisions[3].event_type == 'status_update'
    assert decisions[5].event_type == 'status_update'
    assert decisions[6].event_type == 'status_update'
    assert decisions[7].is_issue is True
    assert decisions[7].category == 'security_access'
    assert decisions[7].event_type == 'new_issue'
    assert decisions[8].is_issue is True
    assert decisions[8].category == 'other'
    assert decisions[8].event_type == 'new_issue'
    assert all(decision.is_issue is False for decision in decisions[9:])
    assert all(decision.chosen_source == 'guardrail_reviewed_non_issue' for decision in decisions[9:])


def test_non_elevator_problem_is_not_reframed_by_recent_elevator_context(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'off')

    outage = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'South elevator is out.',
        'sender': 'Karen',
        'ts_epoch': 1785520000,
    })
    laundry = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'The laundry app is not working because there is no WiFi in the basement.',
        'sender': 'Nez',
        'ts_epoch': 1785520060,
    })

    assert outage.status_code == 200, outage.text
    assert laundry.status_code == 200, laundry.text
    with get_session() as session:
        decision = session.get(MessageDecision, laundry.json()['message_id'])
        assert decision is not None
        assert decision.is_issue is True
        assert decision.category == 'other'
        assert decision.event_type == 'new_issue'


def test_contextual_elevator_operational_updates_and_hypothetical_guard(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'off')

    outage = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out.',
        'sender': 'Karen',
        'ts_epoch': 1785600000,
    })
    mechanic = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Mechanic is here',
        'sender': 'Darby',
        'ts_epoch': 1785600060,
    })
    still_out = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Still out.',
        'sender': 'Karen',
        'ts_epoch': 1785600090,
    })
    fixed_soon = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': '(Also just told they will be fixed “soon”)',
        'sender': 'Molly',
        'ts_epoch': 1785600100,
    })
    elapsed = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': "This is terrible. It’s been 12 hours. Even though they're being replaced, "
                'we still deserve some explanation. My plans to do laundry are out.',
        'sender': 'Nez',
        'ts_epoch': 1785600110,
    })
    moving = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': "It's moving",
        'sender': 'Karen',
        'ts_epoch': 1785600115,
    })
    restored = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North & South working',
        'sender': 'Karen',
        'ts_epoch': 1785600120,
    })
    hypothetical = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': "Fingers crossed that it doesn't go out of service.",
        'sender': 'Molly',
        'ts_epoch': 1785600180,
    })

    assert outage.status_code == 200, outage.text
    assert mechanic.status_code == 200, mechanic.text
    assert still_out.status_code == 200, still_out.text
    assert fixed_soon.status_code == 200, fixed_soon.text
    assert elapsed.status_code == 200, elapsed.text
    assert moving.status_code == 200, moving.text
    assert restored.status_code == 200, restored.text
    assert hypothetical.status_code == 200, hypothetical.text
    with get_session() as session:
        mechanic_decision = session.get(MessageDecision, mechanic.json()['message_id'])
        still_out_decision = session.get(MessageDecision, still_out.json()['message_id'])
        fixed_soon_decision = session.get(MessageDecision, fixed_soon.json()['message_id'])
        elapsed_decision = session.get(MessageDecision, elapsed.json()['message_id'])
        moving_decision = session.get(MessageDecision, moving.json()['message_id'])
        restored_decision = session.get(MessageDecision, restored.json()['message_id'])
        hypothetical_decision = session.get(MessageDecision, hypothetical.json()['message_id'])
        assert mechanic_decision is not None
        assert mechanic_decision.category == 'elevator'
        assert mechanic_decision.event_type == 'status_update'
        assert still_out_decision is not None
        assert still_out_decision.category == 'elevator'
        assert still_out_decision.event_type == 'still_out'
        assert fixed_soon_decision is not None
        assert fixed_soon_decision.category == 'elevator'
        assert fixed_soon_decision.event_type == 'status_update'
        assert elapsed_decision is not None
        assert elapsed_decision.category == 'elevator'
        assert elapsed_decision.event_type == 'status_update'
        assert moving_decision is not None
        assert moving_decision.category == 'elevator'
        assert moving_decision.event_type == 'status_update'
        assert restored_decision is not None
        assert restored_decision.category == 'elevator'
        assert restored_decision.event_type == 'restore'
        assert json.loads(restored_decision.final_json or '{}').get('asset') == 'elevator_both'
        filing_job = session.query(FilingJob).one()
        assert filing_job.state == 'skipped'
        assert 'no longer auto-eligible' in (filing_job.notes or '')
        assert hypothetical_decision is not None
        assert hypothetical_decision.is_issue is False


def test_both_elevators_restore_closes_all_eligible_open_elevator_incidents(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'off')

    def incident(incident_id, asset, last_ts_epoch):
        return Incident(
            incident_id=incident_id,
            category='elevator',
            asset=asset,
            severity=4,
            status='open',
            start_ts='2026-08-01T00:00:00Z',
            start_ts_epoch=last_ts_epoch,
            last_ts_epoch=last_ts_epoch,
            title='Elevator outage',
            summary='Elevator outage reported.',
            proof_refs='',
            report_count=1,
            witness_count=1,
            confidence=90,
            needs_review=False,
        )

    with get_session() as session:
        session.add_all(
            [
                incident('older-north', 'elevator_north', 1785590000),
                incident('older-south', 'elevator_south', 1785591000),
                incident('older-both', 'elevator_both', 1785592000),
                incident('newer-both', 'elevator_both', 1785601000),
            ]
        )
        session.commit()

    response = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators working',
        'sender': 'Karen',
        'ts_epoch': 1785600000,
    })
    assert response.status_code == 200, response.text

    with get_session() as session:
        for incident_id in ('older-north', 'older-south', 'older-both'):
            row = session.get(Incident, incident_id)
            assert row.status == 'closed'
            assert row.end_ts_epoch == 1785600000
        assert session.get(Incident, 'newer-both').status == 'open'


def test_contextual_elevator_followups_do_not_become_heat_issue(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'off')

    first = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'South elevator not working.',
        'sender': 'Molly',
        'ts_epoch': 1781262566,
    })
    second = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Oh sorry, it was out at 10pm or so last night. Wojtek/Valentine cited excessive heat as the cause.',
        'sender': 'Karen',
        'ts_epoch': 1781262741,
    })

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text

    with get_session() as session:
        decision = session.get(MessageDecision, second.json()['message_id'])
        assert decision is not None
        assert decision.chosen_source == 'rules_context'
        assert decision.is_issue is True
        assert decision.category == 'elevator'
        assert decision.event_type == 'status_update'


def test_contextual_elevator_mechanism_update_does_not_become_door_issue(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'off')

    first = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'South lift out on 16th floor.',
        'sender': 'Karen',
        'ts_epoch': 1784945167,
    })
    second = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': (
            "It burns itself out when it can't properly level itself to open on a floor correctly. "
            "Bars keep it from opening doors when it is between floors. The super is calling a technician now."
        ),
        'sender': 'Karen',
        'ts_epoch': 1784945874,
    })

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text

    with get_session() as session:
        decision = session.get(MessageDecision, second.json()['message_id'])
        assert decision is not None
        assert decision.chosen_source == 'rules_context'
        assert decision.category == 'elevator'
        assert decision.event_type == 'status_update'
        assert session.query(Incident).filter_by(category='security_access').count() == 0


def test_context_does_not_turn_an_outage_question_into_a_report(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'off')

    first = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out.',
        'sender': 'Karen',
        'ts_epoch': 1782563500,
    })
    question = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Have both been out since last night?',
        'sender': 'Molly',
        'ts_epoch': 1782563921,
    })

    assert first.status_code == 200, first.text
    assert question.status_code == 200, question.text

    with get_session() as session:
        decision = session.get(MessageDecision, question.json()['message_id'])
        assert decision is not None
        assert decision.is_issue is False
        assert decision.event_type is None
        assert session.query(Incident).filter_by(category='elevator').count() == 1


def test_context_does_not_turn_historical_elevator_discussion_into_current_issue(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'off')

    first = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both elevators are out.',
        'sender': 'Karen',
        'ts_epoch': 1781183500,
    })
    history = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Last year, the 3 day stretch of no elevators was June 14-17.',
        'sender': 'Molly',
        'ts_epoch': 1781183985,
    })

    assert first.status_code == 200, first.text
    assert history.status_code == 200, history.text

    with get_session() as session:
        decision = session.get(MessageDecision, history.json()['message_id'])
        assert decision is not None
        assert decision.is_issue is False
        assert session.query(Incident).filter_by(category='elevator').count() == 1


def test_shorthand_restore_can_use_bounded_older_elevator_context(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'off')

    first = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'South elevator is out.',
        'sender': 'Karen',
        'ts_epoch': 1783260000,
    })
    restored = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both currently working',
        'sender': 'Larissa',
        'ts_epoch': 1783951200,
    })

    assert first.status_code == 200, first.text
    assert restored.status_code == 200, restored.text

    with get_session() as session:
        decision = session.get(MessageDecision, restored.json()['message_id'])
        assert decision is not None
        assert decision.category == 'elevator'
        assert decision.event_type == 'restore'
        final = json.loads(decision.final_json or '{}')
        assert final["asset"] == "elevator_both"


def test_contextual_entrapment_followup_closes_recent_elevator_incident(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'off')

    first = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Elevator is stuck and my child is on it',
        'sender': 'Ani',
        'ts_epoch': 1781176244,
    })
    second = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': "He's out now",
        'sender': 'Ani',
        'ts_epoch': 1781176858,
    })

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text

    with get_session() as session:
        decision = session.get(MessageDecision, second.json()['message_id'])
        assert decision is not None
        assert decision.chosen_source == 'rules_context'
        assert decision.is_issue is True
        assert decision.category == 'elevator'
        assert decision.event_type == 'restore'
        assert session.query(Incident).filter_by(status='closed').count() == 1


def test_contextual_same_day_both_work_now_closes_elevator_incident(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'off')

    first = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'No elevators.',
        'sender': 'Karen',
        'ts_epoch': 1782558383,
    })
    second = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Both work now',
        'sender': 'Karen',
        'ts_epoch': 1782564364,
    })

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text

    with get_session() as session:
        decision = session.get(MessageDecision, second.json()['message_id'])
        assert decision is not None
        assert decision.chosen_source == 'rules_context'
        assert decision.is_issue is True
        assert decision.category == 'elevator'
        assert decision.event_type == 'restore'
        assert session.query(Incident).filter_by(status='closed').count() == 1


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
    claim = claim_next_filing(client)
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


def test_mobile_filing_claims_current_payload_automatically(client):
    client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North lift dead',
        'sender': 'Karen',
        'ts_epoch': 1770000100,
    })
    with get_session() as session:
        job_id = session.query(FilingJob).one().job_id
    preview = client.get(f'/mobile/filings/{job_id}/preview', headers=mobile_headers())
    assert preview.status_code == 200
    payload_sha256 = preview.json()['preview']['payload_sha256']

    claim = client.post('/mobile/filings/claim_next', headers=mobile_headers())
    assert claim.status_code == 200
    assert claim.json()['job']['job_id'] == job_id
    assert claim.json()['job']['state'] == 'claimed'

    with get_session() as session:
        job = session.get(FilingJob, job_id)
        assert f'claimed_payload_sha256={payload_sha256}' in (job.notes or '')


def test_legacy_mobile_approval_rejects_wrong_or_stale_preview(client):
    client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'North lift dead',
        'sender': 'Karen',
        'ts_epoch': 1770000100,
    })
    with get_session() as session:
        job = session.query(FilingJob).one()
        job.state = 'awaiting_approval'
        job_id = job.job_id
        session.commit()
    preview = client.get(f'/mobile/filings/{job_id}/preview', headers=mobile_headers())
    payload_sha256 = preview.json()['preview']['payload_sha256']

    wrong_phrase = client.post(
        f'/mobile/filings/{job_id}/approve',
        headers=mobile_headers(),
        json={'payload_sha256': payload_sha256, 'approval_phrase': 'approve'},
    )
    assert wrong_phrase.status_code == 409

    stale_preview = client.post(
        f'/mobile/filings/{job_id}/approve',
        headers=mobile_headers(),
        json={'payload_sha256': '0' * 64, 'approval_phrase': 'APPROVED \u2014 GO LIVE'},
    )
    assert stale_preview.status_code == 409

    approved = client.post(
        f'/mobile/filings/{job_id}/approve',
        headers=mobile_headers(),
        json={'payload_sha256': payload_sha256, 'approval_phrase': 'APPROVED \u2014 GO LIVE'},
    )
    assert approved.status_code == 200
    assert approved.json()['job']['state'] == 'approved'


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
    claim = claim_next_filing(client)
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
    claim = claim_next_filing(client)
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
    claim = claim_next_filing(client).json()['job']
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


def test_sensitive_interpersonal_security_report_stays_in_review_queue(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'all')

    def fake_llm(*_args, **_kwargs):
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'security_access',
            'asset': None,
            'event_type': 'new_issue',
            'severity': 3,
            'confidence': 90,
            'title': 'Unwanted close contact incident reported',
            'summary': 'A resident reported an unwanted close-contact incident.',
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)

    response = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'A man tried walking up from behind me and squeezing himself next to me.',
        'sender': 'Karen',
        'ts_epoch': 1776802250,
    })

    assert response.status_code == 200, response.text
    with get_session() as session:
        incident = session.query(Incident).one()
        decision = session.query(MessageDecision).one()
        final = json.loads(decision.final_json or '{}')

        assert incident.needs_review is True
        assert decision.needs_review is True
        assert final['needs_review'] is True


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


def test_rules_do_not_turn_recordkeeping_question_into_restore():
    text = (
        "Is the common form listing the exact hours of breakages, when the elevator repair people are called, "
        "when they come, and when the elevators are fixed? Obviously it would be inexact, but it would be good "
        "to have the records for court."
    )

    decision = classify_rules(text)

    assert decision["is_issue"] is False
    assert decision["kind"] == "nonissue"


def test_rules_distinguish_elevator_history_questions_and_elapsed_updates():
    historical = classify_rules(
        "Last year, the 3 day stretch of no elevators was June 14-17! "
        "I took photos to memorialize how unwell I looked by the 3rd day."
    )
    question = classify_rules("Have both been out since last night?")
    elapsed = classify_rules("Which is 1 hr and 40' since stuck lift event.")

    assert historical["is_issue"] is False
    assert question["is_issue"] is False
    assert elapsed["is_issue"] is True
    assert elapsed["category"] == "elevator"
    assert elapsed["event_type"] == "status_update"


def test_rules_capture_accessibility_and_entrance_safety_reports():
    accessibility = classify_rules(
        "The interior lobby door is incredibly difficult and heavy to open when the doorman is away. "
        "There is zero chance a wheelchair person can navigate it."
    )
    safety = classify_rules(
        "I think u should warn women. I have seen him for three days in front of the building wearing the same clothes."
    )

    assert accessibility["is_issue"] is True
    assert accessibility["category"] == "security_access"
    assert safety["is_issue"] is True
    assert safety["category"] == "security_access"


def test_choice_normalization_keeps_issue_state_internally_consistent():
    non_issue = _normalize_choice({
        "is_issue": True,
        "signal_type": "discussion",
        "category": "security_access",
        "event_type": "non_issue",
        "close_incident": False,
    })
    report = _normalize_choice({
        "is_issue": True,
        "signal_type": "status_update",
        "category": "elevator",
        "event_type": "status_update",
        "close_incident": False,
    })

    assert non_issue is not None
    assert non_issue["is_issue"] is False
    assert non_issue["category"] == "other"
    assert non_issue["event_type"] == "non_issue"
    assert report is not None
    assert report["signal_type"] == "report"


def test_elevator_asset_uses_affected_lift_not_first_lift_named():
    assert explicit_elevator_asset("At time of this message, north elevator is functioning, South still out of order") == "elevator_south"
    assert explicit_elevator_asset("South lift working, but not the north lift!") == "elevator_north"
    decision = classify_rules("South lift working, but not the north lift!")
    assert decision["is_issue"] is True
    assert decision["category"] == "elevator"
    assert decision["asset"] == "elevator_north"


def test_elevator_rules_handle_no_side_elevator_and_floor_service_restore():
    outage_cases = [
        ("No north elevator", "elevator_north", "outage"),
        ("No south lift", "elevator_south", "outage"),
        ("The north one is down again", "elevator_north", "still_out"),
        ("south side is out", "elevator_south", "outage"),
        ("Only south lift working", "elevator_north", "outage"),
        ("Only the north elevator is working", "elevator_south", "outage"),
    ]

    for text, asset, event_type in outage_cases:
        decision = classify_rules(text)
        assert decision["is_issue"] is True, text
        assert decision["category"] == "elevator", text
        assert decision["asset"] == asset, text
        assert decision["event_type"] == event_type, text

    south_normal = classify_rules("At least the South one is not stopping every floor")
    assert south_normal["is_issue"] is True
    assert south_normal["category"] == "elevator"
    assert south_normal["asset"] == "elevator_south"
    assert south_normal["kind"] == "restore"

    rough_north = classify_rules("North lift made a loud clunk, bounced, and the door opened in slo-mo.")
    assert rough_north["is_issue"] is True
    assert rough_north["category"] == "elevator"
    assert rough_north["asset"] == "elevator_north"
    assert rough_north["event_type"] == "new_issue"

    floor_call = classify_rules("It seems impossible to call the elevator to the third floor.")
    assert floor_call["is_issue"] is True
    assert floor_call["category"] == "elevator"
    assert floor_call["event_type"] == "new_issue"
    assert floor_call["title"] == "Elevator not responding to floor call"


def test_no_side_elevator_ingest_queues_311_job(client):
    response = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Only south lift working',
        'sender': 'Karen',
        'ts_epoch': 1778065860,
    })

    assert response.status_code == 200, response.text
    with get_session() as session:
        incident = session.query(Incident).one()
        job = session.query(FilingJob).one()
        decision = session.query(MessageDecision).one()

        assert incident.category == 'elevator'
        assert incident.asset == 'elevator_north'
        assert decision.is_issue is True
        assert job.incident_id == incident.incident_id
        assert job.state == 'pending'


def test_311_auto_file_uses_classifier_decision_not_second_regex(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'all')

    def fake_llm(*args, **kwargs):
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': 'elevator_north',
            'event_type': 'outage',
            'severity': 4,
            'confidence': 90,
            'title': 'North elevator unavailable',
            'summary': 'Classifier identified this as a north elevator outage.',
            'refers_to_open_incident': False,
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)

    response = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'The north vertical transport is unavailable again',
        'sender': 'Karen',
        'ts_epoch': 1778066000,
    })

    assert response.status_code == 200, response.text
    with get_session() as session:
        decision = session.query(MessageDecision).one()
        incident = session.query(Incident).one()
        job = session.query(FilingJob).one()

        assert decision.event_type == 'outage'
        assert decision.auto_file_candidate is True
        assert incident.asset == 'elevator_north'
        assert job.incident_id == incident.incident_id


def test_unsupported_other_issue_from_llm_is_blocked(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'all')

    def fake_llm(*args, **kwargs):
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'other',
            'asset': None,
            'event_type': 'new_issue',
            'severity': 2,
            'confidence': 80,
            'title': 'Partial functionality issue reported',
            'summary': 'Two pages only work sometimes.',
            'refers_to_open_incident': False,
            'close_incident': False,
            'needs_review': True,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)

    response = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': "It's three pages but two of them only work sometimes",
        'sender': 'Karen',
        'ts_epoch': 1778019900,
    })

    assert response.status_code == 200, response.text
    with get_session() as session:
        decision = session.query(MessageDecision).one()
        assert decision.is_issue is False
        assert decision.chosen_source == 'guardrail_unsupported_other'
        assert session.query(Incident).count() == 0


def test_repair_status_only_elevator_update_does_not_queue_311(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'all')

    def fake_llm(*args, **kwargs):
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'elevator',
            'asset': None,
            'event_type': 'status_update',
            'severity': 2,
            'confidence': 85,
            'title': 'Elevator mechanic on site',
            'summary': 'Elevator mechanic is on site.',
            'refers_to_open_incident': True,
            'close_incident': False,
            'needs_review': False,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)

    response = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Elevator mechanic is here',
        'sender': 'Karen',
        'ts_epoch': 1777914420,
    })

    assert response.status_code == 200, response.text
    with get_session() as session:
        incident = session.query(Incident).one()
        decision = session.query(MessageDecision).one()
        assert incident.category == 'elevator'
        assert decision.auto_file_candidate is False
        assert session.query(FilingJob).count() == 0


def test_unrelated_other_rows_do_not_merge(client, monkeypatch):
    monkeypatch.setattr('packages.incident.extractor.LLM_MODE', 'all')

    def fake_llm(message_text, *args, **kwargs):
        if 'FDNY' in message_text:
            return {
                'is_issue': True,
                'signal_type': 'report',
                'category': 'other',
                'asset': None,
                'event_type': 'status_update',
                'severity': 2,
                'confidence': 80,
                'title': 'FDNY vehicle parked at building',
                'summary': 'FDNY is parked at the building.',
                'refers_to_open_incident': True,
                'close_incident': False,
                'needs_review': False,
            }
        return {
            'is_issue': True,
            'signal_type': 'report',
            'category': 'other',
            'asset': None,
            'event_type': 'new_issue',
            'severity': 2,
            'confidence': 85,
            'title': 'Liquid spill on stair A',
            'summary': 'Liquid spill on stair A 12th to 14th floors.',
            'refers_to_open_incident': False,
            'close_incident': False,
            'needs_review': True,
        }

    monkeypatch.setattr('packages.incident.extractor.llm_classify_message', fake_llm)

    first = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'FDNY parking too',
        'sender': 'Karen',
        'ts_epoch': 1777502040,
    })
    second = client.post('/ingest/whatsapp_web', headers=auth_headers(), json={
        'chat_name': '455 Tenants',
        'text': 'Liquid spill on stair A 12th to 14th flrs, possibly from beer tins seen earlier today',
        'sender': 'Karen',
        'ts_epoch': 1777502520,
    })

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    with get_session() as session:
        incidents = session.query(Incident).all()
        assert len(incidents) == 1
        assert incidents[0].title == 'Liquid spill on stair A'
        assert 'FDNY' not in incidents[0].summary


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

    captured_context = {}

    def fake_llm(message_text, open_incidents=None, recent_related=None, recent_chat=None):
        captured_context['open_incidents'] = open_incidents or []
        captured_context['recent_related'] = recent_related or []
        captured_context['recent_chat'] = recent_chat or []
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
        assert any('North elevator is out again' in row['text'] for row in (recent_chat or []))
        assert any(row.get('decision_category') == 'elevator' for row in (recent_related or []))
        assert any(row['category'] == 'elevator' for row in (open_incidents or []))
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

    assert any('North elevator is out again' in row['text'] for row in captured_context['recent_chat'])
    assert any(row.get('decision_category') == 'elevator' for row in captured_context['recent_related'])
    assert any(row['category'] == 'elevator' for row in captured_context['open_incidents'])

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

    with get_session() as session:
        incident_ids_before = {
            decision.message_id: decision.incident_id
            for decision in session.query(MessageDecision).all()
        }

    replay = client.post('/admin/reprocess_last/2', headers=auth_headers())
    assert replay.status_code == 200, replay.text

    with get_session() as session:
        incidents = session.query(Incident).all()
        witnesses = session.query(IncidentWitness).all()
        decisions = session.query(MessageDecision).all()
        assert len(incidents) == 1
        assert len(witnesses) == 1
        assert len(decisions) == 2
        assert {decision.message_id: decision.incident_id for decision in decisions} == incident_ids_before


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
