from packages.nyc311.portal import _extract_lookup_status, _lookup_matches
from packages.db import get_session, MessageDecision, Incident
from packages.nyc311.planner import incident_is_auto_eligible


def test_closed_dob_page_without_status_label_is_closed():
    text = ('SR Number\n311-28759855\nUpdated On\n09/09/2026, 07:18 AM\n'
            'Date Closed\n09/08/2026, 12:00 AM\nProblem\nElevator')
    assert _extract_lookup_status(text) == 'Closed'
    assert _lookup_matches(text, '311-28759855')
    assert not _lookup_matches(text, '311-28951783')


def test_lookup_does_not_accept_navigation_or_unlabeled_number():
    assert _extract_lookup_status('Service Request Status\nSign In | Sign Up') is None
    assert _extract_lookup_status('Status\nProblem\nElevator') is None
    assert not _lookup_matches('Searching for 311-28759855', '311-28759855')
    assert _extract_lookup_status('Date Closed\n-\nSR Status\nIn Progress') == 'In Progress'
    assert _extract_lookup_status('Date Closed\n09/08/2026, 12:00 AM\nSR Status\nIn Progress') == 'In Progress'


def test_311_health_keeps_failure_and_no_private_details(tmp_path, monkeypatch):
    from packages.nyc311.health import public_status, record_status_sync
    monkeypatch.setenv('NYC311_STATUS_STATE_PATH', str(tmp_path / '311.json'))
    assert public_status()['state'] == 'unverified'
    record_status_sync({'ok': False, 'updated': 0, 'total': 2, 'errors': 2,
                        'error_details': ['private portal text']})
    status = public_status()
    assert status['state'] == 'degraded'
    assert status['has_error']
    assert 'private' not in str(status)
    record_status_sync({'ok': True, 'updated': 2, 'total': 2, 'errors': 0,
                        'coverage_complete': True})
    assert public_status()['state'] == 'ready'


def test_explicit_outage_after_agreement_is_not_downgraded(client):
    headers = {'Authorization': 'Bearer test-token'}
    base = 1789408800
    for offset, text in enumerate(('Only one working at 2:20.', 'Yeah. South lift is out.', 'Mechanic is here.')):
        response = client.post('/ingest/tasker', headers=headers, json={
            'chat_name': '455 Tenants', 'sender': 'Test Resident',
            'text': text, 'ts_epoch': base + offset * 60,
        })
        assert response.status_code == 200
    with get_session() as session:
        decision = session.query(MessageDecision).filter(MessageDecision.event_type == 'outage').one()
        incident = session.get(Incident, decision.incident_id)
        assert incident.asset == 'elevator_south'
        assert incident_is_auto_eligible(incident)


def test_only_one_working_with_elevator_context_is_actionable(client):
    from packages.incident.extractor import _contextual_elevator_followup_choice
    from packages.db import RawMessage
    import unittest.mock
    with get_session() as session, unittest.mock.patch(
        'packages.incident.extractor._has_recent_same_chat_elevator_context', return_value=True
    ):
        row = RawMessage(message_id='context-fragment', chat_name='455 Tenants',
                         ts_epoch=1789408800, text='Only one working at 2:20.')
        choice = _contextual_elevator_followup_choice(session, row, {})
        assert choice['event_type'] == 'outage'
        assert choice['asset'] is None
        row.text = 'Mechanic is here.'
        assert _contextual_elevator_followup_choice(session, row, {})['event_type'] == 'status_update'
        row.text = 'Only one working at 2:20?'
        assert _contextual_elevator_followup_choice(session, row, {}) is None
