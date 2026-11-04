import json
from datetime import datetime, timezone

import pytest

from packages.db import Incident, MessageDecision, RawMessage, get_session
from packages.incident import extractor
from packages.incident.rules import classify_rules, explicit_elevator_asset
from packages.nyc311.planner import incident_is_auto_eligible
from tests.test_outage_word_boundaries import _stored_outage


@pytest.mark.parametrize('text', [
    'The south elevator skipped the lobby, went back up to 12, and would not open its doors.',
    'The south elevator went to random floors and then went back up. We took the stairs.',
    'Does anyone know why the south lift has not been fixed? This is worrying.',
    'North elevator is not working now.',
])
def test_motion_or_negated_repair_is_not_restoration(text):
    result = classify_rules(text)
    assert result.get('kind') != 'restore'
    assert result.get('event_type') != 'restore'


def test_question_about_other_car_does_not_change_reported_asset():
    text = 'South lift is not going to basement. Is North lift going to B?'
    assert explicit_elevator_asset(text) == 'elevator_south'
    choice = classify_rules(text)
    assert choice['category'] == 'elevator' and choice['is_issue']
    assert choice['event_type'] == 'new_issue'


@pytest.mark.parametrize('text', ['No elevators at all now', '6 days, 1 lift only'])
def test_canonical_outage_language_qualifies_without_second_regex_drift(client, text):
    with get_session() as session:
        incident, _ = _stored_outage(session, text)
        assert incident_is_auto_eligible(incident)


def _ingest(client, text, offset=0):
    return client.post('/ingest/whatsapp_web', headers={'Authorization':'Bearer test-token'},
        json={'chat_name':'Fixture building','sender':'Test tenant','text':text,
              'ts_epoch':int(datetime.now(timezone.utc).timestamp())-1000+offset}).json()['message_id']


@pytest.mark.parametrize('outage,restoration', [
    ('South elevator is broken.', 'The left side is working.'),
    ('Both elevators are out.', 'North is working now.'),
    ('Both elevators are out.', 'One elevator working now, mechanic came.'),
])
def test_partial_or_unmapped_restoration_cannot_close_unresolved_cars(client, monkeypatch, outage, restoration):
    monkeypatch.setattr(extractor, 'LLM_MODE', 'all')
    def model(text, *a, **k):
        restoring = text == restoration
        return dict(is_issue=True, signal_type='report', category='elevator',
            asset=explicit_elevator_asset(text), event_type='restore' if restoring else 'outage',
            severity=3, confidence=95, needs_review=False, close_incident=restoring)
    monkeypatch.setattr(extractor, 'llm_classify_message', model)
    monkeypatch.setattr(extractor, 'llm_review_decision', model)
    first = _ingest(client, outage)
    _ingest(client, restoration, 60)
    with get_session() as session:
        incident = session.get(Incident, session.get(MessageDecision, first).incident_id)
        assert incident.status == 'open'
        assert incident.end_ts_epoch is None


@pytest.mark.parametrize('text', ['Is the south elevator skipping the basement?', 'If the south elevator cannot go to the basement, use the stairs.', 'Tomorrow the south elevator will not stop at the basement.'])
def test_floor_questions_and_hypotheticals_are_not_fault_reports(text):
    assert classify_rules(text)['is_issue'] is False


def test_reclassifying_wrong_restoration_removes_its_closure(client, monkeypatch):
    monkeypatch.setattr(extractor, 'LLM_MODE', 'off')
    mid = _ingest(client, 'One elevator working now, mechanic came.')
    with get_session() as session:
        raw = session.get(RawMessage, mid)
        decision = session.get(MessageDecision, mid)
        incident = session.get(Incident, decision.incident_id)
        decision.event_type = 'restore'
        incident.status = 'closed'
        incident.end_ts_epoch = raw.ts_epoch
        incident.end_ts = raw.ts_iso
        session.flush()
        extractor.classify_and_upsert_incident(session, raw, allow_filing_job=False)
        assert incident.status == 'open'
        assert incident.end_ts_epoch is None


def test_source_cleanup_preserves_floor_call_failure():
    result = extractor._source_summary_from_text('It just cannot be called from\n\nbasement')
    assert 'called from' in result.casefold()
    assert 'basement' in result.casefold()


def test_both_working_still_closes_combined_outage(client, monkeypatch):
    monkeypatch.setattr(extractor, 'LLM_MODE', 'off')
    first = _ingest(client, 'Both elevators are out.')
    _ingest(client, 'Both elevators are working now.', 60)
    with get_session() as session:
        incident = session.get(Incident, session.get(MessageDecision, first).incident_id)
        assert incident.status == 'closed'


@pytest.mark.parametrize('text', ['North elevator is stuck, but south elevator is working now.', 'South elevator is not working, but north elevator is fixed.'])
def test_one_named_car_working_does_not_erase_other_named_fault(text):
    result = classify_rules(text)
    assert result['event_type'] == 'outage'
    assert result['asset'] == ('elevator_north' if text.startswith('North') else 'elevator_south')


def test_failed_model_review_is_not_recorded_as_confident_nonissue(client, monkeypatch):
    monkeypatch.setattr(extractor, 'LLM_MODE', 'all')
    error = dict(is_issue=False, signal_type='discussion', category='other', asset=None,
        event_type='non_issue', confidence=0, severity=2, needs_review=True,
        review_status='error', review_error='insufficient_quota')
    monkeypatch.setattr(extractor, 'llm_classify_message', lambda *a, **k: error)
    monkeypatch.setattr(extractor, 'llm_review_decision', lambda *a, **k: None)
    mid = _ingest(client, 'The elevator is behaving strangely.')
    with get_session() as session:
        decision = session.get(MessageDecision, mid)
        assert decision.needs_review is True
        assert json.loads(decision.llm_json)['review_status'] == 'error'


def test_smoke_report_summary_does_not_publish_personal_medical_context():
    text = 'As an asthma patient, I struggled because the stairs are filled with smoke and piles of cigarette butts.'
    choice = classify_rules(text)
    assert choice['is_issue'] and choice['category'] == 'other'
    summary = extractor._fact_safe_summary(text, choice, choice)
    assert summary == 'Smoke and cigarette debris were reported in the stairwell.'
    from packages.sheets.sync import _public_event_summary
    assert _public_event_summary(Incident(category='other'), RawMessage(message_id='fictional-smoke', text=text)) == summary


@pytest.mark.parametrize('text,required', [
    ('It just cannot be called from\n\nbasement', 'unable to be called from the basement'),
    ('South lift is not going to basement. Is North lift going to B?', 'South elevator was reported failing to serve the basement'),
    ('The south elevator went to random floors and then went back up.', 'unintended floors'),
])
def test_public_summary_preserves_the_actual_fault(text, required):
    from packages.sheets.sync import _public_event_summary
    summary = _public_event_summary(Incident(category='elevator'), RawMessage(message_id='fictional-floor', text=text))
    assert required in summary
    assert 'restored' not in summary.casefold()


def test_shutdown_instruction_is_not_literal_outage():
    from packages.incident.rules import CONTEXTUAL_CONFIRMED_OUTAGE
    assert not CONTEXTUAL_CONFIRMED_OUTAGE.search('The repair company says to shut it down for an hour.')


def test_named_one_car_restore_is_not_downgraded_to_unspecified_partial(client, monkeypatch):
    monkeypatch.setattr(extractor,'LLM_MODE','off')
    _ingest(client,'South elevator is out.')
    mid=_ingest(client,'South one is working now.',60)
    with get_session() as s:
        d=s.get(MessageDecision,mid)
        assert d.event_type=='restore'
        assert json.loads(d.final_json)['asset']=='elevator_south'


@pytest.mark.parametrize('text', ['Down to report them to 311 alongside the landlord. It is illegal under the smoke free air act.', 'Managing broken elevators means less time for common areas. They could put up no smoking signs today.'])
def test_advocacy_is_not_a_new_condition(text):
    assert classify_rules(text)['is_issue'] is False


def test_uncertain_current_operation_is_not_outage_or_restore():
    result=classify_rules("North lift was working yesterday. Don't know about this morning, though.")
    assert result['event_type']=='status_update'


def test_successful_basement_ride_does_not_prove_call_button_fixed(client,monkeypatch):
    monkeypatch.setattr(extractor,'LLM_MODE','off')
    _ingest(client,'South elevator is out.')
    mid=_ingest(client,'It was going to the basement all the times I took it today to do laundry',60)
    with get_session() as s:
        assert s.get(MessageDecision,mid).event_type=='status_update'



def test_partial_recovery_prevents_filing_old_both_down_claim(client, monkeypatch):
    monkeypatch.setattr(extractor,'LLM_MODE','off')
    first=_ingest(client,'Both elevators are out.')
    _ingest(client,'One elevator working now, mechanic came.',60)
    with get_session() as s:
        inc=s.get(Incident,s.get(MessageDecision,first).incident_id)
        assert not incident_is_auto_eligible(inc)


def test_historical_audit_correction_cannot_create_a_new_filing(client):
    with get_session() as s:
        inc,decision=_stored_outage(s,'Both elevators are out.')
        # The fixture returns its raw source; locate the stored decision.
        d=s.query(MessageDecision).filter_by(incident_id=inc.incident_id).one()
        final=json.loads(d.final_json or '{}');final['historical_audit']=True
        d.final_json=json.dumps(final)
        assert not incident_is_auto_eligible(inc)



def test_basement_call_complaint_does_not_claim_total_outage(client):
    from packages.nyc311.drafts import build_filing_draft
    with get_session() as s:
        inc,_=_stored_outage(s,'The elevator cannot be called from the basement.')
        draft=build_filing_draft(inc)
        assert draft.description=='Elevator does not respond to the basement call button.'
        assert 'defective operation' in draft.payload['title']
