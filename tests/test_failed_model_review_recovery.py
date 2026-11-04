import json
from datetime import datetime, timezone

import pytest

from packages import worker_jobs
from packages.db import FilingJob, Incident, MessageDecision, RawMessage, get_session
from packages.incident import extractor
from packages.run_lock import try_file_lock


def error():
    return dict(is_issue=False, signal_type='discussion', category='other', asset=None,
        event_type='non_issue', severity=2, confidence=0, needs_review=True,
        review_status='error', review_error='insufficient_quota')


def success(*a, **k):
    return dict(is_issue=True, signal_type='report', category='elevator', asset='elevator_south',
        event_type='outage', severity=4, confidence=90, needs_review=False,
        close_incident=False, review_status='completed', title='South elevator outage', summary='South elevator out.')


def seed(source='whatsapp_web', age=60, mid='failed-review', incident_id=None):
    epoch = int(datetime.now(timezone.utc).timestamp()) - age
    with get_session() as s:
        s.add(RawMessage(message_id=mid, chat_name='Fictional Building', sender_hash='fixture',
            text='South elevator is out.', ts_epoch=epoch, ts_iso=datetime.fromtimestamp(epoch, timezone.utc).isoformat(), source=source))
        s.add(MessageDecision(message_id=mid, incident_id=incident_id, is_issue=False,
            chosen_source='none', needs_review=False, llm_json=json.dumps(error())))
        s.commit()


@pytest.fixture
def model(monkeypatch):
    monkeypatch.setattr('packages.llm.openai_client.llm_enabled', lambda: True)
    monkeypatch.setattr(extractor, 'llm_classify_message', success)
    monkeypatch.setattr(extractor, 'llm_review_decision', lambda *a, **k: None)
    monkeypatch.setattr(worker_jobs, '_safe_sync_sheets', lambda: True)


@pytest.mark.parametrize('source,age,expected_jobs', [('whatsapp_web',60,1),('whatsapp_web',172800,0),('zip_import',60,0)])
def test_recovered_recent_live_report_can_queue_but_history_cannot(client, model, tmp_path, source, age, expected_jobs):
    seed(source, age)
    result = worker_jobs.retry_failed_model_reviews(lock_path=tmp_path/'lock')
    assert result['recovered'] == 1
    assert result['queued_jobs'] == expected_jobs
    with get_session() as s:
        assert s.query(FilingJob).count() == expected_jobs
        assert json.loads(s.get(MessageDecision,'failed-review').llm_json)['review_status'] == 'completed'


def test_quota_failure_stops_batch_and_stays_visible(client, model, monkeypatch, tmp_path):
    seed(mid='first',age=120); seed(mid='second',age=60)
    calls=[]
    monkeypatch.setattr(extractor,'llm_classify_message',lambda *a,**k:calls.append(True) or error())
    mode=extractor.LLM_MODE
    result=worker_jobs.retry_failed_model_reviews(lock_path=tmp_path/'lock')
    assert result['ok'] is False and result['recovered']==0
    assert len(calls)==1 and extractor.LLM_MODE==mode
    with get_session() as s:
        assert s.get(MessageDecision,'first').needs_review
        assert s.query(FilingJob).count()==0


def test_recovery_waits_for_export_audit_lock(client, model, tmp_path):
    seed()
    path=tmp_path/'lock'
    with try_file_lock(path) as held:
        assert held
        assert worker_jobs.retry_failed_model_reviews(lock_path=path)['state']=='export_pipeline_busy'
    with get_session() as s:
        assert s.query(FilingJob).count()==0


@pytest.mark.parametrize('state',['submitted','submission_unknown','awaiting_human_verification'])
def test_existing_submission_or_hold_is_never_requeued(client, model, tmp_path, state):
    with get_session() as s:
        s.add(Incident(incident_id='protected',category='elevator',asset='elevator_south',status='open',title='Fixture'))
        s.add(FilingJob(incident_id='protected',dedupe_key='fixture-protected',state=state,attempts=1,payload_json='{}'))
        s.commit()
    seed(incident_id='protected')
    result=worker_jobs.retry_failed_model_reviews(lock_path=tmp_path/'lock')
    assert result['queued_jobs']==0
    with get_session() as s:
        job=s.query(FilingJob).one()
        assert job.state==state and job.attempts==1


def test_completed_secondary_review_does_not_starve_failed_reviews(client, model, tmp_path):
    seed(mid='reviewed',age=120)
    seed(mid='failed',age=60)
    with get_session() as s:
        row=s.get(MessageDecision,'reviewed')
        row.chosen_source='review_rule_state'
        row.final_json=json.dumps(success())
        s.commit()
    result=worker_jobs.retry_failed_model_reviews(limit=1,lock_path=tmp_path/'lock')
    assert result['recovered']==1
    with get_session() as s:
        assert json.loads(s.get(MessageDecision,'failed').llm_json)['review_status']=='completed'
