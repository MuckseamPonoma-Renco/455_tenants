import json
from datetime import datetime,timezone
import pytest
from packages.db import RawMessage,MessageDecision,FilingJob,get_session
from packages.incident import extractor,source_observation as observations
from packages.llm import provider_health,openai_client
from packages import worker_jobs


def _raw(mid,text,age=60):
    epoch=int(datetime.now(timezone.utc).timestamp())-age
    return RawMessage(message_id=mid,chat_name='Fixture Tenants',sender_hash=mid,text=text,
                      source='whatsapp_web',ts_epoch=epoch,ts_iso=datetime.fromtimestamp(epoch,timezone.utc).isoformat())


@pytest.fixture
def strict(monkeypatch):
    monkeypatch.setenv('SOURCE_OBSERVATIONS_REQUIRED','1')
    monkeypatch.setenv('PUBLIC_UPDATES_CHAT_NAMES','Fixture Tenants')
    monkeypatch.setenv('AUTO_FILE_MAX_INCIDENT_AGE_HOURS','24')
    monkeypatch.setattr('packages.llm.openai_client.llm_enabled',lambda:True)
    monkeypatch.setattr(worker_jobs,'_safe_sync_sheets',lambda:True)


def test_explicit_live_outage_queues_with_provider_down_without_faking_model_review(client,strict,monkeypatch):
    monkeypatch.setattr(extractor,'llm_classify_message',lambda *a,**kw:pytest.fail('Exact report must not call paid provider'))
    with get_session() as s:
        r=_raw('exact','South lift out.');s.add(r);s.flush()
        extractor.classify_and_upsert_incident(s,r,allow_filing_job=True);s.commit()
        d=s.get(MessageDecision,r.message_id);final=json.loads(d.final_json)
        assert final['review_method']=='exact_statement_rule_v1'
        assert json.loads(d.llm_json)=={}
        assert final['observation']['state']=='outage'
        assert s.query(FilingJob).count()==1


@pytest.mark.parametrize('text', ['Is the South lift out?', 'South lift out tomorrow.', 'I heard South lift out.',
    'The South lift is not out.', 'If the South lift is out, call me.', 'Both elevators might be working',
    'South lift out. North working.', 'The lift in another building is out.', 'Nope only one', 'I was stuck earlier'])
def test_exact_path_does_not_guess_from_questions_context_or_history(text):
    assert observations.deterministic_live_observation(text) is None


def test_bounded_recovery_never_queues_outage_before_later_restoration(client,strict,monkeypatch,tmp_path):
    monkeypatch.setattr(extractor,'llm_classify_message',lambda *a,**kw:pytest.fail('Exact recovery should be independent'))
    with get_session() as s:
        for mid,text,age in [('out','South lift out.',120),('back','Both elevators currently working',60)]:
            s.add(_raw(mid,text,age));s.add(MessageDecision(message_id=mid,chosen_source='source_observation_pending',needs_review=True,llm_json=json.dumps({'review_status':'error','review_error':'insufficient_quota'})))
        s.commit()
    first=worker_jobs.retry_failed_model_reviews(limit=1,lock_path=tmp_path/'lock')
    assert first['remaining_reviews']==1 and first['queued_jobs']==0
    second=worker_jobs.retry_failed_model_reviews(limit=1,lock_path=tmp_path/'lock')
    assert second['remaining_reviews']==0 and second['queued_jobs']==0
    with get_session() as s: assert s.query(FilingJob).count()==0


def test_credit_breaker_is_explicit_bounded_and_never_echoes_credentials(monkeypatch):
    provider_health.record('credit_balance_exhausted')
    assert provider_health.public_status()['cause']=='credit_exhausted'
    monkeypatch.setattr(openai_client.httpx,'Client',lambda **kw:pytest.fail('Must not hammer exhausted account'))
    with pytest.raises(openai_client.OpenAIError,match='insufficient_quota'):
        openai_client.call_openai_json('private message','test-model')
    path=provider_health._path();d=json.loads(path.read_text());d['checked_at']='2000-01-01T00:00:00+00:00';path.write_text(json.dumps(d))
    assert provider_health.deferred_error() is None
    provider_health.record()
    assert provider_health.public_status()['has_error'] is False


def test_quota_does_not_request_second_model_review():
    assert extractor._should_request_review(None,{'needs_review':True,'review_status':'error','review_error':'insufficient_quota'},True) is False


def test_invalid_final_observation_retries_even_if_primary_model_said_completed(client,strict,tmp_path):
    with get_session() as s:
        s.add(_raw('invalid','South lift out.'))
        s.add(MessageDecision(message_id='invalid',chosen_source='source_observation_pending',needs_review=True,
            llm_json=json.dumps({'review_status':'completed'}),final_json=json.dumps({'observation':{'review_status':'error'}})))
        s.commit()
    result=worker_jobs.retry_failed_model_reviews(lock_path=tmp_path/'lock')
    assert result['selected']==1 and result['recovered']==1 and result['remaining_reviews']==0
