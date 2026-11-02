import json
from types import SimpleNamespace

import pytest

from packages.llm import classifier, openai_client as api
from packages.llm.triage_schema import SCHEMA, validate


def valid():
    return dict(is_issue=False, signal_type='discussion', category='other', asset=None,
        event_type='non_issue', severity=1, confidence=90, title='', summary='',
        refers_to_open_incident=False, close_incident=False, needs_review=False)


@pytest.mark.parametrize('field,value', [
    ('is_issue', 'false'), ('confidence', True), ('confidence', 110), ('severity', 0),
    ('needs_review', 'no'), ('asset', 'elevator_left'), ('event_type', 'guess'), ('summary', 'x' * 201),
])
def test_schema_rejects_coercion_and_out_of_range_values(field, value):
    output = valid(); output[field] = value
    with pytest.raises(api.OpenAIError):
        validate(output)


@pytest.mark.parametrize('mutation', ['missing', 'extra', 'inconsistent'])
def test_schema_rejects_missing_extra_and_inconsistent_fields(mutation):
    output = valid()
    if mutation == 'missing': output.pop('needs_review')
    if mutation == 'extra': output['submit'] = True
    if mutation == 'inconsistent': output['close_incident'] = True
    with pytest.raises(api.OpenAIError):
        validate(output)


def fake_api(monkeypatch, responses):
    calls = []
    class Client:
        def __init__(self, **_): pass
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def post(self, url, **kwargs):
            calls.append((url, kwargs['json']))
            status, body = responses.pop(0)
            return SimpleNamespace(status_code=status, json=lambda: body)
    monkeypatch.setattr(api.httpx, 'Client', Client)
    monkeypatch.setenv('OPENAI_API_KEY', 'test-key')
    return calls


@pytest.mark.parametrize('status,code', [(429, 'insufficient_quota'), (429, 'credit_balance_exhausted'), (401, 'invalid_api_key'), (400, 'invalid_request')])
def test_account_errors_are_not_retried_through_another_endpoint(monkeypatch, status, code):
    calls = fake_api(monkeypatch, [(status, {'error': {'code': code, 'message': 'PRIVATE MESSAGE'}})])
    with pytest.raises(api.OpenAIError) as exc:
        api.call_openai_json('fixture', 'fixture')
    assert len(calls) == 1 and code in str(exc.value)
    assert 'PRIVATE' not in str(exc.value)


@pytest.mark.parametrize('body', [
    [], {'status': 'incomplete', 'output_text': '{}'}, {'output_text': '[]'},
    {'output': 1}, {'output': [{'content': 1}]}, {'output': [{'content': [{'type': 'output_text', 'text': {}}]}]},
    {'output': [{'content': [{'type': 'refusal', 'refusal': 'no'}]}]},
    {'output_text': '{bad json}'},
])
def test_malformed_incomplete_and_refused_responses_are_errors(monkeypatch, body):
    fake_api(monkeypatch, [(200, body)])
    with pytest.raises(api.OpenAIError):
        api.call_openai_json('fixture', 'fixture')


@pytest.mark.parametrize('fallback', [False, True])
def test_strict_schema_is_sent_on_both_supported_endpoints(monkeypatch, fallback):
    responses = [(404, {})] if fallback else []
    responses.append((200, {'choices': [{'message': {'content': json.dumps(valid())}, 'finish_reason': 'stop'}]} if fallback
                      else {'status': 'completed', 'output_text': json.dumps(valid())}))
    calls = fake_api(monkeypatch, responses)
    assert api.call_openai_json('fixture', 'fixture', response_schema=SCHEMA, instructions='Trusted rules') == valid()
    assert calls[0][1]['instructions'] == 'Trusted rules'
    assert calls[0][1]['text']['format']['strict'] is True
    assert calls[0][1]['text']['format']['schema'] == SCHEMA
    if fallback:
        assert calls[-1][1]['response_format']['json_schema']['schema'] == SCHEMA
        assert calls[-1][1]['messages'][0]['content'] == 'Trusted rules'


def test_invalid_model_output_remains_an_explicit_failed_review(monkeypatch):
    monkeypatch.setattr(classifier, 'llm_enabled', lambda: True)
    monkeypatch.setattr(classifier, 'call_openai_json', lambda *a, **k: {'is_issue': 'true', 'confidence': 99})
    result = classifier.llm_classify_message('North elevator out')
    assert result['review_status'] == 'error'
    assert result['review_error'] == 'invalid_model_output'
    assert result['needs_review'] is True and result['confidence'] == 0


def test_reasoning_review_has_a_separate_bounded_budget(monkeypatch):
    monkeypatch.setenv('LLM_MAX_OUTPUT_TOKENS', '220')
    monkeypatch.setenv('LLM_REASONING_MAX_OUTPUT_TOKENS', '2048')
    monkeypatch.setenv('LLM_REASONING_EFFORT', 'low')
    assert classifier._model_options('gpt-4.1-mini')['max_output_tokens'] == 220
    options = classifier._model_options('gpt-5-mini')
    assert options['max_output_tokens'] == 2048
    assert options['reasoning_effort'] == 'low'
    calls = fake_api(monkeypatch, [(404, {}), (200, {'choices': [{'message': {'content': json.dumps(valid())}}]})])
    api.call_openai_json('fixture', 'gpt-5-mini', **options)
    assert calls[0][1]['reasoning'] == {'effort': 'low'}
    assert calls[-1][1]['max_completion_tokens'] == 2048
    assert 'temperature' not in calls[-1][1] and 'max_tokens' not in calls[-1][1]
