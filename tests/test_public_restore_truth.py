import pytest
from packages.db import Incident,RawMessage
from packages.sheets import sync


def rendered(text,asset='elevator_south'):
    incident=Incident(incident_id='fixture',category='elevator',asset=asset,title='Historical incident')
    raw=RawMessage(message_id='fictional-render-source',text=text)
    return sync._public_event_issue_label(incident,raw),sync._public_event_summary(incident,raw)


@pytest.mark.parametrize('negation',["couldn't",'could not','was unable to','failed to'])
def test_failed_repair_never_becomes_successful_operation(negation):
    text=f'Our mechanic {negation} get the South lift operational yesterday. Can anyone confirm or deny this rumour?'
    label,summary=rendered(text)
    assert label=='South elevator repair report unconfirmed'
    assert 'could not be made operational the previous day' in summary
    assert not sync._public_elevator_text_is_working_status(text)


def test_one_tentatively_working_does_not_inherit_both_asset():
    label,summary=rendered('Looks like 1 currently working','elevator_both')
    assert label=='One elevator tentatively reported working'
    assert 'full service was not confirmed' in summary


def test_unidentified_past_ride_does_not_inherit_south_asset():
    label,summary=rendered('We had one running about an hour ago')
    assert label=='One elevator reported working earlier'
    assert 'South' not in label+summary and 'current service was not confirmed' in summary


def test_running_cable_is_not_running_elevator():
    text='The laborers were running the comms cable for South lift emergency button. NOW techs are on site. South lift opened, but still not in use.'
    label,summary=rendered(text)
    assert label=='South elevator not in service'
    assert 'still out of use' in summary
    assert not sync._public_elevator_text_is_working_status(text)


def test_failed_attempt_does_not_erase_separate_later_success():
    text="The mechanic couldn't get the South lift operational yesterday. South elevator is working now."
    assert sync._public_elevator_text_is_working_status(text)


def test_smoke_is_not_an_electrical_category():
    incident=Incident(category='other')
    raw=RawMessage(message_id='fictional-smoke-source',text='The stairs are filled with smoke and piles of cigarette butts.')
    assert sync._public_event_category_label(incident,raw)=='Other building issue'
