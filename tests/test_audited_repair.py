import pytest
from packages.db import Incident,get_session
from packages.incident.audited_repair import apply_audited_patch


def fixture(s):
    s.add(Incident(incident_id='repair',category='elevator',asset='elevator_south',title='Fixture',summary='Wrong summary'))
    s.commit()
    row=s.get(Incident,'repair')
    before={c.name:getattr(row,c.name) for c in row.__table__.columns}
    after={**before,'summary':'Verified source summary'}
    return {'schema_version':1,'changes':[{'table':'incidents','key':{'incident_id':'repair'},'before':before,'after':after}]}


def test_audited_patch_is_exact_and_idempotent(client):
    with get_session() as s:
        patch=fixture(s)
        assert apply_audited_patch(s,patch)['changed_rows']==1
        s.commit()
        assert apply_audited_patch(s,patch)['changed_rows']==0


def test_audited_patch_rejects_stale_preview_without_any_write(client):
    with get_session() as s:
        patch=fixture(s)
        s.get(Incident,'repair').summary='New live report'
        s.commit()
        with pytest.raises(ValueError,match='stale'): apply_audited_patch(s,patch)
        assert s.get(Incident,'repair').summary=='New live report'


def test_audited_patch_cannot_touch_filing_queue(client):
    with get_session() as s:
        with pytest.raises(ValueError,match='protected'):
            apply_audited_patch(s,{'schema_version':1,'changes':[{'table':'filing_jobs'}]})


def test_audited_patch_preview_performs_no_writes(client):
    with get_session() as s:
        patch=fixture(s)
        assert apply_audited_patch(s,patch,dry_run=True)['would_change_rows']==1
        assert s.get(Incident,'repair').summary=='Wrong summary'


def test_historical_patch_cannot_outrun_witness_sequence(client):
    with get_session() as s:
        patch=fixture(s)
        patch['changes']=[{'table':'incident_witnesses','key':{'id':999999},'before':None,'after':{'id':999999,'incident_id':'repair','sender_hash':'new-sender'}}]
        with pytest.raises(ValueError,match='database sequence'):
            apply_audited_patch(s,patch)
