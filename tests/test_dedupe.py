from packages.db import FilingJob, Incident, IncidentWitness, MessageDecision, ServiceRequestCase, get_session
from packages.incident.dedupe import dedupe_open_incidents


def test_dedupe_open_incidents_merges_same_cluster_into_case_backed_incident():
    with get_session() as session:
        older = Incident(
            incident_id='old-inc',
            category='elevator',
            asset='elevator_both',
            severity=4,
            status='open',
            start_ts='2026-03-18T12:00:00Z',
            start_ts_epoch=1770000000,
            last_ts_epoch=1770000000,
            title='Older outage',
            summary='Older summary',
            proof_refs='m1',
            report_count=1,
            witness_count=1,
            confidence=80,
            needs_review=False,
            updated_at='2026-03-18T12:00:00Z',
        )
        newer = Incident(
            incident_id='new-inc',
            category='elevator',
            asset='elevator_both',
            severity=5,
            status='open',
            start_ts='2026-03-20T12:00:00Z',
            start_ts_epoch=1770172800,
            last_ts_epoch=1770172800,
            title='Newer outage',
            summary='Newer summary',
            proof_refs='m2',
            report_count=2,
            witness_count=1,
            confidence=90,
            needs_review=False,
            updated_at='2026-03-20T12:00:00Z',
        )
        session.add_all([older, newer])
        session.add(IncidentWitness(incident_id='old-inc', sender_hash='a'))
        session.add(IncidentWitness(incident_id='new-inc', sender_hash='b'))
        session.add(MessageDecision(message_id='m1', incident_id='old-inc'))
        session.add(MessageDecision(message_id='m2', incident_id='new-inc'))
        session.add(FilingJob(job_id=10, dedupe_key='311:old-inc', incident_id='old-inc', state='skipped'))
        session.add(ServiceRequestCase(id=10, service_request_number='311-12345678', incident_id='new-inc', status='submitted'))
        session.commit()

        summary = dedupe_open_incidents(session, gap_seconds=7 * 24 * 3600, dry_run=False)
        session.commit()

        assert summary.merged_incidents == 1
        assert summary.deleted_jobs == 1

    with get_session() as session:
        incidents = {row.incident_id: row for row in session.query(Incident).all()}
        assert 'old-inc' not in incidents
        assert 'new-inc' in incidents
        merged = incidents['new-inc']
        assert merged.report_count == 3
        assert merged.witness_count == 2
        assert 'Older summary' in (merged.summary or '')
        assert 'm1' in (merged.proof_refs or '')
        decision = session.get(MessageDecision, 'm1')
        assert decision is not None
        assert decision.incident_id == 'new-inc'
        assert session.query(FilingJob).count() == 0
