from packages.db import FilingJob, Incident, IncidentWitness, MessageDecision, ServiceRequestCase, get_session
from packages.incident.dedupe import dedupe_open_incidents
from packages.nyc311.planner import ensure_filing_jobs


def test_dedupe_open_incidents_merges_same_cluster_into_case_backed_incident(client):
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


def test_ensure_filing_jobs_closes_superseded_open_elevator_incidents(client):
    with get_session() as session:
        stale_open = Incident(
            incident_id='stale-open',
            category='elevator',
            asset='elevator_north',
            severity=4,
            status='open',
            start_ts='2026-04-10T13:15:37Z',
            start_ts_epoch=1775826937,
            last_ts_epoch=1775826937,
            title='North elevator alarm',
            summary='Older open elevator incident',
            proof_refs='m-open',
            report_count=1,
            witness_count=0,
            confidence=90,
            needs_review=False,
            updated_at='2026-04-10T13:15:37Z',
        )
        later_closed = Incident(
            incident_id='later-closed',
            category='elevator',
            asset=None,
            severity=5,
            status='closed',
            start_ts='2026-04-12T12:00:37Z',
            start_ts_epoch=1776000037,
            end_ts='2026-04-12T13:47:10Z',
            end_ts_epoch=1776001630,
            last_ts_epoch=1776001630,
            title='Elevator service resumed',
            summary='Later restore closed the outage.',
            proof_refs='m-closed',
            report_count=4,
            witness_count=2,
            confidence=95,
            needs_review=False,
            updated_at='2026-04-12T13:47:10Z',
        )
        session.add_all([stale_open, later_closed])
        session.commit()

        jobs = ensure_filing_jobs(session)
        session.commit()

        assert jobs == []

    with get_session() as session:
        repaired = session.get(Incident, 'stale-open')
        assert repaired is not None
        assert repaired.status == 'closed'
        assert repaired.end_ts == '2026-04-12T13:47:10Z'
        assert repaired.end_ts_epoch == 1776001630
        assert session.query(FilingJob).count() == 0
