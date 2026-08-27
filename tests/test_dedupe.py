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
from packages.incident.dedupe import dedupe_open_elevator_continuations, dedupe_open_incidents
from packages.incident.reconcile import (
    close_superseded_open_elevator_incidents,
    repair_unbounded_elevator_restore_attachments,
)
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


def test_dedupe_merges_unscoped_elevator_continuation_into_specific_outage(client):
    with get_session() as session:
        unknown = Incident(
            incident_id='unknown-continuation',
            category='elevator',
            asset=None,
            severity=4,
            status='open',
            start_ts_epoch=1787763180,
            last_ts_epoch=1787763180,
            title='Elevator service reduced',
            summary='Down to one elevator.',
            proof_refs='m-unknown',
            report_count=1,
            witness_count=1,
            confidence=90,
            needs_review=False,
        )
        north = Incident(
            incident_id='north-continuation',
            category='elevator',
            asset='elevator_north',
            severity=4,
            status='open',
            start_ts_epoch=1787778180,
            last_ts_epoch=1787831880,
            title='North elevator still down',
            summary='North elevator still down.',
            proof_refs='m-north',
            report_count=2,
            witness_count=1,
            confidence=95,
            needs_review=False,
        )
        session.add_all([unknown, north])
        session.add_all([
            RawMessage(
                message_id='m-unknown',
                chat_name='455 Tenants',
                sender='Karen',
                sender_hash='sender-a',
                ts_epoch=1787763180,
                text='Down to one elevator.',
                source='tasker',
            ),
            RawMessage(
                message_id='m-north',
                chat_name='455 Tenants',
                sender='Molly',
                sender_hash='sender-b',
                ts_epoch=1787831880,
                text='North elevator still down.',
                source='tasker',
            ),
        ])
        session.add_all([
            MessageDecision(
                message_id='m-unknown',
                incident_id=unknown.incident_id,
                is_issue=True,
                category='elevator',
                event_type='still_out',
            ),
            MessageDecision(
                message_id='m-north',
                incident_id=north.incident_id,
                is_issue=True,
                category='elevator',
                event_type='still_out',
            ),
        ])
        session.add_all([
            FilingJob(dedupe_key='311:unknown-continuation', incident_id=unknown.incident_id, state='awaiting_approval'),
            FilingJob(dedupe_key='311:north-continuation', incident_id=north.incident_id, state='awaiting_approval'),
        ])
        session.add(WatchdogAction(
            action_type='elevator_outage',
            severity='high',
            title='Track current outage',
            related_incident_id=unknown.incident_id,
        ))
        session.commit()

        summary = dedupe_open_elevator_continuations(session)
        session.commit()
        assert summary.merged_incidents == 1
        assert summary.deleted_jobs == 1

    with get_session() as session:
        incident = session.query(Incident).one()
        assert incident.incident_id == 'north-continuation'
        assert incident.asset == 'elevator_north'
        assert incident.report_count == 3
        assert {row.incident_id for row in session.query(MessageDecision).all()} == {'north-continuation'}
        assert session.query(FilingJob).count() == 1
        assert session.query(WatchdogAction).one().related_incident_id == 'north-continuation'


def test_unbounded_restore_attachment_is_repaired_without_reclosing_stale_incident(client):
    outage_epoch = 1780000000
    restore_epoch = outage_epoch + 20 * 24 * 3600
    with get_session() as session:
        stale = Incident(
            incident_id='stale-restore-attachment',
            category='elevator',
            asset='elevator_both',
            severity=4,
            status='closed',
            start_ts_epoch=outage_epoch,
            last_ts_epoch=restore_epoch,
            end_ts_epoch=restore_epoch,
            title='North elevator outage',
            summary='North elevator was reported down. | Both are working currently.',
            proof_refs='old-outage,late-restore',
            report_count=2,
            witness_count=2,
            confidence=90,
            needs_review=False,
        )
        canonical_restore = Incident(
            incident_id='canonical-restore',
            category='elevator',
            asset='elevator_both',
            severity=2,
            status='closed',
            start_ts_epoch=restore_epoch,
            last_ts_epoch=restore_epoch,
            end_ts_epoch=restore_epoch,
            title='Elevator restored',
            summary='Both are working currently.',
            proof_refs='late-restore',
            report_count=1,
            witness_count=1,
            confidence=95,
            needs_review=False,
        )
        session.add_all([stale, canonical_restore])
        session.add_all([
            RawMessage(
                message_id='old-outage',
                chat_name='455 Tenants',
                sender='Karen',
                sender_hash='sender-a',
                ts_iso='2026-05-27T00:00:00Z',
                ts_epoch=outage_epoch,
                text='North elevator is down.',
                source='zip_import',
            ),
            RawMessage(
                message_id='late-restore',
                chat_name='455 Tenants',
                sender='Molly',
                sender_hash='sender-b',
                ts_iso='2026-06-16T00:00:00Z',
                ts_epoch=restore_epoch,
                text='Both are working currently.',
                source='zip_import',
            ),
        ])
        session.add_all([
            MessageDecision(
                message_id='old-outage',
                incident_id=stale.incident_id,
                is_issue=True,
                category='elevator',
                event_type='outage',
                final_json='{"asset":"elevator_north"}',
            ),
            MessageDecision(
                message_id='late-restore',
                incident_id=canonical_restore.incident_id,
                is_issue=True,
                category='elevator',
                event_type='restore',
                final_json='{"asset":"elevator_both"}',
            ),
        ])
        session.commit()

        repaired = repair_unbounded_elevator_restore_attachments(session)
        reconciled = close_superseded_open_elevator_incidents(session)
        session.commit()
        assert repaired.repaired_incidents == 1
        assert reconciled.closed_superseded == 0

    with get_session() as session:
        row = session.get(Incident, 'stale-restore-attachment')
        assert row.status == 'open'
        assert row.asset == 'elevator_north'
        assert row.last_ts_epoch == outage_epoch
        assert row.end_ts_epoch is None
        assert row.proof_refs == 'old-outage'
        assert row.report_count == 1
        assert row.witness_count == 1
        assert 'working currently' not in row.summary
