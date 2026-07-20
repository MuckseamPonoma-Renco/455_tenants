from sqlalchemy import text

from packages.audit import sender_hash
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
from packages.incident.cross_source_reconciliation import reconcile_exact_cross_source_duplicates
from packages.incident.extractor import _merge_choices
from packages.nyc311.planner import ensure_filing_job_for_incident
from packages.tasker_capture import LIVE_CAPTURE_SOURCES, find_recent_cross_source_duplicate


def _incident(incident_id: str, *, start: int, proof_refs: str, report_count: int = 1) -> Incident:
    return Incident(
        incident_id=incident_id,
        category="elevator",
        asset="elevator_south",
        severity=4,
        status="open",
        start_ts="2026-07-17T23:20:00Z",
        start_ts_epoch=start,
        last_ts_epoch=start,
        title="South elevator outage",
        summary="South elevator is out.",
        proof_refs=proof_refs,
        report_count=report_count,
        witness_count=1,
        confidence=90,
        needs_review=False,
        updated_at="2026-07-17T23:20:00Z",
    )


def _raw(message_id: str, *, source: str, text: str, ts_epoch: int, sender: str) -> RawMessage:
    return RawMessage(
        message_id=message_id,
        chat_name="455 Tenants" if source == "whatsapp_web" else "Tenants WhatsApp",
        sender=sender,
        sender_hash=sender_hash(sender),
        ts_iso="2026-07-17T23:27:00Z",
        ts_epoch=ts_epoch,
        text=text,
        source=source,
    )


def test_cross_source_match_recognizes_concise_outage_update(client):
    with get_session() as session:
        live = _raw(
            "live-short-update",
            source="whatsapp_web",
            text="South lift out again.",
            ts_epoch=1784330820,
            sender="+1 (347) 581-0269",
        )
        short_reply = _raw(
            "live-short-reply",
            source="whatsapp_web",
            text="Thanks.",
            ts_epoch=1784330920,
            sender="+1 (347) 581-0269",
        )
        session.add_all([live, short_reply])
        session.commit()

        assert find_recent_cross_source_duplicate(
            session,
            text="South lift out again.",
            ts_epoch=1784330858,
            sources=LIVE_CAPTURE_SOURCES,
        ) == live
        assert find_recent_cross_source_duplicate(
            session,
            text="Thanks.",
            ts_epoch=1784330930,
            sources=LIVE_CAPTURE_SOURCES,
        ) is None


def test_authoritative_rule_state_locks_still_out_and_restore():
    rule = {
        "is_issue": True,
        "signal_type": "report",
        "category": "elevator",
        "asset": "elevator_south",
        "event_type": "still_out",
        "severity": 4,
        "confidence": 85,
        "title": "South elevator outage",
        "summary": "South elevator remains out.",
        "close_incident": False,
        "needs_review": False,
    }
    llm = {
        **rule,
        "event_type": "new_issue",
        "close_incident": True,
        "confidence": 95,
        "summary": "Model says this is a new issue.",
    }

    chosen, source = _merge_choices(rule, llm)

    assert source == "hybrid_rule_state"
    assert chosen is not None
    assert chosen["event_type"] == "still_out"
    assert chosen["close_incident"] is False
    assert chosen["needs_review"] is True

    restored, restored_source = _merge_choices(
        {**rule, "event_type": "restore", "close_incident": True},
        {**llm, "is_issue": False, "signal_type": "discussion"},
    )

    assert restored_source == "guardrail_non_issue_rule_state"
    assert restored is not None
    assert restored["event_type"] == "restore"
    assert restored["close_incident"] is True
    assert restored["is_issue"] is True


def test_reconciliation_merges_exact_aliases_without_losing_submitted_cases(client):
    archive_message_id = "south-archive-alias"
    live_message_id = "south-live-alias"
    live_sender = "+1 (347) 581-0269"
    first = _raw(
        "south-initial",
        source="tasker",
        text="South lift not working, and north lift rattling.",
        ts_epoch=1784200000,
        sender="Molly",
    )
    archive = _raw(
        archive_message_id,
        source="zip_import",
        text="South lift out again.",
        ts_epoch=1784330858,
        sender="~ Molly",
    )
    live = _raw(
        live_message_id,
        source="whatsapp_web",
        text="South lift out again.",
        ts_epoch=1784330820,
        sender=live_sender,
    )
    with get_session() as session:
        session.execute(text("PRAGMA foreign_keys = ON"))
        canonical = _incident("south-canonical", start=first.ts_epoch or 0, proof_refs="south-initial,south-archive-alias", report_count=2)
        duplicate = _incident("south-duplicate", start=live.ts_epoch or 0, proof_refs="south-live-alias")
        session.add_all([first, archive, live, canonical, duplicate])
        session.add_all([
            MessageDecision(message_id=first.message_id, incident_id=canonical.incident_id, is_issue=True, category="elevator", event_type="outage"),
            MessageDecision(message_id=archive.message_id, incident_id=canonical.incident_id, is_issue=True, category="elevator", event_type="still_out"),
            MessageDecision(message_id=live.message_id, incident_id=duplicate.incident_id, is_issue=True, category="elevator", event_type="new_issue"),
            IncidentWitness(incident_id=canonical.incident_id, sender_hash=first.sender_hash),
            IncidentWitness(incident_id=canonical.incident_id, sender_hash=archive.sender_hash),
            IncidentWitness(incident_id=duplicate.incident_id, sender_hash=live.sender_hash),
            FilingJob(job_id=10, dedupe_key="311:south-canonical", incident_id=canonical.incident_id, state="submitted"),
            FilingJob(job_id=11, dedupe_key="311:south-duplicate", incident_id=duplicate.incident_id, state="submitted"),
            ServiceRequestCase(id=10, service_request_number="311-28291674", incident_id=canonical.incident_id, filing_job_id=10, status="In Progress"),
            ServiceRequestCase(id=11, service_request_number="311-28291836", incident_id=duplicate.incident_id, filing_job_id=11, status="In Progress"),
            WatchdogAction(
                id=10,
                action_type="elevator_followup",
                severity="watch",
                title="Follow up on south elevator",
                status="open",
                related_incident_id=duplicate.incident_id,
            ),
        ])
        session.commit()

        preview = reconcile_exact_cross_source_duplicates(session, dry_run=True)
        assert preview.pairs_found == 1
        assert preview.issue_identity_pairs == 1
        assert preview.identity_only_pairs == 0
        assert preview.rows[0].canonical_message_id == archive.message_id
        assert session.get(RawMessage, live.message_id) is not None

        summary = reconcile_exact_cross_source_duplicates(session)
        session.commit()

        assert summary.pairs_found == 1
        assert summary.issue_identity_pairs == 1
        assert summary.identity_only_pairs == 0
        assert summary.reconciled == 1
        assert summary.rows[0].moved_service_cases == 1
        assert summary.rows[0].moved_filing_jobs == 1
        assert summary.rows[0].moved_watchdog_actions == 1
        # SQLite keeps this connection-level pragma until it is reset. Keep
        # later fixture tests on their normal permissive test configuration.
        session.connection().connection.driver_connection.execute("PRAGMA foreign_keys = OFF")

    with get_session() as session:
        repaired = session.get(Incident, "south-canonical")
        assert repaired is not None
        assert session.get(Incident, "south-duplicate") is None
        assert repaired.report_count == 2
        assert repaired.witness_count == 2
        assert repaired.proof_refs == "south-initial,south-archive-alias"
        assert session.get(RawMessage, live.message_id) is None
        canonical_raw = session.get(RawMessage, archive_message_id)
        assert canonical_raw is not None
        assert canonical_raw.source == "whatsapp_web"
        assert canonical_raw.sender == live_sender
        assert canonical_raw.ts_epoch == 1784330820
        assert session.get(MessageDecision, live_message_id) is None
        assert session.get(MessageDecision, archive_message_id).incident_id == repaired.incident_id
        assert {case.incident_id for case in session.query(ServiceRequestCase).all()} == {repaired.incident_id}
        assert {job.incident_id for job in session.query(FilingJob).all()} == {repaired.incident_id}
        assert session.get(WatchdogAction, 10).related_incident_id == repaired.incident_id
        assert session.query(FilingJob).count() == 2
        assert ensure_filing_job_for_incident(session, repaired) is not None
        assert session.query(FilingJob).count() == 2


def test_reconciliation_rosters_identity_only_alias_without_deleting_evidence(client):
    archive_message_id = "archive-identity-only"
    live_message_id = "live-identity-only"
    archive = _raw(
        archive_message_id,
        source="export",
        text="The superintendent said the lobby package room will reopen tomorrow.",
        ts_epoch=1784330858,
        sender="Molly",
    )
    live = _raw(
        live_message_id,
        source="whatsapp_web",
        text="The superintendent said the lobby package room will reopen tomorrow.",
        ts_epoch=1784330820,
        sender="+1 (347) 581-0269",
    )
    with get_session() as session:
        session.add_all([archive, live])
        session.add_all([
            MessageDecision(message_id=archive.message_id, is_issue=False, event_type="non_issue"),
            MessageDecision(message_id=live.message_id, is_issue=False, event_type="non_issue"),
        ])
        session.commit()

        summary = reconcile_exact_cross_source_duplicates(session)
        session.commit()

        assert summary.pairs_found == 1
        assert summary.issue_identity_pairs == 0
        assert summary.identity_only_pairs == 1
        assert summary.reconciled == 0
        assert summary.rows[0].action == "review_identity_only"

    with get_session() as session:
        assert session.get(RawMessage, archive_message_id) is not None
        assert session.get(RawMessage, live_message_id) is not None
