from __future__ import annotations

import datetime as dt
from dataclasses import asdict, dataclass, field

from sqlalchemy import func, select

from packages.db import FilingJob, Incident, IncidentWitness, MessageDecision, RawMessage, ServiceRequestCase, WatchdogAction
from packages.tasker_capture import (
    CROSS_SOURCE_DUPLICATE_SOURCES,
    LIVE_CAPTURE_SOURCES,
    cross_source_duplicate_min_text_chars,
    cross_source_duplicate_window_seconds,
    cross_source_text_signature,
)
from packages.whatsapp.attachments import merge_attachment_manifests


@dataclass(frozen=True)
class CrossSourceDuplicatePair:
    archive_message_id: str
    live_message_id: str
    delta_seconds: int


@dataclass
class ReconciliationRow:
    archive_message_id: str
    live_message_id: str
    delta_seconds: int
    canonical_message_id: str
    duplicate_message_id: str
    canonical_incident_id: str = ""
    duplicate_incident_id: str = ""
    moved_service_cases: int = 0
    moved_filing_jobs: int = 0
    moved_watchdog_actions: int = 0
    action: str = "would_reconcile"

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass
class ReconciliationSummary:
    pairs_found: int = 0
    issue_identity_pairs: int = 0
    identity_only_pairs: int = 0
    reconciled: int = 0
    rows: list[ReconciliationRow] = field(default_factory=list)

    def as_dict(self) -> dict[str, int]:
        return {
            "pairs_found": self.pairs_found,
            "issue_identity_pairs": self.issue_identity_pairs,
            "identity_only_pairs": self.identity_only_pairs,
            "reconciled": self.reconciled,
        }


@dataclass(frozen=True)
class _ReconciliationIndex:
    raw_by_id: dict[str, RawMessage]
    decisions_by_message_id: dict[str, MessageDecision]
    incidents_by_id: dict[str, Incident]
    case_counts_by_incident_id: dict[str, int]
    job_counts_by_incident_id: dict[str, int]


def find_exact_cross_source_duplicate_pairs(session) -> list[CrossSourceDuplicatePair]:
    """Pair each archive/live alias once by exact normalized text and time."""
    window = cross_source_duplicate_window_seconds()
    min_chars = cross_source_duplicate_min_text_chars()
    if window <= 0:
        return []

    sources = CROSS_SOURCE_DUPLICATE_SOURCES + LIVE_CAPTURE_SOURCES
    rows = session.scalars(
        select(RawMessage)
        .where(RawMessage.source.in_(sources), RawMessage.ts_epoch.is_not(None))
        .order_by(RawMessage.ts_epoch, RawMessage.message_id)
    ).all()
    archives: dict[str, list[RawMessage]] = {}
    live: dict[str, list[RawMessage]] = {}
    for row in rows:
        signature = cross_source_text_signature(row.text)
        if len(signature) < min_chars:
            continue
        target = archives if row.source in CROSS_SOURCE_DUPLICATE_SOURCES else live
        target.setdefault(signature, []).append(row)

    pairs: list[CrossSourceDuplicatePair] = []
    for signature, archive_rows in archives.items():
        live_rows = live.get(signature, [])
        used_live_ids: set[str] = set()
        for archive in archive_rows:
            candidates = [
                item
                for item in live_rows
                if item.message_id not in used_live_ids
                and abs(int(item.ts_epoch or 0) - int(archive.ts_epoch or 0)) <= window
            ]
            if not candidates:
                continue
            candidate = min(
                candidates,
                key=lambda item: (abs(int(item.ts_epoch or 0) - int(archive.ts_epoch or 0)), item.message_id),
            )
            used_live_ids.add(candidate.message_id)
            pairs.append(
                CrossSourceDuplicatePair(
                    archive_message_id=archive.message_id,
                    live_message_id=candidate.message_id,
                    delta_seconds=abs(int(candidate.ts_epoch or 0) - int(archive.ts_epoch or 0)),
                )
            )
    return pairs


def _build_index(session, pairs: list[CrossSourceDuplicatePair]) -> _ReconciliationIndex:
    message_ids = {
        message_id
        for pair in pairs
        for message_id in (pair.archive_message_id, pair.live_message_id)
    }
    if not message_ids:
        return _ReconciliationIndex({}, {}, {}, {}, {})
    raws = session.scalars(select(RawMessage).where(RawMessage.message_id.in_(message_ids))).all()
    decisions = session.scalars(select(MessageDecision).where(MessageDecision.message_id.in_(message_ids))).all()
    incident_ids = {row.incident_id for row in decisions if row.incident_id}
    incidents = session.scalars(select(Incident).where(Incident.incident_id.in_(incident_ids))).all() if incident_ids else []
    case_counts: dict[str, int] = {}
    job_counts: dict[str, int] = {}
    if incident_ids:
        case_counts = {
            str(incident_id): int(count)
            for incident_id, count in session.execute(
                select(ServiceRequestCase.incident_id, func.count(ServiceRequestCase.id))
                .where(ServiceRequestCase.incident_id.in_(incident_ids))
                .group_by(ServiceRequestCase.incident_id)
            ).all()
            if incident_id
        }
        job_counts = {
            str(incident_id): int(count)
            for incident_id, count in session.execute(
                select(FilingJob.incident_id, func.count(FilingJob.job_id))
                .where(FilingJob.incident_id.in_(incident_ids))
                .group_by(FilingJob.incident_id)
            ).all()
            if incident_id
        }
    return _ReconciliationIndex(
        raw_by_id={row.message_id: row for row in raws},
        decisions_by_message_id={row.message_id: row for row in decisions},
        incidents_by_id={row.incident_id: row for row in incidents},
        case_counts_by_incident_id=case_counts,
        job_counts_by_incident_id=job_counts,
    )


def _decision(index: _ReconciliationIndex, raw: RawMessage) -> MessageDecision | None:
    return index.decisions_by_message_id.get(raw.message_id)


def _incident_for_decision(index: _ReconciliationIndex, decision: MessageDecision | None) -> Incident | None:
    if not decision or not decision.incident_id:
        return None
    return index.incidents_by_id.get(decision.incident_id)


def _raw_rank(index: _ReconciliationIndex, raw: RawMessage) -> tuple[int, int, int, int, int]:
    decision = _decision(index, raw)
    incident = _incident_for_decision(index, decision)
    case_count = 0
    job_count = 0
    if incident:
        case_count = index.case_counts_by_incident_id.get(incident.incident_id, 0)
        job_count = index.job_counts_by_incident_id.get(incident.incident_id, 0)
    rule_state = int((decision.event_type or "") in {"outage", "still_out", "restore"}) if decision else 0
    # Existing archive rows win ties because a later live capture can then
    # promote their metadata without changing evidence IDs already in use.
    archive_preference = int(raw.source in CROSS_SOURCE_DUPLICATE_SOURCES)
    return (case_count, job_count, int(incident is not None), rule_state, archive_preference)


def _pick_canonical_raw(index: _ReconciliationIndex, archive: RawMessage, live: RawMessage) -> tuple[RawMessage, RawMessage]:
    if _raw_rank(index, live) > _raw_rank(index, archive):
        return live, archive
    return archive, live


def _preview_row(index: _ReconciliationIndex, pair: CrossSourceDuplicatePair, *, action: str) -> ReconciliationRow | None:
    archive = index.raw_by_id.get(pair.archive_message_id)
    live = index.raw_by_id.get(pair.live_message_id)
    if archive is None or live is None:
        return None
    canonical, duplicate = _pick_canonical_raw(index, archive, live)
    canonical_decision = _decision(index, canonical)
    duplicate_decision = _decision(index, duplicate)
    canonical_incident = _incident_for_decision(index, canonical_decision)
    duplicate_incident = _incident_for_decision(index, duplicate_decision)
    return ReconciliationRow(
        archive_message_id=archive.message_id,
        live_message_id=live.message_id,
        delta_seconds=pair.delta_seconds,
        canonical_message_id=canonical.message_id,
        duplicate_message_id=duplicate.message_id,
        canonical_incident_id=canonical_incident.incident_id if canonical_incident else "",
        duplicate_incident_id=duplicate_incident.incident_id if duplicate_incident else "",
        action=action,
    )


def _is_issue_identity_pair(index: _ReconciliationIndex, pair: CrossSourceDuplicatePair) -> bool:
    """Return whether a duplicate has already changed incident/public state."""
    archive_decision = index.decisions_by_message_id.get(pair.archive_message_id)
    live_decision = index.decisions_by_message_id.get(pair.live_message_id)
    return bool(
        archive_decision
        and live_decision
        and archive_decision.is_issue
        and live_decision.is_issue
        and archive_decision.incident_id
        and live_decision.incident_id
    )


def _copy_decision(decision: MessageDecision, *, message_id: str) -> MessageDecision:
    return MessageDecision(
        message_id=message_id,
        incident_id=decision.incident_id,
        created_at=decision.created_at,
        chosen_source=decision.chosen_source,
        is_issue=decision.is_issue,
        category=decision.category,
        event_type=decision.event_type,
        confidence=decision.confidence,
        needs_review=decision.needs_review,
        auto_file_candidate=decision.auto_file_candidate,
        rules_json=decision.rules_json,
        llm_json=decision.llm_json,
        final_json=decision.final_json,
    )


def _promote_live_metadata(canonical: RawMessage, live: RawMessage) -> None:
    merged = merge_attachment_manifests(canonical.attachments, live.attachments)
    if merged:
        canonical.attachments = merged
    canonical.chat_name = live.chat_name or canonical.chat_name
    canonical.sender = live.sender or canonical.sender
    canonical.sender_hash = live.sender_hash or canonical.sender_hash
    canonical.ts_iso = live.ts_iso or canonical.ts_iso
    canonical.ts_epoch = live.ts_epoch if live.ts_epoch is not None else canonical.ts_epoch
    canonical.text = live.text or canonical.text
    canonical.source = live.source


def _move_incident_references(session, *, duplicate_incident_id: str, canonical_incident_id: str, duplicate_message_id: str) -> tuple[int, int, int]:
    moved_cases = 0
    moved_jobs = 0
    moved_actions = 0
    for decision in session.scalars(
        select(MessageDecision).where(
            MessageDecision.incident_id == duplicate_incident_id,
            MessageDecision.message_id != duplicate_message_id,
        )
    ).all():
        decision.incident_id = canonical_incident_id
    for case in session.scalars(select(ServiceRequestCase).where(ServiceRequestCase.incident_id == duplicate_incident_id)).all():
        case.incident_id = canonical_incident_id
        moved_cases += 1
    for job in session.scalars(select(FilingJob).where(FilingJob.incident_id == duplicate_incident_id)).all():
        # Preserve the submitted job and its original dedupe key. The canonical
        # incident already has a job/case, so queueing cannot create a third one.
        job.incident_id = canonical_incident_id
        moved_jobs += 1
    for action in session.scalars(select(WatchdogAction).where(WatchdogAction.related_incident_id == duplicate_incident_id)).all():
        action.related_incident_id = canonical_incident_id
        action.updated_at = _now_iso()
        moved_actions += 1
    session.query(IncidentWitness).filter(IncidentWitness.incident_id == duplicate_incident_id).delete(
        synchronize_session=False
    )
    return moved_cases, moved_jobs, moved_actions


def _now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")


def _recompute_incident_materialization(session, incident: Incident) -> None:
    rows = session.execute(
        select(MessageDecision, RawMessage)
        .join(RawMessage, RawMessage.message_id == MessageDecision.message_id)
        .where(MessageDecision.incident_id == incident.incident_id)
        .order_by(RawMessage.ts_epoch, RawMessage.message_id)
    ).all()
    issue_rows = [(decision, raw) for decision, raw in rows if decision.is_issue]
    incident.report_count = len(issue_rows)

    session.query(IncidentWitness).filter(IncidentWitness.incident_id == incident.incident_id).delete(
        synchronize_session=False
    )
    session.flush()
    sender_hashes = {raw.sender_hash for _, raw in issue_rows if raw.sender_hash}
    for sender in sorted(sender_hashes):
        session.add(IncidentWitness(incident_id=incident.incident_id, sender_hash=sender))
    incident.witness_count = len(sender_hashes)

    valid_ids = [raw.message_id for _, raw in issue_rows]
    existing_refs = [ref.strip() for ref in (incident.proof_refs or "").split(",") if ref.strip() in valid_ids]
    for message_id in valid_ids:
        if message_id not in existing_refs:
            existing_refs.append(message_id)
    incident.proof_refs = ",".join(existing_refs[:3])

    dated_rows = [raw for _, raw in issue_rows if raw.ts_epoch is not None]
    if dated_rows:
        first = min(dated_rows, key=lambda raw: (int(raw.ts_epoch or 0), raw.message_id))
        last = max(dated_rows, key=lambda raw: (int(raw.ts_epoch or 0), raw.message_id))
        incident.start_ts_epoch = first.ts_epoch
        incident.start_ts = first.ts_iso or incident.start_ts
        incident.last_ts_epoch = max(int(last.ts_epoch or 0), int(incident.end_ts_epoch or 0))
    incident.updated_at = _now_iso()


def _reconcile_pair(session, pair: CrossSourceDuplicatePair) -> ReconciliationRow | None:
    index = _build_index(session, [pair])
    archive = index.raw_by_id.get(pair.archive_message_id)
    live = index.raw_by_id.get(pair.live_message_id)
    if archive is None or live is None:
        return None

    canonical, duplicate = _pick_canonical_raw(index, archive, live)
    canonical_decision = _decision(index, canonical)
    duplicate_decision = _decision(index, duplicate)
    canonical_incident = _incident_for_decision(index, canonical_decision)
    duplicate_incident = _incident_for_decision(index, duplicate_decision)
    row = ReconciliationRow(
        archive_message_id=archive.message_id,
        live_message_id=live.message_id,
        delta_seconds=pair.delta_seconds,
        canonical_message_id=canonical.message_id,
        duplicate_message_id=duplicate.message_id,
        canonical_incident_id=canonical_incident.incident_id if canonical_incident else "",
        duplicate_incident_id=duplicate_incident.incident_id if duplicate_incident else "",
        action="reconciled",
    )

    if canonical_decision is None and duplicate_decision is not None:
        canonical_decision = _copy_decision(duplicate_decision, message_id=canonical.message_id)
        session.add(canonical_decision)
        canonical_incident = _incident_for_decision(index, canonical_decision)
        row.canonical_incident_id = canonical_incident.incident_id if canonical_incident else ""

    if (
        canonical_incident is not None
        and duplicate_incident is not None
        and canonical_incident.incident_id != duplicate_incident.incident_id
    ):
        row.moved_service_cases, row.moved_filing_jobs, row.moved_watchdog_actions = _move_incident_references(
            session,
            duplicate_incident_id=duplicate_incident.incident_id,
            canonical_incident_id=canonical_incident.incident_id,
            duplicate_message_id=duplicate.message_id,
        )

    if duplicate_decision is not None:
        session.delete(duplicate_decision)
        # RawMessage has no ORM relationship back to MessageDecision, so make
        # the dependent delete visible before deleting the raw alias. PostgreSQL
        # enforces this foreign key even when SQLite test defaults do not.
        session.flush()
    if canonical.source in CROSS_SOURCE_DUPLICATE_SOURCES and duplicate.source in LIVE_CAPTURE_SOURCES:
        _promote_live_metadata(canonical, duplicate)
    session.delete(duplicate)
    session.flush()

    if duplicate_incident and duplicate_incident.incident_id != row.canonical_incident_id:
        remaining = (
            session.query(MessageDecision).filter(MessageDecision.incident_id == duplicate_incident.incident_id).count()
            + session.query(FilingJob).filter(FilingJob.incident_id == duplicate_incident.incident_id).count()
            + session.query(ServiceRequestCase).filter(ServiceRequestCase.incident_id == duplicate_incident.incident_id).count()
            + session.query(WatchdogAction).filter(WatchdogAction.related_incident_id == duplicate_incident.incident_id).count()
        )
        if remaining == 0:
            stale = session.get(Incident, duplicate_incident.incident_id)
            if stale is not None:
                session.delete(stale)

    if canonical_incident is not None:
        _recompute_incident_materialization(session, canonical_incident)
    return row


def reconcile_exact_cross_source_duplicates(session, *, dry_run: bool = False) -> ReconciliationSummary:
    summary = ReconciliationSummary()
    pairs = find_exact_cross_source_duplicate_pairs(session)
    index = _build_index(session, pairs)
    issue_pairs: list[CrossSourceDuplicatePair] = []
    for pair in pairs:
        summary.pairs_found += 1
        if not _is_issue_identity_pair(index, pair):
            summary.identity_only_pairs += 1
            row = _preview_row(index, pair, action="review_identity_only")
            if row is not None:
                summary.rows.append(row)
            continue
        summary.issue_identity_pairs += 1
        issue_pairs.append(pair)
        if dry_run:
            row = _preview_row(index, pair, action="would_reconcile_issue_identity")
            if row is not None:
                summary.rows.append(row)
    if dry_run:
        return summary
    for pair in issue_pairs:
        row = _reconcile_pair(session, pair)
        if row is not None:
            summary.reconciled += 1
            summary.rows.append(row)
    return summary
