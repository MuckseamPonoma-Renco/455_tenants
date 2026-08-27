from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from sqlalchemy import select

from packages.db import FilingJob, Incident, IncidentWitness, MessageDecision, ServiceRequestCase, WatchdogAction

DEFAULT_DEDUPE_GAP_SECONDS = 7 * 24 * 3600


@dataclass
class DedupeSummary:
    merged_incidents: int = 0
    deleted_jobs: int = 0
    moved_jobs: int = 0
    updated_decisions: int = 0
    updated_witnesses: int = 0
    updated_watchdog_actions: int = 0
    clusters_merged: int = 0
    clusters_skipped_multi_case: int = 0


def _merge_text(base: str | None, extra: str | None, *, limit: int = 2000) -> str:
    left = (base or "").strip()
    right = (extra or "").strip()
    if not right:
        return left[:limit]
    if not left:
        return right[:limit]
    if right in left:
        return left[:limit]
    return f"{left} | {right}"[:limit]


def _merge_proof_refs(base: str | None, extra: str | None, *, limit: int = 20) -> str:
    seen: list[str] = []
    for raw in (base, extra):
        for ref in (raw or "").split(","):
            clean = ref.strip()
            if clean and clean not in seen:
                seen.append(clean)
    return ",".join(seen[:limit])


def _job_rank(job: FilingJob) -> tuple[int, str, int]:
    order = {
        "submitted": 0,
        "claimed": 1,
        "pending": 2,
        "failed": 3,
        "skipped": 4,
    }
    return (order.get(job.state or "", 9), job.created_at or "", int(job.job_id or 0))


def _pick_canonical(session, cluster: list[Incident]) -> Incident | None:
    case_counts = {
        inc.incident_id: session.query(ServiceRequestCase).filter_by(incident_id=inc.incident_id).count()
        for inc in cluster
    }
    cased = [inc for inc in cluster if case_counts[inc.incident_id] > 0]
    if len(cased) > 1:
        return None
    if len(cased) == 1:
        return cased[0]

    job_map = {
        inc.incident_id: session.scalars(select(FilingJob).where(FilingJob.incident_id == inc.incident_id)).all()
        for inc in cluster
    }
    live_job_incidents = [
        inc for inc in cluster
        if any(job.state in {"submitted", "claimed", "pending", "failed"} for job in job_map[inc.incident_id])
    ]
    candidates = live_job_incidents or cluster
    return max(candidates, key=lambda inc: int(inc.last_ts_epoch or 0))


def _merge_cluster(session, canonical: Incident, cluster: list[Incident], summary: DedupeSummary) -> None:
    canonical_jobs = session.scalars(select(FilingJob).where(FilingJob.incident_id == canonical.incident_id)).all()
    canonical_has_case = session.query(ServiceRequestCase).filter_by(incident_id=canonical.incident_id).count() > 0
    existing_hashes = {
        row.sender_hash
        for row in session.scalars(
            select(IncidentWitness).where(IncidentWitness.incident_id == canonical.incident_id)
        ).all()
    }

    for inc in cluster:
        if inc.incident_id == canonical.incident_id:
            continue

        canonical.start_ts_epoch = min(
            value for value in [canonical.start_ts_epoch, inc.start_ts_epoch] if value is not None
        ) if (canonical.start_ts_epoch is not None or inc.start_ts_epoch is not None) else None
        if canonical.start_ts_epoch == inc.start_ts_epoch and inc.start_ts:
            canonical.start_ts = inc.start_ts
        canonical.last_ts_epoch = max(
            value for value in [canonical.last_ts_epoch, inc.last_ts_epoch] if value is not None
        ) if (canonical.last_ts_epoch is not None or inc.last_ts_epoch is not None) else None
        canonical.severity = max(int(canonical.severity or 0), int(inc.severity or 0))
        canonical.confidence = max(int(canonical.confidence or 0), int(inc.confidence or 0))
        canonical.report_count = int(canonical.report_count or 0) + int(inc.report_count or 0)
        canonical.needs_review = bool(canonical.needs_review or inc.needs_review)
        canonical.summary = _merge_text(canonical.summary, inc.summary)
        canonical.proof_refs = _merge_proof_refs(canonical.proof_refs, inc.proof_refs)

        for row in session.scalars(select(MessageDecision).where(MessageDecision.incident_id == inc.incident_id)).all():
            row.incident_id = canonical.incident_id
            row.incident = canonical
            summary.updated_decisions += 1

        for witness in session.scalars(select(IncidentWitness).where(IncidentWitness.incident_id == inc.incident_id)).all():
            if witness.sender_hash not in existing_hashes:
                session.add(IncidentWitness(incident_id=canonical.incident_id, sender_hash=witness.sender_hash))
                existing_hashes.add(witness.sender_hash)
                summary.updated_witnesses += 1
            session.delete(witness)

        for action in session.scalars(
            select(WatchdogAction).where(WatchdogAction.related_incident_id == inc.incident_id)
        ).all():
            action.related_incident_id = canonical.incident_id
            summary.updated_watchdog_actions += 1

        incoming_jobs = sorted(
            session.scalars(select(FilingJob).where(FilingJob.incident_id == inc.incident_id)).all(),
            key=_job_rank,
        )
        for job in incoming_jobs:
            if canonical_has_case:
                session.delete(job)
                summary.deleted_jobs += 1
                continue
            if job.state == "skipped":
                session.delete(job)
                summary.deleted_jobs += 1
                continue
            if canonical_jobs:
                session.delete(job)
                summary.deleted_jobs += 1
                continue
            job.incident_id = canonical.incident_id
            job.dedupe_key = f"311:{canonical.incident_id}"
            canonical_jobs.append(job)
            summary.moved_jobs += 1

        session.delete(inc)
        summary.merged_incidents += 1

    session.flush()
    canonical.witness_count = session.query(IncidentWitness).filter_by(incident_id=canonical.incident_id).count()
    summary.clusters_merged += 1


def _incident_gap_seconds(left: Incident, right: Incident) -> int:
    left_start = int(left.start_ts_epoch or left.last_ts_epoch or 0)
    left_end = int(left.last_ts_epoch or left.start_ts_epoch or 0)
    right_start = int(right.start_ts_epoch or right.last_ts_epoch or 0)
    right_end = int(right.last_ts_epoch or right.start_ts_epoch or 0)
    if left_end < right_start:
        return right_start - left_end
    if right_end < left_start:
        return left_start - right_end
    return 0


def _continuation_only(session, incident: Incident) -> bool:
    events = session.scalars(
        select(MessageDecision.event_type).where(
            MessageDecision.incident_id == incident.incident_id,
            MessageDecision.is_issue.is_(True),
        )
    ).all()
    return bool(events) and all(event in {"still_out", "status_update"} for event in events)


def dedupe_open_elevator_continuations(
    session,
    *,
    gap_seconds: int = DEFAULT_DEDUPE_GAP_SECONDS,
    dry_run: bool = False,
) -> DedupeSummary:
    """Merge an unscoped continuation into the closest open elevator incident."""
    summary = DedupeSummary()
    used_ids: set[str] = set()

    while True:
        incidents = session.scalars(
            select(Incident)
            .where(Incident.category == "elevator", Incident.status != "closed")
            .order_by(Incident.last_ts_epoch)
        ).all()
        latest_activity = max((int(row.last_ts_epoch or 0) for row in incidents), default=0)
        if latest_activity:
            incidents = [
                row
                for row in incidents
                if int(row.last_ts_epoch or 0) >= latest_activity - gap_seconds
            ]
        pair: tuple[Incident, Incident] | None = None
        for unknown in (row for row in incidents if row.asset is None and row.incident_id not in used_ids):
            candidates = [
                row
                for row in incidents
                if row.incident_id != unknown.incident_id
                and row.incident_id not in used_ids
                and row.asset is not None
                and _incident_gap_seconds(unknown, row) <= gap_seconds
                and _continuation_only(session, unknown)
            ]
            if not candidates:
                continue
            candidates.sort(key=lambda row: (_incident_gap_seconds(unknown, row), -int(row.last_ts_epoch or 0)))
            if len(candidates) > 1 and _incident_gap_seconds(unknown, candidates[0]) == _incident_gap_seconds(unknown, candidates[1]):
                continue
            pair = (unknown, candidates[0])
            break

        if pair is None:
            break
        canonical = _pick_canonical(session, list(pair))
        if canonical is None:
            summary.clusters_skipped_multi_case += 1
            used_ids.update(row.incident_id for row in pair)
            continue
        specific_asset = next((row.asset for row in pair if row.asset is not None), None)
        if dry_run:
            summary.merged_incidents += 1
            summary.clusters_merged += 1
            used_ids.update(row.incident_id for row in pair)
            continue
        _merge_cluster(session, canonical, list(pair), summary)
        if canonical.asset is None:
            canonical.asset = specific_asset

    return summary


def dedupe_open_incidents(session, *, gap_seconds: int = DEFAULT_DEDUPE_GAP_SECONDS, dry_run: bool = False) -> DedupeSummary:
    incidents = session.scalars(
        select(Incident)
        .where(Incident.status != "closed")
        .order_by(Incident.category, Incident.asset, Incident.last_ts_epoch)
    ).all()

    groups: dict[tuple[str | None, str | None], list[Incident]] = defaultdict(list)
    for inc in incidents:
        groups[(inc.category, inc.asset)].append(inc)

    summary = DedupeSummary()

    for _, rows in groups.items():
        if len(rows) < 2:
            continue

        clusters: list[list[Incident]] = []
        current: list[Incident] = []
        prev: Incident | None = None
        for inc in rows:
            if not current:
                current = [inc]
            else:
                prev_ts = int(prev.last_ts_epoch or 0) if prev else 0
                current_ts = int(inc.last_ts_epoch or 0)
                if current_ts - prev_ts <= gap_seconds:
                    current.append(inc)
                else:
                    clusters.append(current)
                    current = [inc]
            prev = inc
        if current:
            clusters.append(current)

        for cluster in clusters:
            if len(cluster) < 2:
                continue
            canonical = _pick_canonical(session, cluster)
            if canonical is None:
                summary.clusters_skipped_multi_case += 1
                continue
            if dry_run:
                summary.merged_incidents += len(cluster) - 1
                summary.clusters_merged += 1
                continue
            _merge_cluster(session, canonical, cluster, summary)

    return summary
