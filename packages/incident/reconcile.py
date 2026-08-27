from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import dataclass

from packages.db import Incident, IncidentWitness, MessageDecision, RawMessage
from packages.timeutil import normalize_timestamp

DEFAULT_ELEVATOR_RECONCILE_GAP_SECONDS = 7 * 24 * 3600


@dataclass
class ElevatorReconcileSummary:
    closed_superseded: int = 0


@dataclass
class ElevatorRestoreRepairSummary:
    repaired_incidents: int = 0


def _assets_compatible(open_asset: str | None, closed_asset: str | None) -> bool:
    if open_asset == closed_asset:
        return True
    if open_asset in {None, "elevator_both"}:
        return True
    if closed_asset in {None, "elevator_both"}:
        return True
    return False


def _normalized_text(value: str | None) -> str:
    return re.sub(r"\W+", " ", value or "").strip().casefold()


def _decision_asset(decision: MessageDecision) -> str | None:
    try:
        payload = json.loads(decision.final_json or "{}")
    except (TypeError, ValueError):
        return None
    asset = payload.get("asset") if isinstance(payload, dict) else None
    return asset if asset in {"elevator_north", "elevator_south", "elevator_both"} else None


def _materialized_asset(incident: Incident, decisions: list[MessageDecision]) -> str | None:
    assets = {_decision_asset(decision) for decision in decisions}
    assets.discard(None)
    if "elevator_both" in assets or {"elevator_north", "elevator_south"}.issubset(assets):
        return "elevator_both"
    if len(assets) == 1:
        return next(iter(assets))
    title = (incident.title or "").casefold()
    if "north" in title and "south" not in title:
        return "elevator_north"
    if "south" in title and "north" not in title:
        return "elevator_south"
    return None


def repair_unbounded_elevator_restore_attachments(
    session,
    *,
    max_gap_seconds: int = DEFAULT_ELEVATOR_RECONCILE_GAP_SECONDS,
) -> ElevatorRestoreRepairSummary:
    """Undo a restore that was attached to an incident far outside its event window."""
    incidents = session.query(Incident).filter(Incident.category == "elevator").all()
    if not incidents:
        return ElevatorRestoreRepairSummary()

    joined = (
        session.query(MessageDecision, RawMessage)
        .join(RawMessage, RawMessage.message_id == MessageDecision.message_id)
        .filter(MessageDecision.is_issue.is_(True))
        .all()
    )
    assigned: dict[str, list[tuple[MessageDecision, RawMessage]]] = {}
    restores_by_epoch: dict[int, list[tuple[MessageDecision, RawMessage]]] = {}
    for decision, raw in joined:
        if decision.incident_id:
            assigned.setdefault(decision.incident_id, []).append((decision, raw))
        if decision.category == "elevator" and decision.event_type == "restore" and raw.ts_epoch is not None:
            restores_by_epoch.setdefault(int(raw.ts_epoch), []).append((decision, raw))

    summary = ElevatorRestoreRepairSummary()
    for incident in incidents:
        end_epoch = int(incident.end_ts_epoch or 0)
        incident_rows = assigned.get(incident.incident_id, [])
        dated_rows = [(decision, raw) for decision, raw in incident_rows if raw.ts_epoch is not None]
        if not end_epoch or not dated_rows or end_epoch not in restores_by_epoch:
            continue
        assigned_last = max(int(raw.ts_epoch or 0) for _decision, raw in dated_rows)
        assigned_restore_at_end = any(
            decision.event_type == "restore" and int(raw.ts_epoch or 0) == end_epoch
            for decision, raw in dated_rows
        )
        if assigned_restore_at_end or end_epoch - assigned_last <= max_gap_seconds:
            continue

        restore_rows = restores_by_epoch[end_epoch]
        restore_ids = {raw.message_id for _decision, raw in restore_rows}
        restore_texts = {_normalized_text(raw.text) for _decision, raw in restore_rows if raw.text}
        refs = [ref.strip() for ref in (incident.proof_refs or "").split(",") if ref.strip()]
        incident.proof_refs = ",".join(ref for ref in refs if ref not in restore_ids)
        summary_parts = [part.strip() for part in (incident.summary or "").split(" | ") if part.strip()]
        incident.summary = " | ".join(
            part for part in summary_parts if _normalized_text(part) not in restore_texts
        )[:2000]
        decisions = [decision for decision, _raw in incident_rows]
        incident.report_count = len(incident_rows)
        incident.start_ts_epoch = min(int(raw.ts_epoch or 0) for _decision, raw in dated_rows)
        first_raw = min(dated_rows, key=lambda item: int(item[1].ts_epoch or 0))[1]
        incident.start_ts = first_raw.ts_iso or incident.start_ts
        incident.last_ts_epoch = assigned_last
        incident.asset = _materialized_asset(incident, decisions)
        incident.status = "open"
        incident.end_ts = None
        incident.end_ts_epoch = None
        incident.updated_at = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")

        session.query(IncidentWitness).filter(IncidentWitness.incident_id == incident.incident_id).delete(
            synchronize_session=False
        )
        sender_hashes = {raw.sender_hash for _decision, raw in incident_rows if raw.sender_hash}
        for sender_hash in sorted(sender_hashes):
            session.add(IncidentWitness(incident_id=incident.incident_id, sender_hash=sender_hash))
        incident.witness_count = len(sender_hashes)
        summary.repaired_incidents += 1

    if summary.repaired_incidents:
        session.flush()
    return summary


def close_superseded_open_elevator_incidents(
    session,
    *,
    max_gap_seconds: int = DEFAULT_ELEVATOR_RECONCILE_GAP_SECONDS,
) -> ElevatorReconcileSummary:
    open_rows = session.query(Incident).filter(
        Incident.category == "elevator",
        Incident.status != "closed",
    ).order_by(Incident.last_ts_epoch.asc().nullsfirst()).all()
    if not open_rows:
        return ElevatorReconcileSummary()

    closed_rows = session.query(Incident).filter(
        Incident.category == "elevator",
        Incident.status == "closed",
    ).all()
    closed_rows.sort(key=lambda row: int(row.end_ts_epoch or row.last_ts_epoch or 0))
    if not closed_rows:
        return ElevatorReconcileSummary()

    summary = ElevatorReconcileSummary()
    for row in open_rows:
        open_last = int(row.last_ts_epoch or 0)
        if open_last <= 0:
            continue
        later_closed = next(
            (
                candidate
                for candidate in closed_rows
                if int(candidate.end_ts_epoch or candidate.last_ts_epoch or 0) >= open_last
                and int(candidate.end_ts_epoch or candidate.last_ts_epoch or 0) - open_last <= max_gap_seconds
                and _assets_compatible(row.asset, candidate.asset)
            ),
            None,
        )
        if later_closed is None:
            continue
        row.status = "closed"
        row.end_ts_epoch = later_closed.end_ts_epoch or later_closed.last_ts_epoch
        row.end_ts = (
            normalize_timestamp(later_closed.end_ts, fallback=later_closed.end_ts_epoch)
            or normalize_timestamp(later_closed.start_ts, fallback=later_closed.last_ts_epoch)
            or row.end_ts
        )
        row.updated_at = normalize_timestamp(later_closed.updated_at, fallback=later_closed.last_ts_epoch) or row.updated_at
        summary.closed_superseded += 1

    return summary
