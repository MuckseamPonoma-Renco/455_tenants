from __future__ import annotations

from dataclasses import dataclass

from packages.db import Incident
from packages.timeutil import normalize_timestamp


@dataclass
class ElevatorReconcileSummary:
    closed_superseded: int = 0


def _assets_compatible(open_asset: str | None, closed_asset: str | None) -> bool:
    if open_asset == closed_asset:
        return True
    if open_asset in {None, "elevator_both"}:
        return True
    if closed_asset in {None, "elevator_both"}:
        return True
    return False


def close_superseded_open_elevator_incidents(session) -> ElevatorReconcileSummary:
    open_rows = session.query(Incident).filter(
        Incident.category == "elevator",
        Incident.status != "closed",
    ).order_by(Incident.last_ts_epoch.asc().nullsfirst()).all()
    if not open_rows:
        return ElevatorReconcileSummary()

    closed_rows = session.query(Incident).filter(
        Incident.category == "elevator",
        Incident.status == "closed",
    ).order_by(Incident.last_ts_epoch.asc().nullsfirst()).all()
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
                if int(candidate.last_ts_epoch or 0) >= open_last
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
