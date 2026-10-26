from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List

from sqlalchemy import BigInteger, case, cast, func, select

from packages.db import get_session, RawMessage

@dataclass
class CoverageRow:
    day: str
    messages: int
    first_ts_epoch: int | None
    last_ts_epoch: int | None

def compute_daily_coverage(limit_days: int = 60) -> List[CoverageRow]:
    # Integer division truncates toward zero in both SQLite and PostgreSQL.
    # Adjust negative epochs so their buckets still start at UTC midnight.
    epoch = cast(RawMessage.ts_epoch, BigInteger)
    day_bucket = case((epoch < 0, epoch - 86399), else_=epoch) // 86400
    statement = (
        select(func.count(), func.min(RawMessage.ts_epoch), func.max(RawMessage.ts_epoch))
        .where(RawMessage.ts_epoch.is_not(None), RawMessage.ts_epoch != 0)
        .group_by(day_bucket)
    )
    if limit_days > 0:
        statement = statement.order_by(day_bucket.desc()).limit(limit_days)
    else:
        # Preserve the previous sorted_days[-limit_days:] behavior: zero
        # returns every day, and a negative value skips that many oldest days.
        statement = statement.order_by(day_bucket).offset(-limit_days)

    with get_session() as s:
        rows = s.execute(statement).all()
    if limit_days > 0:
        rows.reverse()
    return [
        CoverageRow(
            day=datetime.fromtimestamp(first_ts, timezone.utc).date().isoformat(),
            messages=count,
            first_ts_epoch=first_ts,
            last_ts_epoch=last_ts,
        )
        for count, first_ts, last_ts in rows
    ]

def detect_gaps(coverage: List[CoverageRow], min_messages_per_day: int = 1) -> List[str]:
    gaps = []
    for r in coverage:
        if r.messages < min_messages_per_day:
            gaps.append(r.day)
    return gaps
