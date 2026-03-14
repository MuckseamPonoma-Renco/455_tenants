from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple
from packages.db import get_session, RawMessage

@dataclass
class CoverageRow:
    day: str
    messages: int
    first_ts_epoch: int | None
    last_ts_epoch: int | None

def compute_daily_coverage(limit_days: int = 60) -> List[CoverageRow]:
    with get_session() as s:
        rows = s.query(RawMessage).all()

    by_day: Dict[str, List[RawMessage]] = defaultdict(list)
    for r in rows:
        if not r.ts_epoch:
            continue
        # day bucket by epoch -> utc day, good enough for coverage; can upgrade to NY local day.
        import datetime
        day = datetime.datetime.utcfromtimestamp(r.ts_epoch).date().isoformat()
        by_day[day].append(r)

    out = []
    for day, msgs in sorted(by_day.items())[-limit_days:]:
        epochs = sorted([m.ts_epoch for m in msgs if m.ts_epoch is not None])
        out.append(CoverageRow(day=day, messages=len(msgs), first_ts_epoch=epochs[0] if epochs else None, last_ts_epoch=epochs[-1] if epochs else None))
    return out

def detect_gaps(coverage: List[CoverageRow], min_messages_per_day: int = 1) -> List[str]:
    gaps = []
    for r in coverage:
        if r.messages < min_messages_per_day:
            gaps.append(r.day)
    return gaps
