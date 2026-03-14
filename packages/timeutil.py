from __future__ import annotations
import re
from datetime import datetime
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

TS_RE = re.compile(
    r"^(?P<m>\d{1,2})[\./](?P<d>\d{1,2})[\./](?P<y>\d{2,4})\s+"
    r"(?P<h>\d{1,2}):(?P<min>\d{2})(?::(?P<sec>\d{2}))?"
    r"(?:\s*(?P<ampm>AM|PM))?$",
    re.IGNORECASE,
)


def parse_ts_to_epoch(value: str | int | float | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if value > 10_000_000_000:
            return int(value / 1000)
        return int(value)

    raw = str(value).replace("\u202f", " ").replace("\u200e", "").strip()
    if not raw:
        return None

    if re.fullmatch(r"\d+(?:\.\d+)?", raw):
        numeric = float(raw)
        if numeric > 10_000_000_000:
            return int(numeric / 1000)
        return int(numeric)

    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=NY)
        return int(dt.timestamp())
    except Exception:
        pass

    m = TS_RE.match(raw)
    if not m:
        return None

    mm = int(m.group("m"))
    dd = int(m.group("d"))
    yy = int(m.group("y"))
    if yy < 100:
        yy += 2000
    hh = int(m.group("h"))
    mi = int(m.group("min"))
    ss = int(m.group("sec") or 0)
    ampm = (m.group("ampm") or "").upper().strip()

    if ampm:
        if ampm == "PM" and hh != 12:
            hh += 12
        if ampm == "AM" and hh == 12:
            hh = 0

    dt = datetime(yy, mm, dd, hh, mi, ss, tzinfo=NY)
    return int(dt.timestamp())
