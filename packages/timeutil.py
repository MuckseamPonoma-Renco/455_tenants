from __future__ import annotations

import re
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

TS_RE = re.compile(
    r"^(?P<m>\d{1,2})[\./](?P<d>\d{1,2})[\./](?P<y>\d{2,4})\s+"
    r"(?P<h>\d{1,2}):(?P<min>\d{2})(?::(?P<sec>\d{2}))?"
    r"(?:\s*(?P<ampm>AM|PM))?$",
    re.IGNORECASE,
)

TIMESTAMP_FIELD_NAMES = {
    "ts_iso",
    "start_ts",
    "end_ts",
    "last_ts",
    "created_at",
    "updated_at",
    "claimed_at",
    "completed_at",
    "submitted_at",
    "last_checked_at",
    "closed_at",
}


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


def epoch_to_iso(value: str | int | float | None) -> str | None:
    epoch = parse_ts_to_epoch(value)
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=UTC).isoformat().replace("+00:00", "Z")


def normalize_timestamp(value: str | int | float | None, *, fallback: str | int | float | None = None) -> str | None:
    normalized = epoch_to_iso(value)
    if normalized:
        return normalized
    if fallback is not None:
        normalized = epoch_to_iso(fallback)
        if normalized:
            return normalized
    if value is None:
        return None
    raw = str(value).replace("\u202f", " ").replace("\u200e", "").strip()
    return raw or None


def _timestamp_fallback_key(field_name: str) -> str | None:
    if field_name == "ts_iso":
        return "ts_epoch"
    if field_name.endswith("_ts"):
        return f"{field_name}_epoch"
    return None


def normalize_timestamp_fields(value: Any) -> Any:
    if isinstance(value, list):
        return [normalize_timestamp_fields(item) for item in value]
    if not isinstance(value, dict):
        return value

    normalized: dict[str, Any] = {}
    for key, item in value.items():
        if key in TIMESTAMP_FIELD_NAMES:
            fallback_key = _timestamp_fallback_key(key)
            normalized[key] = normalize_timestamp(
                item,
                fallback=value.get(fallback_key) if fallback_key else None,
            )
            continue
        normalized[key] = normalize_timestamp_fields(item)
    return normalized
