from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file(ROOT / ".env")

from packages.db import Incident, MessageDecision, RawMessage, ServiceRequestCase, get_session
from packages.sheets import sync as sheets_sync

NY = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class PublicRow:
    updated: str
    issue: str
    category: str
    follow_up: str
    summary: str

    @property
    def key(self) -> tuple[str, str, str, str, str]:
        return (
            _normalize_public_time(self.updated),
            _normalize_text(self.issue),
            _normalize_text(self.category),
            _normalize_text(self.follow_up),
            _normalize_text(self.summary),
        )


@dataclass(frozen=True)
class SourcePublicRow:
    message_id: str
    epoch: int
    row: PublicRow
    text_key: str = ""


class _FakeRequest:
    def __init__(self, calls: list[tuple[str, dict]], kind: str, kwargs: dict, response: dict | None = None):
        self.calls = calls
        self.kind = kind
        self.kwargs = kwargs
        self.response = response or {}

    def execute(self):
        self.calls.append((self.kind, self.kwargs))
        return self.response


class _FakeValues:
    def __init__(self, calls: list[tuple[str, dict]]):
        self.calls = calls

    def clear(self, **kwargs):
        return _FakeRequest(self.calls, "clear", kwargs)

    def update(self, **kwargs):
        return _FakeRequest(self.calls, "update", kwargs)


class _FakeSpreadsheets:
    def __init__(self, calls: list[tuple[str, dict]]):
        self.calls = calls

    def values(self):
        return _FakeValues(self.calls)

    def get(self, **kwargs):
        return _FakeRequest(
            self.calls,
            "get",
            kwargs,
            response={"sheets": [{"properties": {"title": sheets_sync._public_updates_tab(), "sheetId": 1}}]},
        )

    def batchUpdate(self, **kwargs):
        return _FakeRequest(self.calls, "batchUpdate", kwargs)


class _FakeService:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def spreadsheets(self):
        return _FakeSpreadsheets(self.calls)


def _normalize_text(value: object) -> str:
    return " ".join(str(value or "").replace("\u202f", " ").split()).casefold()


def _normalize_public_time(value: object) -> str:
    clean = " ".join(str(value or "").replace("\u202f", " ").split())
    for fmt in ("%Y-%m-%d %I:%M %p", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(clean, fmt).strftime("%Y-%m-%d %H:%M")
        except ValueError:
            continue
    return clean


def _row_time(value: object) -> datetime | None:
    normalized = _normalize_public_time(value)
    try:
        return datetime.strptime(normalized, "%Y-%m-%d %H:%M").replace(tzinfo=NY)
    except ValueError:
        return None


def _public_rows(values: list[list[object]]) -> list[PublicRow]:
    try:
        start = next(idx for idx, row in enumerate(values) if row and row[0] == "Public update log") + 2
    except StopIteration:
        return []
    rows: list[PublicRow] = []
    for row in values[start:]:
        first = str(row[0] if row else "")
        if not first:
            break
        if first == "311 case watch":
            break
        padded = list(row) + [""] * 7
        rows.append(
            PublicRow(
                updated=str(padded[0] or ""),
                issue=str(padded[1] or ""),
                category=str(padded[2] or ""),
                follow_up=str(padded[3] or ""),
                summary=str(padded[6] or ""),
            )
        )
    return rows


def _metric(values: list[list[object]], name: str) -> str:
    for row in values:
        if row and str(row[0]) == name:
            return str(row[1] if len(row) > 1 else "")
    return ""


def _expected_values() -> list[list[object]]:
    fake = _FakeService()
    original_service = sheets_sync._service
    try:
        sheets_sync._service = lambda: fake
        sheets_sync.sync_public_updates_to_sheets()
    finally:
        sheets_sync._service = original_service
    for kind, kwargs in fake.calls:
        if kind == "update" and kwargs.get("range") == f"{sheets_sync._public_updates_tab()}!A1":
            return kwargs["body"]["values"]
    raise RuntimeError("public tenant log renderer did not produce Tenant Log values")


def _live_values() -> list[list[object]]:
    svc = sheets_sync._service()
    sheet_id = sheets_sync._public_sheet_id()
    tab = sheets_sync._public_updates_tab()
    return (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=sheet_id, range=f"{tab}!A1:G500")
        .execute()
        .get("values", [])
    )


def _recent(rows: list[PublicRow], *, days: int) -> list[PublicRow]:
    cutoff = datetime.now(tz=NY) - timedelta(days=days)
    out: list[PublicRow] = []
    for row in rows:
        row_dt = _row_time(row.updated)
        if row_dt is not None and row_dt >= cutoff:
            out.append(row)
    return out


def _row_dicts(rows: list[PublicRow], *, limit: int) -> list[dict[str, str]]:
    return [asdict(row) for row in rows[:limit]]


def _source_row_dicts(rows: list[SourcePublicRow], *, limit: int) -> list[dict[str, object]]:
    return [{"message_id": row.message_id, **asdict(row.row)} for row in rows[:limit]]


def _source_public_rows(*, days: int) -> list[SourcePublicRow]:
    cutoff = datetime.now(tz=NY) - timedelta(days=days)
    allowed_chat_names = sheets_sync._allowed_public_chat_names()
    with get_session() as session:
        decision_rows = (
            session.query(MessageDecision, RawMessage, Incident)
            .join(RawMessage, MessageDecision.message_id == RawMessage.message_id)
            .join(Incident, MessageDecision.incident_id == Incident.incident_id)
            .filter(RawMessage.ts_epoch >= int(cutoff.timestamp()))
            .order_by(RawMessage.ts_epoch.desc())
            .all()
        )
        incident_ids = sorted({incident.incident_id for _decision, _raw, incident in decision_rows})
        case_rows = (
            session.query(ServiceRequestCase).filter(ServiceRequestCase.incident_id.in_(incident_ids)).all()
            if incident_ids
            else []
        )

    case_map: dict[str, list[ServiceRequestCase]] = {}
    for case in case_rows:
        if not case.incident_id:
            continue
        case_map.setdefault(case.incident_id, []).append(case)

    out: list[SourcePublicRow] = []
    for decision, raw, incident in decision_rows:
        if not sheets_sync._raw_message_is_public(raw, allowed_chat_names):
            continue
        if not sheets_sync._public_should_include_update(incident, raw, decision):
            continue
        cases = case_map.get(incident.incident_id, []) if sheets_sync._public_is_actionable_311_update(incident, raw) else []
        out.append(
            SourcePublicRow(
                message_id=raw.message_id,
                epoch=int(raw.ts_epoch or 0),
                row=PublicRow(
                    updated=sheets_sync._public_ts(raw.ts_iso, fallback=raw.ts_epoch) or "",
                    issue=sheets_sync._public_event_issue_label(incident, raw),
                    category=sheets_sync._public_event_category_label(incident, raw),
                    follow_up=sheets_sync._public_case_badge(cases) if cases else "",
                    summary=sheets_sync._public_event_summary(incident, raw),
                ),
                text_key=re.sub(r"\W+", " ", sheets_sync._public_update_detection_text(raw).casefold()).strip(),
            )
        )
    return _dedupe_source_rows(out)


def _dedupe_source_rows(rows: list[SourcePublicRow]) -> list[SourcePublicRow]:
    deduped: list[SourcePublicRow] = []
    for source_row in sorted(rows, key=lambda row: (row.epoch, row.message_id), reverse=True):
        duplicate_key = (
            _normalize_text(source_row.row.issue),
            _normalize_text(source_row.row.category),
            _normalize_text(source_row.row.summary),
        )
        if any(
            (
                _normalize_text(existing.row.issue),
                _normalize_text(existing.row.category),
                _normalize_text(existing.row.summary),
            )
            == duplicate_key
            and (
                abs(source_row.epoch - existing.epoch) <= sheets_sync.PUBLIC_UPDATE_DUPLICATE_WINDOW_SECONDS
                or (
                    source_row.text_key
                    and source_row.text_key == existing.text_key
                    and abs(source_row.epoch - existing.epoch) <= sheets_sync.PUBLIC_DUPLICATE_WINDOW_SECONDS
                )
            )
            for existing in deduped
        ):
            continue
        deduped.append(source_row)
    return deduped


def run_audit(*, days: int, resync: bool, retries: int, retry_sleep: float, limit: int) -> dict[str, object]:
    if resync:
        sheets_sync.sync_public_updates_to_sheets()
        time.sleep(retry_sleep)

    expected_values = _expected_values()
    expected_rows = _recent(_public_rows(expected_values), days=days)

    live_values: list[list[object]] = []
    last_error = ""
    for attempt in range(max(1, retries)):
        try:
            live_values = _live_values()
            last_error = ""
            break
        except Exception as exc:
            last_error = str(exc)
            if attempt + 1 >= retries:
                break
            time.sleep(retry_sleep)

    live_rows = _recent(_public_rows(live_values), days=days) if live_values else []
    live_keys = {row.key for row in live_rows}
    expected_keys = {row.key for row in expected_rows}
    missing = [row for row in expected_rows if row.key not in live_keys]
    unexpected = [row for row in live_rows if row.key not in expected_keys]
    source_rows = _source_public_rows(days=days)
    missing_source = [row for row in source_rows if row.row.key not in live_keys]

    expected_latest = expected_rows[0].issue if expected_rows else ""
    live_latest = _metric(live_values, "Latest update") if live_values else ""
    latest_ok = _normalize_text(expected_latest) == _normalize_text(live_latest)
    ok = not last_error and not missing and not unexpected and not missing_source and latest_ok

    return {
        "ok": ok,
        "days": days,
        "resynced": resync,
        "expected_recent_rows": len(expected_rows),
        "live_recent_rows": len(live_rows),
        "source_recent_rows": len(source_rows),
        "missing_recent_rows": _row_dicts(missing, limit=limit),
        "unexpected_recent_rows": _row_dicts(unexpected, limit=limit),
        "missing_source_rows": _source_row_dicts(missing_source, limit=limit),
        "expected_latest_update": expected_latest,
        "live_latest_update": live_latest,
        "latest_update_ok": latest_ok,
        "live_read_error": last_error,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit the public Tenant Log against the current WhatsApp/message-decision renderer."
    )
    parser.add_argument("--days", type=int, default=7, help="Recent public update window to compare.")
    parser.add_argument("--resync", action="store_true", help="Run the public Tenant Log sync before auditing.")
    parser.add_argument("--retries", type=int, default=3, help="Live sheet read attempts.")
    parser.add_argument("--retry-sleep", type=float, default=5.0, help="Seconds between sync/read retries.")
    parser.add_argument("--limit", type=int, default=20, help="Maximum missing/unexpected rows to print.")
    args = parser.parse_args()

    result = run_audit(
        days=args.days,
        resync=args.resync,
        retries=args.retries,
        retry_sleep=args.retry_sleep,
        limit=args.limit,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    raise SystemExit(0 if result["ok"] else 1)


if __name__ == "__main__":
    main()
