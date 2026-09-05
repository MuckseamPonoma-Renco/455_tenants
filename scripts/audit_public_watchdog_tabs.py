from __future__ import annotations

import argparse
import json
import re
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Iterator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file(ROOT / ".env")

from packages.db import SessionLocal
from packages.sheets import sync as sheets_sync
from scripts.init_sheet import PUBLIC_WATCHDOG_TABS


DEFAULT_MAX_LAST_CHECKED_AGE_MINUTES = 90.0
DEFAULT_MAX_LAST_CHECKED_DRIFT_MINUTES = 90.0
DEFAULT_RETRIES = 3
DEFAULT_RETRY_SLEEP_SECONDS = 5.0

TENANT_ELEVATOR_EVIDENCE_TOPIC = "Actual elevator service reported by tenants"
SYSTEM_WATCHDOG_FRESHNESS_TOPIC = "What residents should do"

TAB_ENV_VARS = {
    "ElevatorWatch": "SHEETS_ELEVATOR_WATCH_TAB",
    "ProjectStatus": "SHEETS_PROJECT_STATUS_TAB",
    "PublicRecords": "SHEETS_PUBLIC_RECORDS_TAB",
    "WatchdogChecks": "SHEETS_WATCHDOG_CHECKS_TAB",
    "ActionQueue": "SHEETS_WATCHDOG_ACTIONS_TAB",
    "WeeklyDigest": "SHEETS_WEEKLY_DIGEST_TAB",
}

DATE_HEADERS = {
    "ElevatorWatch": {"Last checked"},
    "ProjectStatus": {"updated_at"},
    "PublicRecords": {
        "filed_at",
        "approved_at",
        "permit_issued_at",
        "inspection_date",
        "expires_at",
        "machine_verified_at",
        "human_verified_at",
    },
    "WatchdogChecks": {"checked_at"},
    "ActionQueue": {"due_at", "created_at", "completed_at"},
    "WeeklyDigest": {"period_start", "period_end", "generated_at"},
}


@dataclass(frozen=True)
class TabSpec:
    logical_name: str
    title: str
    headers: tuple[str, ...]
    date_columns: frozenset[int]
    volatile_timestamp_column: int | None = None
    evidence_timestamp_topics: frozenset[str] = frozenset()
    system_freshness_topic: str | None = None


@dataclass(frozen=True)
class ExpectedTab:
    values: list[list[object]]
    value_input_option: str


@dataclass(frozen=True)
class LiveTab:
    display_values: list[list[object]]
    formula_values: list[list[object]]


@dataclass(frozen=True)
class LiveWorkbook:
    metadata: dict[str, object]
    tabs: dict[str, LiveTab]


@dataclass(frozen=True)
class ParsedDateCell:
    instant: datetime
    local_date: date
    precision: str


def _resolved_tab_specs() -> tuple[TabSpec, ...]:
    specs: list[TabSpec] = []
    for logical_name, headers in PUBLIC_WATCHDOG_TABS.items():
        title = sheets_sync._tab(TAB_ENV_VARS[logical_name], default=logical_name)
        header_tuple = tuple(headers)
        date_columns = frozenset(
            index for index, header in enumerate(header_tuple) if header in DATE_HEADERS[logical_name]
        )
        volatile_column = header_tuple.index("Last checked") if logical_name == "ElevatorWatch" else None
        specs.append(
            TabSpec(
                logical_name=logical_name,
                title=title,
                headers=header_tuple,
                date_columns=date_columns,
                volatile_timestamp_column=volatile_column,
                evidence_timestamp_topics=(
                    frozenset({TENANT_ELEVATOR_EVIDENCE_TOPIC})
                    if logical_name == "ElevatorWatch"
                    else frozenset()
                ),
                system_freshness_topic=(
                    SYSTEM_WATCHDOG_FRESHNESS_TOPIC if logical_name == "ElevatorWatch" else None
                ),
            )
        )
    titles = [spec.title for spec in specs]
    if len(titles) != len(set(titles)):
        raise RuntimeError("replacement-watchdog tab names must be unique")
    return tuple(specs)


class _CaptureRequest:
    def __init__(self, response: dict[str, object] | None = None):
        self._response = response or {}

    def execute(self) -> dict[str, object]:
        return self._response


class _CaptureValues:
    def __init__(self, updates: list[dict[str, object]]):
        self._updates = updates

    def update(self, **kwargs: object) -> _CaptureRequest:
        self._updates.append(dict(kwargs))
        return _CaptureRequest()

    def clear(self, **_kwargs: object) -> _CaptureRequest:
        return _CaptureRequest()


class _CaptureSpreadsheets:
    def __init__(self, titles: tuple[str, ...], updates: list[dict[str, object]]):
        self._updates = updates
        self._metadata = {
            "properties": {"title": sheets_sync.PUBLIC_WORKBOOK_TITLE},
            "sheets": [
                {"properties": {"title": title, "sheetId": index + 1, "hidden": False}}
                for index, title in enumerate(titles)
            ],
        }

    def values(self) -> _CaptureValues:
        return _CaptureValues(self._updates)

    def get(self, **_kwargs: object) -> _CaptureRequest:
        return _CaptureRequest(self._metadata)

    def batchUpdate(self, **_kwargs: object) -> _CaptureRequest:
        return _CaptureRequest()


class _CaptureService:
    def __init__(self, titles: tuple[str, ...]):
        self.updates: list[dict[str, object]] = []
        self._spreadsheets = _CaptureSpreadsheets(titles, self.updates)

    def spreadsheets(self) -> _CaptureSpreadsheets:
        return self._spreadsheets


class _NonCommittingSession:
    """Delegate reads/flushes while making renderer-only commit calls a no-op."""

    def __init__(self, session: object):
        self._session = session

    def commit(self) -> None:
        return None

    def __getattr__(self, name: str) -> object:
        return getattr(self._session, name)


@contextmanager
def _noncommitting_get_session() -> Iterator[_NonCommittingSession]:
    # Use the already-configured session factory directly so a read-only audit
    # cannot invoke init_db() and create or migrate schema. Closing rolls back
    # the transaction; the proxy also blocks the ProjectStatus renderer's one
    # explicit commit.
    session = SessionLocal()
    try:
        yield _NonCommittingSession(session)
    finally:
        session.close()


def _expected_update_range(tab: str) -> str:
    return f"{tab}!A1"


def _capture_expected_values(
    sheet_id: str,
    specs: tuple[TabSpec, ...],
    *,
    renderer: Callable[[], object] | None = None,
) -> dict[str, ExpectedTab]:
    """Run the normal renderer against a no-op service and capture its payloads."""

    capture = _CaptureService(tuple(spec.title for spec in specs))
    original_service = sheets_sync._service
    original_watchdog_sheet_id = sheets_sync._watchdog_sheet_id
    original_get_session = sheets_sync.get_session
    try:
        sheets_sync._service = lambda: capture
        sheets_sync._watchdog_sheet_id = lambda: sheet_id
        sheets_sync.get_session = _noncommitting_get_session
        (renderer or sheets_sync.sync_replacement_watchdog_to_sheets)()
    finally:
        sheets_sync._service = original_service
        sheets_sync._watchdog_sheet_id = original_watchdog_sheet_id
        sheets_sync.get_session = original_get_session

    captured: dict[str, ExpectedTab] = {}
    for spec in specs:
        matching = [
            update for update in capture.updates if update.get("range") == _expected_update_range(spec.title)
        ]
        if len(matching) != 1:
            raise RuntimeError(
                f"renderer produced {len(matching)} payloads for {spec.logical_name}; expected exactly one"
            )
        update = matching[0]
        body = update.get("body")
        values = body.get("values") if isinstance(body, dict) else None
        if not isinstance(values, list) or not all(isinstance(row, list) for row in values):
            raise RuntimeError(f"renderer produced invalid values for {spec.logical_name}")
        captured[spec.logical_name] = ExpectedTab(
            values=[list(row) for row in values],
            value_input_option=str(update.get("valueInputOption") or ""),
        )
    return captured


def _quoted_range(tab: str, a1_range: str) -> str:
    return f"'{tab.replace(chr(39), chr(39) * 2)}'!{a1_range}"


def _value_ranges(response: object, *, count: int, render_name: str) -> list[list[list[object]]]:
    if not isinstance(response, dict):
        raise RuntimeError(f"{render_name} values response was not an object")
    entries = response.get("valueRanges")
    if not isinstance(entries, list) or len(entries) != count:
        actual_count = len(entries) if isinstance(entries, list) else 0
        raise RuntimeError(f"{render_name} values response returned {actual_count} ranges; expected {count}")
    out: list[list[list[object]]] = []
    for entry in entries:
        values = entry.get("values", []) if isinstance(entry, dict) else []
        if not isinstance(values, list) or not all(isinstance(row, list) for row in values):
            raise RuntimeError(f"{render_name} values response contained an invalid row set")
        out.append([list(row) for row in values])
    return out


def _read_live_workbook(service: object, sheet_id: str, specs: tuple[TabSpec, ...]) -> LiveWorkbook:
    spreadsheets = service.spreadsheets()
    metadata = (
        spreadsheets.get(
            spreadsheetId=sheet_id,
            fields=(
                "properties(title,timeZone),"
                "sheets(properties(sheetId,title,hidden,gridProperties(rowCount,columnCount)))"
            ),
        )
        .execute()
    )
    if not isinstance(metadata, dict):
        raise RuntimeError("spreadsheet metadata response was not an object")

    available_titles = {
        str(properties.get("title"))
        for sheet in metadata.get("sheets", [])
        if isinstance(sheet, dict)
        and isinstance((properties := sheet.get("properties")), dict)
        and properties.get("title") is not None
    }
    present_specs = [spec for spec in specs if spec.title in available_titles]
    if not present_specs:
        return LiveWorkbook(metadata=metadata, tabs={})

    ranges = [_quoted_range(spec.title, "A:ZZ") for spec in present_specs]
    formatted_response = (
        spreadsheets.values()
        .batchGet(
            spreadsheetId=sheet_id,
            ranges=ranges,
            majorDimension="ROWS",
            valueRenderOption="FORMATTED_VALUE",
        )
        .execute()
    )
    formula_response = (
        spreadsheets.values()
        .batchGet(
            spreadsheetId=sheet_id,
            ranges=ranges,
            majorDimension="ROWS",
            valueRenderOption="FORMULA",
        )
        .execute()
    )
    formatted = _value_ranges(formatted_response, count=len(present_specs), render_name="formatted")
    formulas = _value_ranges(formula_response, count=len(present_specs), render_name="formula")
    tabs = {
        spec.logical_name: LiveTab(display_values=display, formula_values=formula)
        for spec, display, formula in zip(present_specs, formatted, formulas)
    }
    return LiveWorkbook(metadata=metadata, tabs=tabs)


def _cell_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def _is_populated(value: object) -> bool:
    return value is not None and _cell_text(value) != ""


def _cell(values: list[list[object]], row_index: int, column_index: int) -> object:
    if row_index >= len(values) or column_index >= len(values[row_index]):
        return ""
    return values[row_index][column_index]


def _used_shape(*value_sets: list[list[object]]) -> tuple[int, int]:
    used_rows = 0
    used_columns = 0
    for values in value_sets:
        for row_index, row in enumerate(values):
            for column_index, value in enumerate(row):
                if not _is_populated(value):
                    continue
                used_rows = max(used_rows, row_index + 1)
                used_columns = max(used_columns, column_index + 1)
    return used_rows, used_columns


_US_DATE_RE = re.compile(
    r"^(?P<month>\d{1,2})[/.](?P<day>\d{1,2})[/.](?P<year>\d{2,4})"
    r"(?:\s*,?\s*(?P<hour>\d{1,2}):(?P<minute>\d{2})"
    r"(?::(?P<second>\d{2})(?:\.(?P<fraction>\d{1,6}))?)?"
    r"(?:\s*(?P<ampm>AM|PM))?)?$",
    re.IGNORECASE,
)
_ISO_DATE_RE = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})"
    r"(?:(?:T|\s)(?P<hour>\d{1,2}):(?P<minute>\d{2})"
    r"(?::(?P<second>\d{2})(?:\.(?P<fraction>\d{1,6}))?)?"
    r"(?P<offset>Z|[+-]\d{2}:?\d{2})?)?$",
    re.IGNORECASE,
)


def _parsed_date_parts(match: re.Match[str], workbook_tz: ZoneInfo) -> ParsedDateCell | None:
    try:
        year = int(match.group("year"))
        if year < 100:
            year += 2000
        month = int(match.group("month"))
        day = int(match.group("day"))
        hour_group = match.groupdict().get("hour")
        if hour_group is None:
            local = datetime(year, month, day, tzinfo=workbook_tz)
            return ParsedDateCell(instant=local.astimezone(timezone.utc), local_date=local.date(), precision="date")

        hour = int(hour_group)
        minute = int(match.group("minute"))
        second_group = match.groupdict().get("second")
        second = int(second_group or 0)
        fraction = (match.groupdict().get("fraction") or "")[:6]
        microsecond = int(fraction.ljust(6, "0")) if fraction else 0
        ampm = (match.groupdict().get("ampm") or "").upper()
        if ampm:
            if not 1 <= hour <= 12:
                return None
            if ampm == "PM" and hour != 12:
                hour += 12
            elif ampm == "AM" and hour == 12:
                hour = 0
        offset = match.groupdict().get("offset")
        if offset:
            normalized_offset = "+00:00" if offset.upper() == "Z" else offset
            if len(normalized_offset) == 5 and normalized_offset[3] != ":":
                normalized_offset = f"{normalized_offset[:3]}:{normalized_offset[3:]}"
            raw = (
                f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}"
                f"{f'.{fraction}' if fraction else ''}{normalized_offset}"
            )
            value = datetime.fromisoformat(raw)
        else:
            value = datetime(year, month, day, hour, minute, second, microsecond, tzinfo=workbook_tz)
        local_value = value.astimezone(workbook_tz)
        precision = "microsecond" if fraction else ("second" if second_group is not None else "minute")
        return ParsedDateCell(
            instant=value.astimezone(timezone.utc),
            local_date=local_value.date(),
            precision=precision,
        )
    except (TypeError, ValueError):
        return None


def _parse_date_cell(value: object, workbook_tz: ZoneInfo) -> ParsedDateCell | None:
    if isinstance(value, datetime):
        parsed = value if value.tzinfo is not None else value.replace(tzinfo=workbook_tz)
        local = parsed.astimezone(workbook_tz)
        return ParsedDateCell(parsed.astimezone(timezone.utc), local.date(), "microsecond")
    if isinstance(value, date):
        parsed = datetime(value.year, value.month, value.day, tzinfo=workbook_tz)
        return ParsedDateCell(parsed.astimezone(timezone.utc), value, "date")
    raw = _cell_text(value)
    if not raw:
        return None
    match = _ISO_DATE_RE.fullmatch(raw) or _US_DATE_RE.fullmatch(raw)
    return _parsed_date_parts(match, workbook_tz) if match else None


def _date_cells_equivalent(expected: object, live: object, workbook_tz: ZoneInfo) -> bool:
    expected_parsed = _parse_date_cell(expected, workbook_tz)
    live_parsed = _parse_date_cell(live, workbook_tz)
    if expected_parsed is None or live_parsed is None:
        return False
    if "date" in {expected_parsed.precision, live_parsed.precision}:
        return expected_parsed.local_date == live_parsed.local_date
    if "minute" in {expected_parsed.precision, live_parsed.precision}:
        return expected_parsed.instant.replace(second=0, microsecond=0) == live_parsed.instant.replace(
            second=0, microsecond=0
        )
    if "second" in {expected_parsed.precision, live_parsed.precision}:
        return expected_parsed.instant.replace(microsecond=0) == live_parsed.instant.replace(microsecond=0)
    return expected_parsed.instant == live_parsed.instant


def _cells_equivalent(
    expected: object,
    live: object,
    *,
    is_date: bool,
    workbook_tz: ZoneInfo,
) -> bool:
    if _cell_text(expected) == _cell_text(live):
        return True
    return is_date and _date_cells_equivalent(expected, live, workbook_tz)


def _timestamp_check(
    expected: object,
    live: object,
    *,
    workbook_tz: ZoneInfo,
    now: datetime,
    max_age_seconds: float,
    max_drift_seconds: float,
) -> dict[str, object]:
    expected_parsed = _parse_date_cell(expected, workbook_tz)
    live_parsed = _parse_date_cell(live, workbook_tz)
    expected_is_timestamp = expected_parsed is not None and expected_parsed.precision != "date"
    live_is_timestamp = live_parsed is not None and live_parsed.precision != "date"
    result: dict[str, object] = {
        "expected": _cell_text(expected),
        "live": _cell_text(live),
        "timestamp_semantics": "automatic_check",
        "freshness_required": True,
        "expected_parseable_timestamp": expected_is_timestamp,
        "live_parseable_timestamp": live_is_timestamp,
        "max_age_seconds": max_age_seconds,
        "max_drift_seconds": max_drift_seconds,
    }
    if not expected_is_timestamp or not live_is_timestamp:
        result.update({"ok": False, "reason": "Last checked must contain a timestamp with time"})
        return result

    assert expected_parsed is not None and live_parsed is not None
    normalized_now = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    normalized_now = normalized_now.astimezone(timezone.utc)
    drift_seconds = abs((live_parsed.instant - expected_parsed.instant).total_seconds())
    age_seconds = (normalized_now - live_parsed.instant).total_seconds()
    drift_ok = drift_seconds <= max_drift_seconds
    freshness_ok = -max_drift_seconds <= age_seconds <= max_age_seconds
    result.update(
        {
            "ok": drift_ok and freshness_ok,
            "reason": "" if drift_ok and freshness_ok else "Last checked is stale or differs from the current renderer",
            "expected_iso": expected_parsed.instant.isoformat().replace("+00:00", "Z"),
            "live_iso": live_parsed.instant.isoformat().replace("+00:00", "Z"),
            "drift_seconds": drift_seconds,
            "age_seconds": age_seconds,
            "within_drift_bound": drift_ok,
            "within_freshness_bound": freshness_ok,
        }
    )
    return result


def _evidence_timestamp_check(
    expected: object,
    live: object,
    *,
    workbook_tz: ZoneInfo,
    now: datetime,
    max_future_seconds: float,
) -> dict[str, object]:
    """Compare a source-evidence timestamp without pretending it is a check heartbeat."""

    expected_text = _cell_text(expected)
    live_text = _cell_text(live)
    expected_parsed = _parse_date_cell(expected, workbook_tz)
    live_parsed = _parse_date_cell(live, workbook_tz)
    expected_is_timestamp = expected_parsed is not None and expected_parsed.precision != "date"
    live_is_timestamp = live_parsed is not None and live_parsed.precision != "date"
    both_empty = not expected_text and not live_text
    result: dict[str, object] = {
        "expected": expected_text,
        "live": live_text,
        "timestamp_semantics": "tenant_evidence",
        "freshness_required": False,
        "evidence_timestamp_present": not both_empty,
        "expected_parseable_timestamp": expected_is_timestamp,
        "live_parseable_timestamp": live_is_timestamp,
        "max_future_seconds": max_future_seconds,
    }
    if both_empty:
        result.update(
            {
                "ok": True,
                "reason": "",
                "source_live_equivalent": True,
                "within_future_bound": True,
            }
        )
        return result
    if not expected_is_timestamp or not live_is_timestamp:
        result.update(
            {
                "ok": False,
                "reason": "Tenant evidence Last checked must be blank in both views or contain a timestamp with time",
                "source_live_equivalent": False,
                "within_future_bound": False,
            }
        )
        return result

    assert expected_parsed is not None and live_parsed is not None
    normalized_now = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    normalized_now = normalized_now.astimezone(timezone.utc)
    equivalent = _date_cells_equivalent(expected, live, workbook_tz)
    drift_seconds = abs((live_parsed.instant - expected_parsed.instant).total_seconds())
    age_seconds = (normalized_now - live_parsed.instant).total_seconds()
    future_ok = age_seconds >= -max_future_seconds
    ok = equivalent and future_ok
    if not equivalent:
        reason = "Tenant evidence timestamp differs from the current renderer"
    elif not future_ok:
        reason = "Tenant evidence timestamp is implausibly in the future"
    else:
        reason = ""
    result.update(
        {
            "ok": ok,
            "reason": reason,
            "expected_iso": expected_parsed.instant.isoformat().replace("+00:00", "Z"),
            "live_iso": live_parsed.instant.isoformat().replace("+00:00", "Z"),
            "drift_seconds": drift_seconds,
            "age_seconds": age_seconds,
            "source_live_equivalent": equivalent,
            "within_future_bound": future_ok,
        }
    )
    return result


def _grid_metadata(metadata: dict[str, object], title: str) -> dict[str, object]:
    for sheet in metadata.get("sheets", []):
        if not isinstance(sheet, dict):
            continue
        properties = sheet.get("properties")
        if isinstance(properties, dict) and properties.get("title") == title:
            grid = properties.get("gridProperties")
            return grid if isinstance(grid, dict) else {}
    return {}


def _extra_populated_cells(
    live: LiveTab,
    *,
    expected_rows: int,
    expected_columns: int,
    limit: int,
) -> tuple[int, list[dict[str, object]]]:
    coordinates: set[tuple[int, int]] = set()
    for values in (live.display_values, live.formula_values):
        for row_index, row in enumerate(values):
            for column_index, value in enumerate(row):
                if _is_populated(value) and (row_index >= expected_rows or column_index >= expected_columns):
                    coordinates.add((row_index, column_index))
    details: list[dict[str, object]] = []
    for row_index, column_index in sorted(coordinates)[:limit]:
        display = _cell(live.display_values, row_index, column_index)
        formula = _cell(live.formula_values, row_index, column_index)
        details.append(
            {
                "row": row_index + 1,
                "column": column_index + 1,
                "display": _cell_text(display),
                "formula": _cell_text(formula) if _cell_text(formula).startswith("=") else "",
            }
        )
    return len(coordinates), details


def _unexpected_formula_cells(live: LiveTab, *, limit: int) -> tuple[int, list[dict[str, object]]]:
    details: list[dict[str, object]] = []
    count = 0
    for row_index, row in enumerate(live.formula_values):
        for column_index, value in enumerate(row):
            text = _cell_text(value)
            if not text.startswith("="):
                continue
            count += 1
            if len(details) < limit:
                details.append({"row": row_index + 1, "column": column_index + 1, "formula": text})
    return count, details


def _audit_tab(
    spec: TabSpec,
    expected: ExpectedTab,
    live: LiveTab,
    *,
    metadata: dict[str, object],
    workbook_tz: ZoneInfo,
    now: datetime,
    max_age_seconds: float,
    max_drift_seconds: float,
    limit: int,
) -> dict[str, object]:
    contract_headers = list(spec.headers)
    expected_rows = expected.values
    expected_row_count = len(expected_rows)
    expected_columns = len(contract_headers)
    live_row_count, live_used_columns = _used_shape(live.display_values, live.formula_values)
    renderer_used_rows, renderer_used_columns = _used_shape(expected_rows)

    renderer_header = [_cell_text(_cell(expected_rows, 0, index)) for index in range(expected_columns)]
    live_header = [_cell_text(_cell(live.display_values, 0, index)) for index in range(expected_columns)]
    renderer_header_ok = renderer_header == contract_headers
    live_header_ok = live_header == contract_headers

    mismatches: list[dict[str, object]] = []
    volatile_checks: list[dict[str, object]] = []
    mismatch_count = 0
    for row_index in range(expected_row_count):
        for column_index in range(expected_columns):
            expected_value = _cell(expected_rows, row_index, column_index)
            live_value = _cell(live.display_values, row_index, column_index)
            if row_index > 0 and column_index == spec.volatile_timestamp_column:
                topic = _cell_text(_cell(expected_rows, row_index, 0))
                if topic in spec.evidence_timestamp_topics:
                    timestamp_result = _evidence_timestamp_check(
                        expected_value,
                        live_value,
                        workbook_tz=workbook_tz,
                        now=now,
                        max_future_seconds=max_drift_seconds,
                    )
                else:
                    timestamp_result = _timestamp_check(
                        expected_value,
                        live_value,
                        workbook_tz=workbook_tz,
                        now=now,
                        max_age_seconds=max_age_seconds,
                        max_drift_seconds=max_drift_seconds,
                    )
                timestamp_result.update(
                    {
                        "row": row_index + 1,
                        "topic": topic,
                    }
                )
                volatile_checks.append(timestamp_result)
                if timestamp_result["ok"]:
                    continue
                mismatch_count += 1
                if len(mismatches) < limit:
                    mismatches.append(
                        {
                            "row": row_index + 1,
                            "column": column_index + 1,
                            "header": contract_headers[column_index],
                            "expected": _cell_text(expected_value),
                            "live": _cell_text(live_value),
                            "reason": timestamp_result["reason"],
                        }
                    )
                continue

            equivalent = _cells_equivalent(
                expected_value,
                live_value,
                is_date=row_index > 0 and column_index in spec.date_columns,
                workbook_tz=workbook_tz,
            )
            if equivalent:
                continue
            mismatch_count += 1
            if len(mismatches) < limit:
                mismatches.append(
                    {
                        "row": row_index + 1,
                        "column": column_index + 1,
                        "header": contract_headers[column_index],
                        "expected": _cell_text(expected_value),
                        "live": _cell_text(live_value),
                        "reason": "source and live values differ",
                    }
                )

    system_freshness_matches = [
        check for check in volatile_checks if check.get("topic") == spec.system_freshness_topic
    ]
    if spec.system_freshness_topic is None:
        system_freshness_ok = True
        system_freshness: dict[str, object] = {
            "required": False,
            "ok": True,
            "topic": "",
        }
    elif len(system_freshness_matches) == 1:
        system_freshness = {
            **system_freshness_matches[0],
            "required": True,
        }
        system_freshness_ok = bool(
            system_freshness.get("ok") and system_freshness.get("freshness_required")
        )
    else:
        system_freshness_ok = False
        system_freshness = {
            "required": True,
            "ok": False,
            "topic": spec.system_freshness_topic,
            "matching_row_count": len(system_freshness_matches),
            "reason": "Expected exactly one public watchdog freshness row",
        }

    extra_count, extra_cells = _extra_populated_cells(
        live,
        expected_rows=expected_row_count,
        expected_columns=expected_columns,
        limit=limit,
    )
    formula_count, formula_cells = _unexpected_formula_cells(live, limit=limit)
    row_count_ok = live_row_count == expected_row_count
    used_width_ok = live_used_columns == expected_columns
    renderer_shape_ok = renderer_used_rows == expected_row_count and renderer_used_columns == expected_columns
    input_option_ok = expected.value_input_option == "USER_ENTERED"
    content_ok = mismatch_count == 0
    ok = all(
        (
            renderer_header_ok,
            live_header_ok,
            renderer_shape_ok,
            input_option_ok,
            row_count_ok,
            used_width_ok,
            content_ok,
            system_freshness_ok,
            extra_count == 0,
            formula_count == 0,
        )
    )
    grid = _grid_metadata(metadata, spec.title)
    return {
        "ok": ok,
        "tab_title": spec.title,
        "expected_headers": contract_headers,
        "renderer_headers": renderer_header,
        "live_headers": live_header,
        "renderer_headers_ok": renderer_header_ok,
        "live_headers_ok": live_header_ok,
        "renderer_value_input_option": expected.value_input_option,
        "renderer_value_input_option_ok": input_option_ok,
        "expected_row_count": expected_row_count,
        "renderer_used_row_count": renderer_used_rows,
        "live_used_row_count": live_row_count,
        "row_count_ok": row_count_ok,
        "expected_column_width": expected_columns,
        "renderer_used_column_width": renderer_used_columns,
        "live_used_column_width": live_used_columns,
        "used_column_width_ok": used_width_ok,
        "renderer_shape_ok": renderer_shape_ok,
        "grid_row_count": grid.get("rowCount"),
        "grid_column_count": grid.get("columnCount"),
        "source_live_mismatch_count": mismatch_count,
        "source_live_mismatches": mismatches,
        "source_live_content_ok": content_ok,
        "extra_populated_cell_count": extra_count,
        "extra_populated_cells": extra_cells,
        "no_populated_extra_cells": extra_count == 0,
        "unexpected_formula_cell_count": formula_count,
        "unexpected_formula_cells": formula_cells,
        "volatile_last_checked": volatile_checks,
        "system_watchdog_freshness": system_freshness,
    }


def _audit_workbook_metadata(metadata: dict[str, object], specs: tuple[TabSpec, ...]) -> dict[str, object]:
    properties = metadata.get("properties")
    properties = properties if isinstance(properties, dict) else {}
    workbook_title = str(properties.get("title") or "")
    timezone_name = str(properties.get("timeZone") or "America/New_York")

    sheets: list[dict[str, object]] = []
    for sheet in metadata.get("sheets", []):
        if not isinstance(sheet, dict):
            continue
        sheet_properties = sheet.get("properties")
        if not isinstance(sheet_properties, dict):
            continue
        sheets.append(
            {
                "title": str(sheet_properties.get("title") or ""),
                "hidden": bool(sheet_properties.get("hidden", False)),
            }
        )

    title_to_hidden = {str(sheet["title"]): bool(sheet["hidden"]) for sheet in sheets}
    expected_titles = [spec.title for spec in specs]
    missing = [title for title in expected_titles if title not in title_to_hidden]
    hidden_expected = [title for title in expected_titles if title_to_hidden.get(title) is True]
    visible_expected = [title for title in expected_titles if title in title_to_hidden and not title_to_hidden[title]]
    hidden_stale_qa = sorted(
        title
        for title, hidden in title_to_hidden.items()
        if title.startswith(sheets_sync.STALE_PUBLIC_QA_TAB_PREFIX) and hidden
    )
    visible_stale_qa = sorted(
        title
        for title, hidden in title_to_hidden.items()
        if title.startswith(sheets_sync.STALE_PUBLIC_QA_TAB_PREFIX) and not hidden
    )
    other_tabs = sorted(
        ({"title": title, "hidden": hidden} for title, hidden in title_to_hidden.items() if title not in expected_titles),
        key=lambda item: str(item["title"]),
    )
    title_ok = workbook_title == sheets_sync.PUBLIC_WORKBOOK_TITLE
    expected_tabs_visible_ok = not missing and not hidden_expected
    stale_qa_visibility_ok = not visible_stale_qa
    return {
        "ok": title_ok and expected_tabs_visible_ok and stale_qa_visibility_ok,
        "expected_title": sheets_sync.PUBLIC_WORKBOOK_TITLE,
        "live_title": workbook_title,
        "title_ok": title_ok,
        "time_zone": timezone_name,
        "expected_tabs": expected_titles,
        "visible_expected_tabs": visible_expected,
        "missing_expected_tabs": missing,
        "hidden_expected_tabs": hidden_expected,
        "expected_tabs_visible_ok": expected_tabs_visible_ok,
        "hidden_stale_qa_tabs": hidden_stale_qa,
        "visible_stale_qa_tabs": visible_stale_qa,
        "stale_qa_visibility_ok": stale_qa_visibility_ok,
        "other_tabs": other_tabs,
    }


def _workbook_timezone(metadata_audit: dict[str, object]) -> ZoneInfo:
    timezone_name = str(metadata_audit.get("time_zone") or "America/New_York")
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise RuntimeError(f"spreadsheet has unknown time zone {timezone_name!r}") from exc


def run_audit(
    *,
    resync: bool = False,
    retries: int = DEFAULT_RETRIES,
    retry_sleep: float = DEFAULT_RETRY_SLEEP_SECONDS,
    post_resync_wait: float = 2.0,
    max_last_checked_age_minutes: float = DEFAULT_MAX_LAST_CHECKED_AGE_MINUTES,
    max_last_checked_drift_minutes: float = DEFAULT_MAX_LAST_CHECKED_DRIFT_MINUTES,
    limit: int = 20,
    now: datetime | None = None,
    service_factory: Callable[[], object] | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "ok": False,
        "read_only": not resync,
        "resync_requested": resync,
        "resynced": False,
        "bounds": {
            "max_last_checked_age_minutes": max_last_checked_age_minutes,
            "max_last_checked_drift_minutes": max_last_checked_drift_minutes,
        },
        "workbook": {},
        "tabs": {},
        "renderer_error": "",
        "live_read_error": "",
        "resync_error": "",
    }
    if retries < 1 or retry_sleep < 0 or post_resync_wait < 0 or limit < 1:
        result["renderer_error"] = "retry, wait, and limit arguments are invalid"
        return result
    if max_last_checked_age_minutes < 0 or max_last_checked_drift_minutes < 0:
        result["renderer_error"] = "freshness and drift bounds must be non-negative"
        return result

    try:
        specs = _resolved_tab_specs()
        sheet_id = sheets_sync._watchdog_sheet_id()
        result["spreadsheet_id"] = sheet_id
    except Exception as exc:
        result["renderer_error"] = str(exc)
        return result

    if resync:
        try:
            sheets_sync.sync_replacement_watchdog_to_sheets()
            result["resynced"] = True
            if post_resync_wait:
                time.sleep(post_resync_wait)
        except Exception as exc:
            result["resync_error"] = str(exc)

    expected: dict[str, ExpectedTab] = {}
    try:
        expected = _capture_expected_values(sheet_id, specs)
    except Exception as exc:
        result["renderer_error"] = str(exc)

    live_workbook: LiveWorkbook | None = None
    live_error = ""
    attempts = 0
    factory = service_factory or sheets_sync._service
    for attempt in range(retries):
        attempts = attempt + 1
        try:
            live_workbook = _read_live_workbook(factory(), sheet_id, specs)
            live_error = ""
            break
        except Exception as exc:
            live_error = str(exc)
            if attempt + 1 < retries and retry_sleep:
                time.sleep(retry_sleep)
    result["live_read_attempts"] = attempts
    result["live_read_error"] = live_error
    if live_workbook is None:
        return result

    metadata_audit = _audit_workbook_metadata(live_workbook.metadata, specs)
    result["workbook"] = metadata_audit
    try:
        workbook_tz = _workbook_timezone(metadata_audit)
    except RuntimeError as exc:
        result["live_read_error"] = str(exc)
        return result

    checked_now = (now or datetime.now(tz=timezone.utc)).astimezone(timezone.utc)
    max_age_seconds = max_last_checked_age_minutes * 60.0
    max_drift_seconds = max_last_checked_drift_minutes * 60.0
    tab_results: dict[str, object] = {}
    for spec in specs:
        expected_tab = expected.get(spec.logical_name)
        live_tab = live_workbook.tabs.get(spec.logical_name)
        if expected_tab is None or live_tab is None:
            missing_parts = []
            if expected_tab is None:
                missing_parts.append("renderer payload")
            if live_tab is None:
                missing_parts.append("live tab")
            tab_results[spec.logical_name] = {
                "ok": False,
                "tab_title": spec.title,
                "error": f"missing {' and '.join(missing_parts)}",
            }
            continue
        tab_results[spec.logical_name] = _audit_tab(
            spec,
            expected_tab,
            live_tab,
            metadata=live_workbook.metadata,
            workbook_tz=workbook_tz,
            now=checked_now,
            max_age_seconds=max_age_seconds,
            max_drift_seconds=max_drift_seconds,
            limit=limit,
        )
    result["tabs"] = tab_results
    result["ok"] = bool(
        not result["renderer_error"]
        and not result["live_read_error"]
        and not result["resync_error"]
        and metadata_audit["ok"]
        and len(tab_results) == len(specs)
        and all(isinstance(tab, dict) and tab.get("ok") for tab in tab_results.values())
    )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read back and audit all six public replacement-watchdog Google Sheet tabs."
    )
    parser.add_argument(
        "--resync",
        action="store_true",
        help="Run the normal replacement-watchdog Sheet sync before the audit. Without this flag the audit is read-only.",
    )
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help="Live Sheet read attempts.")
    parser.add_argument(
        "--retry-sleep",
        type=float,
        default=DEFAULT_RETRY_SLEEP_SECONDS,
        help="Seconds between failed live reads.",
    )
    parser.add_argument(
        "--post-resync-wait",
        type=float,
        default=2.0,
        help="Seconds to wait after an explicitly requested resync.",
    )
    parser.add_argument(
        "--max-last-checked-age-minutes",
        type=float,
        default=DEFAULT_MAX_LAST_CHECKED_AGE_MINUTES,
        help=(
            "Maximum age of automatic-check timestamps in ElevatorWatch. "
            "Tenant-evidence timestamps must match the renderer but may legitimately be older."
        ),
    )
    parser.add_argument(
        "--max-last-checked-drift-minutes",
        type=float,
        default=DEFAULT_MAX_LAST_CHECKED_DRIFT_MINUTES,
        help=(
            "Maximum difference between live and freshly rendered automatic-check timestamps, "
            "and maximum allowed future skew for tenant evidence."
        ),
    )
    parser.add_argument("--limit", type=int, default=20, help="Maximum mismatch details per tab.")
    args = parser.parse_args(argv)
    result = run_audit(
        resync=args.resync,
        retries=args.retries,
        retry_sleep=args.retry_sleep,
        post_resync_wait=args.post_resync_wait,
        max_last_checked_age_minutes=args.max_last_checked_age_minutes,
        max_last_checked_drift_minutes=args.max_last_checked_drift_minutes,
        limit=args.limit,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
