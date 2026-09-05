from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Mapping
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file(ROOT / ".env")

from packages.db import Incident, MessageDecision, RawMessage, ServiceRequestCase, get_session
from packages.sheets import sync as sheets_sync

NY = ZoneInfo("America/New_York")
PUBLIC_MANAGED_COLUMNS = 10
PUBLIC_READ_RANGE = "A:ZZ"
APPROVED_PUBLIC_FORMULA_PREFIXES = ("=HYPERLINK(", "=IMAGE(")

REQUIRED_PUBLIC_ROWS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("section", "At a glance", ("At a glance",)),
    ("header", "At-a-glance header", ("Item", "Count / detail", "What this means")),
    ("section", "Category snapshot", ("Category snapshot",)),
    (
        "header",
        "Category snapshot header",
        ("Category", "Incidents", "311 filings", "Latest update", "Latest issue"),
    ),
    ("section", "Public update log", ("Public update log",)),
    (
        "header",
        "Public update log header",
        ("Updated", "Issue", "Category", "311 follow-up", "Preview", "Open evidence", "Summary"),
    ),
    ("section", "311 case watch", ("311 case watch",)),
    (
        "header",
        "311 case watch header",
        ("Case", "NYC status", "Complaint", "Related issue", "Submitted", "NYC lookup", "Notes"),
    ),
)


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

    @property
    def content_key(self) -> tuple[str, str, str, str]:
        return (
            _normalize_public_time(self.updated),
            _normalize_text(self.issue),
            _normalize_text(self.category),
            _normalize_text(self.summary),
        )


@dataclass(frozen=True)
class SourcePublicRow:
    message_id: str
    epoch: int
    row: PublicRow
    text_key: str = ""


@dataclass(frozen=True)
class LiveTenantLog:
    formatted_values: list[list[object]]
    formula_values: list[list[object]]
    metadata: dict[str, object]
    tab_properties: dict[str, object] | None


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


def _coverage_text(value: object) -> str:
    """Normalize only known renderer wording variants used for source coverage."""
    clean = _normalize_text(value)
    clean = re.sub(r"\b(?:likely|possibly)\b", "possibly", clean)
    clean = re.sub(r"\balarm\s+rung\b", "alarm rang", clean)
    clean = re.sub(r"\s+video\s+omitted\.?$", ".", clean)
    return clean


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


def _quoted_range(tab: str, a1_range: str) -> str:
    return f"'{tab.replace(chr(39), chr(39) * 2)}'!{a1_range}"


def _response_values(response: object, *, render_name: str) -> list[list[object]]:
    if not isinstance(response, dict):
        raise RuntimeError(f"{render_name} Tenant Log response was not an object")
    values = response.get("values", [])
    if not isinstance(values, list) or not all(isinstance(row, list) for row in values):
        raise RuntimeError(f"{render_name} Tenant Log response contained an invalid row set")
    return [list(row) for row in values]


def _live_values() -> LiveTenantLog:
    svc = sheets_sync._service()
    sheet_id = sheets_sync._public_sheet_id()
    tab = sheets_sync._public_updates_tab()
    spreadsheets = svc.spreadsheets()
    metadata = (
        spreadsheets.get(
            spreadsheetId=sheet_id,
            fields=(
                "properties(title,timeZone),"
                "sheets(properties(sheetId,title,hidden,gridProperties(rowCount,columnCount,frozenRowCount)))"
            ),
        )
        .execute()
    )
    if not isinstance(metadata, dict):
        raise RuntimeError("public workbook metadata response was not an object")

    matching_properties = [
        properties
        for sheet in metadata.get("sheets", [])
        if isinstance(sheet, dict)
        and isinstance((properties := sheet.get("properties")), dict)
        and properties.get("title") == tab
    ]
    if len(matching_properties) != 1:
        return LiveTenantLog([], [], metadata, None)

    range_name = _quoted_range(tab, PUBLIC_READ_RANGE)
    formatted = (
        spreadsheets.values()
        .get(
            spreadsheetId=sheet_id,
            range=range_name,
            majorDimension="ROWS",
            valueRenderOption="FORMATTED_VALUE",
        )
        .execute()
    )
    formulas = (
        spreadsheets.values()
        .get(
            spreadsheetId=sheet_id,
            range=range_name,
            majorDimension="ROWS",
            valueRenderOption="FORMULA",
        )
        .execute()
    )
    return LiveTenantLog(
        formatted_values=_response_values(formatted, render_name="formatted"),
        formula_values=_response_values(formulas, render_name="formula"),
        metadata=metadata,
        tab_properties=dict(matching_properties[0]),
    )


def _cell_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def _cell(values: list[list[object]], row_index: int, column_index: int) -> object:
    if row_index >= len(values) or column_index >= len(values[row_index]):
        return ""
    return values[row_index][column_index]


def _is_populated(value: object) -> bool:
    return _cell_text(value) != ""


def _is_formula(value: object) -> bool:
    return isinstance(value, str) and value.startswith("=")


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


def _parse_public_datetime(value: object) -> datetime | None:
    clean = _cell_text(value).replace("\u202f", " ").strip()
    for fmt in ("%Y-%m-%d %I:%M %p", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(clean, fmt)
        except ValueError:
            continue
    return None


def _display_cells_equivalent(expected: object, live: object) -> bool:
    if _cell_text(expected) == _cell_text(live):
        return True
    expected_datetime = _parse_public_datetime(expected)
    live_datetime = _parse_public_datetime(live)
    return bool(
        expected_datetime is not None
        and live_datetime is not None
        and expected_datetime == live_datetime
    )


def _populated_coordinates(*value_sets: list[list[object]]) -> set[tuple[int, int]]:
    coordinates: set[tuple[int, int]] = set()
    for values in value_sets:
        for row_index, row in enumerate(values):
            for column_index, value in enumerate(row):
                if _is_populated(value):
                    coordinates.add((row_index, column_index))
    return coordinates


def _coordinate_details(
    live: LiveTenantLog,
    coordinates: set[tuple[int, int]],
    *,
    limit: int,
) -> list[dict[str, object]]:
    details: list[dict[str, object]] = []
    for row_index, column_index in sorted(coordinates)[: max(limit, 0)]:
        formula = _cell(live.formula_values, row_index, column_index)
        details.append(
            {
                "row": row_index + 1,
                "column": column_index + 1,
                "display": _cell_text(_cell(live.formatted_values, row_index, column_index)),
                "formula": _cell_text(formula) if _is_formula(formula) else "",
            }
        )
    return details


def _required_rows_audit(
    expected_values: list[list[object]],
    live_values: list[list[object]],
) -> dict[str, object]:
    details: list[dict[str, object]] = []
    for kind, name, signature in REQUIRED_PUBLIC_ROWS:
        expected_matches = [
            row_index
            for row_index, row in enumerate(expected_values)
            if _cell_text(row[0] if row else "") == signature[0]
        ]
        live_matches = [
            row_index
            for row_index, row in enumerate(live_values)
            if _cell_text(row[0] if row else "") == signature[0]
        ]
        expected_row = expected_matches[0] if len(expected_matches) == 1 else None
        expected_signature_ok = bool(
            expected_row is not None
            and tuple(
                _cell_text(_cell(expected_values, expected_row, column_index))
                for column_index in range(len(signature))
            )
            == signature
        )
        live_signature_ok = bool(
            expected_row is not None
            and live_matches == [expected_row]
            and tuple(
                _cell_text(_cell(live_values, expected_row, column_index))
                for column_index in range(len(signature))
            )
            == signature
        )
        details.append(
            {
                "kind": kind,
                "name": name,
                "expected_row": expected_row + 1 if expected_row is not None else None,
                "live_matching_rows": [row + 1 for row in live_matches],
                "renderer_signature_ok": expected_signature_ok,
                "live_signature_ok": live_signature_ok,
                "ok": expected_signature_ok and live_signature_ok,
            }
        )
    sections = [row for row in details if row["kind"] == "section"]
    headers = [row for row in details if row["kind"] == "header"]
    sections_ok = all(bool(row["ok"]) for row in sections)
    headers_ok = all(bool(row["ok"]) for row in headers)
    return {
        "ok": sections_ok and headers_ok,
        "required_section_rows_ok": sections_ok,
        "required_header_rows_ok": headers_ok,
        "rows": details,
    }


def _tab_metadata_audit(
    tab_properties: Mapping[str, object] | None,
    *,
    expected_rows: int,
) -> dict[str, object]:
    if tab_properties is None:
        return {
            "ok": False,
            "tab_present": False,
            "tab_visible": False,
            "title": "",
            "sheet_id": None,
            "grid_row_count": None,
            "grid_column_count": None,
            "grid_capacity_ok": False,
        }
    grid = tab_properties.get("gridProperties")
    grid = grid if isinstance(grid, dict) else {}
    grid_rows = grid.get("rowCount")
    grid_columns = grid.get("columnCount")
    grid_capacity_ok = bool(
        isinstance(grid_rows, int)
        and grid_rows >= expected_rows
        and isinstance(grid_columns, int)
        and grid_columns >= PUBLIC_MANAGED_COLUMNS
    )
    visible = not bool(tab_properties.get("hidden", False))
    title = _cell_text(tab_properties.get("title"))
    title_ok = title == sheets_sync._public_updates_tab()
    return {
        "ok": visible and title_ok and grid_capacity_ok,
        "tab_present": True,
        "tab_visible": visible,
        "title": title,
        "title_ok": title_ok,
        "sheet_id": tab_properties.get("sheetId"),
        "grid_row_count": grid_rows,
        "grid_column_count": grid_columns,
        "grid_capacity_ok": grid_capacity_ok,
        "frozen_row_count": grid.get("frozenRowCount"),
    }


def _audit_logical_tenant_log(
    expected_values: list[list[object]],
    live: LiveTenantLog,
    *,
    limit: int,
) -> dict[str, object]:
    expected_row_count = len(expected_values)
    renderer_payload_width = max((len(row) for row in expected_values), default=0)
    renderer_managed_width_ok = bool(
        expected_values
        and renderer_payload_width == PUBLIC_MANAGED_COLUMNS
        and all(len(row) == PUBLIC_MANAGED_COLUMNS for row in expected_values)
    )
    expected_used_rows, expected_used_columns = _used_shape(expected_values)
    live_used_rows, live_used_columns = _used_shape(
        live.formatted_values, live.formula_values
    )

    mismatch_count = 0
    mismatches: list[dict[str, object]] = []
    renderer_formula_errors: list[dict[str, object]] = []
    expected_formula_count = 0
    exact_formula_match_count = 0
    for row_index in range(expected_row_count):
        for column_index in range(PUBLIC_MANAGED_COLUMNS):
            expected = _cell(expected_values, row_index, column_index)
            display = _cell(live.formatted_values, row_index, column_index)
            formula = _cell(live.formula_values, row_index, column_index)
            reason = ""
            if _is_formula(expected):
                expected_formula_count += 1
                expected_formula = _cell_text(expected)
                if not expected_formula.startswith(APPROVED_PUBLIC_FORMULA_PREFIXES):
                    reason = "renderer emitted a formula outside the approved HYPERLINK/IMAGE set"
                    renderer_formula_errors.append(
                        {
                            "row": row_index + 1,
                            "column": column_index + 1,
                            "formula": expected_formula,
                        }
                    )
                elif _cell_text(formula) != expected_formula:
                    reason = "formula differs from the exact renderer formula"
                else:
                    exact_formula_match_count += 1
            elif _is_formula(formula):
                reason = "unexpected formula in a renderer value cell"
            elif not _display_cells_equivalent(expected, display):
                reason = "formatted value differs from the renderer payload"

            if not reason:
                continue
            mismatch_count += 1
            if len(mismatches) < max(limit, 0):
                mismatches.append(
                    {
                        "row": row_index + 1,
                        "column": column_index + 1,
                        "expected": _cell_text(expected),
                        "live_display": _cell_text(display),
                        "live_formula": _cell_text(formula) if _is_formula(formula) else "",
                        "reason": reason,
                    }
                )

    live_formula_coordinates = {
        (row_index, column_index)
        for row_index, row in enumerate(live.formula_values)
        for column_index, value in enumerate(row)
        if _is_formula(value)
    }
    unexpected_formula_coordinates = {
        (row_index, column_index)
        for row_index, column_index in live_formula_coordinates
        if not (
            row_index < expected_row_count
            and column_index < PUBLIC_MANAGED_COLUMNS
            and _is_formula(_cell(expected_values, row_index, column_index))
        )
    }

    populated_coordinates = _populated_coordinates(
        live.formatted_values, live.formula_values
    )
    extra_coordinates = {
        coordinate
        for coordinate in populated_coordinates
        if coordinate[0] >= expected_row_count
        or coordinate[1] >= PUBLIC_MANAGED_COLUMNS
    }
    stale_extra_row_coordinates = {
        coordinate for coordinate in populated_coordinates if coordinate[0] >= expected_row_count
    }
    beyond_managed_coordinates = {
        coordinate
        for coordinate in populated_coordinates
        if coordinate[1] >= PUBLIC_MANAGED_COLUMNS
    }

    required_rows = _required_rows_audit(expected_values, live.formatted_values)
    metadata = _tab_metadata_audit(
        live.tab_properties,
        expected_rows=expected_row_count,
    )
    renderer_row_shape_ok = expected_used_rows == expected_row_count
    row_count_ok = live_used_rows == expected_row_count
    used_width_ok = live_used_columns == expected_used_columns
    formula_ok = bool(
        not renderer_formula_errors
        and exact_formula_match_count == expected_formula_count
        and not unexpected_formula_coordinates
    )
    content_ok = mismatch_count == 0
    ok = all(
        (
            renderer_managed_width_ok,
            renderer_row_shape_ok,
            row_count_ok,
            used_width_ok,
            content_ok,
            formula_ok,
            not extra_coordinates,
            not stale_extra_row_coordinates,
            not beyond_managed_coordinates,
            required_rows["ok"],
            metadata["ok"],
        )
    )
    workbook_properties = live.metadata.get("properties")
    workbook_properties = workbook_properties if isinstance(workbook_properties, dict) else {}
    return {
        "ok": ok,
        "managed_column_count": PUBLIC_MANAGED_COLUMNS,
        "renderer_payload_row_count": expected_row_count,
        "renderer_payload_column_width": renderer_payload_width,
        "renderer_managed_width_ok": renderer_managed_width_ok,
        "renderer_used_row_count": expected_used_rows,
        "renderer_row_shape_ok": renderer_row_shape_ok,
        "renderer_used_column_width": expected_used_columns,
        "live_used_row_count": live_used_rows,
        "row_count_ok": row_count_ok,
        "live_used_column_width": live_used_columns,
        "used_column_width_ok": used_width_ok,
        "source_live_mismatch_count": mismatch_count,
        "source_live_mismatches": mismatches,
        "source_live_content_ok": content_ok,
        "expected_formula_count": expected_formula_count,
        "exact_formula_match_count": exact_formula_match_count,
        "renderer_formula_errors": renderer_formula_errors[: max(limit, 0)],
        "unexpected_formula_cell_count": len(unexpected_formula_coordinates),
        "unexpected_formula_cells": _coordinate_details(
            live, unexpected_formula_coordinates, limit=limit
        ),
        "formula_cells_ok": formula_ok,
        "extra_populated_cell_count": len(extra_coordinates),
        "extra_populated_cells": _coordinate_details(live, extra_coordinates, limit=limit),
        "stale_extra_row_cell_count": len(stale_extra_row_coordinates),
        "stale_extra_row_cells": _coordinate_details(
            live, stale_extra_row_coordinates, limit=limit
        ),
        "no_stale_extra_rows": not stale_extra_row_coordinates,
        "beyond_managed_column_cell_count": len(beyond_managed_coordinates),
        "beyond_managed_column_cells": _coordinate_details(
            live, beyond_managed_coordinates, limit=limit
        ),
        "no_populated_or_formula_cells_beyond_managed_range": not beyond_managed_coordinates,
        "required_rows": required_rows,
        "tab_metadata": metadata,
        "workbook_title": _cell_text(workbook_properties.get("title")),
        "workbook_time_zone": _cell_text(workbook_properties.get("timeZone")),
    }


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


def _public_row_covers_source_row(public_row: PublicRow, source_row: PublicRow) -> bool:
    if public_row.content_key == source_row.content_key:
        return True
    public_time = _row_time(public_row.updated)
    source_time = _row_time(source_row.updated)
    exact_time = _normalize_public_time(public_row.updated) == _normalize_public_time(source_row.updated)
    within_renderer_dedupe_window = bool(
        public_time is not None
        and source_time is not None
        and abs((public_time - source_time).total_seconds()) <= sheets_sync.PUBLIC_DUPLICATE_WINDOW_SECONDS
    )
    cross_time_alarm_alias = False
    if not exact_time:
        cross_time_alarm_alias = (
            within_renderer_dedupe_window
            and "alarm rang" in _coverage_text(public_row.issue)
            and "alarm rang" in _coverage_text(source_row.issue)
            and _coverage_text(public_row.summary) == _coverage_text(source_row.summary)
        )
        if not cross_time_alarm_alias:
            return False
    public_category = _normalize_text(public_row.category)
    source_category = _normalize_text(source_row.category)
    if source_category != public_category and source_category not in public_category:
        return False
    if cross_time_alarm_alias:
        return True
    public_issue = _coverage_text(public_row.issue)
    source_issue = _coverage_text(source_row.issue)
    if (
        _normalize_text(public_row.category) == "elevator"
        and "working" in public_issue
        and "working" in source_issue
        and "both elevator" in public_issue
        and any(side in source_issue for side in ("north elevator", "south elevator"))
    ):
        return True
    if public_issue != source_issue and source_issue not in public_issue:
        return False
    public_summary = _coverage_text(public_row.summary)
    source_summary = _coverage_text(source_row.summary)
    return public_summary == source_summary or source_summary in public_summary


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
    all_expected_rows = _public_rows(expected_values)
    expected_rows = _recent(all_expected_rows, days=days)

    live = LiveTenantLog([], [], {}, None)
    last_error = ""
    for attempt in range(max(1, retries)):
        try:
            readback = _live_values()
            if isinstance(readback, LiveTenantLog):
                live = readback
            else:
                # Compatibility for callers/tests which supplied the old
                # formatted-only helper. Production always returns both views
                # and real metadata from ``_live_values``.
                legacy_values = [list(row) for row in readback]
                tab = sheets_sync._public_updates_tab()
                live = LiveTenantLog(
                    formatted_values=legacy_values,
                    formula_values=legacy_values,
                    metadata={
                        "properties": {"timeZone": "America/New_York"},
                        "sheets": [],
                    },
                    tab_properties={
                        "title": tab,
                        "sheetId": 0,
                        "hidden": False,
                        "gridProperties": {
                            "rowCount": max(len(legacy_values), 1),
                            "columnCount": max(PUBLIC_MANAGED_COLUMNS, 1),
                        },
                    },
                )
            last_error = ""
            break
        except Exception as exc:
            last_error = str(exc)
            if attempt + 1 >= retries:
                break
            time.sleep(retry_sleep)

    live_values = live.formatted_values
    logical_tenant_log = _audit_logical_tenant_log(
        expected_values,
        live,
        limit=limit,
    )
    live_rows = _recent(_public_rows(live_values), days=days) if live_values else []
    live_keys = {row.key for row in live_rows}
    expected_keys = {row.key for row in expected_rows}
    missing = [row for row in expected_rows if row.key not in live_keys]
    unexpected = [row for row in live_rows if row.key not in expected_keys]
    source_rows = _source_public_rows(days=days)
    missing_source = [
        row
        for row in source_rows
        if row.row.key not in live_keys and not any(_public_row_covers_source_row(live_row, row.row) for live_row in live_rows)
    ]

    # The activity window may be quiet while the public sheet correctly retains
    # an older latest update. Compare that summary metric against the complete
    # current renderer, not only the recent slice, so a quiet week cannot cause
    # an unnecessary write/repair loop.
    expected_latest = all_expected_rows[0].issue if all_expected_rows else ""
    live_latest = _metric(live_values, "Latest update") if live_values else ""
    latest_ok = _normalize_text(expected_latest) == _normalize_text(live_latest)
    ok = bool(
        not last_error
        and logical_tenant_log["ok"]
        and not missing
        and not unexpected
        and not missing_source
        and latest_ok
    )

    return {
        "ok": ok,
        "read_only": not resync,
        "days": days,
        "resynced": resync,
        "full_tenant_log_ok": logical_tenant_log["ok"],
        "logical_tenant_log": logical_tenant_log,
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
