from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from packages.db import FilingJob, Incident, MessageDecision, RawMessage, ServiceRequestCase, get_session
from packages.tasker_capture import is_noise_tasker_capture, normalize_tasker_capture, tasker_duplicate_window_seconds
from packages.timeutil import normalize_timestamp, parse_ts_to_epoch
from packages.verification.coverage import compute_daily_coverage, detect_gaps
from packages.whatsapp.media import attachment_context, public_attachment_entries
from packages.whatsapp.parser import is_media_placeholder_text

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
NY = ZoneInfo("America/New_York")
DECISION_LOG_LIMIT = 500
DECISION_LOG_FETCH_LIMIT = 1500
SHEET_MEDIA_LINK_LIMIT = 3
SHEET_EXTERNAL_LINK_LIMIT = 2
PUBLIC_CURRENT_ISSUE_MAX_AGE_HOURS = 72
PUBLIC_RECENT_ISSUE_MAX_AGE_HOURS = 168
PUBLIC_LAYOUT_COLUMNS = 10
PUBLIC_WORKBOOK_TITLE = "455 Tenants Log"
PUBLIC_FROZEN_ROWS = 1
PUBLIC_THUMBNAIL_HEIGHT = 110
PUBLIC_THUMBNAIL_WIDTH = 240
LEGACY_PUBLIC_UPDATE_TABS = ("PublicUpdates",)
PUBLIC_PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[\s.\-]?)?(?:\(\d{3}\)|\d{3})[\s.\-]\d{3}[\s.\-]\d{4}(?!\d)")
PUBLIC_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PUBLIC_UNIT_LINE_RE = re.compile(r"^(?:apt\.?|apartment|unit)?\s*\d{1,3}[A-Z]?(?:\s+here)?\.?$", re.IGNORECASE)
PUBLIC_INLINE_UNIT_RE = re.compile(
    r"(^|[.!?]\s+)(?:apt\.?|apartment|unit)\s*\d{1,3}[A-Z]?[.!?:,\-]?\s+"
    r"|(^|[.!?]\s+)\d{1,3}[A-Z](?:\s+here)?[.!?:,\-]?\s+"
    r"|(^|[.!?]\s+)\d{1,3}[.!?:,\-]\s+",
    re.IGNORECASE,
)
PUBLIC_TIME_LINE_RE = re.compile(r"^\d{1,2}:\d{2}\s*(?:AM|PM)?$", re.IGNORECASE)
PUBLIC_PERSON_ACTION_RE = re.compile(
    r"(?:\s+(?:and|,)?\s*)?\b(?:has\s+)?(?:reported|informed|notified|told|texted|called|contacted|messaged|sent)\s+"
    r"(?:it\s+)?(?!to\s+)[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\b\.?",
    re.IGNORECASE,
)
PUBLIC_REPORT_RECIPIENT_ACTION_RE = re.compile(
    r"\b(?:has\s+)?(?:reported|informed|notified|told|texted|called|contacted|messaged|sent)\s+"
    r"(?:it\s+)?to\s+(?P<person>[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\b\.?",
    re.IGNORECASE,
)
PUBLIC_LEADING_PERSON_RE = re.compile(r"^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\s+(?:said|says|reported|asked|texted|called|wrote)\b[:,]?\s*", re.IGNORECASE)
PUBLIC_AS_PER_PERSON_RE = re.compile(r"(?:\s*[,;:]?\s*)\b(?:as per|according to)\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\b", re.IGNORECASE)
PUBLIC_SUBJECT_PERSON_RE = re.compile(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\s+(?=(?:mans|said|says|reported|asked|texted|called|wrote)\b)", re.IGNORECASE)
PUBLIC_DEFAULT_REDACTED_NAMES = (
    "Emma",
    "Greg",
    "Hercules",
    "Jack",
    "Jacob",
    "Karen",
    "Meredith",
    "Molly",
    "Nic",
    "Piotr",
    "Tibor",
    "Val",
    "Wattle",
    "Weinreb",
    "Wojtek",
    "Yvonne",
)
PUBLIC_DETAIL_STOP_WORDS = {
    "a",
    "an",
    "and",
    "again",
    "are",
    "at",
    "has",
    "have",
    "in",
    "is",
    "it",
    "of",
    "on",
    "reported",
    "that",
    "the",
    "there",
    "to",
}
PUBLIC_REPORT_RECIPIENT_ROLE_HINTS = (
    "super",
    "superintendent",
    "doorman",
    "concierge",
    "building manager",
    "porter",
    "maintenance",
)


def _disabled() -> bool:
    return os.environ.get("DISABLE_SHEETS_SYNC", "0").strip().lower() in {"1", "true", "yes", "on"}


def _creds_path() -> str:
    candidates = [
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        "/run/secrets/gcp_sa.json",
        "/run/secrets/gcp_sa_json",
        "/etc/secrets/gcp_sa.json",
        "secrets/gcp_sa.json",
    ]
    for path in candidates:
        if path and os.path.exists(path):
            return path
    raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or missing")


def _env_first(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value is not None and value != "":
            return value
    return default


def _service():
    if _disabled():
        raise RuntimeError("Sheets sync disabled")
    creds_path = _creds_path()
    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def _sheet_id():
    sid = os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not sid:
        raise RuntimeError("GOOGLE_SHEETS_SPREADSHEET_ID not set")
    return sid


def _tab(*names: str, default: str) -> str:
    return _env_first(*names, default=default) or default


def _clean_text(value: str | None) -> str:
    return (value or "").replace("\u202f", " ").replace("\u200e", "").replace("\u200f", "").strip()


def _split_names(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    out: list[str] = []
    seen: set[str] = set()
    for item in value.replace("\n", ",").split(","):
        clean = _clean_text(item)
        if not clean:
            continue
        lowered = clean.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append(clean)
    return tuple(out)


def _allowed_public_chat_names() -> set[str]:
    configured = _env_first("PUBLIC_UPDATES_CHAT_NAMES", "TENANT_PUBLIC_CHAT_NAMES", "WHATSAPP_CAPTURE_CHAT_NAMES", default="") or ""
    return {item.casefold() for item in _split_names(configured)}


def _public_sheet_id() -> str:
    return _env_first("GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID", "GOOGLE_SHEETS_SPREADSHEET_ID") or _sheet_id()


def _configured_public_sheet_id() -> str:
    return (os.environ.get("GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID") or "").strip()


def _public_updates_tab() -> str:
    return _tab("SHEETS_PUBLIC_UPDATES_TAB", default="Tenant Log")


def _column_label(index: int) -> str:
    if index < 1:
        raise ValueError("column index must be >= 1")
    label = ""
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        label = chr(65 + remainder) + label
    return label


def _replace_tab_values(
    svc,
    sheet_id: str,
    tab: str,
    values: list[list[object]],
    *,
    value_input_option: str = "RAW",
) -> None:
    if not values:
        svc.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range=f"{tab}!A:ZZ",
            body={},
        ).execute()
        return

    max_width = max(max((len(row) for row in values), default=0), 1)
    padded_values = [list(row) + [""] * (max_width - len(row)) for row in values]
    row_count = len(padded_values)

    svc.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"{tab}!A1",
        valueInputOption=value_input_option,
        body={"values": padded_values},
    ).execute()

    # Clear stale rows after the write so the sheet never flashes fully empty mid-sync.
    svc.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=f"{tab}!A{row_count + 1}:ZZ",
        body={},
    ).execute()

    # Clear stale cells to the right if the new shape is narrower than the old one.
    if max_width < 702:
        svc.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range=f"{tab}!{_column_label(max_width + 1)}1:ZZ{row_count}",
            body={},
        ).execute()


def _quoted_sheet_range(tab: str, a1_range: str) -> str:
    escaped = tab.replace("'", "''")
    return f"'{escaped}'!{a1_range}"


def _clear_legacy_public_update_tabs(svc, public_sheet_id: str) -> None:
    configured_public_sheet = _configured_public_sheet_id()
    if not configured_public_sheet:
        return
    try:
        internal_sheet_id = _sheet_id()
    except RuntimeError:
        return
    if internal_sheet_id == public_sheet_id:
        return
    titles = _sheet_title_to_id_map(svc, internal_sheet_id)
    for tab in LEGACY_PUBLIC_UPDATE_TABS:
        if tab not in titles:
            continue
        svc.spreadsheets().values().clear(
            spreadsheetId=internal_sheet_id,
            range=_quoted_sheet_range(tab, "A:ZZ"),
            body={},
        ).execute()


def _ensure_tab_exists(svc, sheet_id: str, tab: str, *, rename_single_existing: bool = False) -> None:
    titles = _sheet_title_to_id_map(svc, sheet_id)
    if tab in titles:
        return
    if rename_single_existing and len(titles) == 1:
        existing_title, sheet_gid = next(iter(titles.items()))
        if existing_title != tab:
            svc.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={
                    "requests": [
                        {
                            "updateSheetProperties": {
                                "properties": {"sheetId": sheet_gid, "title": tab},
                                "fields": "title",
                            }
                        }
                    ]
                },
            ).execute()
            return
    svc.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": tab}}}]},
    ).execute()


def _sheet_title_to_id_map(svc, sheet_id: str) -> dict[str, int]:
    meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
    out: dict[str, int] = {}
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        title = props.get("title")
        sheet_gid = props.get("sheetId")
        if isinstance(title, str) and isinstance(sheet_gid, int):
            out[title] = sheet_gid
    return out


def _set_spreadsheet_title(svc, sheet_id: str, title: str) -> None:
    svc.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={
            "requests": [
                {
                    "updateSpreadsheetProperties": {
                        "properties": {"title": title},
                        "fields": "title",
                    }
                }
            ]
        },
    ).execute()


def _dimension_resize_request(sheet_gid: int, *, dimension: str, start: int, end: int, pixel_size: int) -> dict[str, object]:
    return {
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_gid,
                "dimension": dimension,
                "startIndex": start,
                "endIndex": end,
            },
            "properties": {"pixelSize": pixel_size},
            "fields": "pixelSize",
        }
    }


def _wrap_range_request(
    sheet_gid: int,
    *,
    start_col: int,
    end_col: int,
    start_row: int = 1,
    end_row: int | None = None,
) -> dict[str, object]:
    grid_range: dict[str, object] = {
        "sheetId": sheet_gid,
        "startRowIndex": start_row,
        "startColumnIndex": start_col,
        "endColumnIndex": end_col,
    }
    if end_row is not None:
        grid_range["endRowIndex"] = end_row
    return {
        "repeatCell": {
            "range": grid_range,
            "cell": {
                "userEnteredFormat": {
                    "wrapStrategy": "WRAP",
                    "verticalAlignment": "TOP",
                }
            },
            "fields": "userEnteredFormat.wrapStrategy,userEnteredFormat.verticalAlignment",
        }
    }


def _apply_tab_layout(
    svc,
    sheet_id: str,
    tab: str,
    *,
    row_count: int,
    column_count: int,
    layout: str,
    layout_meta: dict[str, object] | None = None,
) -> None:
    sheet_gid = _sheet_title_to_id_map(svc, sheet_id).get(tab)
    if sheet_gid is None:
        return

    end_row = max(row_count, 1)
    meta = layout_meta or {}
    frozen_rows = int(meta.get("freeze_rows", 1))
    requests: list[dict[str, object]] = [
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_gid, "gridProperties": {"frozenRowCount": frozen_rows}},
                "fields": "gridProperties.frozenRowCount",
            }
        }
    ]

    if layout == "public_updates":
        title_row = max(int(meta.get("title_row", 1)) - 1, 0)
        subtitle_row = max(int(meta.get("subtitle_row", 2)) - 1, 0)
        stats_row = max(int(meta.get("stats_row", 3)) - 1, 0)
        stats_row_count = max(int(meta.get("stats_row_count", 2)), 1)
        section_rows = meta.get("section_rows")
        if isinstance(section_rows, list):
            section_rows = [max(int(row) - 1, 0) for row in section_rows]
        else:
            section_rows = [
                max(int(meta.get("right_now_title_row", 5)) - 1, 0),
                max(int(meta.get("recent_log_title_row", 0)) - 1, 0),
            ]
        header_rows = meta.get("header_rows")
        if isinstance(header_rows, list):
            header_rows = [max(int(row) - 1, 0) for row in header_rows]
        else:
            header_rows = [
                max(int(meta.get("right_now_header_row", 6)) - 1, 0),
                max(int(meta.get("recent_log_header_row", 0)) - 1, 0),
            ]
        section_rows = [row for row in section_rows if row >= 0]
        header_rows = [row for row in header_rows if row >= 0]

        requests.extend(
            [
                {
                    "clearBasicFilter": {
                        "sheetId": sheet_gid,
                    }
                },
                {
                    "unmergeCells": {
                        "range": {
                            "sheetId": sheet_gid,
                            "startRowIndex": 0,
                            "endRowIndex": max(end_row, 80),
                            "startColumnIndex": 0,
                            "endColumnIndex": max(column_count, PUBLIC_LAYOUT_COLUMNS),
                        }
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_gid,
                            "startRowIndex": 0,
                            "endRowIndex": max(end_row, 80),
                            "startColumnIndex": 0,
                            "endColumnIndex": max(column_count, PUBLIC_LAYOUT_COLUMNS),
                        },
                        "cell": {"userEnteredFormat": {}},
                        "fields": (
                            "userEnteredFormat.backgroundColor,"
                            "userEnteredFormat.horizontalAlignment,"
                            "userEnteredFormat.verticalAlignment,"
                            "userEnteredFormat.textFormat,"
                            "userEnteredFormat.wrapStrategy"
                        ),
                    }
                },
                _wrap_range_request(sheet_gid, start_col=0, end_col=max(column_count, PUBLIC_LAYOUT_COLUMNS), start_row=0, end_row=end_row),
                _dimension_resize_request(sheet_gid, dimension="ROWS", start=0, end=end_row, pixel_size=122),
                _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=0, end=1, pixel_size=145),
                _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=1, end=2, pixel_size=180),
                _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=2, end=3, pixel_size=210),
                _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=3, end=4, pixel_size=120),
                _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=4, end=5, pixel_size=205),
                _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=5, end=6, pixel_size=250),
                _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=6, end=7, pixel_size=130),
                _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=7, end=8, pixel_size=340),
                _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=8, end=10, pixel_size=80),
                {
                    "mergeCells": {
                        "range": {
                            "sheetId": sheet_gid,
                            "startRowIndex": title_row,
                            "endRowIndex": title_row + 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": PUBLIC_LAYOUT_COLUMNS,
                        },
                        "mergeType": "MERGE_ALL",
                    }
                },
                {
                    "mergeCells": {
                        "range": {
                            "sheetId": sheet_gid,
                            "startRowIndex": subtitle_row,
                            "endRowIndex": subtitle_row + 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": PUBLIC_LAYOUT_COLUMNS,
                        },
                        "mergeType": "MERGE_ALL",
                    }
                },
                _dimension_resize_request(sheet_gid, dimension="ROWS", start=title_row, end=title_row + 1, pixel_size=38),
                _dimension_resize_request(sheet_gid, dimension="ROWS", start=subtitle_row, end=subtitle_row + 1, pixel_size=42),
                _dimension_resize_request(sheet_gid, dimension="ROWS", start=stats_row, end=stats_row + stats_row_count, pixel_size=32),
                {
                    "repeatCell": {
                        "range": {"sheetId": sheet_gid, "startRowIndex": title_row, "endRowIndex": title_row + 1},
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {"red": 0.16, "green": 0.18, "blue": 0.18},
                                "horizontalAlignment": "LEFT",
                                "verticalAlignment": "MIDDLE",
                                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 14},
                                "wrapStrategy": "WRAP",
                            }
                        },
                        "fields": (
                            "userEnteredFormat.backgroundColor,"
                            "userEnteredFormat.horizontalAlignment,"
                            "userEnteredFormat.verticalAlignment,"
                            "userEnteredFormat.textFormat,"
                            "userEnteredFormat.wrapStrategy"
                        ),
                    }
                },
                {
                    "repeatCell": {
                        "range": {"sheetId": sheet_gid, "startRowIndex": subtitle_row, "endRowIndex": subtitle_row + 1},
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.93},
                                "horizontalAlignment": "LEFT",
                                "verticalAlignment": "MIDDLE",
                                "textFormat": {"foregroundColor": {"red": 0.18, "green": 0.19, "blue": 0.19}, "fontSize": 10},
                                "wrapStrategy": "WRAP",
                            }
                        },
                        "fields": (
                            "userEnteredFormat.backgroundColor,"
                            "userEnteredFormat.horizontalAlignment,"
                            "userEnteredFormat.verticalAlignment,"
                            "userEnteredFormat.textFormat,"
                            "userEnteredFormat.wrapStrategy"
                        ),
                    }
                },
                {
                    "repeatCell": {
                        "range": {"sheetId": sheet_gid, "startRowIndex": stats_row, "endRowIndex": stats_row + stats_row_count},
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {"red": 0.98, "green": 0.96, "blue": 0.90},
                                "verticalAlignment": "MIDDLE",
                                "wrapStrategy": "WRAP",
                            }
                        },
                        "fields": (
                            "userEnteredFormat.backgroundColor,"
                            "userEnteredFormat.verticalAlignment,"
                            "userEnteredFormat.wrapStrategy"
                        ),
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_gid,
                            "startRowIndex": stats_row,
                            "endRowIndex": stats_row + stats_row_count,
                            "startColumnIndex": 0,
                            "endColumnIndex": 2,
                        },
                        "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                        "fields": "userEnteredFormat.textFormat",
                    }
                },
            ]
        )

        spacer_rows = sorted({row - 1 for row in section_rows[1:] if row > 0})
        for row in spacer_rows:
            requests.append(_dimension_resize_request(sheet_gid, dimension="ROWS", start=row, end=row + 1, pixel_size=14))

        compact_table_count = min(2, len(header_rows), max(len(section_rows) - 1, 0))
        for table_index in range(compact_table_count):
            data_start = header_rows[table_index] + 1
            data_end = max(data_start, section_rows[table_index + 1] - 1)
            if data_end > data_start:
                requests.append(
                    _dimension_resize_request(
                        sheet_gid,
                        dimension="ROWS",
                        start=data_start,
                        end=data_end,
                        pixel_size=34,
                    )
                )

        for row in section_rows:
            requests.extend(
                [
                    {
                        "mergeCells": {
                            "range": {
                                "sheetId": sheet_gid,
                                "startRowIndex": row,
                                "endRowIndex": row + 1,
                                "startColumnIndex": 0,
                                "endColumnIndex": PUBLIC_LAYOUT_COLUMNS,
                            },
                            "mergeType": "MERGE_ALL",
                        }
                    },
                    _dimension_resize_request(sheet_gid, dimension="ROWS", start=row, end=row + 1, pixel_size=30),
                    {
                        "repeatCell": {
                            "range": {"sheetId": sheet_gid, "startRowIndex": row, "endRowIndex": row + 1},
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": {"red": 0.23, "green": 0.34, "blue": 0.35},
                                    "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                                    "verticalAlignment": "MIDDLE",
                                }
                            },
                            "fields": (
                                "userEnteredFormat.backgroundColor,"
                                "userEnteredFormat.textFormat,"
                                "userEnteredFormat.verticalAlignment"
                            ),
                        }
                    },
                ]
            )

        for row in header_rows:
            requests.extend(
                [
                    _dimension_resize_request(sheet_gid, dimension="ROWS", start=row, end=row + 1, pixel_size=28),
                    {
                        "repeatCell": {
                            "range": {"sheetId": sheet_gid, "startRowIndex": row, "endRowIndex": row + 1},
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": {"red": 0.92, "green": 0.92, "blue": 0.89},
                                    "textFormat": {"bold": True},
                                    "wrapStrategy": "WRAP",
                                    "verticalAlignment": "MIDDLE",
                                }
                            },
                            "fields": (
                                "userEnteredFormat.backgroundColor,"
                                "userEnteredFormat.textFormat,"
                                "userEnteredFormat.wrapStrategy,"
                                "userEnteredFormat.verticalAlignment"
                            ),
                        }
                    },
                ]
            )
    else:
        requests.append(
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_gid, "startRowIndex": 0, "endRowIndex": 1},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 0.95, "green": 0.92, "blue": 0.86},
                            "textFormat": {"bold": True},
                            "wrapStrategy": "WRAP",
                            "verticalAlignment": "MIDDLE",
                        }
                    },
                    "fields": (
                        "userEnteredFormat.backgroundColor,"
                        "userEnteredFormat.textFormat,"
                        "userEnteredFormat.wrapStrategy,"
                        "userEnteredFormat.verticalAlignment"
                    ),
                }
            }
        )

        if row_count > 0 and column_count > 0:
            requests.append(
                {
                    "setBasicFilter": {
                        "filter": {
                            "range": {
                                "sheetId": sheet_gid,
                                "startRowIndex": 0,
                                "endRowIndex": row_count,
                                "startColumnIndex": 0,
                                "endColumnIndex": column_count,
                            }
                        }
                    }
                }
            )

    if layout == "dashboard":
        requests.extend([
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=0, end=1, pixel_size=220),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=1, end=2, pixel_size=520),
        ])
    elif layout == "incidents":
        requests.extend([
            _dimension_resize_request(sheet_gid, dimension="ROWS", start=1, end=end_row, pixel_size=120),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=0, end=1, pixel_size=170),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=1, end=3, pixel_size=120),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=8, end=10, pixel_size=260),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=11, end=15, pixel_size=180),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=15, end=18, pixel_size=220),
            _wrap_range_request(sheet_gid, start_col=8, end_col=10, end_row=end_row),
            _wrap_range_request(sheet_gid, start_col=15, end_col=18, end_row=end_row),
        ])
    elif layout == "decisions":
        requests.extend([
            _dimension_resize_request(sheet_gid, dimension="ROWS", start=1, end=end_row, pixel_size=120),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=0, end=3, pixel_size=180),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=4, end=5, pixel_size=420),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=13, end=17, pixel_size=180),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=17, end=20, pixel_size=220),
            _wrap_range_request(sheet_gid, start_col=4, end_col=5, end_row=end_row),
            _wrap_range_request(sheet_gid, start_col=17, end_col=20, end_row=end_row),
        ])
    elif layout == "cases311":
        requests.extend([
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=0, end=2, pixel_size=170),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=3, end=6, pixel_size=150),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=6, end=9, pixel_size=180),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=9, end=10, pixel_size=320),
            _wrap_range_request(sheet_gid, start_col=9, end_col=10, end_row=end_row),
        ])
    elif layout == "queue311":
        requests.extend([
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=0, end=4, pixel_size=140),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=4, end=6, pixel_size=180),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=7, end=10, pixel_size=180),
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=10, end=11, pixel_size=320),
            _wrap_range_request(sheet_gid, start_col=10, end_col=11, end_row=end_row),
        ])
    elif layout == "coverage":
        requests.extend([
            _dimension_resize_request(sheet_gid, dimension="COLUMNS", start=0, end=4, pixel_size=150),
        ])

    svc.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body={"requests": requests}).execute()


def _should_skip_duplicate_tasker_decision(
    raw: RawMessage | None,
    decision: MessageDecision | None,
    kept_tasker_signatures: dict[tuple[str, str, str], tuple[int, str | None, bool]],
) -> bool:
    if raw is None or raw.source != "tasker" or raw.ts_epoch is None:
        return False
    if is_noise_tasker_capture(raw.chat_name, raw.sender, raw.text):
        return True
    signature = normalize_tasker_capture(raw.chat_name, raw.sender, raw.text).signature
    kept = kept_tasker_signatures.get(signature)
    if kept is None:
        return False
    kept_ts_epoch, kept_incident_id, kept_is_issue = kept
    if not bool(getattr(decision, "is_issue", False)) and not kept_is_issue:
        return True
    if decision is not None and decision.incident_id and kept_incident_id and decision.incident_id == kept_incident_id:
        return True
    return abs(int(kept_ts_epoch) - int(raw.ts_epoch)) <= tasker_duplicate_window_seconds()


def _duration_minutes(inc: Incident) -> int | None:
    if inc.start_ts_epoch and inc.end_ts_epoch and inc.end_ts_epoch >= inc.start_ts_epoch:
        return int((inc.end_ts_epoch - inc.start_ts_epoch) // 60)
    return None


def _fmt_ts(epoch: int | None) -> str:
    if not epoch:
        return ""
    return datetime.fromtimestamp(epoch, tz=NY).strftime("%Y-%m-%d %I:%M %p")


def _public_ts(value: str | int | float | None, *, fallback: str | int | float | None = None) -> str:
    return _fmt_ts(parse_ts_to_epoch(value) or parse_ts_to_epoch(fallback))


def _spreadsheet_url(spreadsheet_id: str | None = None) -> str:
    sid = spreadsheet_id or os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID", "")
    return f"https://docs.google.com/spreadsheets/d/{sid}/edit" if sid else ""


def _formula_escape(value: str) -> str:
    return str(value).replace('"', '""')


def _hyperlink_formula(url: str | None, label: str | None) -> str:
    if not url:
        return ""
    rendered_label = _formula_escape(label or "Open")
    return f'=HYPERLINK("{_formula_escape(url)}","{rendered_label}")'


def _image_formula(url: str | None) -> str:
    if not url:
        return ""
    return f'=IMAGE("{_formula_escape(url)}")'


def _public_thumbnail_formula(url: str | None) -> str:
    if not url:
        return ""
    return f'=IMAGE("{_formula_escape(url)}",4,{PUBLIC_THUMBNAIL_HEIGHT},{PUBLIC_THUMBNAIL_WIDTH})'


def _attachment_cells(message_id: str, attachments: str | None) -> tuple[str, list[str], str, list[str]]:
    shareable = public_attachment_entries(message_id, attachments)
    context = attachment_context(attachments)
    preview_item = next(
        (item for item in shareable if item.get("kind") == "image"),
        shareable[0] if shareable else None,
    )
    preview = ""
    if preview_item and preview_item.get("kind") == "image":
        preview = _image_formula(str(preview_item.get("public_url") or ""))

    media_links = [
        _hyperlink_formula(
            str(item.get("public_url") or ""),
            str(item.get("filename") or item.get("label") or item.get("kind") or f"Media {idx + 1}"),
        )
        for idx, item in enumerate(shareable[:SHEET_MEDIA_LINK_LIMIT])
    ]
    while len(media_links) < SHEET_MEDIA_LINK_LIMIT:
        media_links.append("")

    reply_text = str((context.get("message_context") or {}).get("reply_text") or "")[:250]
    external_links = [
        _hyperlink_formula(url, f"Link {idx + 1}")
        for idx, url in enumerate((context.get("links") or [])[:SHEET_EXTERNAL_LINK_LIMIT])
    ]
    while len(external_links) < SHEET_EXTERNAL_LINK_LIMIT:
        external_links.append("")
    return preview, media_links, reply_text, external_links


def _incident_attachment_cells(incident: Incident, raw_map: dict[str, RawMessage]) -> tuple[str, list[str], str, list[str]]:
    preview = ""
    media_links: list[str] = []
    reply_text = ""
    external_links: list[str] = []
    seen_urls: set[str] = set()
    seen_external_links: set[str] = set()

    for message_id in [item.strip() for item in (incident.proof_refs or "").split(",") if item.strip()]:
        raw = raw_map.get(message_id)
        if raw is None:
            continue
        row_preview, row_media_links, row_reply_text, row_external_links = _attachment_cells(raw.message_id, raw.attachments)
        if not preview and row_preview:
            preview = row_preview
        if not reply_text and row_reply_text:
            reply_text = row_reply_text
        for link in row_media_links:
            if not link or link in seen_urls or len(media_links) >= SHEET_MEDIA_LINK_LIMIT:
                continue
            seen_urls.add(link)
            media_links.append(link)
        for link in row_external_links:
            if not link or link in seen_external_links or len(external_links) >= SHEET_EXTERNAL_LINK_LIMIT:
                continue
            seen_external_links.add(link)
            external_links.append(link)

    while len(media_links) < SHEET_MEDIA_LINK_LIMIT:
        media_links.append("")
    while len(external_links) < SHEET_EXTERNAL_LINK_LIMIT:
        external_links.append("")
    return preview, media_links, reply_text, external_links


def _case_activity_epoch(case: ServiceRequestCase) -> int:
    return int(
        parse_ts_to_epoch(case.closed_at)
        or parse_ts_to_epoch(case.last_checked_at)
        or parse_ts_to_epoch(case.submitted_at)
        or 0
    )


def _latest_case_for_incident(cases: list[ServiceRequestCase]) -> ServiceRequestCase | None:
    if not cases:
        return None
    return sorted(
        cases,
        key=lambda row: (
            1 if not row.closed_at else 0,
            _case_activity_epoch(row),
        ),
        reverse=True,
    )[0]


def _raw_message_is_public(raw: RawMessage | None, allowed_chat_names: set[str]) -> bool:
    if raw is None:
        return False
    if not allowed_chat_names:
        return True
    return _clean_text(raw.chat_name).casefold() in allowed_chat_names


def _incident_is_public(incident: Incident, raw_map: dict[str, RawMessage], allowed_chat_names: set[str]) -> bool:
    message_ids = [item.strip() for item in (incident.proof_refs or "").split(",") if item.strip()]
    if not message_ids:
        return not allowed_chat_names
    return any(_raw_message_is_public(raw_map.get(message_id), allowed_chat_names) for message_id in message_ids)


def _incident_last_epoch(incident: Incident) -> int:
    return int(
        incident.last_ts_epoch
        or parse_ts_to_epoch(incident.updated_at)
        or parse_ts_to_epoch(incident.start_ts)
        or 0
    )


def _incident_is_current_public_issue(incident: Incident, linked_cases: list[ServiceRequestCase], now_epoch: int) -> bool:
    if incident.status == "closed":
        return False
    last_epoch = _incident_last_epoch(incident)
    if last_epoch and now_epoch - last_epoch <= PUBLIC_CURRENT_ISSUE_MAX_AGE_HOURS * 3600:
        return True
    latest_case = _latest_case_for_incident(linked_cases)
    return bool(latest_case and not latest_case.closed_at)


def _incident_is_recent_public_issue(incident: Incident, linked_cases: list[ServiceRequestCase], now_epoch: int) -> bool:
    last_epoch = _incident_last_epoch(incident)
    if last_epoch and now_epoch - last_epoch <= PUBLIC_RECENT_ISSUE_MAX_AGE_HOURS * 3600:
        return True
    latest_case = _latest_case_for_incident(linked_cases)
    if latest_case is None:
        return False
    latest_case_epoch = _case_activity_epoch(latest_case)
    return bool(latest_case_epoch and now_epoch - latest_case_epoch <= PUBLIC_RECENT_ISSUE_MAX_AGE_HOURS * 3600)


def _public_status_summary(row: dict[str, str], latest_case: ServiceRequestCase | None) -> str:
    last_report = row.get("last_report_received") or row.get("last_evidence") or "Unknown"
    confidence = row.get("confidence") or "Low"
    if latest_case is not None and latest_case.service_request_number:
        return f"Last report {last_report}. Confidence {confidence}. Latest 311 case {latest_case.service_request_number}."
    return f"Last report {last_report}. Confidence {confidence}."


def _public_case_summary(case: ServiceRequestCase, incident: Incident | None) -> str:
    issue_title = getattr(incident, "title", "") or ""
    complaint_type = case.complaint_type or "311 complaint"
    if issue_title and complaint_type:
        return f"{complaint_type} filed for {issue_title}."
    if issue_title:
        return f"311 case tied to {issue_title}."
    if complaint_type:
        return complaint_type
    return "311 case"


_PUBLIC_ASSET_LABELS = {
    "elevator_north": "North elevator",
    "elevator_south": "South elevator",
    "elevator_both": "Both elevators",
}


def _public_focus_label(incident: Incident | None) -> str:
    if incident is None:
        return "Building update"
    asset = _clean_text(getattr(incident, "asset", ""))
    if asset in _PUBLIC_ASSET_LABELS:
        return _PUBLIC_ASSET_LABELS[asset]
    title = _clean_text(getattr(incident, "title", ""))
    if title:
        return _public_sanitize_text(_public_strip_report_prefix(title)) or "Building update"
    category = _clean_text(getattr(incident, "category", ""))
    return category.replace("_", " ").title() if category else "Building update"


def _strip_leading_label(text: str, label: str) -> str:
    clean_text = _clean_text(text)
    clean_label = _clean_text(label)
    if not clean_text or not clean_label:
        return clean_text
    if clean_text.casefold().startswith(clean_label.casefold()):
        trimmed = clean_text[len(clean_label):].lstrip(" .,:;-")
        if trimmed:
            return trimmed[0].upper() + trimmed[1:]
    return clean_text


def _truncate_public_text(value: str, *, limit: int) -> str:
    clean = _clean_text(value)
    if len(clean) <= limit:
        return clean
    return clean[: limit - 1].rstrip() + "…"


def _public_redact_sensitive_text(value: str) -> str:
    clean = PUBLIC_EMAIL_RE.sub("[email removed]", _clean_text(value))
    clean = PUBLIC_PHONE_RE.sub("[phone removed]", clean)
    return PUBLIC_INLINE_UNIT_RE.sub(lambda match: next((group for group in match.groups() if group), ""), clean).strip()


def _public_allowed_report_recipients() -> set[str]:
    recipients = set(_split_names(os.environ.get("PUBLIC_ALLOWED_REPORT_RECIPIENTS", "Jack")))
    return {name.strip().casefold() for name in recipients if name.strip()}


def _public_is_allowed_report_recipient(name: str) -> bool:
    clean = _clean_text(name)
    if not clean:
        return False
    lowered = clean.casefold()
    if lowered in _public_allowed_report_recipients():
        return True
    return any(role in lowered for role in PUBLIC_REPORT_RECIPIENT_ROLE_HINTS)


def _public_is_report_recipient_context(clean: str, match_start: int) -> bool:
    return bool(re.search(r"\bto\s*$", clean[:match_start].rstrip()))


def _public_retain_allowed_report_recipient(match: re.Match[str]) -> str:
    person = _clean_text(match.group("person") or "")
    if _public_is_allowed_report_recipient(person):
        return match.group(0).strip()
    return ""


def _public_remove_person_references(value: str) -> str:
    clean = _clean_text(value)
    clean = PUBLIC_LEADING_PERSON_RE.sub("", clean)
    clean = PUBLIC_REPORT_RECIPIENT_ACTION_RE.sub(_public_retain_allowed_report_recipient, clean)
    clean = PUBLIC_PERSON_ACTION_RE.sub("", clean)
    clean = PUBLIC_AS_PER_PERSON_RE.sub("", clean)
    clean = PUBLIC_SUBJECT_PERSON_RE.sub("Someone ", clean)
    redacted_names = _split_names(os.environ.get("PUBLIC_REDACT_NAMES") or ",".join(PUBLIC_DEFAULT_REDACTED_NAMES))
    for name in redacted_names:
        name_pattern = re.compile(rf"\b{re.escape(name)}\b", re.IGNORECASE)

        def _redact(match: re.Match[str]) -> str:
            if _public_is_allowed_report_recipient(name) and _public_is_report_recipient_context(clean, match.start()):
                return match.group(0)
            return "someone"

        clean = name_pattern.sub(_redact, clean)
    clean = re.sub(r"\s+([.,;:!?])", r"\1", clean)
    clean = re.sub(r"\s*;\s*([.;,])", r"\1", clean)
    clean = re.sub(r"\s{2,}", " ", clean).strip(" ,;:-")
    return clean


def _public_sanitize_text(value: str) -> str:
    return _public_redact_sensitive_text(_public_remove_person_references(value))


def _public_similarity_tokens(value: str) -> set[str]:
    clean = _clean_text(value).casefold()
    clean = re.sub(r"\b(\d+)(?:st|nd|rd|th)\b", r"\1", clean)
    clean = re.sub(r"\bflr\b|\bfloor\b", "floor", clean)
    tokens = set(re.findall(r"[a-z0-9]+", clean))
    return {token for token in tokens if token not in PUBLIC_DETAIL_STOP_WORDS}


def _public_dedupe_parts(parts: list[str]) -> list[str]:
    out: list[str] = []
    token_sets: list[set[str]] = []
    seen_exact: set[str] = set()
    for raw_part in parts:
        part = _public_sanitize_text(_public_strip_report_prefix(raw_part))
        if not part:
            continue
        exact = re.sub(r"\W+", "", part).casefold()
        if exact in seen_exact:
            continue
        tokens = _public_similarity_tokens(part)
        duplicate = False
        for existing_tokens in token_sets:
            if not tokens or not existing_tokens:
                continue
            overlap = len(tokens & existing_tokens) / max(len(tokens | existing_tokens), 1)
            if overlap >= 0.72:
                duplicate = True
                break
        if duplicate:
            continue
        seen_exact.add(exact)
        token_sets.append(tokens)
        out.append(part)
    return out


def _public_is_summary_message_style(value: str) -> bool:
    clean = value.casefold()
    return any(token in clean for token in (" it's ", "it's ", "it’s", " no one", "has informed", "again.", "indicating"))


def _public_summarize_issue_value(*, summary: str, title: str) -> str:
    cleaned_summary = _public_strip_report_prefix(summary) if summary else ""
    cleaned_title = _public_strip_report_prefix(title) if title else ""
    if not cleaned_summary:
        return _public_safe_summary_text(cleaned_title)

    summary_words = len((cleaned_summary or "").split())
    title_words = len((cleaned_title or "").split())
    if "|" in cleaned_summary or _public_is_summary_message_style(cleaned_summary):
        if cleaned_title:
            return _public_safe_summary_text(cleaned_title)
    if title_words and summary_words >= title_words + 8:
        return _public_safe_summary_text(cleaned_title)
    return _public_safe_summary_text(cleaned_summary)


def _public_strip_report_prefix(value: str) -> str:
    clean = _clean_text(value)
    patterns = (
        r"^(?:a\s+)?tenant\s+[A-Z][A-Za-z.'-]*(?:\s+[A-Z][A-Za-z.'-]*)?\s+reports\s+that\s+",
        r"^(?:a\s+)?tenant\s+[A-Z][A-Za-z.'-]*(?:\s+[A-Z][A-Za-z.'-]*)?\s+reports\s+",
        r"^(?:a\s+)?resident\s+[A-Z][A-Za-z.'-]*(?:\s+[A-Z][A-Za-z.'-]*)?\s+reports\s+that\s+",
        r"^(?:a\s+)?resident\s+[A-Z][A-Za-z.'-]*(?:\s+[A-Z][A-Za-z.'-]*)?\s+reports\s+",
        r"^(?:a\s+)?tenant\s+reports\s+that\s+",
        r"^(?:a\s+)?tenant\s+reports\s+",
        r"^tenants\s+report\s+that\s+",
        r"^tenants\s+report\s+",
        r"^user\s+reports\s+that\s+",
        r"^user\s+reports\s+",
        r"^message\s+reports\s+that\s+",
        r"^message\s+reports\s+",
        r"^message\s+indicates\s+that\s+",
        r"^message\s+indicates\s+",
    )
    for pattern in patterns:
        stripped = re.sub(pattern, "", clean, flags=re.IGNORECASE)
        if stripped != clean:
            return stripped[:1].upper() + stripped[1:]
    return clean


def _public_safe_summary_text(value: str) -> str:
    clean = _clean_text(value)
    replacements = (
        (r"\bresolved or working\b", "reported as working"),
        (r"\bunresolved\b", "not fixed"),
        (r"\bresolved\b", "reported as working"),
        (r"\bresolution\b", "follow-up"),
    )
    for pattern, replacement in replacements:
        clean = re.sub(pattern, replacement, clean, flags=re.IGNORECASE)
    return " | ".join(_public_dedupe_parts(clean.split(" | ")))


def _public_visible_context_text(value: str) -> str:
    lines = [line.strip() for line in str(value or "").replace("\u202f", " ").replace("\u200e", "").replace("\u200f", "").splitlines()]
    cleaned: list[str] = []
    for idx, line in enumerate(lines):
        if not line:
            continue
        previous_line = lines[idx - 1].strip() if idx > 0 else ""
        next_line = lines[idx + 1].strip() if idx + 1 < len(lines) else ""
        if PUBLIC_PHONE_RE.search(line) or PUBLIC_EMAIL_RE.search(line):
            if line == PUBLIC_PHONE_RE.sub("", line).strip() or line == PUBLIC_EMAIL_RE.sub("", line).strip():
                continue
            cleaned.append(_public_sanitize_text(line))
            continue
        if next_line and (PUBLIC_PHONE_RE.search(next_line) or PUBLIC_EMAIL_RE.search(next_line)):
            continue
        if previous_line and (PUBLIC_PHONE_RE.search(previous_line) or PUBLIC_EMAIL_RE.search(previous_line)) and PUBLIC_UNIT_LINE_RE.match(line):
            continue
        if PUBLIC_TIME_LINE_RE.match(line) and cleaned:
            continue
        cleaned.append(_public_sanitize_text(line))

    deduped: list[str] = []
    seen: set[str] = set()
    for line in cleaned:
        key = line.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(line)
    return _clean_text("\n".join(deduped))


def _public_detail_text(incident: Incident | None, focus_label: str) -> str:
    if incident is None:
        return ""
    summary = _public_summarize_issue_value(
        summary=_clean_text(getattr(incident, "summary", "")),
        title=_clean_text(getattr(incident, "title", "")),
    )
    title = _clean_text(getattr(incident, "title", ""))
    for candidate in (summary, title):
        if not candidate:
            continue
        trimmed = _strip_leading_label(candidate, focus_label)
        if trimmed:
            return _truncate_public_text(_public_safe_summary_text(trimmed), limit=320)
        truncated = _truncate_public_text(_public_safe_summary_text(candidate), limit=320)
        if truncated:
            return truncated
    return "Resident update logged."


def _public_status_label(incident: Incident | None, cases: list[ServiceRequestCase]) -> str:
    if incident is None:
        return "Unknown"
    if incident.status == "closed":
        return "Closed"
    return "Active"


def _public_case_badge(cases: list[ServiceRequestCase]) -> str:
    if not cases:
        return "No 311 case yet"
    latest = _latest_case_for_incident(cases)
    if latest is None:
        return "No 311 case yet"
    open_cases = [case for case in cases if not case.closed_at]
    status = _clean_text(latest.status) or ("submitted" if not latest.closed_at else "closed")
    number = latest.service_request_number or "311 case"
    if len(open_cases) > 1:
        return f"{len(open_cases)} active 311 cases; latest {number} ({status})"
    return f"{number} ({status})"


def _public_case_status_label(case: ServiceRequestCase) -> str:
    status = _clean_text(case.status).replace("_", " ")
    if case.closed_at or status.casefold() == "closed":
        return "Closed"
    if status.casefold() == "submitted":
        return "Submitted"
    if status:
        return status.title()
    return "Status pending"


def _case_status_payload(case: ServiceRequestCase) -> dict[str, object]:
    if not case.raw_status_json:
        return {}
    try:
        payload = json.loads(case.raw_status_json)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _public_case_updated_label(case: ServiceRequestCase) -> str:
    if not _case_status_payload(case) and not case.closed_at:
        return "Not checked yet"
    return _public_ts(case.last_checked_at or case.closed_at)


def _public_case_note(case: ServiceRequestCase, incident: Incident | None) -> str:
    raw_status = _case_status_payload(case)
    if raw_status and raw_status.get("found") is False:
        return "No public NYC status result yet."
    if not raw_status and not case.closed_at:
        return "Waiting for first NYC status lookup."
    resolution = _truncate_public_text(_clean_text(case.resolution_description), limit=220)
    if resolution:
        return _public_sanitize_text(resolution)
    related_issue = _public_sanitize_text(_clean_text(getattr(incident, "title", ""))) or _public_focus_label(incident)
    if related_issue:
        return _truncate_public_text(f"Filed for {related_issue}.", limit=220)
    return "311 follow-up is being tracked."


def _public_category_label(category: str | None) -> str:
    clean = _clean_text(category)
    if not clean:
        return "Other"
    labels = {
        "elevator": "Elevator",
        "pests": "Pest activity",
        "leaks_water_damage": "Leaks / water damage",
        "security_access": "Security / access",
        "heat_hot_water": "Heat / hot water",
        "other": "Other building issues",
    }
    return labels.get(clean, clean.replace("_", " ").title())


def _public_source_label(source: str | None) -> str:
    clean = _clean_text(source)
    labels = {
        "whatsapp_web": "WhatsApp",
        "tasker": "WhatsApp",
        "export": "WhatsApp export",
    }
    return labels.get(clean, clean.replace("_", " ").title() if clean else "Message")


def _public_attachment_label(item: dict[str, object], *, default: str = "Open evidence") -> str:
    kind = _clean_text(str(item.get("kind") or "")).casefold()
    if kind == "image":
        return "Open photo"
    if kind == "video":
        return "Open video"
    if kind in {"audio", "voice"}:
        return "Open audio"
    if kind in {"document", "file"}:
        return "Open file"
    return default


def _public_attachment_preview(item: dict[str, object]) -> str:
    kind = _clean_text(str(item.get("kind") or "")).casefold()
    if kind != "image":
        return ""
    return _public_thumbnail_formula(str(item.get("public_url") or ""))


def _public_type_label(kind: str | None) -> str:
    clean = _clean_text(str(kind or "")).casefold()
    labels = {
        "image": "Photo",
        "video": "Video",
        "audio": "Audio",
        "voice": "Audio",
        "document": "File",
        "file": "File",
    }
    return labels.get(clean, clean.replace("_", " ").title() if clean else "Evidence")


def _public_issue_label(incident: Incident | None) -> str:
    return _public_strip_report_prefix(_public_focus_label(incident))


def _public_evidence_cells(incident: Incident, raw_map: dict[str, RawMessage]) -> tuple[str, str]:
    fallback_open_cell = ""
    for message_id in [item.strip() for item in (incident.proof_refs or "").split(",") if item.strip()]:
        raw = raw_map.get(message_id)
        if raw is None:
            continue
        for item in public_attachment_entries(raw.message_id, raw.attachments):
            url = str(item.get("public_url") or "")
            label = _public_attachment_label(item)
            open_cell = _hyperlink_formula(url, label)
            preview = _public_attachment_preview(item)
            if preview:
                return preview, open_cell
            if not fallback_open_cell:
                fallback_open_cell = open_cell
    return "", fallback_open_cell


def _public_incident_sort_key(incident: Incident) -> tuple[int, int, str]:
    return (
        _incident_last_epoch(incident),
        int(parse_ts_to_epoch(incident.start_ts) or 0),
        _clean_text(incident.title).casefold(),
    )


def _public_case_status_rank(case: ServiceRequestCase) -> int:
    status = _clean_text(case.status).casefold()
    if case.closed_at or status == "closed":
        return 0
    if status in {"in progress", "open", "pending", "assigned"}:
        return 3
    if status == "submitted":
        return 2
    return 1


def _public_case_submitted_epoch(case: ServiceRequestCase) -> int:
    return int(parse_ts_to_epoch(case.submitted_at) or _case_activity_epoch(case) or 0)


def _public_case_sort_key(case: ServiceRequestCase) -> tuple[int, int, int, str]:
    return (
        _public_case_status_rank(case),
        _public_case_submitted_epoch(case),
        _case_activity_epoch(case),
        _clean_text(case.service_request_number),
    )


def _public_incident_evidence_count(incident: Incident, raw_map: dict[str, RawMessage]) -> int:
    count = 0
    for message_id in [item.strip() for item in (incident.proof_refs or "").split(",") if item.strip()]:
        raw = raw_map.get(message_id)
        if raw is None:
            continue
        shareable = public_attachment_entries(raw.message_id, raw.attachments)
        count += len(shareable) if shareable else 1
    return count


def _public_evidence_rows(
    incidents: list[Incident],
    raw_map: dict[str, RawMessage],
    case_map: dict[str, list[ServiceRequestCase]],
) -> list[list[object]]:
    rows: list[list[object]] = []
    seen: set[tuple[str, int]] = set()
    for incident in incidents:
        cases = case_map.get(incident.incident_id, [])
        focus_label = _public_issue_label(incident)
        for message_id in [item.strip() for item in (incident.proof_refs or "").split(",") if item.strip()]:
            raw = raw_map.get(message_id)
            if raw is None:
                continue
            context = attachment_context(raw.attachments)
            context_text = _public_visible_context_text(
                _clean_text(str((context.get("message_context") or {}).get("reply_text") or "")) or raw.text
            )
            external_link = ""
            links = context.get("links") or []
            if links:
                external_link = _hyperlink_formula(str(links[0]), "Related link")
            message_time = _public_ts(raw.ts_iso, fallback=raw.ts_epoch)
            shareable = public_attachment_entries(raw.message_id, raw.attachments)
            if shareable:
                for item in shareable:
                    attachment_index = int(item.get("attachment_index") or 0)
                    key = (raw.message_id, attachment_index)
                    if key in seen:
                        continue
                    seen.add(key)
                    kind = _public_type_label(str(item.get("kind") or "media"))
                    label = _public_attachment_label(item)
                    rows.append([
                        message_time,
                        focus_label,
                        _public_attachment_preview(item),
                        _hyperlink_formula(str(item.get("public_url") or ""), label),
                        kind,
                        _public_source_label(raw.source),
                        _truncate_public_text(context_text, limit=260),
                        external_link,
                        _public_case_badge(cases),
                        "",
                    ])
            else:
                rows.append([
                    message_time,
                    focus_label,
                    "",
                    "Message proof",
                    "Message",
                    _public_source_label(raw.source),
                    _truncate_public_text(context_text, limit=260),
                    external_link,
                    _public_case_badge(cases),
                    "",
                ])
    return rows


def _public_category_rows(
    incidents: list[Incident],
    raw_map: dict[str, RawMessage],
    case_map: dict[str, list[ServiceRequestCase]],
) -> list[list[object]]:
    stats: dict[str, dict[str, object]] = {}
    for incident in incidents:
        label = _public_category_label(incident.category)
        row = stats.setdefault(
            label,
            {
                "total": 0,
                "evidence": 0,
                "cases": 0,
                "latest_epoch": 0,
                "latest_issue": "",
            },
        )
        row["total"] = int(row["total"]) + 1
        row["evidence"] = int(row["evidence"]) + _public_incident_evidence_count(incident, raw_map)
        row["cases"] = int(row["cases"]) + len(case_map.get(incident.incident_id, []))
        latest_epoch = _incident_last_epoch(incident)
        if latest_epoch >= int(row["latest_epoch"]):
            row["latest_epoch"] = latest_epoch
            row["latest_issue"] = _public_issue_label(incident)

    out: list[list[object]] = []
    for label, row in sorted(
        stats.items(),
        key=lambda item: (int(item[1]["total"]), int(item[1]["latest_epoch"])),
        reverse=True,
    ):
        out.append([
            label,
            int(row["total"]),
            int(row["evidence"]),
            int(row["cases"]),
            _fmt_ts(int(row["latest_epoch"])) if int(row["latest_epoch"]) else "",
            row["latest_issue"],
            "",
            "",
        ])
    return out


def _public_proof_cell(incident: Incident | None, raw_map: dict[str, RawMessage]) -> str:
    if incident is None:
        return ""
    for message_id in [item.strip() for item in (incident.proof_refs or "").split(",") if item.strip()]:
        raw = raw_map.get(message_id)
        if raw is None:
            continue
        shareable = public_attachment_entries(raw.message_id, raw.attachments)
        if not shareable:
            continue
        item = next((entry for entry in shareable if entry.get("kind") == "image"), shareable[0])
        kind = _clean_text(str(item.get("kind") or ""))
        label = "Open photo" if kind == "image" else "Open file"
        return _hyperlink_formula(str(item.get("public_url") or ""), label)
    return ""


def sync_public_updates_to_sheets():
    svc = _service()
    sheet_id = _public_sheet_id()
    tab = _public_updates_tab()
    _set_spreadsheet_title(svc, sheet_id, PUBLIC_WORKBOOK_TITLE)
    _ensure_tab_exists(svc, sheet_id, tab, rename_single_existing=True)

    allowed_chat_names = _allowed_public_chat_names()
    now_epoch = int(datetime.now(tz=timezone.utc).timestamp())

    with get_session() as session:
        incidents = session.query(Incident).all()
        proof_message_ids = {
            message_id
            for incident in incidents
            for message_id in [item.strip() for item in (incident.proof_refs or "").split(",") if item.strip()]
        }
        raw_map = (
            {row.message_id: row for row in session.query(RawMessage).filter(RawMessage.message_id.in_(sorted(proof_message_ids))).all()}
            if proof_message_ids
            else {}
        )
        public_incidents = [row for row in incidents if _incident_is_public(row, raw_map, allowed_chat_names)]
        incident_ids = [row.incident_id for row in public_incidents]
        all_cases = session.query(ServiceRequestCase).filter(ServiceRequestCase.incident_id.in_(incident_ids)).all() if incident_ids else []

    case_map: dict[str, list[ServiceRequestCase]] = {}
    for case in all_cases:
        if not case.incident_id:
            continue
        case_map.setdefault(case.incident_id, []).append(case)

    public_incidents.sort(key=_public_incident_sort_key, reverse=True)

    public_incident_ids = {row.incident_id for row in public_incidents}
    public_cases = [case for case in all_cases if case.incident_id in public_incident_ids]
    public_cases.sort(key=_public_case_sort_key, reverse=True)
    evidence_rows = _public_evidence_rows(public_incidents, raw_map, case_map)
    category_rows = _public_category_rows(public_incidents, raw_map, case_map)
    refresh_label = _fmt_ts(now_epoch)
    top_category = category_rows[0][0] if category_rows else ""
    latest_issue = _public_focus_label(public_incidents[0]) if public_incidents else ""

    values: list[list[object]] = [
        [PUBLIC_WORKBOOK_TITLE, "", "", "", "", "", "", "", "", ""],
        ["Automatic log from resident messages, captured evidence, and 311 activity. It keeps the tenant record in one place.", "", "", "", "", "", "", "", "", ""],
        ["At a glance", "", "", "", "", "", "", "", "", ""],
        ["Item", "Count / detail", "What this means", "", "", "", "", "", "", ""],
        ["Last refresh", refresh_label, "Updated automatically from resident messages and 311 records.", "", "", "", "", "", "", ""],
        ["Incidents", len(public_incidents), "Issues logged from the tenant chat and connected evidence.", "", "", "", "", "", "", ""],
        ["Evidence items", len(evidence_rows), "Messages, photos, screenshots, and links connected to incidents.", "", "", "", "", "", "", ""],
        ["311 filings", len(public_cases), "311 service requests connected to logged incidents.", "", "", "", "", "", "", ""],
        ["Most common issue type", top_category, "Category with the most logged incidents.", "", "", "", "", "", "", ""],
        ["Latest update", latest_issue, "Newest update in the log.", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", "", "", ""],
        ["Category snapshot", "", "", "", "", "", "", "", "", ""],
        ["Category", "Incidents", "Evidence items", "311 filings", "Latest update", "Latest issue", "", "", "", ""],
    ]

    if category_rows:
        values.extend(category_rows)
    else:
        values.append(["No public incidents yet", "", "", "", "", "", "", "", "", ""])

    values.append(["", "", "", "", "", "", "", "", "", ""])
    incidents_title_row = len(values) + 1
    values.append(["All incidents", "", "", "", "", "", "", "", "", ""])
    incidents_header_row = len(values) + 1
    values.append(["Updated", "Issue", "Category", "Evidence items", "311 follow-up", "Preview", "Open evidence", "Summary", "", ""])

    if public_incidents:
        for incident in public_incidents:
            cases = case_map.get(incident.incident_id, [])
            focus_label = _public_focus_label(incident)
            preview_cell, open_cell = _public_evidence_cells(incident, raw_map)
            values.append([
                _public_ts(incident.last_ts_epoch, fallback=incident.updated_at) or "",
                focus_label,
                _public_category_label(incident.category),
                _public_incident_evidence_count(incident, raw_map),
                _public_case_badge(cases),
                preview_cell,
                open_cell,
                _public_detail_text(incident, focus_label),
                "",
                "",
            ])
    else:
        values.append([refresh_label, "Quiet", "No public incidents yet", "", "", "", "", "", "", ""])

    values.append(["", "", "", "", "", "", "", "", "", ""])
    case_watch_title_row = len(values) + 1
    values.append(["311 case watch", "", "", "", "", "", "", "", "", ""])
    case_watch_header_row = len(values) + 1
    values.append(["Case", "NYC status", "Complaint", "Related issue", "Submitted", "NYC lookup", "Notes", "", "", ""])

    if public_cases:
        incident_lookup = {incident.incident_id: incident for incident in public_incidents}
        for case in public_cases:
            incident = incident_lookup.get(case.incident_id or "")
            values.append([
                case.service_request_number or "311 case",
                _public_case_status_label(case),
                _truncate_public_text(_clean_text(case.complaint_type) or "311 complaint", limit=120),
                _truncate_public_text(_public_focus_label(incident), limit=120),
                _public_ts(case.submitted_at) or "",
                _public_case_updated_label(case),
                _public_case_note(case, incident),
                "",
                "",
                "",
            ])
    else:
        values.append(["", "No 311 cases yet", "", "", "", "", "Verified 311 case activity will appear here automatically when a filing exists.", "", "", ""])

    _replace_tab_values(svc, sheet_id, tab, values, value_input_option="USER_ENTERED")
    _apply_tab_layout(
        svc,
        sheet_id,
        tab,
        row_count=len(values),
        column_count=PUBLIC_LAYOUT_COLUMNS,
        layout="public_updates",
        layout_meta={
            "freeze_rows": PUBLIC_FROZEN_ROWS,
            "title_row": 1,
            "subtitle_row": 2,
            "stats_row": 5,
            "stats_row_count": 6,
            "section_rows": [3, 12, incidents_title_row, case_watch_title_row],
            "header_rows": [4, 13, incidents_header_row, case_watch_header_row],
        },
    )
    _clear_legacy_public_update_tabs(svc, sheet_id)


def sync_incidents_to_sheets():
    svc = _service()
    sheet_id = _sheet_id()
    tab = _tab("SHEETS_INCIDENTS_TAB", default="Incidents")
    with get_session() as session:
        incidents = session.query(Incident).all()
        proof_message_ids = {
            message_id
            for incident in incidents
            for message_id in [item.strip() for item in (incident.proof_refs or "").split(",") if item.strip()]
        }
        raw_map = (
            {row.message_id: row for row in session.query(RawMessage).filter(RawMessage.message_id.in_(sorted(proof_message_ids))).all()}
            if proof_message_ids
            else {}
        )
    values = [[
        "incident_id", "category", "asset", "severity", "status",
        "start_ts", "end_ts", "duration_min",
        "title", "summary", "proof_refs",
        "evidence_preview", "evidence_1", "evidence_2", "evidence_3", "reply_context", "link_1", "link_2",
        "report_count", "witness_count", "confidence", "needs_review", "updated_at",
    ]]
    for inc in sorted(incidents, key=lambda row: row.last_ts_epoch or 0, reverse=True):
        preview, media_links, reply_text, external_links = _incident_attachment_cells(inc, raw_map)
        values.append([
            inc.incident_id,
            inc.category,
            inc.asset or "",
            inc.severity,
            inc.status,
            normalize_timestamp(inc.start_ts, fallback=inc.start_ts_epoch) or "",
            normalize_timestamp(inc.end_ts, fallback=inc.end_ts_epoch) or "",
            _duration_minutes(inc) or "",
            inc.title,
            (inc.summary or "")[:250],
            inc.proof_refs or "",
            preview,
            media_links[0],
            media_links[1],
            media_links[2],
            reply_text,
            external_links[0],
            external_links[1],
            int(inc.report_count or 0),
            int(inc.witness_count or 0),
            int(inc.confidence or 0),
            "YES" if inc.needs_review else "",
            normalize_timestamp(inc.updated_at) or "",
        ])
    _replace_tab_values(svc, sheet_id, tab, values, value_input_option="USER_ENTERED")
    _apply_tab_layout(svc, sheet_id, tab, row_count=len(values), column_count=len(values[0]), layout="incidents")


def _elevator_status_from_incidents(incidents: list[Incident], asset: str) -> dict:
    now_epoch = int(datetime.now(tz=timezone.utc).timestamp())
    relevant = [row for row in incidents if row.category == "elevator" and row.asset in (asset, "elevator_both", None)]
    if not relevant:
        return {"status": "UNKNOWN", "last_evidence": "", "confidence": "Low", "incident_id": ""}
    relevant.sort(key=lambda row: row.last_ts_epoch or 0, reverse=True)
    latest = relevant[0]
    age_sec = now_epoch - int(latest.last_ts_epoch or 0) if latest.last_ts_epoch else 10**9
    age_hours = age_sec / 3600.0
    status = "OUT" if latest.status != "closed" else "WORKING"
    if age_hours > 6:
        if latest.status == "closed":
            return {"status": "WORKING", "last_evidence": _fmt_ts(latest.last_ts_epoch), "confidence": "Low", "incident_id": latest.incident_id}
        return {"status": "UNKNOWN", "last_evidence": _fmt_ts(latest.last_ts_epoch), "confidence": "Low", "incident_id": latest.incident_id}
    wc = int(latest.witness_count or 0)
    confidence = "High" if age_hours <= 2 and wc >= 2 else "Medium" if age_hours <= 6 and wc >= 1 else "Low"
    return {"status": status, "last_evidence": _fmt_ts(latest.last_ts_epoch), "confidence": confidence, "incident_id": latest.incident_id}


def sync_dashboard_to_sheets():
    svc = _service()
    sheet_id = _sheet_id()
    tab = _tab("SHEETS_DASHBOARD_TAB", default="Dashboard")
    with get_session() as session:
        incidents = session.query(Incident).all()
        raw_count = session.query(RawMessage).count()
        last_raw = session.query(RawMessage).order_by(RawMessage.ts_epoch.desc().nullslast()).first()
        open_cases = session.query(ServiceRequestCase).filter(ServiceRequestCase.closed_at.is_(None)).count()
        queue_count = session.query(FilingJob).filter(FilingJob.state.in_(["pending", "claimed", "failed"])).count()
        review_count = session.query(MessageDecision).filter(MessageDecision.needs_review.is_(True)).count()

    total = len(incidents)
    open_incidents = sum(1 for row in incidents if row.status != "closed")
    needs_review = sum(1 for row in incidents if row.needs_review)
    by_cat = {}
    for row in incidents:
        by_cat[row.category] = by_cat.get(row.category, 0) + 1

    north = _elevator_status_from_incidents(incidents, "elevator_north")
    south = _elevator_status_from_incidents(incidents, "elevator_south")
    if north["status"] == "OUT" and south["status"] == "OUT":
        overall = "CRITICAL (both likely out)"
    elif north["status"] == "OUT" or south["status"] == "OUT":
        overall = "DEGRADED (one likely out)"
    elif north["status"] == "WORKING" and south["status"] == "WORKING":
        overall = "OK (both likely working)"
    else:
        overall = "UNKNOWN"

    report_form_url = ((_env_first("PUBLIC_BASE_URL", default="") or "").rstrip("/") + "/report") if _env_first("PUBLIC_BASE_URL", default="") else ""
    public_updates_sheet_url = _spreadsheet_url(_configured_public_sheet_id()) if _configured_public_sheet_id() else ""
    values = [
        ["ELEVATOR STATUS NOW", ""],
        ["north_status", north["status"]],
        ["north_last_evidence", north["last_evidence"]],
        ["north_confidence", north["confidence"]],
        ["north_incident_id", north["incident_id"]],
        ["", ""],
        ["south_status", south["status"]],
        ["south_last_evidence", south["last_evidence"]],
        ["south_confidence", south["confidence"]],
        ["south_incident_id", south["incident_id"]],
        ["", ""],
        ["overall", overall],
        ["", ""],
        ["CONTROL", ""],
        ["spreadsheet_url", _spreadsheet_url()],
        ["public_updates_sheet_url", public_updates_sheet_url],
        ["report_form_url", report_form_url],
        ["llm_mode", os.environ.get("LLM_MODE", "uncertain")],
        ["", ""],
        ["SYSTEM METRICS", ""],
        ["raw_messages_total", raw_count],
        ["raw_last_seen", _fmt_ts(getattr(last_raw, "ts_epoch", None)) if last_raw else ""],
        ["total_incidents", total],
        ["open_incidents", open_incidents],
        ["incidents_needing_review", needs_review],
        ["decision_rows_needing_review", review_count],
        ["open_311_cases", open_cases],
        ["311_queue_depth", queue_count],
        ["", ""],
        ["category", "count"],
    ]
    for key, value in sorted(by_cat.items(), key=lambda item: item[1], reverse=True):
        values.append([key, value])

    _replace_tab_values(svc, sheet_id, tab, values)
    _apply_tab_layout(svc, sheet_id, tab, row_count=len(values), column_count=len(values[0]), layout="dashboard")


def sync_coverage_to_sheets():
    svc = _service()
    sheet_id = _sheet_id()
    tab = _tab("SHEETS_COVERAGE_TAB", default="Coverage")
    coverage = compute_daily_coverage(limit_days=90)
    gaps = detect_gaps(coverage, min_messages_per_day=1)
    values = [["day", "messages", "first_ts_epoch", "last_ts_epoch"]]
    for row in coverage:
        values.append([row.day, row.messages, row.first_ts_epoch or "", row.last_ts_epoch or ""])
    values += [[""], ["gap_days (messages<1)", ", ".join(gaps)]]
    _replace_tab_values(svc, sheet_id, tab, values)
    _apply_tab_layout(svc, sheet_id, tab, row_count=len(values), column_count=max((len(row) for row in values), default=0), layout="coverage")


def sync_311_cases_to_sheets():
    svc = _service()
    sheet_id = _sheet_id()
    tab = _tab("SHEETS_311_CASES_TAB", "SHEETS_CASES_TAB", default="Cases311")
    with get_session() as session:
        cases = session.query(ServiceRequestCase).all()
    values = [["service_request_number", "incident_id", "source", "complaint_type", "status", "agency", "submitted_at", "last_checked_at", "closed_at", "resolution_description"]]
    for case in sorted(cases, key=lambda row: row.submitted_at or "", reverse=True):
        values.append([
            case.service_request_number,
            case.incident_id or "",
            case.source,
            case.complaint_type or "",
            case.status,
            case.agency or "",
            normalize_timestamp(case.submitted_at) or "",
            normalize_timestamp(case.last_checked_at) or "",
            normalize_timestamp(case.closed_at) or "",
            (case.resolution_description or "")[:500],
        ])
    _replace_tab_values(svc, sheet_id, tab, values)
    _apply_tab_layout(svc, sheet_id, tab, row_count=len(values), column_count=len(values[0]), layout="cases311")


def sync_311_queue_to_sheets():
    svc = _service()
    sheet_id = _sheet_id()
    tab = _tab("SHEETS_311_QUEUE_TAB", "SHEETS_QUEUE_TAB", default="Queue311")
    with get_session() as session:
        jobs = (
            session.query(FilingJob)
            .filter(FilingJob.state.in_(["pending", "claimed", "failed"]))
            .all()
        )
    values = [["job_id", "incident_id", "state", "priority", "complaint_type", "form_target", "attempts", "created_at", "claimed_at", "completed_at", "notes"]]
    for job in sorted(jobs, key=lambda row: row.created_at or "", reverse=True):
        values.append([
            job.job_id,
            job.incident_id or "",
            job.state,
            job.priority,
            job.complaint_type or "",
            job.form_target or "",
            int(job.attempts or 0),
            normalize_timestamp(job.created_at) or "",
            normalize_timestamp(job.claimed_at) or "",
            normalize_timestamp(job.completed_at) or "",
            (job.notes or "")[:500],
        ])
    _replace_tab_values(svc, sheet_id, tab, values)
    _apply_tab_layout(svc, sheet_id, tab, row_count=len(values), column_count=len(values[0]), layout="queue311")


def sync_decisions_to_sheets():
    svc = _service()
    sheet_id = _sheet_id()
    tab = _tab("SHEETS_DECISIONS_TAB", default="DecisionLog")
    with get_session() as session:
        decisions = (
            session.query(MessageDecision)
            .outerjoin(RawMessage, RawMessage.message_id == MessageDecision.message_id)
            .order_by(RawMessage.ts_epoch.desc().nullslast(), MessageDecision.created_at.desc().nullslast())
            .limit(DECISION_LOG_FETCH_LIMIT)
            .all()
        )
        raw_map = {row.message_id: row for row in session.query(RawMessage).filter(RawMessage.message_id.in_([d.message_id for d in decisions])).all()} if decisions else {}
    values = [[
        "message_ts", "decision_updated_at", "message_id", "source", "text", "chosen_source", "is_issue", "category", "event_type",
        "confidence", "needs_review", "incident_id", "auto_file_candidate",
        "media_preview", "media_1", "media_2", "media_3", "reply_context", "link_1", "link_2",
        "rules_json", "llm_json", "final_json",
    ]]
    kept_tasker_signatures: dict[tuple[str, str, str], tuple[int, str | None, bool]] = {}
    for row in decisions:
        raw = raw_map.get(row.message_id)
        if raw is not None and is_media_placeholder_text(raw.text):
            continue
        if _should_skip_duplicate_tasker_decision(raw, row, kept_tasker_signatures):
            continue
        if raw is not None and raw.source == "tasker" and raw.ts_epoch is not None:
            signature = normalize_tasker_capture(raw.chat_name, raw.sender, raw.text).signature
            kept_tasker_signatures[signature] = (int(raw.ts_epoch), row.incident_id, bool(row.is_issue))
        preview, media_links, reply_text, external_links = _attachment_cells(row.message_id, getattr(raw, "attachments", None))
        values.append([
            normalize_timestamp(getattr(raw, "ts_iso", None), fallback=getattr(raw, "ts_epoch", None)) or "",
            row.created_at or "",
            row.message_id,
            getattr(raw, "source", ""),
            ((getattr(raw, "text", "") or "")[:250]),
            row.chosen_source,
            "YES" if row.is_issue else "",
            row.category or "",
            row.event_type or "",
            int(row.confidence or 0),
            "YES" if row.needs_review else "",
            row.incident_id or "",
            "YES" if row.auto_file_candidate else "",
            preview,
            media_links[0],
            media_links[1],
            media_links[2],
            reply_text,
            external_links[0],
            external_links[1],
            row.rules_json or "",
            row.llm_json or "",
            row.final_json or "",
        ])
        if len(values) - 1 >= DECISION_LOG_LIMIT:
            break
    _replace_tab_values(svc, sheet_id, tab, values, value_input_option="USER_ENTERED")
    _apply_tab_layout(svc, sheet_id, tab, row_count=len(values), column_count=len(values[0]), layout="decisions")
