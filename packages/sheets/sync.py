from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from packages.db import ComplianceCheck, FilingJob, Incident, MessageDecision, PublicRecordWatch, RawMessage, ServiceRequestCase, WatchdogAction, WeeklyDigest, get_session
from packages.public_records.sync import action_is_tenant_visible, project_state, public_elevator_watch_items, public_record_is_tenant_trusted
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
PUBLIC_DUPLICATE_WINDOW_SECONDS = int(os.environ.get("PUBLIC_DUPLICATE_WINDOW_SECONDS", "86400"))
PUBLIC_UPDATE_DUPLICATE_WINDOW_SECONDS = int(os.environ.get("PUBLIC_UPDATE_DUPLICATE_WINDOW_SECONDS", "900"))
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
    r"(?:\s+(?:and|,)?\s*)?\b(?i:has\s+)?(?i:reported|informed|notified|told|texted|called|contacted|messaged|sent)\s+"
    r"(?:it\s+)?(?!to\s+)[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\b\.?",
)
PUBLIC_REPORT_RECIPIENT_ACTION_RE = re.compile(
    r"\b(?i:has\s+)?(?i:reported|informed|notified|told|texted|called|contacted|messaged|sent)\s+"
    r"(?:it\s+)?to\s+(?P<person>[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\b\.?",
)
PUBLIC_LEADING_PERSON_RE = re.compile(r"^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\s+(?:said|says|reported|asked|texted|called|wrote)\b[:,]?\s*")
PUBLIC_AS_PER_PERSON_RE = re.compile(r"(?:\s*[,;:]?\s*)\b(?:as per|according to)\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\b")
PUBLIC_SUBJECT_PERSON_RE = re.compile(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\s+(?=(?:mans|said|says|reported|asked|texted|called|wrote)\b)")
PUBLIC_ELEVATOR_ONLY_FRAGMENT_RE = re.compile(
    r"\b(?:(?P<asset_before>north|south)\s+(?:lift|elevator)s?\s+(?:is\s+)?"
    r"(?:the\s+)?only(?:\s+(?:one|lift|elevator))?"
    r"|only\s+(?:the\s+)?(?P<asset_after>north|south)\s+(?:lift|elevator)s?)\b",
    re.IGNORECASE,
)
PUBLIC_ELEVATOR_WORKING_RE = re.compile(
    r"\b(working|operational|running|in\s+service|restored|back\s+(?:up|on|in\s+service))\b",
    re.IGNORECASE,
)
PUBLIC_ELEVATOR_AFFECTED_RE = re.compile(
    r"\b(out|down|broken|stuck|stopped|not\s+moving|not\s+working|out\s+of\s+service|shutdown|shut\s*off)\b",
    re.IGNORECASE,
)
PUBLIC_ELEVATOR_WORD_RE = re.compile(r"\b(?:elevators?|lifts?)\b", re.IGNORECASE)
PUBLIC_ELEVATOR_SIDE_REFERENCE_RE = re.compile(r"\b(?:the\s+)?(?:north|south|left|right)\s+(?:one|side)\b", re.IGNORECASE)
PUBLIC_FORM_PROCESS_DISCUSSION_RE = re.compile(
    r"\b(?:form|page|pages|link|sheet|spreadsheet)\b[^.!?\n]{0,120}\b(?:work|works|worked|working|problem|issue|functionality|submit|getting\s+it\s+here)\b"
    r"|\b(?:problem|issue)\b[^.!?\n]{0,80}\b(?:form|page|pages|link|sheet|spreadsheet)\b",
    re.IGNORECASE,
)
PUBLIC_ELEVATOR_REPLACEMENT_DISCUSSION_RE = re.compile(
    r"\b(?:replace|replacing|replacement|construction|project|schedule|late\s+20\d{2}|new\s+elevators?)\b",
    re.IGNORECASE,
)
PUBLIC_RECORDKEEPING_DISCUSSION_RE = re.compile(
    r"\b(?:form|record|records|court|listing|listed|log|logging)\b.*\b(?:hours?|breakages?|called|arrive|come|fixed|repair)\b"
    r"|\b(?:hours?|breakages?|called|arrive|come|fixed|repair)\b.*\b(?:form|record|records|court|listing|listed|log|logging)\b",
    re.IGNORECASE,
)
PUBLIC_REPAIR_CALLED_RE = re.compile(
    r"\b(?:called|calling|contacted|notified|on\s+phone\s+with)\b[^.!?\n]{0,80}\b(?:elevator|repair|mechanic|company)\b"
    r"|\b(?:mechanic|mechanics|repair\s+(?:person|people|crew))\b[^.!?\n]{0,80}\b(?:coming|arriv)",
    re.IGNORECASE,
)
PUBLIC_REPAIR_ON_SITE_RE = re.compile(
    r"\b(?:mechanic|mechanics|repair\s+(?:person|people|crew))\b[^.!?\n]{0,80}\b(?:here|on\s+site|arrived|on\s+\d+|open)\b"
    r"|\b(?:here|arrived|on\s+site)\b[^.!?\n]{0,80}\b(?:mechanic|mechanics|repair\s+(?:person|people|crew))\b",
    re.IGNORECASE,
)
PUBLIC_REPAIR_NOT_ON_SITE_RE = re.compile(
    r"\bno\s+(?:elevator\s+)?(?:mechanic|mechanics|repair\s+(?:person|people|crew))\b[^.!?\n]{0,80}\b(?:here|on\s+site|arriv(?:ed|ing))?\b"
    r"|\b(?:mechanic|mechanics|repair\s+(?:person|people|crew))\b[^.!?\n]{0,80}\b(?:not|isn['’]?t|aren['’]?t|wasn['’]?t|weren['’]?t)\b[^.!?\n]{0,50}\b(?:here|on\s+site|arriv(?:ed|ing))\b"
    r"|\bnothing\s+arriv(?:ed|ing)\b",
    re.IGNORECASE,
)
PUBLIC_REPAIR_NOT_COMPLETE_RE = re.compile(
    r"\b(?:cannot|can't|could\s+not|couldn't)\s+repair\b|\bback\s+tomorrow\b|\bwill\s+be\s+back\b",
    re.IGNORECASE,
)
PUBLIC_APARTMENT_ENTRY_RE = re.compile(
    r"\b(?:apartment|apt|unit)\b[^.!?\n]{0,100}\b(?:entry|enter|entered|access|advise\s+super|without\s+(?:me|anyone)\s+(?:home|there))\b"
    r"|\b(?:entry|enter|entered|access)\b[^.!?\n]{0,100}\b(?:apartment|apt|unit)\b",
    re.IGNORECASE,
)
PUBLIC_APARTMENT_OCCUPANCY_ENTRY_RE = re.compile(
    r"\b(?:someone|somebody|super|staff|maintenance)\b[^.!?\n]{0,80}\b(?:in|inside|entered)\s+(?:my|the)?\s*(?:apartment|apt|unit)\b"
    r"|\b(?:apartment|apt|unit)\b[^.!?\n]{0,120}\b(?:while\s+(?:i\s+was(?:n['’]?t| not)|no\s+one|nobody)\s+(?:home|here|there))\b",
    re.IGNORECASE,
)
PUBLIC_UNDER_SINK_LEAK_RE = re.compile(r"\b(?:leak|leaking)\b[^.!?\n]{0,80}\b(?:under\s+(?:my|the)?\s*sink|sink)\b|(?:under\s+(?:my|the)?\s*sink|sink)[^.!?\n]{0,80}\b(?:leak|leaking)\b", re.IGNORECASE)
PUBLIC_ELEVATOR_ACTIONABLE_RE = re.compile(
    r"\b("
    r"zero\s+(?:elevators?|lifts?)|no\s+(?:elevators?|lifts?)|"
    r"no\s+(?:the\s+)?(?:north|south|left|right)\s+(?:elevator|lift|one|side)|"
    r"out\s+of\s+(?:service|order)|not\s+working|broken|stuck|dead|"
    r"stopped|not\s+moving|doesn['’]?t\s+seem\s+to\s+be\s+moving|won['’]?t\s+move|"
    r"not\s+(?:the\s+)?(?:north|south|left|right)\s+(?:elevator|lift)|"
    r"(?:the\s+)?(?:north|south|left|right)\s+(?:one|side)\s+(?:is\s+|are\s+|was\s+|were\s+|still\s+)?(?:out|down|dead|broken|stuck|not\s+working)|"
    r"only\s+(?:the\s+)?(?:north|south|left|right)\s+(?:elevator|lift|one|side)?\s*(?:is\s+)?(?:working|functioning|operational|running|in\s+service)|"
    r"(?:elevators?|lifts?|north|south|left|right)\s+(?:is\s+|are\s+|was\s+|were\s+|still\s+|again\s+|remains?\s+|remained\s+|currently\s+)?(?:out|down)|"
    r"(?:they(?:'re| are| were| still| again| remain| remained| currently)|it(?:'s| is| was| still| again| remains| remained| currently))\s+(?:out|down)|"
    r"shutdown|shut\s*off|trapped|entrapment|"
    r"alarm|"
    r"stopping\s+(?:(?:at|on)\s+)?(?:each|every|all)\s+floor|floor[- ]by[- ]floor|"
    r"skip(?:s|ped|ping)?\s+(?:a\s+)?floor|irregular\s+floor|"
    r"doors?\s+stuck|one\s+(?:working\s+)?(?:elevator|lift)|"
    r"(?:back|down)\s+to\s+one|only\s+one\s+(?:working\s+)?(?:elevator|lift)|"
    r"reduced\s+service|malfunction(?:ing)?|"
    r"clunk(?:ed|ing)?|bang(?:ed|ing)?|bounce[sd]?|jolt(?:ed|ing)?|shake[sn]?|shook|"
    r"rough\s+ride|door\s+(?:opened|opening|opens)\s+(?:slow(?:ly)?|in\s+slo-?mo)|slow\s+door"
    r")\b",
    re.IGNORECASE,
)
PUBLIC_ELEVATOR_IRREGULAR_OPERATION_RE = re.compile(
    r"\b(?:clunk(?:ed|ing)?|bang(?:ed|ing)?|bounce[sd]?|jolt(?:ed|ing)?|shake[sn]?|shook|"
    r"rough\s+ride|door\s+(?:opened|opening|opens)\s+(?:slow(?:ly)?|in\s+slo-?mo)|slow\s+door)\b",
    re.IGNORECASE,
)
PUBLIC_ELEVATOR_SAME_CONFIRMATION_RE = re.compile(
    r"\b(?:yes[.!?,]?\s+same|same\s+here|me\s+too|same)\b",
    re.IGNORECASE,
)
PUBLIC_ELEVATOR_CALL_RESPONSE_RE = re.compile(
    r"\b(?:impossible|unable|can't|cannot|couldn['’]?t)\b[^.!?\n]{0,90}\b(?:call|summon|get|bring|request)\b[^.!?\n]{0,90}\b(?:elevator|lift)\b"
    r"|\b(?:elevator|lift)\b[^.!?\n]{0,120}\b(?:not\s+respond(?:ing)?|won['’]?t\s+come|wouldn['’]?t\s+come|doesn['’]?t\s+come|didn['’]?t\s+come|never\s+came|won['’]?t\s+stop|wouldn['’]?t\s+stop)\b"
    r"|\b(?:call|summon|get|bring|request)\b[^.!?\n]{0,90}\b(?:elevator|lift)\b[^.!?\n]{0,90}\b(?:not\s+respond(?:ing)?|won['’]?t|wouldn['’]?t|doesn['’]?t|didn['’]?t|never)\b",
    re.IGNORECASE,
)
PUBLIC_ELEVATOR_WORKING_STATUS_RE = re.compile(
    r"\b(?:working\s+(?:normal(?:ly)?|now|rn)|working|functioning|operational|running|in\s+service|restored|back\s+(?:up|on|in\s+service))\b",
    re.IGNORECASE,
)
PUBLIC_ELEVATOR_ONLY_SIDE_WORKING_RE = re.compile(
    r"\bonly\s+(?:the\s+)?(?P<side>north|south|left|right)\s+"
    r"(?:elevator|lift|one|side)?\s*(?:is\s+)?"
    r"(?:working|functioning|operational|running|in\s+service)\b",
    re.IGNORECASE,
)
PUBLIC_ELEVATOR_NEGATED_FLOOR_SERVICE_RE = re.compile(
    r"\b(?:not|no\s+longer|without)\b[^.!?\n]{0,80}\b(?:"
    r"floor[- ]by[- ]floor|going\s+down\s+floor\s+by\s+floor|"
    r"stopping\s+(?:(?:at|on)\s+)?(?:each|every|all)\s+floor"
    r")\b",
    re.IGNORECASE,
)
PUBLIC_GENERIC_RESOLVED_FRAGMENT_RE = re.compile(
    r"^\W*(?:it'?s|its|they(?:'re| are)?|\^?it'?s)\s+(?:working|fixed|resolved)\s+now\W*$",
    re.IGNORECASE,
)
PUBLIC_STAIR_SPILL_RE = re.compile(
    r"\b(?:spill|liquid|leak)\b[^.!?\n]{0,100}\b(?:stair|stairs|stairwell|hallway|corridor|floor)\b"
    r"|\b(?:stair|stairs|stairwell|hallway|corridor|floor)\b[^.!?\n]{0,100}\b(?:spill|liquid|leak)\b",
    re.IGNORECASE,
)
PUBLIC_DEFAULT_REDACTED_NAMES = (
    "Emma",
    "Greg",
    "Hercules",
    "Jack",
    "Jacek",
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


def _watchdog_sheet_id() -> str:
    return _public_sheet_id()


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


def _unmerge_tab_range(svc, sheet_id: str, tab: str, *, row_count: int, column_count: int) -> None:
    sheet_gid = _sheet_title_to_id_map(svc, sheet_id).get(tab)
    if sheet_gid is None:
        return
    svc.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={
            "requests": [
                {
                    "unmergeCells": {
                        "range": {
                            "sheetId": sheet_gid,
                            "startRowIndex": 0,
                            "endRowIndex": max(row_count, 80),
                            "startColumnIndex": 0,
                            "endColumnIndex": max(column_count, PUBLIC_LAYOUT_COLUMNS),
                        }
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


def _media_url_cell(url: str | None) -> str:
    return _clean_text(url)


def _image_formula(url: str | None) -> str:
    if not url:
        return ""
    escaped_url = _formula_escape(url)
    return f'=HYPERLINK("{escaped_url}",IMAGE("{escaped_url}"))'


def _dedupe_repeated_text_lines(value: str | None) -> str:
    lines = [_clean_text(line) for line in str(value or "").splitlines() if _clean_text(line)]
    if not lines:
        return ""
    if len(lines) % 2 == 0:
        half = len(lines) // 2
        if lines[:half] == lines[half:]:
            return "\n".join(lines[:half])
    return "\n".join(lines)


def _raw_reply_context_text(raw: RawMessage | None) -> str:
    if raw is None:
        return ""
    context = attachment_context(getattr(raw, "attachments", None))
    return _dedupe_repeated_text_lines((context.get("message_context") or {}).get("reply_text"))


def _public_update_detection_text(raw: RawMessage | None) -> str:
    if raw is None:
        return ""
    text = _clean_text(getattr(raw, "text", ""))
    reply_text = _raw_reply_context_text(raw)
    if reply_text and reply_text not in text:
        return _clean_text(f"{text}\n{reply_text}")
    return text


def _public_thumbnail_formula(url: str | None) -> str:
    if not url:
        return ""
    escaped_url = _formula_escape(url)
    return (
        f'=HYPERLINK("{escaped_url}",'
        f'IMAGE("{escaped_url}",4,{PUBLIC_THUMBNAIL_HEIGHT},{PUBLIC_THUMBNAIL_WIDTH}))'
    )


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

    media_links = [_media_url_cell(str(item.get("public_url") or "")) for item in shareable[:SHEET_MEDIA_LINK_LIMIT]]
    while len(media_links) < SHEET_MEDIA_LINK_LIMIT:
        media_links.append("")

    reply_text = _dedupe_repeated_text_lines((context.get("message_context") or {}).get("reply_text"))[:250]
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


def _public_incident_message_ids(
    incident: Incident,
    message_ids_by_incident: dict[str, list[str]] | None = None,
) -> list[str]:
    refs: list[str] = []
    seen: set[str] = set()
    for message_id in [item.strip() for item in (incident.proof_refs or "").split(",") if item.strip()]:
        if message_id not in seen:
            refs.append(message_id)
            seen.add(message_id)
    for message_id in (message_ids_by_incident or {}).get(incident.incident_id, []):
        if message_id and message_id not in seen:
            refs.append(message_id)
            seen.add(message_id)
    return refs


def _incident_is_public(
    incident: Incident,
    raw_map: dict[str, RawMessage],
    allowed_chat_names: set[str],
    message_ids_by_incident: dict[str, list[str]] | None = None,
) -> bool:
    message_ids = _public_incident_message_ids(incident, message_ids_by_incident)
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


def _public_normalize_status_fragment(value: str) -> str:
    clean = _clean_text(value)
    lowered = clean.casefold()
    if "unclear whether" in lowered or "working or affected" in lowered:
        return clean
    match = PUBLIC_ELEVATOR_ONLY_FRAGMENT_RE.search(clean)
    if not match:
        return clean
    asset = (match.group("asset_before") or match.group("asset_after") or "").casefold()
    label = f"{asset} lift" if asset else "elevator"
    if PUBLIC_ELEVATOR_WORKING_RE.search(clean):
        return f"Only the {label} is reported working now."
    if PUBLIC_ELEVATOR_AFFECTED_RE.search(clean):
        return f"Only the {label} is reported affected now."
    return f"Status update mentions only the {label} now; unclear whether the {label} is working or affected."


def _public_dedupe_parts(parts: list[str]) -> list[str]:
    out: list[str] = []
    token_sets: list[set[str]] = []
    seen_exact: set[str] = set()
    for raw_part in parts:
        part = _public_sanitize_text(_public_strip_report_prefix(raw_part))
        part = _public_normalize_status_fragment(part)
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
            title_summary = _public_safe_summary_text(cleaned_title)
            if title_summary:
                return title_summary
    if title_words and summary_words >= title_words + 8:
        title_summary = _public_safe_summary_text(cleaned_title)
        if title_summary:
            return title_summary
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
            truncated = _truncate_public_text(_public_safe_summary_text(trimmed), limit=320)
            if truncated:
                return truncated
        truncated = _truncate_public_text(_public_safe_summary_text(candidate), limit=320)
        if truncated:
            return truncated
    return "Resident update logged."


def _public_repair_event_label(text: str) -> str:
    clean = _clean_text(text)
    if not clean or PUBLIC_RECORDKEEPING_DISCUSSION_RE.search(clean):
        return ""
    if PUBLIC_REPAIR_NOT_COMPLETE_RE.search(clean) or PUBLIC_REPAIR_NOT_ON_SITE_RE.search(clean):
        return "Repair not completed"
    if PUBLIC_REPAIR_ON_SITE_RE.search(clean):
        return "Mechanic on site"
    if PUBLIC_REPAIR_CALLED_RE.search(clean):
        return "Repair people called/expected"
    if PUBLIC_ELEVATOR_WORKING_RE.search(clean) and not PUBLIC_RECORDKEEPING_DISCUSSION_RE.search(clean):
        return "Elevator reported working/fixed"
    return ""


def _public_duplicate_key(incident: Incident) -> tuple[str, str] | None:
    focus_label = _public_focus_label(incident)
    detail = _public_detail_text(incident, focus_label)
    normalized_detail = re.sub(r"\W+", " ", detail.casefold()).strip()
    if len(normalized_detail) < 8:
        return None
    return (_public_category_label(incident.category), normalized_detail)


def _public_canonical_incident(cluster: list[Incident], case_map: dict[str, list[ServiceRequestCase]]) -> Incident:
    def sort_key(incident: Incident) -> tuple[int, tuple[int, int, int, str], int, str]:
        cases = case_map.get(incident.incident_id, [])
        latest_case = _latest_case_for_incident(cases)
        return (
            1 if cases else 0,
            _public_case_sort_key(latest_case) if latest_case else (0, 0, 0, ""),
            _incident_last_epoch(incident),
            incident.incident_id,
        )

    return max(cluster, key=sort_key)


def _public_merged_proof_refs(cluster: list[Incident], canonical: Incident) -> str:
    refs: list[str] = []
    seen: set[str] = set()
    ordered = [canonical] + [incident for incident in cluster if incident is not canonical]
    for incident in ordered:
        for ref in [item.strip() for item in (incident.proof_refs or "").split(",") if item.strip()]:
            if ref in seen:
                continue
            seen.add(ref)
            refs.append(ref)
    return ",".join(refs)


def _public_collapse_duplicate_incidents(
    incidents: list[Incident],
    case_map: dict[str, list[ServiceRequestCase]],
    message_ids_by_incident: dict[str, list[str]] | None = None,
) -> tuple[list[Incident], dict[str, list[ServiceRequestCase]]]:
    if len(incidents) < 2:
        return incidents, case_map

    grouped: dict[tuple[str, str], list[Incident]] = {}
    passthrough: list[Incident] = []
    for incident in incidents:
        key = _public_duplicate_key(incident)
        if key is None:
            passthrough.append(incident)
            continue
        grouped.setdefault(key, []).append(incident)

    collapsed: list[Incident] = list(passthrough)
    collapsed_case_map: dict[str, list[ServiceRequestCase]] = {key: list(value) for key, value in case_map.items()}

    for rows in grouped.values():
        rows.sort(key=lambda row: _incident_last_epoch(row))
        cluster: list[Incident] = []

        def flush_cluster() -> None:
            if not cluster:
                return
            if len(cluster) == 1:
                collapsed.append(cluster[0])
                return
            canonical = _public_canonical_incident(cluster, collapsed_case_map)
            seen_cases: set[str] = set()
            merged_cases: list[ServiceRequestCase] = []
            for row in cluster:
                for case in collapsed_case_map.get(row.incident_id, []):
                    case_key = case.service_request_number or str(id(case))
                    if case_key in seen_cases:
                        continue
                    seen_cases.add(case_key)
                    merged_cases.append(case)
            collapsed_case_map[canonical.incident_id] = sorted(merged_cases, key=_public_case_sort_key, reverse=True)
            canonical.proof_refs = _public_merged_proof_refs(cluster, canonical)
            if message_ids_by_incident is not None:
                merged_message_ids: list[str] = []
                seen_message_ids: set[str] = set()
                for row in cluster:
                    for message_id in _public_incident_message_ids(row, message_ids_by_incident):
                        if message_id in seen_message_ids:
                            continue
                        seen_message_ids.add(message_id)
                        merged_message_ids.append(message_id)
                message_ids_by_incident[canonical.incident_id] = merged_message_ids
            collapsed.append(canonical)

        for incident in rows:
            if not cluster:
                cluster = [incident]
                continue
            previous_epoch = _incident_last_epoch(cluster[-1])
            current_epoch = _incident_last_epoch(incident)
            if current_epoch - previous_epoch <= PUBLIC_DUPLICATE_WINDOW_SECONDS:
                cluster.append(incident)
            else:
                flush_cluster()
                cluster = [incident]
        flush_cluster()

    collapsed.sort(key=_public_incident_sort_key, reverse=True)
    collapsed_ids = {row.incident_id for row in collapsed}
    collapsed_case_map = {key: value for key, value in collapsed_case_map.items() if key in collapsed_ids}
    return collapsed, collapsed_case_map


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


def _public_title_issue_label(incident: Incident | None, *, fallback: str) -> str:
    title = _clean_text(getattr(incident, "title", "")) if incident is not None else ""
    if title:
        label = _public_sanitize_text(_public_strip_report_prefix(title))
        if label:
            return label
    return fallback


def _public_evidence_cells(incident: Incident, raw_map: dict[str, RawMessage]) -> tuple[str, str]:
    fallback_open_cell = ""
    for message_id in [item.strip() for item in (incident.proof_refs or "").split(",") if item.strip()]:
        raw = raw_map.get(message_id)
        if raw is None:
            continue
        for item in public_attachment_entries(raw.message_id, raw.attachments):
            url = str(item.get("public_url") or "")
            open_cell = _media_url_cell(url)
            preview = _public_attachment_preview(item)
            if preview:
                return preview, open_cell
            if not fallback_open_cell:
                fallback_open_cell = open_cell
    return "", fallback_open_cell


def _public_raw_evidence_cells(raw: RawMessage | None) -> tuple[str, str]:
    if raw is None:
        return "", ""
    fallback_open_cell = ""
    for item in public_attachment_entries(raw.message_id, raw.attachments):
        url = str(item.get("public_url") or "")
        open_cell = _media_url_cell(url)
        preview = _public_attachment_preview(item)
        if preview:
            return preview, open_cell
        if not fallback_open_cell:
            fallback_open_cell = open_cell
    return "", fallback_open_cell


def _public_elevator_asset_from_text(text: str, fallback_asset: str | None) -> str | None:
    clean = _clean_text(text).casefold()
    if re.search(r"\b(?:both|zero|no|two|2)\s+(?:elevators?|lifts?)\b", clean):
        return "elevator_both"
    if re.search(
        r"\b(?:both|zero|no|two|2)\b[^.!?\n]{0,80}\b"
        r"(?:out|down|dead|broken|stuck|stopped|not\s+moving|not\s+working|out\s+of\s+(?:service|order))\b",
        clean,
        re.IGNORECASE,
    ):
        return "elevator_both"
    only_working = PUBLIC_ELEVATOR_ONLY_SIDE_WORKING_RE.search(clean)
    if only_working:
        side = only_working.group("side").casefold()
        if side == "north":
            return "elevator_south"
        if side == "south":
            return "elevator_north"
    segments = [segment.strip() for segment in re.split(r"[.;!?\n,]+|\bbut\b|\bwhile\b", clean) if segment.strip()] or [clean]

    def side_has(side: str, status: str) -> bool:
        return any(
            re.search(rf"\b(?:the\s+)?{side}\b(?:\s+(?:elevator|lift|one|side))?[^.!?\n]{{0,80}}\b{status}\b", segment, re.IGNORECASE)
            or re.search(rf"\b{status}\b[^.!?\n]{{0,80}}\b(?:the\s+)?{side}\b(?:\s+(?:elevator|lift|one|side))?", segment, re.IGNORECASE)
            or re.search(rf"\bno\s+(?:the\s+)?{side}\s+(?:elevator|lift|one|side)\b", segment, re.IGNORECASE)
            or re.search(rf"\bnot\s+(?:the\s+)?{side}\s+(?:elevator|lift)\b", segment, re.IGNORECASE)
            for segment in segments
        )

    affected_status = r"(?:out|down|dead|broken|stuck|stopped|not\s+moving|doesn['’]?t\s+seem\s+to\s+be\s+moving|won['’]?t\s+move|not\s+working|out\s+of\s+(?:service|order))"
    working_status = r"(?:working|functioning|operational|running|in\s+service|restored|back\s+(?:up|on|in\s+service))"
    north_affected = side_has("north", affected_status)
    south_affected = side_has("south", affected_status)
    north_working = side_has("north", working_status)
    south_working = side_has("south", working_status)
    if north_affected and south_affected:
        return "elevator_both"
    if north_affected:
        return "elevator_north"
    if south_affected:
        return "elevator_south"
    if north_working:
        return "elevator_north"
    if south_working:
        return "elevator_south"
    if "north" in clean:
        return "elevator_north"
    if "south" in clean:
        return "elevator_south"
    return fallback_asset


def _public_elevator_text_has_context(text: str, fallback_asset: str | None) -> bool:
    clean = _clean_text(text)
    if PUBLIC_ELEVATOR_WORD_RE.search(clean) or PUBLIC_ELEVATOR_SIDE_REFERENCE_RE.search(clean):
        return True
    lowered = clean.casefold()
    if fallback_asset == "elevator_both":
        if re.search(
            r"\b(?:both|two|2)\b[^.!?\n]{0,80}\b"
            r"(?:working|functioning|operational|running|in\s+service|restored|fixed|back\s+(?:up|on|in\s+service))\b",
            lowered,
        ):
            return True
        if re.search(
            r"\b(?:both|two|2)\b[^.!?\n]{0,80}\b"
            r"(?:out|down|dead|broken|stuck|not\s+working|out\s+of\s+(?:service|order))\b",
            lowered,
        ):
            return True
        if re.search(r"\b(?:back|down)\s+to\s+one\b|\bonly\s+one\b", lowered):
            return True
    return False


def _public_elevator_text_is_working_status(text: str) -> bool:
    clean = _clean_text(text)
    if not clean:
        return False
    if PUBLIC_ELEVATOR_ONLY_SIDE_WORKING_RE.search(clean):
        return False
    if PUBLIC_ELEVATOR_NEGATED_FLOOR_SERVICE_RE.search(clean):
        return True
    if _public_elevator_text_is_current_working_after_past_outage(clean):
        return True
    if re.search(r"\b(?:working\s+normal(?:ly)?|working\s+rn|working\s+now)\b", clean, re.IGNORECASE):
        return True
    if PUBLIC_ELEVATOR_WORKING_STATUS_RE.search(clean) and not re.search(
        r"\b(?:not\s+(?:the\s+)?(?:north|south|left|right)\s+(?:elevator|lift)|zero\s+(?:elevators?|lifts?)|no\s+(?:elevators?|lifts?)|dead|out|down|out\s+of\s+(?:service|order)|not\s+working|not\s+moving|doesn['’]?t\s+seem\s+to\s+be\s+moving|won['’]?t\s+move|stopped|stuck|trapped|entrapment|alarm)\b",
        clean,
        re.IGNORECASE,
    ):
        return True
    return False


def _public_elevator_text_is_actionable(text: str) -> bool:
    clean = _clean_text(text)
    if not clean:
        return False
    if PUBLIC_RECORDKEEPING_DISCUSSION_RE.search(clean):
        return False
    if _public_elevator_text_is_working_status(clean):
        return False
    if PUBLIC_ELEVATOR_REPLACEMENT_DISCUSSION_RE.search(clean) and not re.search(
        r"\b(?:zero\s+(?:elevators?|lifts?)|no\s+(?:elevators?|lifts?)|both\s+(?:elevators?|lifts?)\s+(?:are\s+)?(?:out|down|dead)|out\s+of\s+(?:service|order)|not\s+working|stuck|trapped|entrapment|floor[- ]by[- ]floor|stopping\s+(?:(?:at|on)\s+)?(?:each|every|all)\s+floor|down\s+to\s+one|only\s+one)\b",
        clean,
        re.IGNORECASE,
    ):
        return False
    return bool(PUBLIC_ELEVATOR_ACTIONABLE_RE.search(clean) or PUBLIC_ELEVATOR_CALL_RESPONSE_RE.search(clean))


def _public_elevator_text_confirms_same_issue(text: str) -> bool:
    clean = _clean_text(text)
    return bool(
        clean
        and (PUBLIC_ELEVATOR_WORD_RE.search(clean) or PUBLIC_ELEVATOR_SIDE_REFERENCE_RE.search(clean))
        and PUBLIC_ELEVATOR_SAME_CONFIRMATION_RE.search(clean)
    )


def _public_elevator_text_is_call_response_issue(text: str) -> bool:
    clean = _clean_text(text)
    return bool(clean and PUBLIC_ELEVATOR_CALL_RESPONSE_RE.search(clean))


def _public_elevator_text_is_current_working_after_past_outage(text: str) -> bool:
    clean = _clean_text(text)
    if not clean:
        return False
    current_working = re.search(
        r"\b(?:looks?\s+like\s+)?(?:both|two|2)\s+(?:elevators?|lifts?)\s+"
        r"(?:are\s+|were\s+)?(?:working|functioning|operational|running|in\s+service)\b",
        clean,
        re.IGNORECASE,
    )
    past_outage = re.search(
        r"\b(?:was|were|had\s+been)\s+(?:still\s+)?(?:out|down|dead|not\s+working)\b",
        clean,
        re.IGNORECASE,
    )
    return bool(current_working and past_outage)


def _public_other_update_issue_label(text: str) -> str:
    clean = _clean_text(text)
    if PUBLIC_STAIR_SPILL_RE.search(clean):
        return "Stairwell liquid spill"
    return ""


def _public_has_apartment_entry_concern(text: str) -> bool:
    clean = _clean_text(text)
    return bool(PUBLIC_APARTMENT_ENTRY_RE.search(clean) or PUBLIC_APARTMENT_OCCUPANCY_ENTRY_RE.search(clean))


def _public_should_include_update(
    incident: Incident,
    raw: RawMessage | None,
    decision: MessageDecision | None = None,
) -> bool:
    if raw is None:
        return True
    text = _clean_text(raw.text)
    if not text:
        return False
    decision_event = _clean_text(getattr(decision, "event_type", ""))
    decision_category = _clean_text(getattr(decision, "category", ""))
    decision_is_elevator_restore = (
        incident.category == "elevator"
        and decision_category == "elevator"
        and decision_event == "restore"
    )
    if PUBLIC_RECORDKEEPING_DISCUSSION_RE.search(text) or PUBLIC_FORM_PROCESS_DISCUSSION_RE.search(text):
        return False
    if PUBLIC_GENERIC_RESOLVED_FRAGMENT_RE.search(text) and not decision_is_elevator_restore:
        return False
    if public_attachment_entries(raw.message_id, raw.attachments):
        return True
    if _public_has_apartment_entry_concern(text):
        return True
    if incident.category == "elevator":
        detection_text = _public_update_detection_text(raw)
        has_elevator_context = _public_elevator_text_has_context(detection_text, incident.asset)
        has_repair_context = bool(
            PUBLIC_REPAIR_NOT_COMPLETE_RE.search(detection_text)
            or PUBLIC_REPAIR_NOT_ON_SITE_RE.search(detection_text)
            or PUBLIC_REPAIR_ON_SITE_RE.search(detection_text)
            or PUBLIC_REPAIR_CALLED_RE.search(detection_text)
        )
        if not has_elevator_context and not has_repair_context and not decision_is_elevator_restore:
            return False
        return bool(
            _public_elevator_text_is_actionable(detection_text)
            or _public_elevator_text_confirms_same_issue(detection_text)
            or _public_elevator_text_is_working_status(detection_text)
            or _public_repair_event_label(detection_text)
            or (decision_is_elevator_restore and _public_elevator_text_is_working_status(detection_text))
        )
    if incident.category == "other":
        return bool(_public_other_update_issue_label(text))
    return True


def _public_incident_has_includeable_update(
    incident: Incident,
    raw_map: dict[str, RawMessage],
    allowed_chat_names: set[str],
    message_ids_by_incident: dict[str, list[str]] | None = None,
    decision_map: dict[str, MessageDecision] | None = None,
) -> bool:
    message_ids = _public_incident_message_ids(incident, message_ids_by_incident)
    if not message_ids:
        return True
    return any(
        (raw := raw_map.get(message_id)) is not None
        and _raw_message_is_public(raw, allowed_chat_names)
        and _public_should_include_update(incident, raw, (decision_map or {}).get(message_id))
        for message_id in message_ids
    )


def _public_is_actionable_311_update(incident: Incident, raw: RawMessage | None) -> bool:
    if incident.category != "elevator" or raw is None:
        return bool(incident.category != "elevator")
    text = _public_update_detection_text(raw)
    return _public_elevator_text_is_actionable(text) or _public_elevator_text_confirms_same_issue(text)


def _public_event_issue_label(incident: Incident, raw: RawMessage | None) -> str:
    text = _clean_text(getattr(raw, "text", ""))
    if _public_has_apartment_entry_concern(text):
        if PUBLIC_UNDER_SINK_LEAK_RE.search(text):
            return "Under-sink leak and apartment entry concern"
        return "Apartment entry / access concern"
    if incident.category == "elevator":
        detection_text = _public_update_detection_text(raw)
        asset = _public_elevator_asset_from_text(detection_text, incident.asset)
        working_status = _public_elevator_text_is_working_status(detection_text)
        actionable = _public_elevator_text_is_actionable(detection_text)
        normal_floor_service = bool(
            re.search(r"\bnormal(?:ly)?\b", detection_text, re.IGNORECASE)
            or PUBLIC_ELEVATOR_NEGATED_FLOOR_SERVICE_RE.search(detection_text)
        )
        if working_status:
            if asset == "elevator_both":
                if normal_floor_service:
                    return "Both elevators working normally"
                return "Both elevators working"
            if asset == "elevator_north":
                if normal_floor_service:
                    return "North elevator working normally"
                return "North elevator working"
            if asset == "elevator_south":
                if normal_floor_service:
                    return "South elevator working normally"
                return "South elevator working"
            return "Elevator working update"
        if _public_elevator_text_confirms_same_issue(detection_text):
            if asset == "elevator_north":
                return "North elevator"
            if asset == "elevator_south":
                return "South elevator"
            if asset == "elevator_both":
                return "Both elevators"
            return "Elevator issue confirmation"
        if _public_elevator_text_is_call_response_issue(detection_text):
            return _public_title_issue_label(incident, fallback="Elevator not responding to floor call")
        if actionable:
            lowered = detection_text.casefold()
            if "alarm" in lowered:
                return _public_issue_label(incident) or "Elevator alarm"
            if PUBLIC_ELEVATOR_IRREGULAR_OPERATION_RE.search(detection_text):
                if asset == "elevator_north":
                    return "North elevator operation issue"
                if asset == "elevator_south":
                    return "South elevator operation issue"
                return "Elevator operation issue"
            if (
                "floor-by-floor" in lowered
                or "floor by floor" in lowered
                or "skipping" in lowered
                or "irregular floor" in lowered
                or re.search(r"\bstopping\s+(?:(?:at|on)\s+)?(?:each|every|all)\s+floor\b", lowered)
            ):
                return "Elevator floor-service issue"
            if re.search(r"\b(?:stopped|not\s+moving|doesn['’]?t\s+seem\s+to\s+be\s+moving|won['’]?t\s+move)\b", lowered):
                if asset == "elevator_both":
                    return "Both elevators"
                if asset == "elevator_north":
                    return "North elevator"
                if asset == "elevator_south":
                    return "South elevator"
            if re.search(r"\b(?:one|1)\s+(?:working\s+)?(?:elevator|lift)\b|\bback\s+to\s+one\b|\bdown\s+to\s+one\b", lowered):
                return "Elevator service reduced"
            if asset == "elevator_both":
                return "Both elevators"
            if asset == "elevator_north":
                return "North elevator"
            if asset == "elevator_south":
                return "South elevator"
            return "Elevator outage"
        repair_label = _public_repair_event_label(detection_text)
        if repair_label == "Mechanic on site":
            return "Elevator repair visit"
        if repair_label == "Repair people called/expected":
            return "Elevator repair expected"
        if repair_label == "Repair not completed":
            return "Elevator repair not completed"
    if incident.category == "other":
        other_label = _public_other_update_issue_label(text)
        if other_label:
            return other_label
    return _public_issue_label(incident)


def _public_event_category_label(incident: Incident, raw: RawMessage | None) -> str:
    text = _clean_text(getattr(raw, "text", ""))
    if _public_has_apartment_entry_concern(text):
        if PUBLIC_UNDER_SINK_LEAK_RE.search(text):
            return "Leaks / water damage / Security / access"
        return "Security / access"
    return _public_category_label(incident.category)


def _public_elevator_text_is_reduced_service(text: str) -> bool:
    lowered = _clean_text(text).casefold()
    return bool(
        re.search(
            r"\b(?:back|down)\s+to\s+one\b"
            r"|\bonly\s+one\s+(?:working\s+)?(?:elevator|lift)\b"
            r"|\bone\s+(?:working\s+)?(?:elevator|lift)\b"
            r"|\bone\s+(?:elevator|lift)\s+(?:is\s+|was\s+|currently\s+)?(?:out|down|dead|broken|not\s+working|out\s+of\s+(?:service|order))\b",
            lowered,
        )
    )


def _public_elevator_outage_summary(asset: str | None, text: str) -> str:
    lowered = _clean_text(text).casefold()
    if PUBLIC_ELEVATOR_IRREGULAR_OPERATION_RE.search(text):
        if asset == "elevator_north":
            return "North elevator was reported making a loud clunk, bouncing, or opening slowly."
        if asset == "elevator_south":
            return "South elevator was reported making a loud clunk, bouncing, or opening slowly."
        return "Elevator was reported making a loud clunk, bouncing, or opening slowly."
    if "alarm" in lowered:
        return "Elevator alarm was reported."
    if re.search(r"\bfloor[- ]by[- ]floor\b|\bstopping\s+(?:(?:at|on)\s+)?(?:each|every|all)\s+floor\b|\bskipping\b|\birregular\s+floor\b", lowered):
        return "Elevator floor-service issue was reported."
    if re.search(r"\btrapped\b|\bentrapment\b", lowered):
        return "A person was reported trapped in an elevator."
    if re.search(r"\b(?:stopped|not\s+moving|doesn['’]?t\s+seem\s+to\s+be\s+moving|won['’]?t\s+move)\b", lowered):
        if asset == "elevator_both":
            return "Both elevators were reported stopped or not moving."
        if asset == "elevator_north":
            return "North elevator was reported stopped or not moving."
        if asset == "elevator_south":
            return "South elevator was reported stopped or not moving."
        return "Elevator was reported stopped or not moving."
    if _public_elevator_text_is_reduced_service(lowered):
        if re.search(
            r"\bone\s+(?:elevator|lift)\s+(?:is\s+|was\s+|currently\s+)?(?:out|down|dead|broken|not\s+working|out\s+of\s+(?:service|order))\b",
            lowered,
        ):
            return "One elevator was reported out of service."
        return "Elevator service was reported reduced to one working elevator."
    still = bool(re.search(r"\bstill\b|\bagain\b|\bcontinued?\b|\bremains?\b", lowered))
    state = "still out" if still else "out"
    if "down" in lowered and "out" not in lowered:
        state = "still down" if still else "down"
    if asset == "elevator_both":
        return f"Both elevators were reported as {state}."
    if asset == "elevator_north":
        return f"North elevator was reported as {state}."
    if asset == "elevator_south":
        return f"South elevator was reported as {state}."
    return f"Elevator outage was reported as {state}."


def _public_event_summary(incident: Incident, raw: RawMessage | None) -> str:
    text = _clean_text(getattr(raw, "text", ""))
    if _public_has_apartment_entry_concern(text):
        if PUBLIC_UNDER_SINK_LEAK_RE.search(text):
            return "Resident reported an under-sink leak and possible apartment entry while no one was home."
        if re.search(r"\badvise\s+super\b", text, re.IGNORECASE):
            return "Resident reported a response about apartment entry; the super would be advised."
        return "Resident reported an apartment entry or access concern."
    if incident.category == "elevator":
        detection_text = _public_update_detection_text(raw)
        asset = _public_elevator_asset_from_text(detection_text, incident.asset)
        if _public_elevator_text_is_working_status(detection_text):
            normal_floor_service = bool(
                re.search(r"\bnormal(?:ly)?\b", detection_text, re.IGNORECASE)
                or PUBLIC_ELEVATOR_NEGATED_FLOOR_SERVICE_RE.search(detection_text)
            )
            if asset == "elevator_both" and _public_elevator_text_is_current_working_after_past_outage(detection_text):
                return "Both elevators were reported working; one had been down earlier."
            if asset == "elevator_both" and normal_floor_service:
                return "Both elevators were reported working normally, without floor-by-floor service."
            if asset == "elevator_both":
                return "Both elevators were reported working."
            if asset == "elevator_north" and normal_floor_service:
                return "North elevator was reported working normally, without floor-by-floor service."
            if asset == "elevator_north":
                return "North elevator was reported working."
            if asset == "elevator_south" and normal_floor_service:
                return "South elevator was reported working normally, without floor-by-floor service."
            if asset == "elevator_south":
                return "South elevator was reported working."
        if _public_elevator_text_confirms_same_issue(detection_text):
            if asset == "elevator_north":
                return "A second report confirmed the same north elevator issue."
            if asset == "elevator_south":
                return "A second report confirmed the same south elevator issue."
            return "A second report confirmed the same elevator issue."
        if _public_elevator_text_is_call_response_issue(detection_text):
            label = _public_title_issue_label(incident, fallback="Elevator not responding to floor call")
            return _public_detail_text(incident, label)
        if _public_elevator_text_is_actionable(detection_text) and PUBLIC_REPAIR_CALLED_RE.search(detection_text):
            if asset == "elevator_both":
                return "Both elevators were reported out, and repair people were expected."
            return "Elevator outage was reported, and repair people were expected."
        repair_label = _public_repair_event_label(detection_text)
        if repair_label == "Mechanic on site":
            return "Elevator mechanic was reported on site."
        if repair_label == "Repair people called/expected":
            return "Elevator repair people were reported called or expected."
        if repair_label == "Repair not completed":
            return "Elevator repair was reported not completed yet."
        if _public_elevator_text_is_actionable(detection_text):
            return _public_elevator_outage_summary(asset, detection_text)
    if incident.category == "other" and PUBLIC_STAIR_SPILL_RE.search(text):
        return "Liquid spill was reported in the stairwell/common area."
    context = _public_visible_context_text(text)
    summary = _truncate_public_text(_public_safe_summary_text(context), limit=320)
    if summary and summary[-1] not in ".!?":
        summary += "."
    return summary or _public_detail_text(incident, _public_focus_label(incident))


def _public_update_rows(
    incidents: list[Incident],
    raw_map: dict[str, RawMessage],
    case_map: dict[str, list[ServiceRequestCase]],
    allowed_chat_names: set[str],
    message_ids_by_incident: dict[str, list[str]] | None = None,
    decision_map: dict[str, MessageDecision] | None = None,
) -> list[list[object]]:
    rows: list[tuple[int, str, str, list[object]]] = []
    seen_messages: set[str] = set()
    for incident in incidents:
        message_ids = _public_incident_message_ids(incident, message_ids_by_incident)
        if not message_ids:
            focus_label = _public_focus_label(incident)
            rows.append((
                _incident_last_epoch(incident),
                incident.incident_id,
                "",
                [
                    _public_ts(incident.last_ts_epoch, fallback=incident.updated_at) or "",
                    focus_label,
                    _public_category_label(incident.category),
                    _public_case_badge(case_map.get(incident.incident_id, [])),
                    "",
                    "",
                    _public_detail_text(incident, focus_label),
                    "",
                    "",
                ],
            ))
            continue
        for message_id in message_ids:
            if message_id in seen_messages:
                continue
            raw = raw_map.get(message_id)
            if raw is None or not _raw_message_is_public(raw, allowed_chat_names):
                continue
            if not _public_should_include_update(incident, raw, (decision_map or {}).get(message_id)):
                continue
            seen_messages.add(message_id)
            source_text_key = re.sub(r"\W+", " ", _public_update_detection_text(raw).casefold()).strip()
            preview_cell, open_cell = _public_raw_evidence_cells(raw)
            cases = case_map.get(incident.incident_id, []) if _public_is_actionable_311_update(incident, raw) else []
            rows.append((
                int(raw.ts_epoch or _incident_last_epoch(incident) or 0),
                message_id,
                source_text_key,
                [
                    _public_ts(raw.ts_iso, fallback=raw.ts_epoch) or "",
                    _public_event_issue_label(incident, raw),
                    _public_event_category_label(incident, raw),
                    _public_case_badge(cases) if cases else "",
                    preview_cell,
                    open_cell,
                    _public_event_summary(incident, raw),
                    "",
                    "",
                ],
            ))
    rows.sort(key=lambda item: (item[0], item[1]), reverse=True)
    deduped_rows: list[tuple[tuple[str, str, str], int, str, str, list[object]]] = []
    for epoch, key, source_text_key, row in rows:
        duplicate_key = (str(row[1]).casefold(), str(row[2]).casefold(), str(row[6]).casefold())
        existing_index = next(
            (
                idx
                for idx, (
                    existing_key,
                    existing_epoch,
                    _existing_message_key,
                    existing_source_text_key,
                    _existing_row,
                ) in enumerate(deduped_rows)
                if existing_key == duplicate_key
                and (
                    abs(int(epoch or 0) - int(existing_epoch or 0)) <= PUBLIC_UPDATE_DUPLICATE_WINDOW_SECONDS
                    or (
                        source_text_key
                        and source_text_key == existing_source_text_key
                        and abs(int(epoch or 0) - int(existing_epoch or 0)) <= PUBLIC_DUPLICATE_WINDOW_SECONDS
                    )
                )
            ),
            None,
        )
        if existing_index is None:
            deduped_rows.append((duplicate_key, epoch, key, source_text_key, row))
            continue
        _existing_key, existing_epoch, _existing_message_key, existing_source_text_key, existing_row = deduped_rows[existing_index]
        existing_has_evidence = bool(existing_row[4] or existing_row[5])
        row_has_evidence = bool(row[4] or row[5])
        existing_has_case = bool(existing_row[3])
        row_has_case = bool(row[3])
        if row_has_evidence and not existing_has_evidence:
            deduped_rows[existing_index] = (duplicate_key, epoch, key, source_text_key, row)
        elif row_has_case and not existing_has_case:
            deduped_rows[existing_index] = (duplicate_key, epoch, key, source_text_key, row)
        elif row_has_evidence == existing_has_evidence and row_has_case == existing_has_case and int(epoch or 0) > int(existing_epoch or 0):
            deduped_rows[existing_index] = (duplicate_key, epoch, key, source_text_key, row)
    rows = [(epoch, key, row) for _duplicate_key, epoch, key, _source_text_key, row in deduped_rows]
    rows.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return _public_collapse_same_timestamp_rows([row for _epoch, _key, row in rows])


def _public_join_unique(values: list[object], *, separator: str, collapse_substrings: bool = False) -> str:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        clean = _clean_text(str(value or ""))
        if not clean:
            continue
        key = re.sub(r"\W+", " ", clean.casefold()).strip()
        if key in seen:
            continue
        if collapse_substrings and any(key and (key in existing or existing in key) for existing in seen):
            if any(existing in key for existing in seen):
                out = [existing_value for existing_value in out if re.sub(r"\W+", " ", existing_value.casefold()).strip() not in key]
                seen = {re.sub(r"\W+", " ", existing_value.casefold()).strip() for existing_value in out}
            else:
                continue
        seen.add(key)
        out.append(clean)
    return separator.join(out)


def _public_merge_same_time_group(group: list[list[object]]) -> list[object]:
    if len(group) == 1:
        return group[0]

    ordered = sorted(group, key=lambda row: 0 if row[3] else 1)
    if any(row[3] for row in ordered):
        filtered = [row for row in ordered if row[3] or "working" not in _clean_text(str(row[1])).casefold()]
        if filtered:
            ordered = filtered
    preview_row = next((row for row in ordered if row[4]), None)
    evidence_row = next((row for row in ordered if row[5]), None)
    return [
        ordered[0][0],
        _public_join_unique([row[1] for row in ordered], separator=" / "),
        _public_join_unique([row[2] for row in ordered], separator=" / "),
        _public_join_unique([row[3] for row in ordered], separator="; "),
        preview_row[4] if preview_row else "",
        evidence_row[5] if evidence_row else "",
        _public_join_unique([row[6] for row in ordered], separator=" ", collapse_substrings=True),
        "",
        "",
    ]


def _public_collapse_same_timestamp_rows(rows: list[list[object]]) -> list[list[object]]:
    grouped: dict[str, list[list[object]]] = {}
    order: list[str] = []
    for row in rows:
        key = _clean_text(str(row[0] if row else ""))
        if not key:
            order.append(f"__blank_{len(order)}")
            grouped[order[-1]] = [row]
            continue
        if key not in grouped:
            order.append(key)
            grouped[key] = []
        grouped[key].append(row)
    return [_public_merge_same_time_group(grouped[key]) for key in order]


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
                raw.text or _clean_text(str((context.get("message_context") or {}).get("reply_text") or ""))
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
                    rows.append([
                        message_time,
                        focus_label,
                        _public_attachment_preview(item),
                        _media_url_cell(str(item.get("public_url") or "")),
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
    case_map: dict[str, list[ServiceRequestCase]],
) -> list[list[object]]:
    stats: dict[str, dict[str, object]] = {}
    for incident in incidents:
        label = _public_category_label(incident.category)
        row = stats.setdefault(
            label,
            {
                "total": 0,
                "cases": 0,
                "latest_epoch": 0,
                "latest_issue": "",
            },
        )
        row["total"] = int(row["total"]) + 1
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
        return _media_url_cell(str(item.get("public_url") or ""))
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
        incident_ids_all = [row.incident_id for row in incidents]
        decision_rows = (
            session.query(MessageDecision)
            .filter(MessageDecision.incident_id.in_(incident_ids_all))
            .all()
            if incident_ids_all
            else []
        )
        message_ids_by_incident: dict[str, list[str]] = {}
        decision_map: dict[str, MessageDecision] = {}
        for decision in decision_rows:
            if not decision.incident_id:
                continue
            message_ids_by_incident.setdefault(decision.incident_id, []).append(decision.message_id)
            decision_map[decision.message_id] = decision
        proof_message_ids = {
            message_id
            for incident in incidents
            for message_id in [item.strip() for item in (incident.proof_refs or "").split(",") if item.strip()]
        }
        proof_message_ids.update(decision_map.keys())
        raw_map = (
            {row.message_id: row for row in session.query(RawMessage).filter(RawMessage.message_id.in_(sorted(proof_message_ids))).all()}
            if proof_message_ids
            else {}
        )
        public_incidents = [
            row
            for row in incidents
            if _incident_is_public(row, raw_map, allowed_chat_names, message_ids_by_incident)
        ]
        public_incidents = [
            row
            for row in public_incidents
            if _public_incident_has_includeable_update(
                row,
                raw_map,
                allowed_chat_names,
                message_ids_by_incident,
                decision_map,
            )
        ]
        incident_ids = [row.incident_id for row in public_incidents]
        all_cases = session.query(ServiceRequestCase).filter(ServiceRequestCase.incident_id.in_(incident_ids)).all() if incident_ids else []

    case_map: dict[str, list[ServiceRequestCase]] = {}
    for case in all_cases:
        if not case.incident_id:
            continue
        case_map.setdefault(case.incident_id, []).append(case)

    public_incidents.sort(key=_public_incident_sort_key, reverse=True)

    public_incidents, case_map = _public_collapse_duplicate_incidents(public_incidents, case_map, message_ids_by_incident)
    public_cases = [case for cases in case_map.values() for case in cases]
    public_cases.sort(key=_public_case_sort_key, reverse=True)
    category_rows = _public_category_rows(public_incidents, case_map)
    update_rows = _public_update_rows(
        public_incidents,
        raw_map,
        case_map,
        allowed_chat_names,
        message_ids_by_incident,
        decision_map,
    )
    refresh_label = _fmt_ts(now_epoch)
    top_category = category_rows[0][0] if category_rows else ""
    latest_issue = str(update_rows[0][1]) if update_rows else (_public_focus_label(public_incidents[0]) if public_incidents else "")

    values: list[list[object]] = [
        [PUBLIC_WORKBOOK_TITLE, "", "", "", "", "", "", "", "", ""],
        ["Automatic log from resident messages, captured evidence, and 311 activity. It keeps the tenant record in one place.", "", "", "", "", "", "", "", "", ""],
        ["At a glance", "", "", "", "", "", "", "", "", ""],
        ["Item", "Count / detail", "What this means", "", "", "", "", "", "", ""],
        ["Last refresh", refresh_label, "Updated automatically from resident messages and 311 records.", "", "", "", "", "", "", ""],
        ["Incidents", len(public_incidents), "Issues logged from the tenant chat and connected evidence.", "", "", "", "", "", "", ""],
        ["311 filings", len(public_cases), "311 service requests connected to logged incidents.", "", "", "", "", "", "", ""],
        ["Most common issue type", top_category, "Category with the most logged incidents.", "", "", "", "", "", "", ""],
        ["Latest update", latest_issue, "Newest update in the log.", "", "", "", "", "", "", ""],
        ["", "", "", "", "", "", "", "", "", ""],
        ["Category snapshot", "", "", "", "", "", "", "", "", ""],
        ["Category", "Incidents", "311 filings", "Latest update", "Latest issue", "", "", "", "", ""],
    ]

    if category_rows:
        values.extend(category_rows)
    else:
        values.append(["No public incidents yet", "", "", "", "", "", "", "", "", ""])

    values.append(["", "", "", "", "", "", "", "", "", ""])
    incidents_title_row = len(values) + 1
    values.append(["Public update log", "", "", "", "", "", "", "", "", ""])
    incidents_header_row = len(values) + 1
    values.append(["Updated", "Issue", "Category", "311 follow-up", "Preview", "Open evidence", "Summary", "", "", ""])

    if update_rows:
        values.extend(update_rows)
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

    _unmerge_tab_range(svc, sheet_id, tab, row_count=len(values), column_count=PUBLIC_LAYOUT_COLUMNS)
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
            "stats_row_count": 5,
            "section_rows": [3, 11, incidents_title_row, case_watch_title_row],
            "header_rows": [4, 12, incidents_header_row, case_watch_header_row],
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


def sync_project_status_to_sheets():
    svc = _service()
    sheet_id = _watchdog_sheet_id()
    tab = _tab("SHEETS_PROJECT_STATUS_TAB", default="ProjectStatus")
    with get_session() as session:
        state = project_state(session)
        session.commit()

    project = state["project"]
    official_records = state["official_records"]
    open_actions = [row for row in state["actions"] if row.get("status") == "open"]
    records_needing_verification = [row for row in official_records if row.get("needs_human_verification")]
    machine_verified_records = [row for row in official_records if row.get("machine_verified_at")]
    last_record_sync = max((row.get("last_seen_at") or "" for row in official_records), default="")
    severity_rank = {"critical": 0, "yellow": 1, "watch": 2, "info": 3}
    next_action = sorted(open_actions, key=lambda row: (severity_rank.get(row.get("severity"), 9), row.get("due_at") or ""))[0] if open_actions else {}
    values = [["section", "item", "status", "detail", "source", "updated_at"]]
    values.extend(
        [
            ["summary", "last_public_record_sync", "", last_record_sync, "watchdog", project.get("updated_at") or ""],
            ["summary", "official_records_total", len(official_records), "Trusted elevator/replacement records shown in PublicRecords.", "watchdog", project.get("updated_at") or ""],
            ["summary", "machine_verified_records", len(machine_verified_records), "Official-source matches accepted automatically from NYC/DOB/Open Data identifiers.", "watchdog", project.get("updated_at") or ""],
            ["summary", "records_needing_review", len(records_needing_verification), "Tenant-visible records needing manual review.", "watchdog", project.get("updated_at") or ""],
            ["summary", "open_actions", len(open_actions), "Tenant actions currently shown in ActionQueue.", "watchdog", project.get("updated_at") or ""],
            ["summary", "next_action", next_action.get("severity") or "", next_action.get("title") or "No open watchdog action.", "watchdog", project.get("updated_at") or ""],
            ["project", "title", project.get("phase") or "", project.get("title") or "", "management/public-record watchdog", project.get("updated_at") or ""],
            ["project", "risk_level", project.get("risk_level") or "", project.get("current_bottleneck") or "", "watchdog", project.get("updated_at") or ""],
            ["project", "next_expected_record", "", project.get("next_expected_record") or "", "watchdog", project.get("updated_at") or ""],
            ["management_claim", "summary", "claimed", project.get("management_summary") or "", "management_pdf", project.get("updated_at") or ""],
        ]
    )
    for milestone in state["management_claims"]["milestones"]:
        values.append([
            "milestone",
            milestone["phase"],
            milestone["status"],
            "; ".join(item for item in [
                f"asset={milestone.get('elevator_asset')}" if milestone.get("elevator_asset") else "",
                f"claimed_start={milestone.get('management_claimed_start')}" if milestone.get("management_claimed_start") else "",
                f"claimed_end={milestone.get('management_claimed_end')}" if milestone.get("management_claimed_end") else "",
                milestone.get("notes") or "",
            ] if item),
            milestone.get("source_type") or "",
            "",
        ])
    _ensure_tab_exists(svc, sheet_id, tab)
    _replace_tab_values(svc, sheet_id, tab, values, value_input_option="USER_ENTERED")
    _apply_tab_layout(svc, sheet_id, tab, row_count=len(values), column_count=len(values[0]), layout="watchdog")


def sync_elevator_watch_public_view_to_sheets():
    svc = _service()
    sheet_id = _watchdog_sheet_id()
    tab = _tab("SHEETS_ELEVATOR_WATCH_TAB", default="ElevatorWatch")
    with get_session() as session:
        items = public_elevator_watch_items(session)
    values = [["What people need to know", "Current clear answer", "Why it matters", "Checked by", "Last checked", "Human needed", "Source"]]
    for item in items:
        values.append([
            item.get("topic") or "",
            item.get("answer") or "",
            item.get("why_it_matters") or "",
            item.get("checked_by") or "",
            normalize_timestamp(item.get("last_checked_at")) or "",
            item.get("human_needed") or "",
            item.get("source_url") or "",
        ])
    _ensure_tab_exists(svc, sheet_id, tab)
    _replace_tab_values(svc, sheet_id, tab, values, value_input_option="USER_ENTERED")
    _apply_tab_layout(svc, sheet_id, tab, row_count=len(values), column_count=len(values[0]), layout="watchdog")


def _public_record_sheet_sort_key(row: PublicRecordWatch) -> tuple[int, int, int, str]:
    status = f"{row.status or ''} {row.status_detail or ''}".casefold()
    if any(word in status for word in ("active", "open", "pending", "in progress")):
        status_rank = 0
    elif any(word in status for word in ("filed", "approved", "issued")):
        status_rank = 1
    else:
        status_rank = 2
    type_rank = {
        "elevator_permit_application": 0,
        "dob_ecb_violation": 1,
        "dob_violation": 2,
        "dob_complaint": 3,
        "nyc_311_service_request": 4,
        "elevator_safety_compliance": 5,
        "elevator_device_detail": 6,
        "oath_hearing_case": 7,
    }.get(row.record_type or "", 9)
    epoch = (
        parse_ts_to_epoch(row.filed_at)
        or parse_ts_to_epoch(row.permit_issued_at)
        or parse_ts_to_epoch(row.inspection_date)
        or parse_ts_to_epoch(row.last_changed_at)
        or 0
    )
    return (status_rank, type_rank, -epoch, row.record_key or "")


def sync_public_records_to_sheets():
    svc = _service()
    sheet_id = _watchdog_sheet_id()
    tab = _tab("SHEETS_PUBLIC_RECORDS_TAB", default="PublicRecords")
    with get_session() as session:
        records = session.query(PublicRecordWatch).all()
    values = [[
        "source_system", "record_type", "record_key", "verification_status", "machine_confidence",
        "verification_summary", "status", "status_detail", "filed_at", "approved_at", "permit_issued_at",
        "inspection_date", "expires_at", "needs_human_verification", "machine_verified_at",
        "human_verified_at", "human_verified_by", "source_url", "bbl", "bin", "job_number",
        "permit_number", "device_number",
    ]]
    for row in sorted(records, key=_public_record_sheet_sort_key):
        if not public_record_is_tenant_trusted(row):
            continue
        values.append([
            row.source_system,
            row.record_type,
            row.record_key,
            row.machine_verification_status or "needs_review",
            row.machine_confidence if row.machine_confidence is not None else "",
            (row.machine_verification_summary or "")[:500],
            row.status or "",
            (row.status_detail or "")[:500],
            normalize_timestamp(row.filed_at) or "",
            normalize_timestamp(row.approved_at) or "",
            normalize_timestamp(row.permit_issued_at) or "",
            normalize_timestamp(row.inspection_date) or "",
            normalize_timestamp(row.expires_at) or "",
            "YES - review needed" if row.needs_human_verification else "",
            normalize_timestamp(row.machine_verified_at) or "",
            normalize_timestamp(row.human_verified_at) or "",
            row.human_verified_by or "",
            row.source_url or "",
            row.bbl or "",
            row.bin or "",
            row.job_number or "",
            row.permit_number or "",
            row.device_number or "",
        ])
    _ensure_tab_exists(svc, sheet_id, tab)
    _replace_tab_values(svc, sheet_id, tab, values, value_input_option="USER_ENTERED")
    _apply_tab_layout(svc, sheet_id, tab, row_count=len(values), column_count=len(values[0]), layout="watchdog")


def sync_watchdog_checks_to_sheets():
    svc = _service()
    sheet_id = _watchdog_sheet_id()
    tab = _tab("SHEETS_WATCHDOG_CHECKS_TAB", default="WatchdogChecks")
    with get_session() as session:
        checks = session.query(ComplianceCheck).all()
    values = [["check_type", "status", "checked_at", "checked_by", "photo_url", "source_url", "notes"]]
    for row in sorted(checks, key=lambda item: item.checked_at or "", reverse=True):
        values.append([
            row.check_type,
            row.status,
            normalize_timestamp(row.checked_at) or "",
            row.checked_by or "",
            row.photo_url or "",
            row.source_url or "",
            (row.notes or "")[:500],
        ])
    _ensure_tab_exists(svc, sheet_id, tab)
    _replace_tab_values(svc, sheet_id, tab, values, value_input_option="USER_ENTERED")
    _apply_tab_layout(svc, sheet_id, tab, row_count=len(values), column_count=len(values[0]), layout="watchdog")


def sync_watchdog_actions_to_sheets():
    svc = _service()
    sheet_id = _watchdog_sheet_id()
    tab = _tab("SHEETS_WATCHDOG_ACTIONS_TAB", default="ActionQueue")
    with get_session() as session:
        actions = [
            row
            for row in session.query(WatchdogAction).filter(WatchdogAction.status.in_(["open", "pending", "failed"])).all()
            if action_is_tenant_visible(row)
        ]
    values = [[
        "severity", "action_type", "title", "detail", "due_at", "owner_role", "status",
        "source_record_id", "related_incident_id", "draft_message", "created_at", "completed_at",
    ]]
    for row in sorted(actions, key=lambda item: (item.status != "open", item.due_at or "", item.created_at or "")):
        values.append([
            row.severity,
            row.action_type,
            row.title,
            (row.detail or "")[:500],
            normalize_timestamp(row.due_at) or "",
            row.owner_role or "",
            row.status,
            row.source_record_id or "",
            row.related_incident_id or "",
            (row.draft_message or "")[:500],
            normalize_timestamp(row.created_at) or "",
            normalize_timestamp(row.completed_at) or "",
        ])
    if len(values) == 1:
        values.append([
            "info",
            "none",
            "No tenant action needed",
            "The system is checking official records and tenant reports automatically. Residents only need to report real conditions when they happen.",
            "",
            "system",
            "automatic",
            "",
            "",
            "",
            "",
            "",
        ])
    _ensure_tab_exists(svc, sheet_id, tab)
    _replace_tab_values(svc, sheet_id, tab, values, value_input_option="USER_ENTERED")
    _apply_tab_layout(svc, sheet_id, tab, row_count=len(values), column_count=len(values[0]), layout="watchdog")


def sync_weekly_digest_to_sheets():
    svc = _service()
    sheet_id = _watchdog_sheet_id()
    tab = _tab("SHEETS_WEEKLY_DIGEST_TAB", default="WeeklyDigest")
    with get_session() as session:
        digests = session.query(WeeklyDigest).all()
        tenant_actions = [
            row
            for row in session.query(WatchdogAction).filter(WatchdogAction.status.in_(["open", "pending", "failed"])).all()
            if action_is_tenant_visible(row)
        ]
    if tenant_actions:
        action_needed = "; ".join((row.title or row.action_type or "Tenant action needed") for row in tenant_actions[:3])
    else:
        action_needed = "No tenant action needed"
    values = [[
        "period_start",
        "period_end",
        "tenant_update",
        "watchdog_status",
        "tenant_action_needed",
        "generated_at",
        "used_llm",
    ]]
    for row in sorted(digests, key=lambda item: item.generated_at or "", reverse=True):
        values.append([
            normalize_timestamp(row.period_start) or "",
            normalize_timestamp(row.period_end) or "",
            row.tenant_update_draft or row.public_summary or "",
            "DOB/NYC record checks run automatically. Management follow-up drafts are internal.",
            action_needed,
            normalize_timestamp(row.generated_at) or "",
            "YES" if row.used_llm else "",
        ])
    _ensure_tab_exists(svc, sheet_id, tab)
    _replace_tab_values(svc, sheet_id, tab, values, value_input_option="USER_ENTERED")
    _apply_tab_layout(svc, sheet_id, tab, row_count=len(values), column_count=len(values[0]), layout="watchdog")


def sync_replacement_watchdog_to_sheets():
    sync_elevator_watch_public_view_to_sheets()
    sync_project_status_to_sheets()
    sync_public_records_to_sheets()
    sync_watchdog_checks_to_sheets()
    sync_watchdog_actions_to_sheets()
    sync_weekly_digest_to_sheets()


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
