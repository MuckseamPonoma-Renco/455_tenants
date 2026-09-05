from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from packages.sheets import sync as sheets_sync
from scripts import audit_public_watchdog_tabs as audit
from scripts.audit_public_watchdog_tabs import ExpectedTab, LiveTab, TabSpec


NY = ZoneInfo("America/New_York")


def test_runtime_env_is_loaded_before_database_session_factory_import():
    source = (Path(audit.__file__)).read_text(encoding="utf-8")

    assert source.index('load_local_env_file(ROOT / ".env")') < source.index(
        "from packages.db import SessionLocal"
    )


def _metadata(*titles: str, qa_hidden: bool = True) -> dict[str, object]:
    sheets = [
        {
            "properties": {
                "title": title,
                "sheetId": index + 1,
                "hidden": False,
                "gridProperties": {"rowCount": 1000, "columnCount": 26},
            }
        }
        for index, title in enumerate(titles)
    ]
    sheets.append(
        {
            "properties": {
                "title": "QA Draft 2026-05-05",
                "sheetId": 99,
                "hidden": qa_hidden,
                "gridProperties": {"rowCount": 100, "columnCount": 26},
            }
        }
    )
    return {
        "properties": {"title": sheets_sync.PUBLIC_WORKBOOK_TITLE, "timeZone": "America/New_York"},
        "sheets": sheets,
    }


def test_date_normalization_is_limited_to_declared_date_cells():
    assert audit._cells_equivalent("08/15/2026", "8/15/2026", is_date=True, workbook_tz=NY)
    assert audit._cells_equivalent("2026-08-15T04:00:00Z", "8/15/2026", is_date=True, workbook_tz=NY)
    assert not audit._cells_equivalent("08/15/2026", "8/15/2026", is_date=False, workbook_tz=NY)
    assert not audit._cells_equivalent("same ", "same", is_date=False, workbook_tz=NY)


def test_elevator_last_checked_allows_bounded_volatile_timestamp():
    spec = TabSpec(
        logical_name="ElevatorWatch",
        title="ElevatorWatch",
        headers=(
            "What people need to know",
            "Current clear answer",
            "Why it matters",
            "Checked by",
            "Last checked",
            "Human needed",
            "Source",
        ),
        date_columns=frozenset({4}),
        volatile_timestamp_column=4,
    )
    expected_values = [
        list(spec.headers),
        ["Permit", "Filed", "Scope", "Automatic check", "2026-09-05T12:00:00Z", "No", "source"],
    ]
    live_values = [
        list(spec.headers),
        ["Permit", "Filed", "Scope", "Automatic check", "9/5/2026 8:02 AM", "No", "source"],
    ]

    result = audit._audit_tab(
        spec,
        ExpectedTab(expected_values, "USER_ENTERED"),
        LiveTab(live_values, live_values),
        metadata=_metadata("ElevatorWatch"),
        workbook_tz=NY,
        now=datetime(2026, 9, 5, 12, 5, tzinfo=timezone.utc),
        max_age_seconds=600,
        max_drift_seconds=180,
        limit=20,
    )

    assert result["ok"] is True
    assert result["volatile_last_checked"][0]["drift_seconds"] == 120
    assert result["volatile_last_checked"][0]["age_seconds"] == 180


def test_elevator_last_checked_rejects_date_only_or_stale_value():
    spec = TabSpec(
        logical_name="ElevatorWatch",
        title="ElevatorWatch",
        headers=("topic", "Last checked"),
        date_columns=frozenset({1}),
        volatile_timestamp_column=1,
    )
    expected = ExpectedTab(
        [["topic", "Last checked"], ["Permit", "2026-09-05T12:00:00Z"]],
        "USER_ENTERED",
    )

    date_only = audit._audit_tab(
        spec,
        expected,
        LiveTab([["topic", "Last checked"], ["Permit", "9/5/2026"]], []),
        metadata=_metadata("ElevatorWatch"),
        workbook_tz=NY,
        now=datetime(2026, 9, 5, 12, 5, tzinfo=timezone.utc),
        max_age_seconds=600,
        max_drift_seconds=180,
        limit=20,
    )
    stale = audit._audit_tab(
        spec,
        expected,
        LiveTab([["topic", "Last checked"], ["Permit", "9/5/2026 6:00 AM"]], []),
        metadata=_metadata("ElevatorWatch"),
        workbook_tz=NY,
        now=datetime(2026, 9, 5, 12, 5, tzinfo=timezone.utc),
        max_age_seconds=600,
        max_drift_seconds=180,
        limit=20,
    )

    assert date_only["ok"] is False
    assert date_only["volatile_last_checked"][0]["live_parseable_timestamp"] is False
    assert stale["ok"] is False
    assert stale["volatile_last_checked"][0]["within_freshness_bound"] is False
    assert stale["volatile_last_checked"][0]["within_drift_bound"] is False


def test_tab_audit_detects_extra_rows_columns_and_formulas():
    spec = TabSpec(
        logical_name="Example",
        title="Example",
        headers=("first", "second"),
        date_columns=frozenset(),
    )
    expected = ExpectedTab([["first", "second"], ["a", "b"]], "USER_ENTERED")
    display = [["first", "second", "stale"], ["a", "b"], ["ghost"]]
    formulas = [["first", "second", "stale"], ["a", "=UPPER(\"b\")"], ["ghost"]]

    result = audit._audit_tab(
        spec,
        expected,
        LiveTab(display, formulas),
        metadata=_metadata("Example"),
        workbook_tz=NY,
        now=datetime(2026, 9, 5, tzinfo=timezone.utc),
        max_age_seconds=600,
        max_drift_seconds=180,
        limit=20,
    )

    assert result["ok"] is False
    assert result["live_used_row_count"] == 3
    assert result["live_used_column_width"] == 3
    assert result["extra_populated_cell_count"] == 2
    assert {(cell["row"], cell["column"]) for cell in result["extra_populated_cells"]} == {(1, 3), (3, 1)}
    assert result["unexpected_formula_cell_count"] == 1


def test_workbook_metadata_requires_expected_tabs_visible_but_allows_hidden_qa():
    specs = tuple(
        TabSpec(name, name, ("header",), frozenset())
        for name in (
            "ElevatorWatch",
            "ProjectStatus",
            "PublicRecords",
            "WatchdogChecks",
            "ActionQueue",
            "WeeklyDigest",
        )
    )
    titles = tuple(spec.title for spec in specs)

    accepted = audit._audit_workbook_metadata(_metadata(*titles, qa_hidden=True), specs)
    exposed_qa = audit._audit_workbook_metadata(_metadata(*titles, qa_hidden=False), specs)

    assert accepted["ok"] is True
    assert accepted["hidden_stale_qa_tabs"] == ["QA Draft 2026-05-05"]
    assert exposed_qa["ok"] is False
    assert exposed_qa["visible_stale_qa_tabs"] == ["QA Draft 2026-05-05"]


def test_expected_payload_capture_runs_renderer_against_noop_sheet_service(monkeypatch):
    spec = TabSpec("Example", "Example", ("header",), frozenset())
    original_service = sheets_sync._service
    original_sheet_id = sheets_sync._watchdog_sheet_id
    original_session = sheets_sync.get_session

    def renderer():
        sheets_sync._service().spreadsheets().values().update(
            spreadsheetId=sheets_sync._watchdog_sheet_id(),
            range="Example!A1",
            valueInputOption="USER_ENTERED",
            body={"values": [["header"], ["value"]]},
        ).execute()

    captured = audit._capture_expected_values("sheet-id", (spec,), renderer=renderer)

    assert captured == {"Example": ExpectedTab([["header"], ["value"]], "USER_ENTERED")}
    assert sheets_sync._service is original_service
    assert sheets_sync._watchdog_sheet_id is original_sheet_id
    assert sheets_sync.get_session is original_session


def test_live_reader_fetches_metadata_and_both_a_to_zz_value_views():
    calls: list[tuple[str, dict[str, object]]] = []
    metadata = _metadata("ElevatorWatch")

    class Request:
        def __init__(self, response):
            self.response = response

        def execute(self):
            return self.response

    class Values:
        def batchGet(self, **kwargs):
            calls.append(("batchGet", kwargs))
            return Request({"valueRanges": [{"values": [["header"], ["value"]]}]})

    class Spreadsheets:
        def get(self, **kwargs):
            calls.append(("get", kwargs))
            return Request(metadata)

        def values(self):
            return Values()

    class Service:
        def spreadsheets(self):
            return Spreadsheets()

    spec = TabSpec("ElevatorWatch", "ElevatorWatch", ("header",), frozenset())
    workbook = audit._read_live_workbook(Service(), "sheet-id", (spec,))

    assert workbook.tabs["ElevatorWatch"].display_values == [["header"], ["value"]]
    assert calls[1][1]["ranges"] == ["'ElevatorWatch'!A:ZZ"]
    assert calls[1][1]["valueRenderOption"] == "FORMATTED_VALUE"
    assert calls[2][1]["ranges"] == ["'ElevatorWatch'!A:ZZ"]
    assert calls[2][1]["valueRenderOption"] == "FORMULA"


def test_main_returns_nonzero_when_any_audit_check_fails(monkeypatch, capsys):
    monkeypatch.setattr(audit, "run_audit", lambda **_kwargs: {"ok": False, "tabs": {}})

    assert audit.main([]) == 1
    assert '"ok": false' in capsys.readouterr().out
