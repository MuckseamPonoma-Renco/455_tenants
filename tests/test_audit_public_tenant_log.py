import scripts.audit_public_tenant_log as public_audit
from scripts.audit_public_tenant_log import (
    LiveTenantLog,
    PublicRow,
    SourcePublicRow,
    _dedupe_source_rows,
    _public_row_covers_source_row,
)


def _pad(*values):
    return [*values, *([""] * (10 - len(values)))]


def _tenant_log_values(*, hour: str = "04:47") -> list[list[object]]:
    preview_formula = '=HYPERLINK("https://example.test/photo.jpg",IMAGE("https://example.test/photo.jpg"))'
    return [
        _pad("455 Ocean Parkway Tenant Record"),
        _pad("Automatic public tenant record."),
        _pad("At a glance"),
        _pad("Item", "Count / detail", "What this means"),
        _pad("Last refresh", f"2026-06-27 {hour} PM", "Updated automatically."),
        _pad("Incidents", 1, "Issues logged."),
        _pad("311 filings", 0, "Connected cases."),
        _pad("Most common issue type", "Elevator", "Most common category."),
        _pad("Latest update", "Both elevators working", "Newest update."),
        _pad(),
        _pad("Category snapshot"),
        _pad("Category", "Incidents", "311 filings", "Latest update", "Latest issue"),
        _pad("Elevator", 1, 0, f"2026-06-27 {hour} PM", "Both elevators working"),
        _pad(),
        _pad("Public update log"),
        _pad("Updated", "Issue", "Category", "311 follow-up", "Preview", "Open evidence", "Summary"),
        _pad(
            f"2026-06-27 {hour} PM",
            "Both elevators working",
            "Elevator",
            "",
            preview_formula,
            "https://example.test/photo.jpg",
            "Both elevators were reported working.",
        ),
        _pad(),
        _pad("311 case watch"),
        _pad("Case", "NYC status", "Complaint", "Related issue", "Submitted", "NYC lookup", "Notes"),
        _pad("", "No 311 cases yet", "", "", "", "", "No cases."),
    ]


def _live_tenant_log(
    expected: list[list[object]],
    *,
    formatted: list[list[object]] | None = None,
    formulas: list[list[object]] | None = None,
) -> LiveTenantLog:
    display = [list(row) for row in (formatted if formatted is not None else expected)]
    formula_values = [list(row) for row in (formulas if formulas is not None else expected)]
    # A formula cell displays its rendered result in FORMATTED_VALUE view.
    for row_index, row in enumerate(expected):
        for column_index, value in enumerate(row):
            if isinstance(value, str) and value.startswith("="):
                display[row_index][column_index] = "Image"
    return LiveTenantLog(
        formatted_values=display,
        formula_values=formula_values,
        metadata={
            "properties": {
                "title": "455 Ocean Parkway Tenant Record",
                "timeZone": "America/New_York",
            },
            "sheets": [],
        },
        tab_properties={
            "title": "Tenant Log",
            "sheetId": 1,
            "hidden": False,
            "gridProperties": {
                "rowCount": 1000,
                "columnCount": 26,
                "frozenRowCount": 1,
            },
        },
    )


def test_public_row_key_includes_follow_up_cell():
    base = PublicRow(
        updated="2026-06-04 04:08 PM",
        issue="Both elevators working",
        category="Elevator",
        follow_up="311-27731770 (In Progress)",
        summary="Both elevators were reported working.",
    )
    wrong_follow_up = PublicRow(
        updated="2026-06-04 04:08 PM",
        issue="Both elevators working",
        category="Elevator",
        follow_up="",
        summary="Both elevators were reported working.",
    )

    assert base.key != wrong_follow_up.key


def test_live_readback_requests_the_complete_tenant_log_columns(monkeypatch):
    calls = []

    class Request:
        def __init__(self, response):
            self.response = response

        def execute(self):
            return self.response

    class Values:
        def get(self, **kwargs):
            calls.append(("values.get", kwargs))
            if kwargs["valueRenderOption"] == "FORMATTED_VALUE":
                return Request({"values": [["Public update log"], ["Image"]]})
            return Request({"values": [["Public update log"], ["=IMAGE(\"url\")"]]})

    class Spreadsheets:
        def get(self, **kwargs):
            calls.append(("spreadsheets.get", kwargs))
            return Request(
                {
                    "properties": {"title": "Tenant Record", "timeZone": "America/New_York"},
                    "sheets": [
                        {
                            "properties": {
                                "title": "Tenant Log",
                                "sheetId": 1,
                                "hidden": False,
                                "gridProperties": {"rowCount": 1000, "columnCount": 26},
                            }
                        }
                    ],
                }
            )

        def values(self):
            return Values()

    class Service:
        def spreadsheets(self):
            return Spreadsheets()

    monkeypatch.setattr(public_audit.sheets_sync, "_service", lambda: Service())
    monkeypatch.setattr(public_audit.sheets_sync, "_public_sheet_id", lambda: "public-sheet")
    monkeypatch.setattr(public_audit.sheets_sync, "_public_updates_tab", lambda: "Tenant Log")

    live = public_audit._live_values()

    assert live.formatted_values == [["Public update log"], ["Image"]]
    assert live.formula_values == [["Public update log"], ['=IMAGE("url")']]
    assert live.tab_properties["title"] == "Tenant Log"
    assert calls[0][0] == "spreadsheets.get"
    assert calls[1] == (
        "values.get",
        {
            "spreadsheetId": "public-sheet",
            "range": "'Tenant Log'!A:ZZ",
            "majorDimension": "ROWS",
            "valueRenderOption": "FORMATTED_VALUE",
        },
    )
    assert calls[2][1]["range"] == "'Tenant Log'!A:ZZ"
    assert calls[2][1]["valueRenderOption"] == "FORMULA"


def test_public_row_covers_source_row_with_aggregate_follow_up():
    live = PublicRow(
        updated="2026-06-27 11:43 AM",
        issue="Both elevators",
        category="Elevator",
        follow_up="3 active 311 cases; latest 311-28007634 (In Progress)",
        summary="Both elevators were reported as out.",
    )
    source = PublicRow(
        updated="2026-06-27 11:43 AM",
        issue="Both elevators",
        category="Elevator",
        follow_up="311-28024527 (In Progress)",
        summary="Both elevators were reported as out.",
    )

    assert _public_row_covers_source_row(live, source)


def test_public_row_covers_source_row_merged_at_same_timestamp():
    live = PublicRow(
        updated="2026-06-11 7:10 AM",
        issue="South elevator / Elevator outage",
        category="Elevator",
        follow_up="",
        summary="South elevator was reported as out. Elevator outage was reported as out.",
    )
    source = PublicRow(
        updated="2026-06-11 7:10 AM",
        issue="Elevator outage",
        category="Elevator",
        follow_up="",
        summary="Elevator outage was reported as out.",
    )

    assert _public_row_covers_source_row(live, source)


def test_public_row_covers_source_row_with_mixed_categories_at_same_timestamp():
    live = PublicRow(
        updated="2026-08-19 7:11 AM",
        issue="Front desk phone number not working / Both elevators",
        category="Security / access / Elevator",
        follow_up="",
        summary=(
            "The number they gave me for the front desk does not go to the front desk. "
            "Both elevators were reported as down."
        ),
    )
    elevator_source = PublicRow(
        updated="2026-08-19 07:11 AM",
        issue="Both elevators",
        category="Elevator",
        follow_up="",
        summary="Both elevators were reported as down.",
    )
    security_source = PublicRow(
        updated="2026-08-19 07:11 AM",
        issue="Front desk phone number not working",
        category="Security / access",
        follow_up="",
        summary="The number they gave me for the front desk does not go to the front desk.",
    )

    assert _public_row_covers_source_row(live, elevator_source)
    assert _public_row_covers_source_row(live, security_source)


def test_both_elevators_working_row_covers_same_time_side_restore():
    live = PublicRow(
        updated="2026-07-27 12:19 PM",
        issue="Both elevators working",
        category="Elevator",
        follow_up="",
        summary="Both elevators were reported working.",
    )
    source = PublicRow(
        updated="2026-07-27 12:19 PM",
        issue="North elevator working",
        category="Elevator",
        follow_up="",
        summary="North elevator was reported working.",
    )

    assert _public_row_covers_source_row(live, source)


def test_later_canonical_alarm_row_covers_duplicate_source_with_minor_title_drift():
    live = PublicRow(
        updated="2026-04-10 09:15 AM",
        issue="Alarm rang on an unknown elevator",
        category="Elevator",
        follow_up="311-27091967 (In Progress)",
        summary="Elevator alarm was reported.",
    )
    source = PublicRow(
        updated="2026-04-09 08:06 PM",
        issue="Alarm rung on unknown elevator",
        category="Elevator",
        follow_up="",
        summary="Elevator alarm was reported.",
    )

    assert _public_row_covers_source_row(live, source)


def test_renderer_wording_and_evidence_suffix_cover_same_cloudy_water_update():
    live = PublicRow(
        updated="2025-10-30 12:55 PM",
        issue="Cloudy water alert possibly storm related",
        category="Leaks / water damage",
        follow_up="",
        summary="Current cloudy water alert (assuming storm related) video omitted.",
    )
    source = PublicRow(
        updated="2025-10-30 12:55 PM",
        issue="Cloudy water alert likely storm related",
        category="Leaks / water damage",
        follow_up="",
        summary="Current cloudy water alert (assuming storm related).",
    )

    assert _public_row_covers_source_row(live, source)


def test_similar_reports_at_different_times_are_not_treated_as_sheet_coverage():
    live = PublicRow(
        updated="2026-06-04 07:24 AM",
        issue="North elevator",
        category="Elevator",
        follow_up="",
        summary="North elevator was reported as out.",
    )
    source = PublicRow(
        updated="2026-06-03 09:52 PM",
        issue="North elevator",
        category="Elevator",
        follow_up="",
        summary="North elevator was reported as out.",
    )

    assert not _public_row_covers_source_row(live, source)


def test_source_public_rows_keep_matching_updates_outside_duplicate_window():
    first = SourcePublicRow(
        message_id="msg-first",
        epoch=1780537920,
        row=PublicRow(
            updated="2026-06-03 09:52 PM",
            issue="North elevator",
            category="Elevator",
            follow_up="",
            summary="North elevator was reported as out.",
        ),
    )
    second = SourcePublicRow(
        message_id="msg-second",
        epoch=1780572240,
        row=PublicRow(
            updated="2026-06-04 07:24 AM",
            issue="North elevator",
            category="Elevator",
            follow_up="",
            summary="North elevator was reported as out.",
        ),
    )

    assert _dedupe_source_rows([first, second]) == [second, first]


def test_source_public_rows_drop_matching_updates_inside_duplicate_window():
    first = SourcePublicRow(
        message_id="msg-first",
        epoch=1780537920,
        row=PublicRow(
            updated="2026-06-03 09:52 PM",
            issue="North elevator",
            category="Elevator",
            follow_up="",
            summary="North elevator was reported as out.",
        ),
    )
    second = SourcePublicRow(
        message_id="msg-second",
        epoch=1780538340,
        row=PublicRow(
            updated="2026-06-03 09:59 PM",
            issue="North elevator",
            category="Elevator",
            follow_up="",
            summary="North elevator was reported as out.",
        ),
    )

    assert _dedupe_source_rows([first, second]) == [second]


def test_quiet_audit_window_keeps_the_rendered_latest_update_as_the_truth(monkeypatch):
    values = _tenant_log_values()
    monkeypatch.setattr(public_audit, "_expected_values", lambda: values)
    monkeypatch.setattr(public_audit, "_live_values", lambda: _live_tenant_log(values))
    monkeypatch.setattr(public_audit, "_source_public_rows", lambda *, days: [])

    result = public_audit.run_audit(days=7, resync=False, retries=1, retry_sleep=0, limit=5)

    assert result["expected_recent_rows"] == 0
    assert result["live_recent_rows"] == 0
    assert result["expected_latest_update"] == "Both elevators working"
    assert result["latest_update_ok"] is True
    assert result["full_tenant_log_ok"] is True
    assert result["ok"] is True


def test_full_log_allows_google_to_drop_leading_zero_from_display_hour():
    expected = _tenant_log_values(hour="04:47")
    formatted = [
        [value.replace(" 04:47 PM", " 4:47 PM") if isinstance(value, str) else value for value in row]
        for row in expected
    ]
    result = public_audit._audit_logical_tenant_log(
        expected,
        _live_tenant_log(expected, formatted=formatted),
        limit=5,
    )

    assert result["source_live_mismatch_count"] == 0
    assert result["row_count_ok"] is True
    assert result["used_column_width_ok"] is True
    assert result["ok"] is True


def test_full_log_rejects_stale_extra_rows_and_bounds_details():
    expected = _tenant_log_values()
    formatted = [list(row) for row in expected]
    formatted.extend([_pad("stale row one"), _pad("stale row two"), _pad("stale row three")])
    result = public_audit._audit_logical_tenant_log(
        expected,
        _live_tenant_log(expected, formatted=formatted),
        limit=2,
    )

    assert result["ok"] is False
    assert result["row_count_ok"] is False
    assert result["live_used_row_count"] == len(expected) + 3
    assert result["stale_extra_row_cell_count"] == 3
    assert len(result["stale_extra_row_cells"]) == 2
    assert result["no_stale_extra_rows"] is False


def test_full_log_rejects_populated_cells_beyond_ten_managed_columns():
    expected = _tenant_log_values()
    formatted = [list(row) for row in expected]
    formatted[0].append("stale column K")
    result = public_audit._audit_logical_tenant_log(
        expected,
        _live_tenant_log(expected, formatted=formatted),
        limit=5,
    )

    assert result["ok"] is False
    assert result["live_used_column_width"] == 11
    assert result["used_column_width_ok"] is False
    assert result["beyond_managed_column_cell_count"] == 1
    assert result["beyond_managed_column_cells"][0]["column"] == 11
    assert result["no_populated_or_formula_cells_beyond_managed_range"] is False


def test_full_log_requires_exact_approved_formulas_and_rejects_stale_formulas():
    expected = _tenant_log_values()
    exact = public_audit._audit_logical_tenant_log(
        expected,
        _live_tenant_log(expected),
        limit=5,
    )
    assert exact["expected_formula_count"] == 1
    assert exact["exact_formula_match_count"] == 1
    assert exact["formula_cells_ok"] is True

    changed_formula_values = [list(row) for row in expected]
    changed_formula_values[16][4] = '=HYPERLINK("https://stale.test/photo.jpg",IMAGE("https://stale.test/photo.jpg"))'
    changed = public_audit._audit_logical_tenant_log(
        expected,
        _live_tenant_log(expected, formulas=changed_formula_values),
        limit=5,
    )
    assert changed["ok"] is False
    assert changed["exact_formula_match_count"] == 0
    assert changed["source_live_mismatches"][0]["reason"] == (
        "formula differs from the exact renderer formula"
    )

    unexpected_formula_values = [list(row) for row in expected]
    unexpected_formula_values[4][9] = '=IMAGE("https://stale.test/unmanaged.jpg")'
    unexpected = public_audit._audit_logical_tenant_log(
        expected,
        _live_tenant_log(expected, formulas=unexpected_formula_values),
        limit=5,
    )
    assert unexpected["ok"] is False
    assert unexpected["unexpected_formula_cell_count"] == 1
    assert unexpected["unexpected_formula_cells"][0]["column"] == 10


def test_full_log_rejects_a_missing_required_section_and_header():
    expected = _tenant_log_values()
    formatted = [list(row) for row in expected]
    formulas = [list(row) for row in expected]
    formatted[18][0] = ""
    formulas[18][0] = ""
    formatted[19][0] = ""
    formulas[19][0] = ""
    result = public_audit._audit_logical_tenant_log(
        expected,
        _live_tenant_log(expected, formatted=formatted, formulas=formulas),
        limit=5,
    )

    required = result["required_rows"]
    assert result["ok"] is False
    assert required["required_section_rows_ok"] is False
    assert required["required_header_rows_ok"] is False
    assert next(row for row in required["rows"] if row["name"] == "311 case watch")["ok"] is False
    assert next(row for row in required["rows"] if row["name"] == "311 case watch header")["ok"] is False


def test_full_log_cell_mismatch_details_are_bounded():
    expected = _tenant_log_values()
    formatted = [list(row) for row in expected]
    for column_index in range(7):
        formatted[16][column_index] = f"stale-{column_index}"
    result = public_audit._audit_logical_tenant_log(
        expected,
        _live_tenant_log(expected, formatted=formatted),
        limit=2,
    )

    assert result["source_live_mismatch_count"] == 6
    assert len(result["source_live_mismatches"]) == 2
