import pytest

from packages.sheets import schema, sync
from scripts import init_sheet
from scripts import audit_public_watchdog_tabs as audit
from scripts import audit_public_tenant_log as tenant_audit


def _row(tab, **values):
    return [values.get(header, "") for header in schema.OPERATOR_HEADERS[tab]]


def test_public_log_reduces_only_empty_columns():
    content = ["time", "issue", "category", "311", "image", "evidence", "summary"]
    assert schema.public_log_values([content + ["", "", ""]]) == [content]
    with pytest.raises(ValueError, match="beyond"):
        schema.public_log_values([content + ["do not discard"]])


def test_record_projection_retains_context_dates_identifiers_and_source():
    raw = _row("PublicRecords", source_system="dob_now", record_type="elevator_permit_application",
               record_key="record-1", status="LAST OBSERVED: Issued",
               status_detail="Absent from latest source check; absence does not prove resolution. " + "detail " * 150,
               filed_at="2026-01-01", approved_at="2026-02-01", permit_issued_at="2026-03-01",
               inspection_date="2026-04-01", expires_at="2027-03-01", machine_verified_at="2026-03-02",
               human_verified_at="2026-03-03", verification_status="accepted",
               verification_summary="Exact building and device match.", bbl="123", bin="456",
               job_number="J1", permit_number="P1", device_number="D1", source_url="https://example.org/record-1")
    projected = schema.public_record_values(raw)
    assert len(projected) == 7
    assert projected[1] == "Last observed: Issued"
    assert projected[2] == raw[7]
    text = "\n".join(str(value) for value in projected)
    for value in ("2026-01-01", "2026-02-01", "2026-03-01", "2026-04-01", "2027-03-01", "2026-03-02", "2026-03-03", "record-1", "123", "456", "J1", "P1", "D1", "https://example.org/record-1"):
        assert value in text
    assert "Matched to official records" in projected[4]
    assert "Exact building and device match." not in text
    assert raw[5] == "Exact building and device match."


def test_public_records_use_reader_labels_but_keep_review_and_absence_caveats():
    raw = _row("PublicRecords", source_system="dob_now_elevator_applications",
               record_type="elevator_permit_application", status="permit_issued",
               verification_status="official_elevator_match",
               verification_summary="official configured NYC/DOB/Open Data source; exact BIN; stable public record key",
               status_detail="Last seen previously. Missing from the latest source; absence does not prove resolution.",
               needs_human_verification="YES - review needed")
    projected = schema.public_record_values(raw)
    assert projected[0] == "Elevator permit application\nDOB NOW"
    assert projected[1] == "Permit issued"
    assert projected[4] == "Matched to building elevators\nReview requested"
    assert projected[2] == raw[7]
    assert "exact BIN" not in str(projected)
    assert "stable public record key" in raw[5]


def test_tenant_summary_layout_uses_only_trailing_blank_cells(monkeypatch):
    requests = []

    class Request:
        def execute(self):
            return {}

    class Service:
        def spreadsheets(self):
            return self

        def batchUpdate(self, **kwargs):
            requests.extend(kwargs["body"]["requests"])
            return Request()

    monkeypatch.setattr(sync, "_sheet_title_to_id_map", lambda *args: {"Tenant Log": 1})
    sync._apply_tab_layout(Service(), "public", "Tenant Log", row_count=30, column_count=7,
        layout="public_updates", layout_meta={"section_rows": [3, 11, 16, 25], "header_rows": [4, 12, 17, 26]})
    merges = [request["mergeCells"]["range"] for request in requests if "mergeCells" in request]
    summary = [cell for cell in merges if cell["startColumnIndex"] in {2, 4}]
    assert [(cell["startRowIndex"], cell["startColumnIndex"], cell["endColumnIndex"]) for cell in summary] == (
        [(row, 2, 7) for row in range(3, 9)] + [(row, 4, 7) for row in range(11, 14)]
    )
    assert not any(cell["startRowIndex"] >= 16 for cell in summary)


def test_watchdog_consolidation_preserves_timeline_photos_and_action_draft():
    elevator = [schema.OPERATOR_HEADERS["ElevatorWatch"], ["Permit", "Issued", "Official", "Automatic", "2026-03-01", "", "source"]]
    project = [schema.OPERATOR_HEADERS["ProjectStatus"], ["milestone", "replacement", "claimed", "Claimed end 2027, north elevator", "management_pdf", "2026-02-01"]]
    checks = [schema.OPERATOR_HEADERS["WatchdogChecks"], _row("WatchdogChecks", check_type="lobby_notice", status="seen", checked_at="2026-03-02", checked_by="Example Person", photo_url="https://example.org/photo", source_url="https://example.org/notice", notes="Private contact information")]
    actions = [schema.OPERATOR_HEADERS["ActionQueue"], _row("ActionQueue", title="Request timing", status="open", severity="watch", detail="Ask for the schedule", due_at="2026-03-05", owner_role="tenant_association", draft_message="Please confirm the work dates.", source_record_id="record-1", related_incident_id="incident-1")]
    projected = schema.public_watchdog_values(elevator, project, checks, actions)
    assert projected[1] == elevator[1]
    assert all(len(row) == 7 for row in projected)
    text = str(projected)
    for value in ("Claimed end 2027", "north elevator", "2026-02-01", "2026-03-02", "Resident report", "https://example.org/photo", "https://example.org/notice", "2026-03-05", "Tenant association", "Please confirm the work dates."):
        assert value in text
    for private in ("Example Person", "Private contact information", "record-1", "incident-1", "tenant_association"):
        assert private not in text
    # Projection never strips the full private audit history.
    assert checks[1][3] == "Example Person"
    assert checks[1][6] == "Private contact information"
    assert actions[1][7:10] == ["record-1", "incident-1", "Please confirm the work dates."]
    # Past evidence remains in the detail, never in the automatic-check column.
    assert all(row[4] == "" for row in projected[2:])


def test_digest_projection_preserves_historical_snapshot():
    raw = _row("WeeklyDigest", period_start="2026-01-01", period_end="2026-01-08", tenant_update="Then-current summary", tenant_action_needed="Original action", generated_at="2026-01-09", used_llm="YES")
    assert schema.public_digest_values(raw) == ["2026-01-01", "2026-01-08", "Then-current summary", "Original action", "2026-01-09"]
    assert "used_llm" not in schema.PUBLIC_WATCHDOG_HEADERS["WeeklyDigest"]


def test_public_and_operator_contracts_are_shared_with_initializer_and_audit():
    assert list(init_sheet.public_tabs_to_initialize()) == ["Tenant Log", "ElevatorWatch", "PublicRecords", "WeeklyDigest"]
    assert {spec.logical_name: list(spec.headers) for spec in audit._resolved_tab_specs()} == schema.PUBLIC_WATCHDOG_HEADERS
    assert {spec.logical_name: list(spec.headers) for spec in audit._resolved_tab_specs("operator")} == schema.OPERATOR_WATCHDOG_HEADERS
    assert len(schema.OPERATOR_HEADERS["PublicRecords"]) == 23
    assert len(schema.PUBLIC_WATCHDOG_HEADERS["PublicRecords"]) == 7


def test_audit_captures_each_audience_without_mixing_same_named_tables(client):
    public = audit._capture_expected_values("public", audit._resolved_tab_specs(), audience="public")
    operator = audit._capture_expected_values("operator", audit._resolved_tab_specs("operator"), audience="operator")
    assert set(public) == set(schema.PUBLIC_WATCHDOG_HEADERS)
    assert set(operator) == set(schema.OPERATOR_WATCHDOG_HEADERS)
    assert public["PublicRecords"].values[0] == schema.PUBLIC_WATCHDOG_HEADERS["PublicRecords"]
    assert operator["PublicRecords"].values[0] == schema.OPERATOR_HEADERS["PublicRecords"]
    assert len(public["PublicRecords"].values[0]) == 7
    assert len(operator["PublicRecords"].values[0]) == 23


def test_tenant_audit_uses_existing_schema_and_rolls_back_renderer_commits(client, monkeypatch):
    from packages import db

    def forbidden_init():
        raise AssertionError("Read-only audit must not initialize or migrate the database")

    monkeypatch.setattr(db, "init_db", forbidden_init)
    monkeypatch.setattr(sync, "_public_sheet_id", lambda: "public")
    assert tenant_audit._source_public_rows(days=7) == []
    assert all(len(row) == 7 for row in tenant_audit._expected_values())
    original_session = sync.get_session

    def renderer_with_commit():
        with sync.get_session() as session:
            session.add(db.Incident(incident_id="audit-must-not-save", title="Temporary", category="other", status="open"))
            session.commit()
        sync._service().spreadsheets().values().update(
            spreadsheetId="public", range=f"{sync._public_updates_tab()}!A1", valueInputOption="USER_ENTERED",
            body={"values": [["preview"]]},
        ).execute()

    monkeypatch.setattr(sync, "sync_public_updates_to_sheets", renderer_with_commit)
    assert tenant_audit._expected_values() == [["preview"]]
    assert sync.get_session is original_session
    with db.SessionLocal() as session:
        assert session.get(db.Incident, "audit-must-not-save") is None


def test_public_initializer_loads_env_and_rejects_operator_target_before_service(monkeypatch):
    monkeypatch.delenv("GOOGLE_SHEETS_SPREADSHEET_ID", raising=False)
    monkeypatch.setattr(init_sheet.sys, "argv", ["init_sheet.py", "--public-tabs", "--spreadsheet-id", "operator"])
    monkeypatch.setattr(init_sheet, "load_local_env_file", lambda path: monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "operator"))

    def forbidden_service():
        raise AssertionError("Invalid destinations must be rejected before API access")

    monkeypatch.setattr(init_sheet, "service", forbidden_service)
    with pytest.raises(SystemExit) as error:
        init_sheet.main()
    assert error.value.code == 2


def test_workbook_destinations_fail_closed(monkeypatch):
    monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "operator")
    monkeypatch.delenv("GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID", raising=False)
    with pytest.raises(RuntimeError, match="dedicated"):
        sync._public_sheet_id()
    monkeypatch.setenv("GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID", "operator")
    with pytest.raises(RuntimeError, match="distinct"):
        sync._public_sheet_id()
    with pytest.raises(RuntimeError, match="distinct"):
        sync._watchdog_sheet_id()
    monkeypatch.setenv("GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID", "public")
    assert sync._public_sheet_id() == "public"
    assert sync._watchdog_sheet_id() == "operator"


def test_sync_keeps_complete_operator_tables_and_only_three_public_watchdog_views(monkeypatch):
    monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "operator")
    monkeypatch.setenv("GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID", "public")
    builders = {
        "ElevatorWatch": "_elevator_watch_public_view_values", "ProjectStatus": "_project_status_values",
        "PublicRecords": "_public_records_values", "WatchdogChecks": "_watchdog_checks_values",
        "ActionQueue": "_watchdog_actions_values", "WeeklyDigest": "_weekly_digest_values",
    }
    original = {name: [headers, _row(name)] for name, headers in schema.OPERATOR_WATCHDOG_HEADERS.items()}
    for name, function in builders.items():
        monkeypatch.setattr(sync, function, lambda name=name: original[name])
    writes = []
    monkeypatch.setattr(sync, "_write_watchdog_view", lambda sheet, name, env, rows, **kw: writes.append((sheet, name, rows)))
    sync.sync_replacement_watchdog_to_sheets()
    operator = {name: rows for sheet, name, rows in writes if sheet == "operator"}
    public = {name: rows for sheet, name, rows in writes if sheet == "public"}
    assert operator == original
    assert set(public) == set(schema.PUBLIC_WATCHDOG_HEADERS)
    assert len(public["PublicRecords"]) == len(original["PublicRecords"])
    assert len(public["WeeklyDigest"]) == len(original["WeeklyDigest"])


def test_operator_failure_does_not_suppress_public_or_later_watchdog_tables(monkeypatch):
    monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "operator")
    monkeypatch.setenv("GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID", "public")
    monkeypatch.setattr(sync, "_public_records_values", lambda: [schema.OPERATOR_HEADERS["PublicRecords"]])
    calls = []

    def write(sheet, name, env, rows, **kwargs):
        calls.append((sheet, name))
        if sheet == "operator":
            raise OSError("private workbook unavailable")

    monkeypatch.setattr(sync, "_write_watchdog_view", write)
    with pytest.raises(RuntimeError, match="operator PublicRecords"):
        sync.sync_public_records_to_sheets()
    assert calls == [("operator", "PublicRecords"), ("public", "PublicRecords")]

    attempted = []
    functions = ["sync_elevator_watch_public_view_to_sheets", "sync_project_status_to_sheets",
                 "sync_public_records_to_sheets", "sync_watchdog_checks_to_sheets",
                 "sync_watchdog_actions_to_sheets", "sync_weekly_digest_to_sheets"]
    for name in functions:
        def run(name=name):
            attempted.append(name)
            if name == functions[0]:
                raise OSError("one view unavailable")
        monkeypatch.setattr(sync, name, run)
    with pytest.raises(RuntimeError, match="one view unavailable"):
        sync.sync_replacement_watchdog_to_sheets()
    assert attempted == functions
