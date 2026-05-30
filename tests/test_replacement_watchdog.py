import os

from packages.db import AccessNeedPrivate, Incident, PublicRecordWatch, ServiceRequestCase, WatchdogAction, get_session
from packages.project_watch.rules import evaluate_project_rules
from packages.public_records import sync as public_record_sync
from packages.public_records.sync import sync_public_records, upsert_public_record
from packages.public_records.verification import apply_machine_verification
from packages.sheets import sync as sheets_sync
from scripts import init_sheet


def auth_headers():
    return {'Authorization': 'Bearer test-token'}


class _FakeRequest:
    def __init__(self, calls, kind, kwargs, response=None):
        self.calls = calls
        self.kind = kind
        self.kwargs = kwargs
        self.response = response or {}

    def execute(self):
        self.calls.append((self.kind, self.kwargs))
        return self.response


class _FakeValues:
    def __init__(self, calls):
        self.calls = calls

    def clear(self, **kwargs):
        return _FakeRequest(self.calls, "clear", kwargs)

    def update(self, **kwargs):
        return _FakeRequest(self.calls, "update", kwargs)


class _FakeSpreadsheets:
    def __init__(self, calls):
        self.calls = calls

    def values(self):
        return _FakeValues(self.calls)

    def get(self, **kwargs):
        return _FakeRequest(
            self.calls,
            "get",
            kwargs,
            response={
                "sheets": [
                    {"properties": {"title": "ElevatorWatch", "sheetId": 9}},
                    {"properties": {"title": "ProjectStatus", "sheetId": 10}},
                    {"properties": {"title": "PublicRecords", "sheetId": 11}},
                    {"properties": {"title": "WatchdogChecks", "sheetId": 12}},
                    {"properties": {"title": "ActionQueue", "sheetId": 13}},
                    {"properties": {"title": "WeeklyDigest", "sheetId": 14}},
                ]
            },
        )

    def batchUpdate(self, **kwargs):
        return _FakeRequest(self.calls, "batchUpdate", kwargs)


class _FakeService:
    def __init__(self):
        self.calls = []

    def spreadsheets(self):
        return _FakeSpreadsheets(self.calls)


def _elevator_application_row(status="Filed"):
    return {
        "job_filing_number": "BTEST-I1",
        "job_number": "BTEST",
        "filing_date": "2026-05-01T00:00:00.000",
        "filing_type": "New Filing",
        "elevatordevicetype": "Elevator",
        "filing_status": status,
        "filingstatus_or_filingincludes": "Alteration/Replacement",
        "bin": "3126839",
        "bbl": "3053900074",
        "borough": "BROOKLYN",
        "block": "5390",
        "lot": "74",
        "house_number": "455",
        "street_name": "OCEAN PARKWAY",
        "descriptionofwork": "Full elevator replacement filing",
    }


def test_sheet_initializer_includes_watchdog_tabs(monkeypatch):
    monkeypatch.delenv("ENABLE_PRIVATE_ACCESS_NEEDS_SHEET", raising=False)
    tabs = init_sheet.tabs_to_initialize()
    for tab in ["ElevatorWatch", "ProjectStatus", "PublicRecords", "WatchdogChecks", "ActionQueue", "WeeklyDigest"]:
        assert tab in tabs
    assert "management_followup_draft" not in tabs["WeeklyDigest"]
    assert "AccessNeeds_Private" not in tabs

    monkeypatch.setenv("ENABLE_PRIVATE_ACCESS_NEEDS_SHEET", "1")
    assert "AccessNeeds_Private" in init_sheet.tabs_to_initialize()


def test_public_record_normalization_dedupes_unchanged_rows(client):
    with get_session() as session:
        first, first_state = upsert_public_record(session, "dob_now_elevator_applications", _elevator_application_row())
        second, second_state = upsert_public_record(session, "dob_now_elevator_applications", _elevator_application_row())
        session.commit()

        assert first.id == second.id
        assert first_state == "created"
        assert second_state == "unchanged"
        assert session.query(PublicRecordWatch).count() == 1


def test_changed_public_record_creates_watchdog_action(client):
    with get_session() as session:
        upsert_public_record(session, "dob_now_elevator_applications", _elevator_application_row("Filed"))
        changed, changed_state = upsert_public_record(session, "dob_now_elevator_applications", _elevator_application_row("Approved"))
        session.commit()

        assert changed_state == "changed"
        assert changed.status == "Approved"
        actions = session.query(WatchdogAction).filter_by(action_type="changed_public_record").all()
        assert len(actions) == 1
        assert actions[0].source_record_id == changed.id


def test_first_public_record_sync_baselines_historical_rows(client, monkeypatch):
    monkeypatch.setattr(
        public_record_sync,
        "fetch_public_record_rows",
        lambda: [("dob_now_elevator_applications", _elevator_application_row(), "https://example.test/source")],
    )

    with get_session() as session:
        result = sync_public_records(session)
        session.commit()

        assert result["created"] == 1
        assert result["baseline_created"] == 1
        assert session.query(PublicRecordWatch).count() == 1
        assert session.query(WatchdogAction).filter_by(action_type="new_record_needs_verification").count() == 0


def test_public_record_sync_continues_after_partial_source_failure(client, monkeypatch):
    def flaky_fetch(source, params, limit=500):
        if source.key == "dob_complaints":
            raise RuntimeError("temporary Socrata outage")
        if source.key == "dob_now_elevator_applications":
            return [_elevator_application_row()]
        return []

    monkeypatch.setattr(public_record_sync, "fetch_rows", flaky_fetch)

    with get_session() as session:
        result = sync_public_records(session)
        session.commit()

        assert result["created"] == 1
        assert result["source_errors"] == 1
        assert session.query(PublicRecordWatch).count() == 1
        action = session.query(WatchdogAction).filter_by(action_type="public_record_source_error").one()
        assert action.status == "open"
        assert "dob_complaints" in (action.detail or "")

    monkeypatch.setattr(public_record_sync, "fetch_rows", lambda source, params, limit=500: [])
    with get_session() as session:
        result = sync_public_records(session)
        session.commit()

        assert result["source_errors"] == 0
        action = session.query(WatchdogAction).filter_by(action_type="public_record_source_error").one()
        assert action.status == "completed"


def test_replacement_watchdog_generates_weekly_digest_automatically(client, monkeypatch):
    monkeypatch.setattr(public_record_sync, "fetch_rows", lambda source, params, limit=500: [])

    with get_session() as session:
        first = public_record_sync.sync_replacement_watchdog(session)
        second = public_record_sync.sync_replacement_watchdog(session)
        session.commit()

        assert first["weekly_digest_created"] == 1
        assert second["weekly_digest_created"] == 0
        assert session.query(public_record_sync.WeeklyDigest).count() == 1


def test_weekly_digest_sheet_keeps_management_draft_internal(client, monkeypatch):
    with get_session() as session:
        session.add(public_record_sync.WeeklyDigest(
            period_start="2026-05-22T00:00:00Z",
            period_end="2026-05-29T00:00:00Z",
            public_summary="Tenant-safe public summary.",
            management_followup_draft=(
                "Please provide the current DOB filing number, permit status, expected start date, "
                "and posting plan for the 455 Ocean Parkway elevator replacement."
            ),
            tenant_update_draft="Residents do not need to search DOB manually.",
            generated_at="2026-05-29T00:00:00Z",
            used_llm=False,
        ))
        session.commit()

    fake = _FakeService()
    monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "sheet-123")
    monkeypatch.setattr(sheets_sync, "_service", lambda: fake)
    sheets_sync.sync_weekly_digest_to_sheets()
    body_text = str([kwargs.get("body") for kind, kwargs in fake.calls if kind == "update"])
    assert "tenant_update" in body_text
    assert "No tenant action needed" in body_text
    assert "Residents do not need to search DOB manually." in body_text
    assert "management_followup_draft" not in body_text
    assert "Please provide the current DOB filing number" not in body_text


def test_unverified_public_records_are_not_presented_as_verified(client, monkeypatch):
    with get_session() as session:
        record, _ = upsert_public_record(session, "dob_now_elevator_applications", _elevator_application_row())
        session.commit()
        record_id = record.id

    response = client.get("/api/project/records", headers=auth_headers())
    assert response.status_code == 200, response.text
    assert all(row["id"] != record_id for row in response.json()["records"])

    fake = _FakeService()
    monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "sheet-123")
    monkeypatch.setattr(sheets_sync, "_service", lambda: fake)
    sheets_sync.sync_public_records_to_sheets()
    body_text = str([kwargs.get("body") for kind, kwargs in fake.calls if kind == "update"])
    assert "YES - review needed" not in body_text


def test_tenant_public_records_exclude_unrelated_trusted_building_rows(client):
    with get_session() as session:
        record, _ = upsert_public_record(session, "hpd_violations", {
            "violationid": "HPD-TRUSTED-1",
            "buildingid": "123",
            "bin": "3126839",
            "bbl": "3053900074",
            "currentstatus": "Open",
            "violationstatus": "Open",
            "novdescription": "Repair plaster in apartment.",
            "inspectiondate": "2026-05-01T00:00:00.000",
        })
        apply_machine_verification(session)
        session.commit()
        record_id = record.id

    response = client.get("/api/project/records", headers=auth_headers())
    assert response.status_code == 200, response.text
    assert all(row["id"] != record_id for row in response.json()["records"])


def test_machine_verification_accepts_official_elevator_matches_and_closes_verification_actions(client):
    with get_session() as session:
        application, _ = upsert_public_record(session, "dob_now_elevator_applications", _elevator_application_row())
        device, _ = upsert_public_record(session, "dob_now_elevator_device_details", {
            "job_filing_number": "BTEST-I1",
            "device_id": "3PTEST",
            "bis_nyc_device_id": "3PTEST",
            "device_type": "Passenger Elevator",
            "device_status": "Active",
            "device_job_description": "Replacement",
        })
        session.add(WatchdogAction(
            action_type="new_record_needs_verification",
            severity="info",
            title="Verify new official record",
            detail="Should be auto-closed after official source match.",
            status="open",
            source_record_id=application.id,
            created_at="2026-05-01T00:00:00Z",
            updated_at="2026-05-01T00:00:00Z",
        ))
        session.add(WatchdogAction(
            action_type="changed_public_record",
            severity="watch",
            title="Review changed official record",
            detail="Should also be auto-closed after official source match.",
            status="open",
            source_record_id=application.id,
            created_at="2026-05-01T00:00:00Z",
            updated_at="2026-05-01T00:00:00Z",
        ))
        result = apply_machine_verification(session)
        session.commit()

        assert result["machine_verified"] == 2
        assert application.needs_human_verification is False
        assert application.machine_verification_status == "official_corroborated"
        assert application.machine_verified_at
        assert application.human_verified_at is None
        assert device.needs_human_verification is False
        actions = session.query(WatchdogAction).filter_by(source_record_id=application.id).all()
        assert actions
        assert {action.status for action in actions} == {"auto_verified"}
        assert result["verification_actions_auto_closed"] >= 2


def test_machine_verification_keeps_weak_matches_in_review(client):
    with get_session() as session:
        device, _ = upsert_public_record(session, "dob_now_elevator_device_details", {
            "job_filing_number": "UNLINKED-I1",
            "device_id": "3PUNKNOWN",
            "bis_nyc_device_id": "3PUNKNOWN",
            "device_type": "Passenger Elevator",
            "device_status": "Active",
        })
        apply_machine_verification(session)
        session.commit()

        assert device.needs_human_verification is True
        assert device.visible_public is False
        assert device.machine_verification_status == "needs_review"
        assert (device.machine_confidence or 0) < 80


def test_watchdog_action_sheet_only_shows_active_actions(client, monkeypatch):
    with get_session() as session:
        session.add(WatchdogAction(
            action_type="new_record_needs_verification",
            severity="info",
            title="Historical baseline action",
            detail="Should not appear in active queue.",
            status="baseline",
            created_at="2026-05-01T00:00:00Z",
            updated_at="2026-05-01T00:00:00Z",
        ))
        session.add(WatchdogAction(
            action_type="permit_issued",
            severity="info",
            title="Resident photo needed: lobby/start-date notice",
            detail="Should appear in active queue.",
            status="open",
            owner_role="resident",
            created_at="2026-05-02T00:00:00Z",
            updated_at="2026-05-02T00:00:00Z",
        ))
        session.commit()

    fake = _FakeService()
    monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "sheet-123")
    monkeypatch.setattr(sheets_sync, "_service", lambda: fake)
    sheets_sync.sync_watchdog_actions_to_sheets()
    body_text = str([kwargs.get("body") for kind, kwargs in fake.calls if kind == "update"])
    assert "Resident photo needed: lobby/start-date notice" in body_text
    assert "Historical baseline action" not in body_text

    response = client.get("/api/project/actions", headers=auth_headers())
    assert response.status_code == 200, response.text
    api_text = str(response.json())
    assert "Resident photo needed: lobby/start-date notice" in api_text
    assert "Historical baseline action" not in api_text


def test_api_project_returns_three_streams(client):
    with get_session() as session:
        upsert_public_record(session, "dob_now_elevator_applications", _elevator_application_row())
        apply_machine_verification(session)
        session.add(Incident(
            incident_id="tenant-elevator-1",
            category="elevator",
            asset="elevator_north",
            severity=4,
            status="open",
            start_ts="2026-05-01T00:00:00Z",
            start_ts_epoch=1777593600,
            last_ts_epoch=1777593600,
            title="North elevator out",
            summary="Tenant observed north elevator outage.",
            proof_refs="",
            report_count=1,
            witness_count=1,
            confidence=80,
            needs_review=False,
            updated_at="2026-05-01T00:00:00Z",
        ))
        session.commit()

    response = client.get("/api/project", headers=auth_headers())
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["management_claims"]["project"]["title"] == "455 Ocean Parkway elevator replacement"
    assert payload["official_records"]
    assert payload["tenant_reality"]["elevator_incidents"]
    assert any(row["topic"] == "Full elevator replacement permit" for row in payload["public_view"])


def test_elevator_watch_public_view_keeps_manual_work_to_physical_checks(client, monkeypatch):
    with get_session() as session:
        upsert_public_record(session, "dob_now_elevator_applications", {
            **_elevator_application_row("Signed Off"),
            "job_filing_number": "BOLD-I1",
            "descriptionofwork": "Provide New Door Lock Monitoring System.",
            "signedoff_date": "2021-07-21T00:00:00.000",
            "permit_expiration_date": "2021-11-08T00:00:00.000",
        })
        upsert_public_record(session, "dob_ecb_violations", {
            "isn_dob_bis_extract": "1",
            "ecb_violation_number": "39188002Y",
            "ecb_violation_status": "ACTIVE",
            "bin": "3126839",
            "boro": "3",
            "block": "05390",
            "lot": "0074",
            "issue_date": "20260511",
            "severity": "CLASS - 2",
            "violation_type": "Elevators",
            "violation_description": "ELEVATOR COMPANY MISSING CAT1 TAGS.",
            "hearing_status": "PENDING",
        })
        evaluate_project_rules(session)
        session.flush()
        assert session.query(WatchdogAction).filter_by(action_type="permit_issued", status="open").count() == 0
        assert session.query(WatchdogAction).filter_by(action_type="active_official_elevator_record", status="open").count() == 0
        management_action = session.query(WatchdogAction).filter_by(action_type="no_public_filing_after_30_days", status="open").one()
        assert management_action.owner_role == "tenant_association"
        session.commit()

    response = client.get("/api/project", headers=auth_headers())
    assert response.status_code == 200, response.text
    public_view = {row["topic"]: row for row in response.json()["public_view"]}
    assert public_view["Full elevator replacement permit"]["answer"].startswith("No current full-replacement permit")
    assert "No resident DOB search needed" in public_view["Full elevator replacement permit"]["human_needed"]
    assert public_view["Active official elevator violation"]["answer"].startswith("Yes: official record 39188002Y")
    assert "2026-05-11" in public_view["Active official elevator violation"]["answer"]
    assert "No DOB lookup needed" in public_view["Active official elevator violation"]["human_needed"]
    assert public_view["Lobby posting / start-date notice"]["answer"] == "No hallway check is needed yet for replacement work."

    fake = _FakeService()
    monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "sheet-123")
    monkeypatch.setattr(sheets_sync, "_service", lambda: fake)
    sheets_sync.sync_elevator_watch_public_view_to_sheets()
    body_text = str([kwargs.get("body") for kind, kwargs in fake.calls if kind == "update"])
    assert "What people need to know" in body_text
    assert "No current full-replacement permit" in body_text
    assert "No resident DOB search needed" in body_text


def test_tenant_queue_shows_management_request_when_no_current_replacement_filing(client, monkeypatch):
    with get_session() as session:
        upsert_public_record(session, "dob_now_elevator_applications", {
            **_elevator_application_row("Signed Off"),
            "job_filing_number": "BOLD-I1",
            "descriptionofwork": "Provide New Door Lock Monitoring System.",
            "signedoff_date": "2021-07-21T00:00:00.000",
            "permit_expiration_date": "2021-11-08T00:00:00.000",
        })
        evaluate_project_rules(session)
        session.commit()

    response = client.get("/api/project/actions", headers=auth_headers())
    assert response.status_code == 200, response.text
    actions = response.json()["actions"]
    assert any(row["action_type"] == "no_public_filing_after_30_days" for row in actions)
    action = next(row for row in actions if row["action_type"] == "no_public_filing_after_30_days")
    assert action["owner_role"] == "tenant_association"
    assert "DOB filing has been submitted" in action["title"]
    assert "official public records" in action["draft_message"]
    assert "If not, please share the expected filing date" in action["draft_message"]

    fake = _FakeService()
    monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "sheet-123")
    monkeypatch.setattr(sheets_sync, "_service", lambda: fake)
    sheets_sync.sync_watchdog_actions_to_sheets()
    body_text = str([kwargs.get("body") for kind, kwargs in fake.calls if kind == "update"])
    assert "Ask management whether the DOB filing has been submitted" in body_text
    assert "If not, please share the expected filing date" in body_text
    assert "No tenant action needed" not in body_text


def test_management_request_closes_when_current_replacement_filing_exists(client):
    with get_session() as session:
        session.add(WatchdogAction(
            action_type="no_public_filing_after_30_days",
            severity="watch",
            title="Ask management whether the DOB filing has been submitted",
            detail="Existing management request should close when a matching filing appears.",
            status="open",
            owner_role="tenant_association",
            created_at="2026-05-01T00:00:00Z",
            updated_at="2026-05-01T00:00:00Z",
        ))
        upsert_public_record(session, "dob_now_elevator_applications", _elevator_application_row("Filed"))
        evaluate_project_rules(session)
        session.commit()

        action = session.query(WatchdogAction).filter_by(action_type="no_public_filing_after_30_days").one()
        assert action.status == "completed"


def test_existing_311_case_suppresses_tenant_visible_elevator_action(client):
    with get_session() as session:
        session.add(Incident(
            incident_id="tenant-elevator-with-case",
            category="elevator",
            asset="elevator_north",
            severity=4,
            status="open",
            start_ts="2026-05-01T00:00:00Z",
            start_ts_epoch=1777593600,
            last_ts_epoch=1777593600,
            title="North elevator out",
            summary="Tenant observed north elevator outage.",
            proof_refs="",
            report_count=1,
            witness_count=1,
            confidence=80,
            needs_review=False,
            updated_at="2026-05-01T00:00:00Z",
        ))
        session.add(ServiceRequestCase(
            service_request_number="311-27654875",
            incident_id="tenant-elevator-with-case",
            source="portal_playwright",
            complaint_type="Elevator",
            status="In Progress",
            submitted_at="2026-05-02T00:00:00Z",
        ))
        session.add(WatchdogAction(
            action_type="active_phase_one_elevator_down",
            severity="yellow",
            title="One elevator down during replacement watch",
            detail="Existing action should close because the system already has a 311 case.",
            status="open",
            owner_role="operator",
            related_incident_id="tenant-elevator-with-case",
            created_at="2026-05-02T00:00:00Z",
            updated_at="2026-05-02T00:00:00Z",
        ))
        evaluate_project_rules(session)
        session.commit()

        action = session.query(WatchdogAction).filter_by(action_type="active_phase_one_elevator_down").one()
        assert action.status == "completed"


def test_report_still_ingests_elevator_outage_and_restore_events(client):
    out = client.post("/report/submit", data={"reporter": "16F", "kind": "elevator_out", "asset": "elevator_north", "note": ""})
    restore = client.post("/report/submit", data={"reporter": "16F", "kind": "elevator_restore", "asset": "elevator_north", "note": ""})
    assert out.status_code == 200
    assert restore.status_code == 200

    with get_session() as session:
        incidents = session.query(Incident).filter_by(category="elevator", asset="elevator_north").all()
        assert incidents
        assert any("working" in (row.summary or "").lower() or row.status == "closed" for row in incidents)


def test_access_need_private_excluded_from_public_endpoints_and_sheet_sync(client, monkeypatch):
    with get_session() as session:
        session.add(AccessNeedPrivate(
            apartment_or_contact_hash="hash-apt-16f",
            need_type="mobility_access_blocked",
            request_text="Wheelchair user needs notice before shutdown",
            status="open",
            created_at="2026-05-01T00:00:00Z",
            updated_at="2026-05-01T00:00:00Z",
        ))
        session.commit()

    response = client.get("/api/project", headers=auth_headers())
    assert response.status_code == 200, response.text
    payload_text = str(response.json())
    assert "Wheelchair user" not in payload_text
    assert "hash-apt-16f" not in payload_text

    fake = _FakeService()
    monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "sheet-123")
    monkeypatch.setattr(sheets_sync, "_service", lambda: fake)
    sheets_sync.sync_replacement_watchdog_to_sheets()
    body_text = str([kwargs.get("body") for kind, kwargs in fake.calls if kind == "update"])
    assert "Wheelchair user" not in body_text
    assert "hash-apt-16f" not in body_text
