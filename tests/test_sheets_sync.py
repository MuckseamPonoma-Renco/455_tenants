from packages.db import FilingJob, Incident, MessageDecision, RawMessage, ServiceRequestCase, get_session
from packages.sheets import sync as sheets_sync
from packages.sheets.sync import _elevator_status_from_incidents, _replace_tab_values, _should_skip_duplicate_tasker_decision
from packages.whatsapp.attachments import build_attachment_manifest


class _FakeRequest:
    def __init__(self, calls, kind, kwargs, response=None):
        self.calls = calls
        self.kind = kind
        self.kwargs = kwargs
        self.response = response

    def execute(self):
        self.calls.append((self.kind, self.kwargs))
        return self.response or {}


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
                    {"properties": {"title": "Incidents", "sheetId": 1}},
                    {"properties": {"title": "Dashboard", "sheetId": 2}},
                    {"properties": {"title": "Coverage", "sheetId": 3}},
                    {"properties": {"title": "Cases311", "sheetId": 4}},
                    {"properties": {"title": "Queue311", "sheetId": 5}},
                    {"properties": {"title": "DecisionLog", "sheetId": 6}},
                    {"properties": {"title": "Tenant Log", "sheetId": 7}},
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


class _SingleSheetService(_FakeService):
    def spreadsheets(self):
        calls = self.calls

        class _SingleSheetSpreadsheets(_FakeSpreadsheets):
            def get(self, **kwargs):
                return _FakeRequest(
                    calls,
                    "get",
                    kwargs,
                    response={"sheets": [{"properties": {"title": "Sheet1", "sheetId": 9}}]},
                )

        return _SingleSheetSpreadsheets(calls)


class _LegacyPublicTabService(_FakeService):
    def spreadsheets(self):
        calls = self.calls

        class _LegacyPublicTabSpreadsheets(_FakeSpreadsheets):
            def get(self, **kwargs):
                return _FakeRequest(
                    calls,
                    "get",
                    kwargs,
                    response={
                        "sheets": [
                            {"properties": {"title": "Incidents", "sheetId": 1}},
                            {"properties": {"title": "PublicUpdates", "sheetId": 8}},
                        ]
                    },
                )

        return _LegacyPublicTabSpreadsheets(calls)


def test_service_uses_bounded_authorized_http(monkeypatch, tmp_path):
    credentials_path = tmp_path / "sheets-service-account.json"
    credentials_path.write_text("{}", encoding="utf-8")
    credentials = object()
    calls = {}

    class FakeCredentials:
        @staticmethod
        def from_service_account_file(path, scopes):
            calls["credentials"] = {"path": path, "scopes": scopes}
            return credentials

    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(credentials_path))
    monkeypatch.setenv("GOOGLE_SHEETS_HTTP_TIMEOUT_SECONDS", "999")
    monkeypatch.setenv("DISABLE_SHEETS_SYNC", "0")
    monkeypatch.setattr(sheets_sync, "Credentials", FakeCredentials)
    monkeypatch.setattr(sheets_sync.httplib2, "Http", lambda *, timeout: {"timeout": timeout})
    monkeypatch.setattr(
        sheets_sync.google_auth_httplib2,
        "AuthorizedHttp",
        lambda supplied_credentials, *, http: {"credentials": supplied_credentials, "http": http},
    )
    monkeypatch.setattr(
        sheets_sync,
        "build",
        lambda service, version, **kwargs: calls.update({"service": service, "version": version, **kwargs}) or "service",
    )

    assert sheets_sync._service() == "service"
    assert calls["credentials"] == {"path": str(credentials_path), "scopes": sheets_sync.SCOPES}
    assert calls["service"] == "sheets"
    assert calls["version"] == "v4"
    assert calls["http"] == {"credentials": credentials, "http": {"timeout": 120}}
    assert calls["cache_discovery"] is False


def test_replace_tab_values_writes_first_then_clears_stale_cells():
    service = _FakeService()

    _replace_tab_values(service, "sheet-123", "Incidents", [["incident_id"], ["inc-1"]])

    assert [kind for kind, _kwargs in service.calls] == ["update", "clear", "clear"]
    assert service.calls[0][1]["spreadsheetId"] == "sheet-123"
    assert service.calls[0][1]["range"] == "Incidents!A1"
    assert service.calls[0][1]["body"] == {"values": [["incident_id"], ["inc-1"]]}
    assert service.calls[1][1]["range"] == "Incidents!A3:ZZ"
    assert service.calls[2][1]["range"] == "Incidents!B1:ZZ2"


def test_ensure_tab_exists_can_reuse_single_default_sheet():
    service = _SingleSheetService()

    sheets_sync._ensure_tab_exists(service, "sheet-123", "Tenant Log", rename_single_existing=True)

    assert [kind for kind, _kwargs in service.calls] == ["get", "batchUpdate"]
    body = service.calls[1][1]["body"]
    request = body["requests"][0]["updateSheetProperties"]
    assert request["properties"]["sheetId"] == 9
    assert request["properties"]["title"] == "Tenant Log"


def test_clear_legacy_public_update_tabs_clears_stale_internal_public_tab(monkeypatch):
    service = _LegacyPublicTabService()
    monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "internal-sheet-123")
    monkeypatch.setenv("GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID", "public-sheet-456")

    sheets_sync._clear_legacy_public_update_tabs(service, "public-sheet-456")

    clear_call = next(kwargs for kind, kwargs in service.calls if kind == "clear")
    assert clear_call["spreadsheetId"] == "internal-sheet-123"
    assert clear_call["range"] == "'PublicUpdates'!A:ZZ"


def test_public_status_summary_uses_last_evidence_when_last_report_missing():
    summary = sheets_sync._public_status_summary(
        {"last_evidence": "2026-04-21 2:44 PM", "confidence": "Low"},
        ServiceRequestCase(service_request_number="311-12345678"),
    )

    assert "2026-04-21 2:44 PM" in summary
    assert "311-12345678" in summary


def test_public_detail_text_dedupes_internal_summary_and_removes_names():
    incident = Incident(
        title="Handrail broken on 10th floor stair A",
        summary=(
            "Tenant reports that the handrail in stair A on the 10th floor is broken again. | "
            "Tenant reports the stair A handrail on the 10th floor is broken again and has informed Jack."
        ),
    )

    detail = sheets_sync._public_detail_text(incident, sheets_sync._public_focus_label(incident))

    assert detail == "Handrail broken on 10th floor stair A"
    assert "|" not in detail
    assert "Jack" not in detail


def test_public_detail_text_prefers_title_for_message_like_summary():
    incident = Incident(
        title="North elevator outage",
        summary="as working, but now the north elevator is service again.",
    )
    detail = sheets_sync._public_detail_text(incident, sheets_sync._public_focus_label(incident))

    assert detail == "North elevator outage"


def test_public_detail_text_cleans_redundant_trapped_outage_phrases():
    incident = Incident(
        title="Elevator outage persists",
        summary="broken again with no one trapped inside, indicating persistent elevator outage issue.",
    )
    detail = sheets_sync._public_detail_text(incident, sheets_sync._public_focus_label(incident))

    assert detail == "Elevator outage persists"


def test_public_detail_text_rewrites_ambiguous_only_lift_fragment():
    incident = Incident(
        title="South elevator status update",
        summary="I think south lift only now.",
    )
    detail = sheets_sync._public_detail_text(incident, sheets_sync._public_focus_label(incident))

    assert detail == (
        "Status update mentions only the south lift now; "
        "unclear whether the south lift is working or affected."
    )
    assert "I think" not in detail


def test_public_visible_context_text_removes_person_followup_phrases():
    text = sheets_sync._public_visible_context_text("The stair A, 10th flr handrail is kaputt AGAIN. Reported to Jack.")

    assert text == "The stair A, 10th flr handrail is kaputt AGAIN. Reported to Jack."
    assert "Jack" in text


def test_public_visible_context_text_removes_disallowed_report_recipient(monkeypatch):
    monkeypatch.setenv("PUBLIC_ALLOWED_REPORT_RECIPIENTS", "")

    text = sheets_sync._public_visible_context_text("The stair A, 10th flr handrail is kaputt AGAIN. Reported to Jack.")

    assert text == "The stair A, 10th flr handrail is kaputt AGAIN."
    assert "Jack" not in text


def test_public_evidence_rows_use_message_text_not_quoted_reply(tmp_path, monkeypatch):
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    photo = media_dir / "photo.jpg"
    photo.write_bytes(b"\xff\xd8\xff\xd9")
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))

    quoted = "Karen KWA\n+1 (917) 257-4844\nNorth lift working!!!"
    manifest = build_attachment_manifest(
        items=[{"kind": "image", "status": "downloaded", "path": str(photo), "filename": "photo.jpg"}],
        message_context={"reply_text": f"{quoted}\n{quoted}"},
        source="whatsapp_web",
    )
    incident = Incident(
        incident_id="inc-reply",
        category="elevator",
        title="North lift reported no longer working",
        proof_refs="msg-reply",
    )
    raw = RawMessage(
        message_id="msg-reply",
        chat_name="455 Tenants",
        sender="Darby",
        sender_hash="hash",
        ts_iso="2026-04-20T20:56:00Z",
        ts_epoch=1776718560,
        text="No longer :(",
        attachments=manifest,
        source="whatsapp_web",
    )

    rows = sheets_sync._public_evidence_rows([incident], {"msg-reply": raw}, {"inc-reply": []})

    assert "No longer" in rows[0][6]
    assert "North lift working" not in rows[0][6]


def test_public_update_rows_keep_same_outage_reports_hours_apart():
    incident_one = Incident(
        incident_id="inc-night",
        category="elevator",
        asset="elevator_north",
        title="North elevator not working",
        summary="North elevator not working.",
        proof_refs="msg-night",
        last_ts_epoch=1780537920,
    )
    incident_two = Incident(
        incident_id="inc-morning",
        category="elevator",
        asset="elevator_north",
        title="North elevator out",
        summary="North elevator out.",
        proof_refs="msg-morning",
        last_ts_epoch=1780572240,
    )
    raw_night = RawMessage(
        message_id="msg-night",
        chat_name="455 Tenants",
        sender="Max",
        ts_iso="2026-06-04T01:52:00Z",
        ts_epoch=1780537920,
        text="North elevator not working.",
        source="whatsapp_web",
    )
    raw_morning = RawMessage(
        message_id="msg-morning",
        chat_name="455 Tenants",
        sender="Max",
        ts_iso="2026-06-04T11:24:00Z",
        ts_epoch=1780572240,
        text="North elevator out.",
        source="whatsapp_web",
    )

    rows = sheets_sync._public_update_rows(
        [incident_one, incident_two],
        {"msg-night": raw_night, "msg-morning": raw_morning},
        {"inc-night": [], "inc-morning": []},
        {"455 tenants"},
    )

    matching_rows = [row for row in rows if row[1] == "North elevator" and row[6] == "North elevator was reported as out."]
    assert [row[0] for row in matching_rows] == ["2026-06-04 07:24 AM", "2026-06-03 09:52 PM"]


def test_public_update_rows_still_dedupes_near_duplicate_outage_reports(monkeypatch):
    monkeypatch.setattr(sheets_sync, "PUBLIC_UPDATE_DUPLICATE_WINDOW_SECONDS", 900)
    incident_one = Incident(
        incident_id="inc-first",
        category="elevator",
        asset="elevator_north",
        title="North elevator not working",
        summary="North elevator not working.",
        proof_refs="msg-first",
        last_ts_epoch=1780537920,
    )
    incident_two = Incident(
        incident_id="inc-second",
        category="elevator",
        asset="elevator_north",
        title="North elevator out",
        summary="North elevator out.",
        proof_refs="msg-second",
        last_ts_epoch=1780538340,
    )
    raw_first = RawMessage(
        message_id="msg-first",
        chat_name="455 Tenants",
        sender="Max",
        ts_iso="2026-06-04T01:52:00Z",
        ts_epoch=1780537920,
        text="North elevator not working.",
        source="whatsapp_web",
    )
    raw_second = RawMessage(
        message_id="msg-second",
        chat_name="455 Tenants",
        sender="Max",
        ts_iso="2026-06-04T01:59:00Z",
        ts_epoch=1780538340,
        text="North elevator out.",
        source="whatsapp_web",
    )

    rows = sheets_sync._public_update_rows(
        [incident_one, incident_two],
        {"msg-first": raw_first, "msg-second": raw_second},
        {"inc-first": [], "inc-second": []},
        {"455 tenants"},
    )

    matching_rows = [row for row in rows if row[1] == "North elevator" and row[6] == "North elevator was reported as out."]
    assert [row[0] for row in matching_rows] == ["2026-06-03 09:59 PM"]


def test_public_elevator_asset_from_text_prefers_both_dead_status():
    text = "South (died 11pm) & North (turned off by mechanic 9pm, no could fix) BOTH dead."

    assert sheets_sync._public_elevator_asset_from_text(text, "elevator_both") == "elevator_both"


def test_public_renderer_treats_both_work_now_as_working_status():
    assert sheets_sync._public_elevator_text_is_working_status("Both work now") is True
    assert sheets_sync._public_event_issue_label(
        Incident(
            incident_id="both-work-now",
            category="elevator",
            asset="elevator_both",
            severity=2,
            status="closed",
            title="Elevator restored",
            summary="Both work now.",
        ),
        RawMessage(
            message_id="both-work-now-message",
            chat_name="455 Tenants",
            sender="Karen",
            sender_hash="sender",
            ts_iso="6/27/26 8:46:04 AM",
            ts_epoch=1782564364,
            text="Both work now",
            source="zip_import",
        ),
    ) == "Both elevators working"


def test_public_renderer_ignores_conditional_elevator_safety_discussion():
    text = (
        "It is worth noting that the location of a stuck lift is UNKNOWN. "
        "If both lifts are stuck, they can't see where the stuck lift is by looking up the adjacent shaft."
    )

    assert sheets_sync._public_elevator_text_is_actionable(text) is False


def test_public_collapse_duplicate_incidents_promotes_both_elevator_asset():
    south = Incident(
        incident_id="south-canonical",
        category="elevator",
        asset="elevator_south",
        severity=4,
        status="open",
        start_ts_epoch=1781176244,
        last_ts_epoch=1781176244,
        title="Elevator outage",
        summary="Elevator outage was reported as out.",
        proof_refs="south-message",
    )
    both = Incident(
        incident_id="both-merged",
        category="elevator",
        asset="elevator_both",
        severity=5,
        status="open",
        start_ts_epoch=1781176740,
        last_ts_epoch=1781176740,
        title="Elevator outage",
        summary="Elevator outage was reported as out.",
        proof_refs="both-message",
    )
    case = ServiceRequestCase(
        service_request_number="311-00000001",
        incident_id=south.incident_id,
        status="In Progress",
        submitted_at="2026-06-11T12:00:00Z",
    )

    collapsed, _case_map = sheets_sync._public_collapse_duplicate_incidents(
        [south, both],
        {south.incident_id: [case]},
        {south.incident_id: ["south-message"], both.incident_id: ["both-message"]},
    )

    assert len(collapsed) == 1
    assert collapsed[0].incident_id == south.incident_id
    assert collapsed[0].asset == "elevator_both"


def test_public_sanitizer_removes_standalone_person_names():
    text = sheets_sync._public_safe_summary_text(
        "Crowds form in the lobby and Jack mans elevator door like a bouncer. | "
        "The laundry room is closed today as per Jack. | "
        "For Meredith and Piotr, Greg called Wojtek/Val."
    )

    assert "Jack" not in text
    assert "Meredith" not in text
    assert "Piotr" not in text
    assert "Greg" not in text
    assert "Wojtek" not in text
    assert "Val" not in text
    assert "Someone mans elevator door" in text
    assert "as per" not in text.casefold()


def test_public_safe_summary_strips_named_tenant_report_prefix():
    text = sheets_sync._public_safe_summary_text("Tenant Nic reports that the south elevator is out at 2:07 PM.")

    assert text == "The south elevator is out at 2:07 PM."
    assert "Nic" not in text


def test_public_safe_summary_removes_unit_prefix_without_dropping_issue_counts():
    unit_text = sheets_sync._public_safe_summary_text("14D. South lift out.")
    unit_here_text = sheets_sync._public_safe_summary_text("14D here. I smell cigarette smoke.")
    count_text = sheets_sync._public_safe_summary_text("2 lifts working. Handrail 10/F, stair A detaching from wall.")

    assert unit_text == "South lift out."
    assert unit_here_text == "I smell cigarette smoke."
    assert count_text == "2 lifts working. Handrail 10/F, stair A detaching from wall."


def test_public_safe_summary_keeps_issue_phrases_that_look_like_reporter_names():
    assert sheets_sync._public_safe_summary_text("Possible elevator outage reported") == "Possible elevator outage reported"
    assert sheets_sync._public_safe_summary_text("South lift irregular floor skipping reported") == "South lift irregular floor skipping reported"
    assert sheets_sync._public_safe_summary_text("Someone stuck in elevator reported") == "Someone stuck in elevator reported"
    assert (
        sheets_sync._public_safe_summary_text("North lift was reported no longer working after an earlier working update.")
        == "North lift was reported no longer working after an earlier working update."
    )
    assert "Jack" not in sheets_sync._public_safe_summary_text("Tenant has reported Jack about the elevator noise.")


def test_public_update_recognizes_no_side_elevator_and_floor_service_restore():
    north_incident = Incident(
        incident_id="inc-north",
        category="elevator",
        asset="elevator_north",
        title="North elevator outage",
        summary="No north elevator.",
        proof_refs="msg-north",
    )
    north_raw = RawMessage(
        message_id="msg-north",
        chat_name="455 Tenants",
        sender="Tenant",
        sender_hash="hash-north",
        ts_iso="2026-05-06T11:11:00Z",
        ts_epoch=1778065860,
        text="No north elevator",
        attachments=None,
        source="whatsapp_web",
    )
    assert sheets_sync._public_should_include_update(north_incident, north_raw) is True
    assert sheets_sync._public_event_issue_label(north_incident, north_raw) == "North elevator"

    north_only_working_raw = RawMessage(
        message_id="msg-only-south",
        chat_name="455 Tenants",
        sender="Tenant",
        sender_hash="hash-only-south",
        ts_iso="2026-05-06T11:12:00Z",
        ts_epoch=1778065920,
        text="Only south lift working",
        attachments=None,
        source="whatsapp_web",
    )
    assert sheets_sync._public_should_include_update(north_incident, north_only_working_raw) is True
    assert sheets_sync._public_event_issue_label(north_incident, north_only_working_raw) == "North elevator"

    generic_working_raw = RawMessage(
        message_id="msg-generic-working",
        chat_name="455 Tenants",
        sender="Tenant",
        sender_hash="hash-generic-working",
        ts_iso="2026-05-06T11:13:00Z",
        ts_epoch=1778065980,
        text="Appears to be working now",
        attachments=None,
        source="whatsapp_web",
    )
    assert sheets_sync._public_should_include_update(north_incident, generic_working_raw) is False

    south_stopped_incident = Incident(
        incident_id="inc-south-stopped",
        category="elevator",
        asset="elevator_south",
        title="South elevator stopped and not moving",
        summary="South elevator stopped and did not seem to be moving.",
        proof_refs="msg-south-stopped",
    )
    south_stopped_raw = RawMessage(
        message_id="msg-south-stopped",
        chat_name="455 Tenants",
        sender="Tenant",
        sender_hash="hash-south-stopped",
        ts_iso="2026-06-04T11:23:00Z",
        ts_epoch=1780572180,
        text=(
            "I heard one elevator running, but it doesn't seem to be moving now. "
            "South elevator stopped for me."
        ),
        attachments=None,
        source="whatsapp_web",
    )
    assert sheets_sync._public_should_include_update(south_stopped_incident, south_stopped_raw) is True
    assert sheets_sync._public_event_issue_label(south_stopped_incident, south_stopped_raw) == "South elevator"
    assert sheets_sync._public_event_summary(south_stopped_incident, south_stopped_raw) == "South elevator was reported stopped or not moving."

    one_out_incident = Incident(
        incident_id="inc-one-out",
        category="elevator",
        asset="elevator_both",
        title="One elevator currently out of service",
        summary="Hi all-currently one elevator out of service.",
        proof_refs="msg-one-out",
    )
    one_out_raw = RawMessage(
        message_id="msg-one-out",
        chat_name="455 Tenants",
        sender="Tenant",
        sender_hash="hash-one-out",
        ts_iso="2026-05-16T20:47:00Z",
        ts_epoch=1778964420,
        text="Hi all-currently one elevator out of service",
        attachments=None,
        source="whatsapp_web",
    )
    assert sheets_sync._public_should_include_update(one_out_incident, one_out_raw) is True
    assert sheets_sync._public_event_issue_label(one_out_incident, one_out_raw) == "Elevator service reduced"
    assert sheets_sync._public_event_summary(one_out_incident, one_out_raw) == "One elevator was reported out of service."

    both_context_restore_raw = RawMessage(
        message_id="msg-both-context-restore",
        chat_name="455 Tenants",
        sender="Tenant",
        sender_hash="hash-both-context-restore",
        ts_iso="2026-05-16T21:36:00Z",
        ts_epoch=1778967360,
        text="Both appear to be working now",
        attachments=None,
        source="whatsapp_web",
    )
    assert sheets_sync._public_should_include_update(one_out_incident, both_context_restore_raw) is True
    assert sheets_sync._public_event_issue_label(one_out_incident, both_context_restore_raw) == "Both elevators working"
    assert sheets_sync._public_event_summary(one_out_incident, both_context_restore_raw) == "Both elevators were reported working."

    back_to_one_raw = RawMessage(
        message_id="msg-back-to-one",
        chat_name="455 Tenants",
        sender="Tenant",
        sender_hash="hash-back-to-one",
        ts_iso="2026-05-16T21:00:00Z",
        ts_epoch=1778965200,
        text="Back to one elevator/lift",
        attachments=None,
        source="whatsapp_web",
    )
    assert sheets_sync._public_event_issue_label(one_out_incident, back_to_one_raw) == "Elevator service reduced"
    assert (
        sheets_sync._public_event_summary(one_out_incident, back_to_one_raw)
        == "Elevator service was reported reduced to one working elevator."
    )

    reply_text = (
        "Molly\n+1 (347) 581-0269\n"
        "Was the north lift ever operational today? It was out when we left at 7, "
        "and out when we just came home, but I don’t know what happened in between."
    )
    reply_raw = RawMessage(
        message_id="msg-reply-context",
        chat_name="455 Tenants",
        sender="Tenant",
        sender_hash="hash-reply",
        ts_iso="2026-05-06T21:36:00Z",
        ts_epoch=1778103360,
        text="Not to my knowledge. I got back around 1pm and still out from when I left",
        attachments=build_attachment_manifest(items=[], message_context={"reply_text": reply_text}, source="whatsapp_web"),
        source="whatsapp_web",
    )
    assert sheets_sync._public_should_include_update(north_incident, reply_raw) is True
    assert sheets_sync._public_is_actionable_311_update(north_incident, reply_raw) is True
    assert sheets_sync._public_event_issue_label(north_incident, reply_raw) == "North elevator"
    assert sheets_sync._public_event_summary(north_incident, reply_raw) == "North elevator was reported as still out."
    assert "+1" not in sheets_sync._public_event_summary(north_incident, reply_raw)

    both_incident = Incident(
        incident_id="inc-both-working",
        category="elevator",
        asset="elevator_both",
        title="Both elevators working",
        summary="Both elevators were reported working.",
        proof_refs="msg-both-working",
    )
    both_working_raw = RawMessage(
        message_id="msg-both-working",
        chat_name="455 Tenants",
        sender="Tenant",
        sender_hash="hash-both",
        ts_iso="2026-05-07T19:29:00Z",
        ts_epoch=1778182140,
        text="Just left for my evening rounds and looks like both elevators working. One was still down ~11am",
        attachments=build_attachment_manifest(
            items=[],
            message_context={"reply_text": "Molly\n+1 (347) 581-0269\nCan someone write here if they see an elevator technician today?"},
            source="whatsapp_web",
        ),
        source="whatsapp_web",
    )
    assert sheets_sync._public_should_include_update(both_incident, both_working_raw) is True
    assert sheets_sync._public_is_actionable_311_update(both_incident, both_working_raw) is False
    assert sheets_sync._public_event_issue_label(both_incident, both_working_raw) == "Both elevators working"
    assert sheets_sync._public_event_summary(both_incident, both_working_raw) == "Both elevators were reported working; one had been down earlier."
    assert "+1" not in sheets_sync._public_event_summary(both_incident, both_working_raw)

    south_incident = Incident(
        incident_id="inc-south",
        category="elevator",
        asset="elevator_south",
        title="South elevator working normally",
        summary="At least the South one is not stopping every floor.",
        proof_refs="msg-south",
    )
    south_raw = RawMessage(
        message_id="msg-south",
        chat_name="455 Tenants",
        sender="Tenant",
        sender_hash="hash-south",
        ts_iso="2026-05-06T11:30:00Z",
        ts_epoch=1778067000,
        text="At least the South one is not stopping every floor",
        attachments=None,
        source="whatsapp_web",
    )
    assert sheets_sync._public_should_include_update(south_incident, south_raw) is True
    assert sheets_sync._public_event_issue_label(south_incident, south_raw) == "South elevator working normally"
    assert "without floor-by-floor service" in sheets_sync._public_event_summary(south_incident, south_raw)


def test_public_detail_text_falls_back_when_title_redaction_would_be_empty():
    incident = Incident(
        incident_id="inc-public-summary",
        category="elevator",
        asset=None,
        status="closed",
        title="Possible elevator outage reported",
        summary="Possibly no lifts. | Someone is stuck for sure. | Flr 6, Wojtek/Val calling for help.",
    )

    detail = sheets_sync._public_detail_text(incident, sheets_sync._public_focus_label(incident))

    assert detail
    assert "Possible elevator outage" in detail or "Possibly no lifts" in detail


def test_public_case_sort_key_orders_active_cases_by_filing_recency():
    older_active = ServiceRequestCase(
        service_request_number="311-older",
        status="In Progress",
        submitted_at="2026-04-08T13:38:00Z",
        last_checked_at="2026-04-24T03:01:30Z",
    )
    newer_active = ServiceRequestCase(
        service_request_number="311-newer",
        status="In Progress",
        submitted_at="2026-04-21T18:09:00Z",
        last_checked_at="2026-04-24T02:59:52Z",
    )
    submitted = ServiceRequestCase(
        service_request_number="311-submitted",
        status="submitted",
        submitted_at="2026-04-22T18:09:00Z",
        last_checked_at="2026-04-24T03:02:00Z",
    )

    ordered = sorted([older_active, submitted, newer_active], key=sheets_sync._public_case_sort_key, reverse=True)

    assert [row.service_request_number for row in ordered] == ["311-newer", "311-older", "311-submitted"]


def test_should_skip_duplicate_tasker_decision_uses_normalized_signature():
    raw = RawMessage(
        message_id="m-2",
        chat_name="455 Tenants (3 messages): ~ Molly",
        sender="%ansubtext",
        sender_hash="ignored",
        ts_iso="2026-04-05T20:35:41Z",
        ts_epoch=1775421341,
        text="Btw, I did forward the News12 story to Weinreb.",
        attachments=None,
        source="tasker",
    )
    decision = MessageDecision(message_id="m-2", is_issue=False, incident_id=None)

    kept = {("455 Tenants", "Molly", "Btw, I did forward the News12 story to Weinreb."): (1775421265, None, False)}

    assert _should_skip_duplicate_tasker_decision(raw, decision, kept) is True


def test_should_skip_duplicate_tasker_decision_skips_non_issue_repeats_even_hours_later():
    raw = RawMessage(
        message_id="m-3",
        chat_name="455 Tenants: ~ Yvonne",
        sender="%ansubtext",
        sender_hash="ignored",
        ts_iso="2026-04-03T22:23:54Z",
        ts_epoch=1775255034,
        text="Good job on the news clip",
        attachments=None,
        source="tasker",
    )
    decision = MessageDecision(message_id="m-3", is_issue=False, incident_id=None)

    kept = {("455 Tenants", "Yvonne", "Good job on the news clip"): (1775248224, None, False)}

    assert _should_skip_duplicate_tasker_decision(raw, decision, kept) is True


def test_should_skip_duplicate_tasker_decision_skips_noise_rows():
    raw = RawMessage(
        message_id="m-4",
        chat_name="455 Tenants",
        sender="",
        sender_hash="ignored",
        ts_iso="2026-04-03T22:23:54Z",
        ts_epoch=1775255034,
        text="2 new messages",
        attachments=None,
        source="tasker",
    )
    decision = MessageDecision(message_id="m-4", is_issue=False, incident_id=None)

    assert _should_skip_duplicate_tasker_decision(raw, decision, {}) is True


def test_sync_311_queue_to_sheets_only_writes_active_jobs(client, monkeypatch):
    service = _FakeService()
    monkeypatch.setattr(sheets_sync, "_service", lambda: service)
    monkeypatch.setattr(sheets_sync, "_sheet_id", lambda: "sheet-123")

    with get_session() as session:
        session.add_all([
            FilingJob(job_id=1, dedupe_key="311:pending", incident_id="inc-1", state="pending", created_at="2026-04-12T10:00:00Z"),
            FilingJob(job_id=2, dedupe_key="311:claimed", incident_id="inc-2", state="claimed", created_at="2026-04-12T11:00:00Z"),
            FilingJob(job_id=3, dedupe_key="311:failed", incident_id="inc-3", state="failed", created_at="2026-04-12T12:00:00Z"),
            FilingJob(job_id=4, dedupe_key="311:submitted", incident_id="inc-4", state="submitted", created_at="2026-04-12T13:00:00Z"),
            FilingJob(job_id=5, dedupe_key="311:skipped", incident_id="inc-5", state="skipped", created_at="2026-04-12T14:00:00Z"),
        ])
        session.commit()

    sheets_sync.sync_311_queue_to_sheets()

    values = next(kwargs["body"]["values"] for kind, kwargs in service.calls if kind == "update")
    assert values[0] == ["job_id", "incident_id", "state", "priority", "complaint_type", "form_target", "attempts", "created_at", "claimed_at", "completed_at", "notes"]
    assert [row[0] for row in values[1:]] == [3, 2, 1]


def test_elevator_status_keeps_last_known_working_state_when_restore_is_stale():
    north = Incident(
        incident_id="inc-north",
        category="elevator",
        asset=None,
        status="closed",
        last_ts_epoch=1,
        title="restore",
        severity=4,
        summary="",
        proof_refs="",
        report_count=1,
        witness_count=2,
        confidence=90,
    )

    status = _elevator_status_from_incidents([north], "elevator_north")

    assert status["status"] == "WORKING"
    assert status["confidence"] == "Low"


def test_elevator_status_keeps_stale_open_outage_as_unknown():
    north = Incident(
        incident_id="inc-open",
        category="elevator",
        asset=None,
        status="open",
        last_ts_epoch=1,
        title="outage",
        severity=4,
        summary="",
        proof_refs="",
        report_count=1,
        witness_count=2,
        confidence=90,
    )

    status = _elevator_status_from_incidents([north], "elevator_north")

    assert status["status"] == "UNKNOWN"
    assert status["confidence"] == "Low"


def test_sync_decisions_to_sheets_skips_media_placeholder_rows(client, monkeypatch):
    service = _FakeService()
    monkeypatch.setattr(sheets_sync, "_service", lambda: service)
    monkeypatch.setattr(sheets_sync, "_sheet_id", lambda: "sheet-123")

    with get_session() as session:
        session.add_all([
            RawMessage(
                message_id="media-row",
                chat_name="Tenants WhatsApp",
                sender="Karen",
                sender_hash="hash-1",
                ts_iso="4/12/26 9:45:00 AM",
                ts_epoch=1776001500,
                text="image omitted",
                attachments="omitted:image",
                source="export",
            ),
            RawMessage(
                message_id="real-row",
                chat_name="Tenants WhatsApp",
                sender="Karen",
                sender_hash="hash-2",
                ts_iso="4/12/26 9:46:00 AM",
                ts_epoch=1776001560,
                text="2 elevators OOS",
                attachments=None,
                source="export",
            ),
            MessageDecision(
                message_id="media-row",
                created_at="2026-04-12T13:45:00Z",
                chosen_source="media_placeholder",
                is_issue=False,
            ),
            MessageDecision(
                message_id="real-row",
                created_at="2026-04-12T13:46:00Z",
                chosen_source="rules",
                is_issue=True,
                category="elevator",
                event_type="outage",
                confidence=85,
            ),
        ])
        session.commit()

    sheets_sync.sync_decisions_to_sheets()

    values = next(kwargs["body"]["values"] for kind, kwargs in service.calls if kind == "update")
    assert values[0][0] == "message_ts"
    assert len(values) == 2
    assert values[1][2] == "real-row"
    assert values[1][4] == "2 elevators OOS"


def test_sync_decisions_to_sheets_exposes_media_preview_and_links(client, monkeypatch, tmp_path):
    service = _FakeService()
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    image_path = media_dir / "evidence.png"
    image_path.write_bytes(b"png-bytes")

    monkeypatch.setattr(sheets_sync, "_service", lambda: service)
    monkeypatch.setattr(sheets_sync, "_sheet_id", lambda: "sheet-123")
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))

    manifest = build_attachment_manifest(
        items=[{"kind": "image", "status": "downloaded", "path": str(image_path), "filename": "evidence.png"}],
        message_context={"reply_text": "north elevator photo"},
        links=["https://example.com/more"],
        source="whatsapp_web",
    )

    with get_session() as session:
        session.add(
            RawMessage(
                message_id="media-row",
                chat_name="455 Tenants",
                sender="Karen",
                sender_hash="hash-1",
                ts_iso="2026-04-21T14:00:00Z",
                ts_epoch=1776780000,
                text="North elevator photo",
                attachments=manifest,
                source="whatsapp_web",
            )
        )
        session.add(
            MessageDecision(
                message_id="media-row",
                created_at="2026-04-21T14:00:10Z",
                chosen_source="rules",
                is_issue=True,
                category="elevator",
                event_type="outage",
                confidence=85,
            )
        )
        session.commit()

    sheets_sync.sync_decisions_to_sheets()

    values = next(kwargs["body"]["values"] for kind, kwargs in service.calls if kind == "update")
    header = values[0]
    row = values[1]
    assert row[header.index("media_preview")].startswith('=HYPERLINK("https://tenant.example/media/whatsapp/media-row/0?v=')
    assert ',IMAGE("https://tenant.example/media/whatsapp/media-row/0?v=' in row[header.index("media_preview")]
    assert row[header.index("media_1")].startswith("https://tenant.example/media/whatsapp/media-row/0?v=")
    assert row[header.index("reply_context")] == "north elevator photo"
    assert row[header.index("link_1")] == '=HYPERLINK("https://example.com/more","Link 1")'
    layout_call = next(kwargs for kind, kwargs in service.calls if kind == "batchUpdate")
    requests = layout_call["body"]["requests"]
    assert any(req.get("updateSheetProperties", {}).get("properties", {}).get("gridProperties", {}).get("frozenRowCount") == 1 for req in requests)
    assert any(req.get("updateDimensionProperties", {}).get("properties", {}).get("pixelSize") == 120 for req in requests)


def test_sync_decisions_to_sheets_orders_by_message_timestamp_not_reprocess_time(client, monkeypatch):
    service = _FakeService()
    monkeypatch.setattr(sheets_sync, "_service", lambda: service)
    monkeypatch.setattr(sheets_sync, "_sheet_id", lambda: "sheet-123")

    with get_session() as session:
        session.add_all([
            RawMessage(
                message_id="older-message",
                chat_name="455 Tenants",
                sender="Karen",
                sender_hash="hash-1",
                ts_iso="2026-04-20T10:00:00Z",
                ts_epoch=1776688800,
                text="Older update",
                attachments=None,
                source="whatsapp_web",
            ),
            RawMessage(
                message_id="newer-message",
                chat_name="455 Tenants",
                sender="Karen",
                sender_hash="hash-2",
                ts_iso="2026-04-21T10:00:00Z",
                ts_epoch=1776775200,
                text="Newer update",
                attachments=None,
                source="whatsapp_web",
            ),
            MessageDecision(
                message_id="older-message",
                created_at="2026-04-22T12:00:00Z",
                chosen_source="rules",
                is_issue=True,
                category="elevator",
            ),
            MessageDecision(
                message_id="newer-message",
                created_at="2026-04-21T12:00:00Z",
                chosen_source="rules",
                is_issue=True,
                category="elevator",
            ),
        ])
        session.commit()

    sheets_sync.sync_decisions_to_sheets()

    values = next(kwargs["body"]["values"] for kind, kwargs in service.calls if kind == "update")
    assert values[1][2] == "newer-message"
    assert values[2][2] == "older-message"


def test_sync_public_updates_to_sheets_writes_clean_resident_rows(client, monkeypatch, tmp_path):
    service = _FakeService()
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    image_path = media_dir / "proof.png"
    image_path.write_bytes(b"proof")

    monkeypatch.setattr(sheets_sync, "_service", lambda: service)
    monkeypatch.setattr(sheets_sync, "_public_sheet_id", lambda: "public-sheet-123")
    monkeypatch.setenv("PUBLIC_UPDATES_CHAT_NAMES", "455 Tenants")
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))

    manifest = build_attachment_manifest(
        items=[{"kind": "image", "status": "downloaded", "path": str(image_path), "filename": "proof.png"}],
        message_context={"reply_text": "south elevator photo"},
        source="whatsapp_web",
    )

    with get_session() as session:
        session.add_all([
            Incident(
                incident_id="inc-public",
                category="elevator",
                asset="elevator_south",
                severity=4,
                status="open",
                start_ts="2026-04-21T18:07:00Z",
                start_ts_epoch=1776804420,
                last_ts_epoch=1776804420,
                title="South elevator out of service",
                summary="Residents report the south elevator is out.",
                proof_refs="msg-public",
                report_count=2,
                witness_count=1,
                confidence=90,
                updated_at="2026-04-21T18:10:00Z",
            ),
            Incident(
                incident_id="inc-old-public",
                category="security_access",
                asset=None,
                severity=2,
                status="closed",
                start_ts="2026-04-01T12:00:00Z",
                start_ts_epoch=1775044800,
                last_ts_epoch=1775044800,
                title="Lobby door did not close",
                summary="Older public incident was resolved but should stay in the full tenant log.",
                proof_refs="msg-old-public",
                report_count=1,
                witness_count=1,
                confidence=80,
                updated_at="2026-04-01T12:05:00Z",
            ),
            Incident(
                incident_id="inc-private",
                category="security_access",
                asset=None,
                severity=4,
                status="open",
                start_ts="2026-04-21T19:07:00Z",
                start_ts_epoch=1776808020,
                last_ts_epoch=1776808020,
                title="Private test issue",
                summary="Should not be shown on public sheet.",
                proof_refs="msg-private",
                report_count=1,
                witness_count=0,
                confidence=85,
                updated_at="2026-04-21T19:08:00Z",
            ),
            RawMessage(
                message_id="msg-public",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-public",
                ts_iso="2026-04-21T18:07:00Z",
                ts_epoch=1776804420,
                text="South lift out",
                attachments=manifest,
                source="whatsapp_web",
            ),
            RawMessage(
                message_id="msg-old-public",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-old-public",
                ts_iso="2026-04-01T12:00:00Z",
                ts_epoch=1775044800,
                text="Lobby door did not close",
                attachments=None,
                source="whatsapp_web",
            ),
            RawMessage(
                message_id="msg-private",
                chat_name="Building Test Group",
                sender="Tester",
                sender_hash="hash-private",
                ts_iso="2026-04-21T19:07:00Z",
                ts_epoch=1776808020,
                text="Private issue",
                attachments=None,
                source="whatsapp_web",
            ),
            ServiceRequestCase(
                service_request_number="311-12345678",
                incident_id="inc-public",
                source="portal_playwright",
                complaint_type="Elevator or Escalator Complaint",
                status="submitted",
                submitted_at="2026-04-21T18:12:00Z",
            ),
        ])
        session.commit()

    sheets_sync.sync_public_updates_to_sheets()

    values = next(
        kwargs["body"]["values"]
        for kind, kwargs in service.calls
        if kind == "update" and kwargs["range"] == f"{sheets_sync._public_updates_tab()}!A1"
    )
    assert values[0][0] == "455 Tenants Log"
    assert "tenant-facing" not in values[1][0]
    assert "internal workflow" not in values[1][0]
    assert values[2][0] == "At a glance"
    assert values[3] == ["Item", "Count / detail", "What this means", "", "", "", "", "", "", ""]
    metrics = {row[0]: row[1] for row in values if row and row[0] in {"Incidents", "311 filings", "Most common issue type", "Latest update"}}
    assert metrics == {
        "Incidents": 2,
        "311 filings": 1,
        "Most common issue type": "Elevator",
        "Latest update": "South elevator",
    }
    assert values[10][0] == "Category snapshot"
    assert values[11] == ["Category", "Incidents", "311 filings", "Latest update", "Latest issue", "", "", "", "", ""]

    all_incidents_row = next(idx for idx, row in enumerate(values) if row[0] == "Public update log")
    assert values[all_incidents_row + 1] == ["Updated", "Issue", "Category", "311 follow-up", "Preview", "Open evidence", "Summary", "", "", ""]
    incident_rows = values[all_incidents_row + 2:]
    public_issue_row = next(row for row in incident_rows if len(row) >= 7 and row[1] == "South elevator")
    assert public_issue_row[2] == "Elevator"
    assert public_issue_row[3] == "311-12345678 (submitted)"
    assert public_issue_row[4].startswith('=HYPERLINK("https://tenant.example/media/whatsapp/msg-public/0?v=')
    assert ',IMAGE("https://tenant.example/media/whatsapp/msg-public/0?v=' in public_issue_row[4]
    assert public_issue_row[4].endswith('",4,110,240))')
    assert public_issue_row[5].startswith("https://tenant.example/media/whatsapp/msg-public/0?v=")
    old_issue_row = next(row for row in incident_rows if len(row) >= 7 and row[1] == "Lobby door did not close")
    assert old_issue_row[2] == "Security / access"

    case_watch_row = next(idx for idx, row in enumerate(values) if row[0] == "311 case watch")
    assert values[case_watch_row + 1] == ["Case", "NYC status", "Complaint", "Related issue", "Submitted", "NYC lookup", "Notes", "", "", ""]
    public_case_row = next(row for row in values if len(row) >= 8 and row[0] == "311-12345678")
    assert public_case_row[0] == "311-12345678"
    assert public_case_row[1] == "Submitted"
    assert public_case_row[2] == "Elevator or Escalator Complaint"
    assert public_case_row[3] == "South elevator"
    assert public_case_row[5] == "Not checked yet"
    assert public_case_row[6] == "Waiting for first NYC status lookup."
    flattened = " ".join(str(cell) for row in values for cell in row if cell)
    assert "Private test issue" not in flattened
    assert "Evidence log" not in flattened
    assert "Evidence items" not in flattened
    layout_call = [
        kwargs for kind, kwargs in service.calls
        if kind == "batchUpdate"
        and any(
            request.get("updateSheetProperties", {}).get("properties", {}).get("gridProperties", {}).get("frozenRowCount")
            for request in kwargs["body"]["requests"]
        )
    ][-1]
    frozen_request = next(
        request for request in layout_call["body"]["requests"]
        if request.get("updateSheetProperties", {}).get("properties", {}).get("gridProperties", {}).get("frozenRowCount")
    )
    assert frozen_request["updateSheetProperties"]["properties"]["gridProperties"]["frozenRowCount"] == 1
    for removed_public_label in ["Smart Log", "Plain English note", "Open issues", "Resolved", "Witnesses", "Reports", "tenant-facing", "internal workflow"]:
        assert removed_public_label not in flattened


def test_sync_public_updates_collapses_duplicate_public_incidents_and_keeps_311_case(client, monkeypatch, tmp_path):
    service = _FakeService()
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    photo = media_dir / "alarm-photo.png"
    photo.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\rIHDR" + (900).to_bytes(4, "big") + (700).to_bytes(4, "big"))

    monkeypatch.setattr(sheets_sync, "_service", lambda: service)
    monkeypatch.setattr(sheets_sync, "_public_sheet_id", lambda: "public-sheet-123")
    monkeypatch.setenv("PUBLIC_UPDATES_CHAT_NAMES", "455 Tenants")
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))

    duplicate_text = "Someone just rang the alarm on the lift. Don't know which one."
    manifest = build_attachment_manifest(
        items=[{"kind": "image", "status": "downloaded", "path": str(photo), "filename": "alarm-photo.png"}],
        source="whatsapp_web",
    )
    with get_session() as session:
        session.add_all([
            Incident(
                incident_id="inc-alarm-old",
                category="elevator",
                asset=None,
                severity=3,
                status="closed",
                start_ts="2026-04-09T23:19:27Z",
                start_ts_epoch=1775776767,
                last_ts_epoch=1775779569,
                title="Alarm rung on unknown elevator",
                summary=duplicate_text,
                proof_refs="msg-alarm-old",
                report_count=1,
                witness_count=1,
                confidence=80,
                updated_at="2026-04-10T00:06:09Z",
            ),
            Incident(
                incident_id="inc-alarm-case",
                category="elevator",
                asset=None,
                severity=3,
                status="closed",
                start_ts="2026-04-10T13:15:37Z",
                start_ts_epoch=1775826937,
                last_ts_epoch=1775826937,
                title="Alarm rang on an unknown elevator",
                summary=duplicate_text,
                proof_refs="msg-alarm-case",
                report_count=1,
                witness_count=1,
                confidence=85,
                updated_at="2026-04-10T13:15:37Z",
            ),
            RawMessage(
                message_id="msg-alarm-old",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-alarm",
                ts_iso="2026-04-09T23:19:27Z",
                ts_epoch=1775776767,
                text=duplicate_text,
                attachments=manifest,
                source="whatsapp_web",
            ),
            RawMessage(
                message_id="msg-alarm-case",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-alarm",
                ts_iso="2026-04-10T13:15:37Z",
                ts_epoch=1775826937,
                text=duplicate_text,
                attachments=None,
                source="tasker",
            ),
            ServiceRequestCase(
                service_request_number="311-27091967",
                incident_id="inc-alarm-case",
                source="portal_playwright",
                complaint_type="Elevator or Escalator Complaint",
                status="In Progress",
                submitted_at="2026-04-10T13:22:00Z",
            ),
        ])
        session.commit()

    sheets_sync.sync_public_updates_to_sheets()

    values = next(
        kwargs["body"]["values"]
        for kind, kwargs in service.calls
        if kind == "update" and kwargs["range"] == f"{sheets_sync._public_updates_tab()}!A1"
    )
    all_incidents_row = next(idx for idx, row in enumerate(values) if row[0] == "Public update log")
    incident_rows = [row for row in values[all_incidents_row + 2:] if row and row[0]]
    alarm_rows = [row for row in incident_rows if len(row) >= 7 and row[6] == "Elevator alarm was reported."]

    assert len(alarm_rows) == 1
    assert alarm_rows[0][1] == "Alarm rang on an unknown elevator"
    assert alarm_rows[0][3] == "311-27091967 (In Progress)"
    assert "/media/whatsapp/msg-alarm-old/0?v=" in alarm_rows[0][4]
    assert alarm_rows[0][5].startswith("https://tenant.example/media/whatsapp/msg-alarm-old/0?v=")
    metrics = {row[0]: row[1] for row in values if row and row[0] in {"Incidents", "311 filings"}}
    assert metrics == {"Incidents": 1, "311 filings": 1}


def test_sync_public_updates_collapses_same_minute_status_rows(client, monkeypatch):
    service = _FakeService()
    monkeypatch.setattr(sheets_sync, "_service", lambda: service)
    monkeypatch.setattr(sheets_sync, "_public_sheet_id", lambda: "public-sheet-123")
    monkeypatch.setenv("PUBLIC_UPDATES_CHAT_NAMES", "455 Tenants")
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")

    with get_session() as session:
        session.add_all([
            Incident(
                incident_id="inc-same-minute",
                category="elevator",
                asset="elevator_south",
                severity=4,
                status="open",
                start_ts="2026-05-05T14:13:00Z",
                start_ts_epoch=1777990380,
                last_ts_epoch=1777990380,
                title="South elevator still out",
                summary="South lift dead. Elevator mechanic is here.",
                proof_refs="msg-out,msg-repair",
                report_count=2,
                witness_count=1,
                confidence=90,
            ),
            RawMessage(
                message_id="msg-out",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-out",
                ts_iso="2026-05-05T14:13:00Z",
                ts_epoch=1777990380,
                text="South lift dead",
                attachments=None,
                source="whatsapp_web",
            ),
            RawMessage(
                message_id="msg-repair",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-repair",
                ts_iso="2026-05-05T14:13:00Z",
                ts_epoch=1777990380,
                text="Elevator mechanic is here",
                attachments=None,
                source="whatsapp_web",
            ),
            ServiceRequestCase(
                service_request_number="311-27374123",
                incident_id="inc-same-minute",
                source="portal_playwright",
                complaint_type="Elevator or Escalator Complaint",
                status="In Progress",
                submitted_at="2026-05-05T14:16:00Z",
            ),
        ])
        session.commit()

    sheets_sync.sync_public_updates_to_sheets()

    values = next(kwargs["body"]["values"] for kind, kwargs in service.calls if kind == "update")
    log_row = next(idx for idx, row in enumerate(values) if row[0] == "Public update log")
    rows = [row for row in values[log_row + 2:] if row and row[0] == "2026-05-05 10:13 AM"]

    assert len(rows) == 1
    assert "South elevator" in rows[0][1]
    assert "Elevator repair visit" in rows[0][1]
    assert rows[0][2] == "Elevator"
    assert rows[0][3] == "311-27374123 (In Progress)"
    assert "South elevator was reported as out." in rows[0][6]
    assert "Elevator mechanic was reported on site." in rows[0][6]


def test_sync_public_updates_uses_decision_messages_beyond_capped_proof_refs(client, monkeypatch):
    service = _FakeService()
    monkeypatch.setattr(sheets_sync, "_service", lambda: service)
    monkeypatch.setattr(sheets_sync, "_public_sheet_id", lambda: "public-sheet-123")
    monkeypatch.setenv("PUBLIC_UPDATES_CHAT_NAMES", "455 Tenants")
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")

    with get_session() as session:
        session.add(
            Incident(
                incident_id="inc-capped-proofs",
                category="elevator",
                asset="elevator_both",
                severity=4,
                status="closed",
                start_ts="2026-06-04T11:24:00Z",
                start_ts_epoch=1780572240,
                end_ts="2026-06-04T20:08:00Z",
                end_ts_epoch=1780603680,
                last_ts_epoch=1780603680,
                title="North elevator out",
                summary=(
                    "North elevator out. | I think no mechanic is on site. "
                    "Nothing arrived on 14 after a prolonged wait. | "
                    "No elevators so far today. | Both of them are working now."
                ),
                proof_refs="msg-north,msg-ride,msg-no-mechanic",
                report_count=5,
                witness_count=2,
                confidence=90,
            )
        )
        messages = [
            ("msg-north", "2026-06-04T11:24:00Z", 1780572240, "North elevator out.", "new_issue"),
            ("msg-ride", "2026-06-04T11:24:00Z", 1780572240, "Went up 2 and then made it straight to lobby.", "status_update"),
            (
                "msg-no-mechanic",
                "2026-06-04T11:44:00Z",
                1780573440,
                "I think no mechanic is on site. I walked down. Nothing arrived on 14 after a prolonged wait.",
                "status_update",
            ),
            (
                "msg-no-elevators",
                "2026-06-04T11:50:00Z",
                1780573800,
                "No elevators so far today. Someone said they tried to work on it overnight with no luck.",
                "still_out",
            ),
            ("msg-restored", "2026-06-04T20:08:00Z", 1780603680, "Both of them are working now", "restore"),
        ]
        for message_id, ts_iso, ts_epoch, text, event_type in messages:
            session.add(
                RawMessage(
                    message_id=message_id,
                    chat_name="455 Tenants",
                    sender="Tenant",
                    sender_hash=f"hash-{message_id}",
                    ts_iso=ts_iso,
                    ts_epoch=ts_epoch,
                    text=text,
                    attachments=None,
                    source="whatsapp_web",
                )
            )
            session.add(
                MessageDecision(
                    message_id=message_id,
                    incident_id="inc-capped-proofs",
                    chosen_source="rules",
                    is_issue=True,
                    category="elevator",
                    event_type=event_type,
                    confidence=85,
                    needs_review=event_type == "status_update",
                )
            )
        session.commit()

    sheets_sync.sync_public_updates_to_sheets()

    values = next(kwargs["body"]["values"] for kind, kwargs in service.calls if kind == "update")
    metrics = {row[0]: row[1] for row in values if row and row[0] in {"Latest update"}}
    assert metrics["Latest update"] == "Both elevators working"
    log_row = next(idx for idx, row in enumerate(values) if row[0] == "Public update log")
    rows = [row for row in values[log_row + 2:] if row and row[0]]

    restored_row = next(row for row in rows if row[0] == "2026-06-04 04:08 PM")
    assert restored_row[1] == "Both elevators working"
    assert restored_row[6] == "Both elevators were reported working."

    no_elevators_row = next(row for row in rows if row[0] == "2026-06-04 07:50 AM")
    assert no_elevators_row[1] == "Both elevators"
    assert no_elevators_row[6] == "Both elevators were reported as out."

    no_mechanic_row = next(row for row in rows if row[0] == "2026-06-04 07:44 AM")
    assert no_mechanic_row[1] == "Elevator repair not completed"
    assert no_mechanic_row[6] == "Elevator repair was reported not completed yet."


def test_sync_public_updates_shows_rough_elevator_ride_and_same_confirmation(client, monkeypatch):
    service = _FakeService()
    monkeypatch.setattr(sheets_sync, "_service", lambda: service)
    monkeypatch.setattr(sheets_sync, "_public_sheet_id", lambda: "public-sheet-123")
    monkeypatch.setenv("PUBLIC_UPDATES_CHAT_NAMES", "455 Tenants")
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")

    with get_session() as session:
        session.add_all([
            Incident(
                incident_id="inc-rough-north",
                category="elevator",
                asset="elevator_north",
                severity=4,
                status="open",
                start_ts="2026-05-27T02:53:00Z",
                start_ts_epoch=1779850380,
                last_ts_epoch=1779851400,
                title="North elevator made loud clunk and bounced",
                summary=(
                    "North lift made a loud clunk, bounced, and opened slowly. "
                    "A second resident confirmed the same north lift issue at 11 pm."
                ),
                proof_refs="msg-rough,msg-same",
                report_count=2,
                witness_count=2,
                confidence=90,
            ),
            RawMessage(
                message_id="msg-rough",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-rough",
                ts_iso="2026-05-27T02:53:00Z",
                ts_epoch=1779850380,
                text=(
                    "North lift made that unpleasant loud clunk sound when it delivered me to my flr, "
                    "the car bounced up and down slightly and the door opened in slo-mo at 10:30pm."
                ),
                attachments=None,
                source="whatsapp_web",
            ),
            RawMessage(
                message_id="msg-same",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-same",
                ts_iso="2026-05-27T03:10:00Z",
                ts_epoch=1779851400,
                text="Yes. Same. North lift at 11 pm.",
                attachments=None,
                source="whatsapp_web",
            ),
            ServiceRequestCase(
                service_request_number="311-27654875",
                incident_id="inc-rough-north",
                source="portal_playwright",
                complaint_type="Elevator or Escalator Complaint",
                status="submitted",
                submitted_at="2026-05-28T22:50:00Z",
            ),
        ])
        session.commit()

    sheets_sync.sync_public_updates_to_sheets()

    values = next(kwargs["body"]["values"] for kind, kwargs in service.calls if kind == "update")
    log_row = next(idx for idx, row in enumerate(values) if row[0] == "Public update log")
    rows = [row for row in values[log_row + 2:] if row and row[0]]

    rough_row = next(row for row in rows if row[1] == "North elevator operation issue")
    assert rough_row[3] == "311-27654875 (submitted)"
    assert rough_row[6] == "North elevator was reported making a loud clunk, bouncing, or opening slowly."

    same_row = next(row for row in rows if row[6] == "A second report confirmed the same north elevator issue.")
    assert same_row[1] == "North elevator"
    assert same_row[3] == "311-27654875 (submitted)"


def test_sync_public_updates_uses_floor_call_description_instead_of_outage_fallback(client, monkeypatch):
    service = _FakeService()
    monkeypatch.setattr(sheets_sync, "_service", lambda: service)
    monkeypatch.setattr(sheets_sync, "_public_sheet_id", lambda: "public-sheet-123")
    monkeypatch.setenv("PUBLIC_UPDATES_CHAT_NAMES", "455 Tenants")
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")

    text = (
        "So it seems impossible to call the elevator to the third floor. "
        "I waited for a very long time this morning, and finally gave up. "
        "I had wanted to go to the basement to do my laundry and there was a lot to carry. "
        "I tried again later, and waited and waited. "
        "I finally jumped into the elevator when it happened to stop on its way up. "
        "I just crowded in there with my laundry and everything and thankfully our neighbors can be so understanding! "
        "So that's how I finally made it down to the basement."
    )
    with get_session() as session:
        session.add_all([
            Incident(
                incident_id="inc-third-floor-call",
                category="elevator",
                asset=None,
                severity=4,
                status="open",
                start_ts="2026-05-29T21:39:00Z",
                start_ts_epoch=1780090740,
                last_ts_epoch=1780090740,
                title="Elevator not responding on third floor",
                summary=text,
                proof_refs="msg-third-floor-call",
                report_count=1,
                witness_count=1,
                confidence=90,
            ),
            RawMessage(
                message_id="msg-third-floor-call",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-third-floor-call",
                ts_iso="2026-05-29T21:39:00Z",
                ts_epoch=1780090740,
                text=text,
                attachments=None,
                source="whatsapp_web",
            ),
        ])
        session.commit()

    sheets_sync.sync_public_updates_to_sheets()

    values = next(kwargs["body"]["values"] for kind, kwargs in service.calls if kind == "update")
    metrics = {
        row[0]: row[1]
        for row in values
        if row and row[0] in {"Most common issue type", "Latest update"}
    }
    assert metrics["Most common issue type"] == "Elevator"
    assert metrics["Latest update"] == "Elevator not responding on third floor"

    category_row = next(row for row in values if row and row[0] == "Elevator")
    assert category_row[4] == "Elevator not responding on third floor"

    log_row = next(idx for idx, row in enumerate(values) if row[0] == "Public update log")
    update_row = next(row for row in values[log_row + 2:] if row and row[1] == "Elevator not responding on third floor")
    assert update_row[2] == "Elevator"
    assert update_row[6] == "Elevator not responding on third floor"
    assert "Elevator outage was reported as still down." not in " ".join(str(cell) for row in values for cell in row)


def test_sync_public_updates_filters_stale_bad_decisions_and_preserves_real_updates(client, monkeypatch):
    service = _FakeService()
    monkeypatch.setattr(sheets_sync, "_service", lambda: service)
    monkeypatch.setattr(sheets_sync, "_public_sheet_id", lambda: "public-sheet-123")
    monkeypatch.setenv("PUBLIC_UPDATES_CHAT_NAMES", "455 Tenants")
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")

    with get_session() as session:
        session.add_all([
            Incident(
                incident_id="inc-form",
                category="other",
                asset=None,
                severity=2,
                status="open",
                start_ts="2026-05-05T22:22:00Z",
                start_ts_epoch=1778019720,
                last_ts_epoch=1778019720,
                title="Partial functionality issue reported",
                summary="Two pages only work sometimes.",
                proof_refs="msg-form",
                report_count=1,
                witness_count=1,
                confidence=80,
            ),
            RawMessage(
                message_id="msg-form",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-form",
                ts_iso="2026-05-05T22:22:00Z",
                ts_epoch=1778019720,
                text="It's three pages but two of them only work sometimes",
                attachments=None,
                source="whatsapp_web",
            ),
            Incident(
                incident_id="inc-records",
                category="elevator",
                asset=None,
                severity=2,
                status="open",
                start_ts="2026-05-05T13:14:00Z",
                start_ts_epoch=1777986840,
                last_ts_epoch=1777986840,
                title="Common form records question",
                summary="Question about repair-hour records.",
                proof_refs="msg-records",
                report_count=1,
                witness_count=1,
                confidence=80,
            ),
            RawMessage(
                message_id="msg-records",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-records",
                ts_iso="2026-05-05T13:14:00Z",
                ts_epoch=1777986840,
                text="Is the common form listing the exact hours of breakages, when repair people are called and when they come for court records?",
                attachments=None,
                source="whatsapp_web",
            ),
            Incident(
                incident_id="inc-discussion",
                category="elevator",
                asset="elevator_both",
                severity=2,
                status="open",
                start_ts="2026-05-05T20:38:00Z",
                start_ts_epoch=1778013480,
                last_ts_epoch=1778013480,
                title="Both elevators",
                summary="Replacement construction discussion.",
                proof_refs="msg-discussion",
                report_count=1,
                witness_count=1,
                confidence=80,
            ),
            RawMessage(
                message_id="msg-discussion",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-discussion",
                ts_iso="2026-05-05T20:38:00Z",
                ts_epoch=1778013480,
                text="I understand that construction takes time, but the replacement schedule is cold comfort if they just fixed elevators that are broken.",
                attachments=None,
                source="whatsapp_web",
            ),
            Incident(
                incident_id="inc-north",
                category="elevator",
                asset="elevator_north",
                severity=4,
                status="open",
                start_ts="2026-05-05T11:25:00Z",
                start_ts_epoch=1777976700,
                last_ts_epoch=1777976700,
                title="North elevator outage",
                summary="South lift working, north lift not working.",
                proof_refs="msg-north",
                report_count=1,
                witness_count=1,
                confidence=90,
            ),
            RawMessage(
                message_id="msg-north",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-north",
                ts_iso="2026-05-05T11:25:00Z",
                ts_epoch=1777976700,
                text="South lift working, but not the north lift!",
                attachments=None,
                source="whatsapp_web",
            ),
            ServiceRequestCase(
                service_request_number="311-27372033",
                incident_id="inc-north",
                source="portal_playwright",
                complaint_type="Elevator or Escalator Complaint",
                status="In Progress",
                submitted_at="2026-05-05T11:30:00Z",
            ),
            Incident(
                incident_id="inc-dead",
                category="elevator",
                asset="elevator_both",
                severity=5,
                status="open",
                start_ts="2026-05-05T12:59:00Z",
                start_ts_epoch=1777982340,
                last_ts_epoch=1777982340,
                title="Both elevators outage",
                summary="Both elevators dead and mechanics expected.",
                proof_refs="msg-dead",
                report_count=1,
                witness_count=1,
                confidence=90,
            ),
            RawMessage(
                message_id="msg-dead",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-dead",
                ts_iso="2026-05-05T12:59:00Z",
                ts_epoch=1777982340,
                text='Both elevators are dead. Jacek says mechanics are "hopefully" coming',
                attachments=None,
                source="whatsapp_web",
            ),
            ServiceRequestCase(
                service_request_number="311-27372034",
                incident_id="inc-dead",
                source="portal_playwright",
                complaint_type="Elevator or Escalator Complaint",
                status="In Progress",
                submitted_at="2026-05-05T13:01:00Z",
            ),
            Incident(
                incident_id="inc-working",
                category="elevator",
                asset="elevator_both",
                severity=2,
                status="closed",
                start_ts="2026-04-30T21:21:00Z",
                start_ts_epoch=1777584060,
                last_ts_epoch=1777584060,
                title="Elevator restored",
                summary="Both lifts appear to be working normally.",
                proof_refs="msg-working",
                report_count=1,
                witness_count=1,
                confidence=85,
            ),
            RawMessage(
                message_id="msg-working",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-working",
                ts_iso="2026-04-30T21:21:00Z",
                ts_epoch=1777584060,
                text="Both lifts appear to be working normal, like not going down floor by floor.",
                attachments=None,
                source="whatsapp_web",
            ),
            ServiceRequestCase(
                service_request_number="311-27336265",
                incident_id="inc-working",
                source="portal_playwright",
                complaint_type="Elevator or Escalator Complaint",
                status="In Progress",
                submitted_at="2026-04-30T21:30:00Z",
            ),
            Incident(
                incident_id="inc-entry-leak",
                category="leaks_water_damage",
                asset=None,
                severity=4,
                status="open",
                start_ts="2026-04-28T22:57:00Z",
                start_ts_epoch=1777417020,
                last_ts_epoch=1777417020,
                title="Leak under sink and unauthorized entry reported",
                summary="Raw first-person apartment entry and leak report.",
                proof_refs="msg-entry-leak",
                report_count=1,
                witness_count=1,
                confidence=85,
            ),
            RawMessage(
                message_id="msg-entry-leak",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-entry-leak",
                ts_iso="2026-04-28T22:57:00Z",
                ts_epoch=1777417020,
                text="someone was in my apartment while i wasn't here. i have a leak under my sink.",
                attachments=None,
                source="whatsapp_web",
            ),
        ])
        session.commit()

    sheets_sync.sync_public_updates_to_sheets()

    values = next(kwargs["body"]["values"] for kind, kwargs in service.calls if kind == "update")
    log_row = next(idx for idx, row in enumerate(values) if row[0] == "Public update log")
    rows = [row for row in values[log_row + 2:] if row and row[0]]
    joined = "\n".join(" | ".join(str(cell) for cell in row[:7]) for row in rows)

    assert "Partial functionality issue reported" not in joined
    assert "Common form records question" not in joined
    assert "Replacement construction discussion" not in joined

    north_row = next(row for row in rows if row[1] == "North elevator")
    assert north_row[3] == "311-27372033 (In Progress)"

    dead_row = next(row for row in rows if row[1] == "Both elevators")
    assert dead_row[3] == "311-27372034 (In Progress)"
    assert "repair people were expected" in dead_row[6]
    assert "Jacek" not in dead_row[6]

    working_row = next(row for row in rows if row[1] == "Both elevators working normally")
    assert working_row[3] == ""
    assert "without floor-by-floor service" in working_row[6]

    entry_leak_row = next(row for row in rows if row[1] == "Under-sink leak and apartment entry concern")
    assert entry_leak_row[2] == "Leaks / water damage / Security / access"
    assert entry_leak_row[6] == "Resident reported an under-sink leak and possible apartment entry while no one was home."


def test_public_sync_excludes_sensitive_interpersonal_security_reports():
    incident = Incident(
        incident_id="inc-sensitive-security",
        category="security_access",
        severity=3,
        status="open",
        title="Unwanted close contact incident reported",
        summary="Private review required.",
        needs_review=True,
    )
    decision = MessageDecision(
        message_id="msg-sensitive-security",
        is_issue=True,
        category="security_access",
        event_type="new_issue",
        needs_review=True,
    )
    sensitive = RawMessage(
        message_id="msg-sensitive-security",
        chat_name="455 Tenants",
        sender="Tenant",
        sender_hash="hash-sensitive",
        text="A man tried walking up from behind me and squeezing himself next to me.",
        source="whatsapp_web",
    )
    building_access = RawMessage(
        message_id="msg-door-lock",
        chat_name="455 Tenants",
        sender="Tenant",
        sender_hash="hash-door",
        text="The lobby door lock is broken.",
        source="whatsapp_web",
    )

    normal_access_incident = Incident(
        incident_id="inc-door-lock",
        category="security_access",
        severity=2,
        status="open",
        title="Lobby door lock broken",
        summary="Door hardware needs repair.",
        needs_review=False,
    )

    assert sheets_sync._public_should_include_update(incident, sensitive, decision) is False
    assert sheets_sync._public_should_include_update(incident, building_access, decision) is False
    assert sheets_sync._public_should_include_update(normal_access_incident, building_access, decision) is True


def test_sync_public_updates_does_not_link_message_screenshots(client, monkeypatch, tmp_path):
    service = _FakeService()
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    tiny = media_dir / "tiny.png"
    tiny.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\rIHDR" + (508).to_bytes(4, "big") + (20).to_bytes(4, "big"))

    monkeypatch.setattr(sheets_sync, "_service", lambda: service)
    monkeypatch.setattr(sheets_sync, "_public_sheet_id", lambda: "public-sheet-123")
    monkeypatch.setenv("PUBLIC_UPDATES_CHAT_NAMES", "455 Tenants")
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))

    manifest = build_attachment_manifest(
        items=[{"kind": "message_screenshot", "status": "captured", "path": str(tiny), "filename": "tiny.png"}],
        source="whatsapp_web",
    )
    with get_session() as session:
        session.add_all([
            Incident(
                incident_id="inc-tiny",
                category="other",
                severity=2,
                status="open",
                last_ts_epoch=1776804420,
                title="Tiny screenshot evidence",
                summary="One-line message screenshot should not be exposed as tenant evidence.",
                proof_refs="msg-tiny",
                report_count=1,
                witness_count=1,
                confidence=85,
            ),
            RawMessage(
                message_id="msg-tiny",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-tiny",
                ts_iso="2026-04-21T18:07:00Z",
                ts_epoch=1776804420,
                text="One-line message",
                attachments=manifest,
                source="whatsapp_web",
            ),
        ])
        session.commit()

    sheets_sync.sync_public_updates_to_sheets()

    values = next(kwargs["body"]["values"] for kind, kwargs in service.calls if kind == "update")
    all_incidents_row = next(idx for idx, row in enumerate(values) if row[0] == "Public update log")
    incident_rows = [row for row in values[all_incidents_row + 2:] if row and row[0]]
    joined = "\n".join(" | ".join(str(cell) for cell in row) for row in incident_rows)
    assert "Tiny screenshot evidence" not in joined
    assert "msg-tiny/0" not in joined


def test_sync_public_updates_uses_real_media_instead_of_bubble_screenshot(client, monkeypatch, tmp_path):
    service = _FakeService()
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    bubble = media_dir / "bubble.png"
    photo = media_dir / "photo.png"
    bubble.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\rIHDR" + (246).to_bytes(4, "big") + (165).to_bytes(4, "big"))
    photo.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\rIHDR" + (900).to_bytes(4, "big") + (700).to_bytes(4, "big"))

    monkeypatch.setattr(sheets_sync, "_service", lambda: service)
    monkeypatch.setattr(sheets_sync, "_public_sheet_id", lambda: "public-sheet-123")
    monkeypatch.setenv("PUBLIC_UPDATES_CHAT_NAMES", "455 Tenants")
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))

    manifest = build_attachment_manifest(
        items=[
            {"kind": "message_screenshot", "status": "captured", "path": str(bubble), "filename": "bubble.png"},
            {"kind": "image", "status": "downloaded", "path": str(photo), "filename": "photo.png"},
        ],
        source="whatsapp_web",
    )
    with get_session() as session:
        session.add_all([
            Incident(
                incident_id="inc-photo",
                category="elevator",
                severity=4,
                status="open",
                last_ts_epoch=1776804420,
                title="Photo evidence",
                summary="Downloaded media should be shown instead of the WhatsApp bubble screenshot.",
                proof_refs="msg-photo",
                report_count=1,
                witness_count=1,
                confidence=90,
            ),
            RawMessage(
                message_id="msg-photo",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-photo",
                ts_iso="2026-04-21T18:07:00Z",
                ts_epoch=1776804420,
                text="Photo attached",
                attachments=manifest,
                source="whatsapp_web",
            ),
        ])
        session.commit()

    sheets_sync.sync_public_updates_to_sheets()

    values = next(kwargs["body"]["values"] for kind, kwargs in service.calls if kind == "update")
    all_incidents_row = next(idx for idx, row in enumerate(values) if row[0] == "Public update log")
    row = next(row for row in values[all_incidents_row + 2:] if len(row) >= 8 and row[1] == "Photo evidence")
    assert "/media/whatsapp/msg-photo/1?v=" in row[4]
    assert row[5].startswith("https://tenant.example/media/whatsapp/msg-photo/1?v=")
    assert "msg-photo/0" not in row[4]
    assert "msg-photo/0" not in row[5]


def test_sync_public_updates_does_not_link_bubble_when_media_was_not_captured(client, monkeypatch, tmp_path):
    service = _FakeService()
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    bubble = media_dir / "bubble.png"
    bubble.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\rIHDR" + (246).to_bytes(4, "big") + (165).to_bytes(4, "big"))

    monkeypatch.setattr(sheets_sync, "_service", lambda: service)
    monkeypatch.setattr(sheets_sync, "_public_sheet_id", lambda: "public-sheet-123")
    monkeypatch.setenv("PUBLIC_UPDATES_CHAT_NAMES", "455 Tenants")
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(media_dir))

    manifest = build_attachment_manifest(
        items=[
            {"kind": "message_screenshot", "status": "captured", "path": str(bubble), "filename": "bubble.png"},
            {"kind": "image", "label": "metadata_only", "status": "metadata_only"},
        ],
        source="whatsapp_web",
    )
    with get_session() as session:
        session.add_all([
            Incident(
                incident_id="inc-missing-media",
                category="elevator",
                severity=4,
                status="open",
                last_ts_epoch=1776804420,
                title="Missing media evidence",
                summary="A WhatsApp card thumbnail should not be exposed as the real media evidence.",
                proof_refs="msg-missing-media",
                report_count=1,
                witness_count=1,
                confidence=90,
            ),
            RawMessage(
                message_id="msg-missing-media",
                chat_name="455 Tenants",
                sender="Tenant",
                sender_hash="hash-missing-media",
                ts_iso="2026-04-21T18:07:00Z",
                ts_epoch=1776804420,
                text="Image message",
                attachments=manifest,
                source="whatsapp_web",
            ),
        ])
        session.commit()

    sheets_sync.sync_public_updates_to_sheets()

    values = next(kwargs["body"]["values"] for kind, kwargs in service.calls if kind == "update")
    all_incidents_row = next(idx for idx, row in enumerate(values) if row[0] == "Public update log")
    incident_rows = [row for row in values[all_incidents_row + 2:] if row and row[0]]
    joined = "\n".join(" | ".join(str(cell) for cell in row) for row in incident_rows)
    assert "Missing media evidence" not in joined
    assert "msg-missing-media/0" not in joined


def test_sync_dashboard_to_sheets_hides_public_share_url_without_dedicated_public_workbook(client, monkeypatch):
    service = _FakeService()
    monkeypatch.setattr(sheets_sync, "_service", lambda: service)
    monkeypatch.setattr(sheets_sync, "_sheet_id", lambda: "internal-sheet-123")
    monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "internal-sheet-123")
    monkeypatch.delenv("GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID", raising=False)
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")

    sheets_sync.sync_dashboard_to_sheets()

    values = next(
        kwargs["body"]["values"]
        for kind, kwargs in service.calls
        if kind == "update" and kwargs["range"] == "Dashboard!A1"
    )
    metrics = {row[0]: row[1] for row in values if len(row) >= 2 and row[0]}
    assert metrics["spreadsheet_url"] == "https://docs.google.com/spreadsheets/d/internal-sheet-123/edit"
    assert metrics["public_updates_sheet_url"] == ""
    assert metrics["report_form_url"] == "https://tenant.example/report"
