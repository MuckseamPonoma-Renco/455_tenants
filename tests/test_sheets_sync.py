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

    all_incidents_row = next(idx for idx, row in enumerate(values) if row[0] == "All incidents")
    assert values[all_incidents_row + 1] == ["Updated", "Issue", "Category", "311 follow-up", "Preview", "Open evidence", "Summary", "", "", ""]
    incident_rows = values[all_incidents_row + 2:]
    public_issue_row = next(row for row in incident_rows if len(row) >= 10 and row[1] == "South elevator")
    assert public_issue_row[2] == "Elevator"
    assert public_issue_row[3] == "311-12345678 (submitted)"
    assert public_issue_row[4].startswith('=HYPERLINK("https://tenant.example/media/whatsapp/msg-public/0?v=')
    assert ',IMAGE("https://tenant.example/media/whatsapp/msg-public/0?v=' in public_issue_row[4]
    assert public_issue_row[4].endswith('",4,110,240))')
    assert public_issue_row[5].startswith("https://tenant.example/media/whatsapp/msg-public/0?v=")
    old_issue_row = next(row for row in incident_rows if len(row) >= 10 and row[1] == "Lobby door did not close")
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
    all_incidents_row = next(idx for idx, row in enumerate(values) if row[0] == "All incidents")
    incident_rows = [row for row in values[all_incidents_row + 2:] if row and row[0]]
    alarm_rows = [row for row in incident_rows if len(row) >= 7 and row[6] == duplicate_text]

    assert len(alarm_rows) == 1
    assert alarm_rows[0][1] == "Alarm rang on an unknown elevator"
    assert alarm_rows[0][3] == "311-27091967 (In Progress)"
    assert "/media/whatsapp/msg-alarm-old/0?v=" in alarm_rows[0][4]
    assert alarm_rows[0][5].startswith("https://tenant.example/media/whatsapp/msg-alarm-old/0?v=")
    metrics = {row[0]: row[1] for row in values if row and row[0] in {"Incidents", "311 filings"}}
    assert metrics == {"Incidents": 1, "311 filings": 1}


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
    all_incidents_row = next(idx for idx, row in enumerate(values) if row[0] == "All incidents")
    row = next(row for row in values[all_incidents_row + 2:] if len(row) >= 8 and row[1] == "Tiny screenshot evidence")
    assert row[4] == ""
    assert row[5] == ""


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
    all_incidents_row = next(idx for idx, row in enumerate(values) if row[0] == "All incidents")
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
    all_incidents_row = next(idx for idx, row in enumerate(values) if row[0] == "All incidents")
    row = next(row for row in values[all_incidents_row + 2:] if len(row) >= 8 and row[1] == "Missing media evidence")
    assert row[4] == ""
    assert row[5] == ""


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
