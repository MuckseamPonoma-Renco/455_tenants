from packages.db import FilingJob, Incident, MessageDecision, RawMessage, get_session
from packages.sheets import sync as sheets_sync
from packages.sheets.sync import _elevator_status_from_incidents, _replace_tab_values, _should_skip_duplicate_tasker_decision


class _FakeRequest:
    def __init__(self, calls, kind, kwargs):
        self.calls = calls
        self.kind = kind
        self.kwargs = kwargs

    def execute(self):
        self.calls.append((self.kind, self.kwargs))
        return {}


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


class _FakeService:
    def __init__(self):
        self.calls = []

    def spreadsheets(self):
        return _FakeSpreadsheets(self.calls)


def test_replace_tab_values_clears_stale_rows_before_update():
    service = _FakeService()

    _replace_tab_values(service, "sheet-123", "Incidents", [["incident_id"], ["inc-1"]])

    assert [kind for kind, _kwargs in service.calls] == ["clear", "update"]
    assert service.calls[0][1]["spreadsheetId"] == "sheet-123"
    assert service.calls[0][1]["range"] == "Incidents!A:ZZ"
    assert service.calls[1][1]["range"] == "Incidents!A1"
    assert service.calls[1][1]["body"] == {"values": [["incident_id"], ["inc-1"]]}


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

    values = service.calls[1][1]["body"]["values"]
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
