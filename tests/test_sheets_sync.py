from packages.db import RawMessage
from packages.sheets.sync import _replace_tab_values, _should_skip_duplicate_tasker_decision


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

    kept = {("455 Tenants", "Molly", "Btw, I did forward the News12 story to Weinreb."): 1775421265}

    assert _should_skip_duplicate_tasker_decision(raw, kept) is True
