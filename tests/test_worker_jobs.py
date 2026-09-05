from contextlib import contextmanager

from packages import worker_jobs


class _Session:
    committed = False

    def commit(self):
        self.committed = True


@contextmanager
def _session_context(session):
    yield session


def test_status_sync_does_not_republish_sheets_without_changes(monkeypatch):
    session = _Session()
    sheet_syncs = []
    audit_events = []
    monkeypatch.setattr(worker_jobs, "get_session", lambda: _session_context(session))
    monkeypatch.setattr(worker_jobs, "sync_all_case_statuses", lambda _session: [])
    monkeypatch.setattr(worker_jobs, "_safe_sync_sheets", lambda: sheet_syncs.append(True))
    monkeypatch.setattr(worker_jobs, "append_audit_event", lambda *args: audit_events.append(args))

    result = worker_jobs.sync_311_statuses()

    assert result == {"ok": True, "updated": 0}
    assert session.committed is True
    assert sheet_syncs == []
    assert audit_events == [
        ("SYNC_311_STATUSES", None, {"updated": 0, "sheet_sync": "skipped_no_changes"})
    ]


def test_status_sync_republishes_sheets_after_a_change(monkeypatch):
    session = _Session()
    sheet_syncs = []
    audit_events = []
    monkeypatch.setattr(worker_jobs, "get_session", lambda: _session_context(session))
    monkeypatch.setattr(
        worker_jobs,
        "sync_all_case_statuses",
        lambda _session: [{"service_request_number": "311-12345678", "status": "Closed"}],
    )
    monkeypatch.setattr(worker_jobs, "_safe_sync_sheets", lambda: sheet_syncs.append(True))
    monkeypatch.setattr(worker_jobs, "append_audit_event", lambda *args: audit_events.append(args))

    result = worker_jobs.sync_311_statuses()

    assert result == {"ok": True, "updated": 1}
    assert session.committed is True
    assert sheet_syncs == [True]
    assert audit_events == [
        ("SYNC_311_STATUSES", None, {"updated": 1, "sheet_sync": "updated"})
    ]
