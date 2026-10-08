import pytest

from packages import worker_jobs


SYNC_FUNCTIONS = (
    "sync_incidents_to_sheets",
    "sync_dashboard_to_sheets",
    "sync_coverage_to_sheets",
    "sync_311_cases_to_sheets",
    "sync_311_queue_to_sheets",
    "sync_decisions_to_sheets",
    "sync_replacement_watchdog_to_sheets",
    "sync_public_updates_to_sheets",
)


def test_private_failure_does_not_suppress_public_views(monkeypatch):
    attempted = []

    def replacement(name):
        def sync():
            attempted.append(name)
            if name in {"sync_incidents_to_sheets", "sync_decisions_to_sheets"}:
                raise RuntimeError("workbook unavailable")
        sync.__name__ = name
        return sync

    for name in SYNC_FUNCTIONS:
        monkeypatch.setattr(worker_jobs, name, replacement(name))

    with pytest.raises(RuntimeError) as failure:
        worker_jobs.sync_all_sheets()

    assert attempted == list(SYNC_FUNCTIONS)
    assert "sync_incidents_to_sheets" in str(failure.value)
    assert "sync_decisions_to_sheets" in str(failure.value)


def test_full_resync_reports_failed_refresh(monkeypatch):
    monkeypatch.setattr(worker_jobs, "_safe_sync_sheets", lambda: False)
    events = []
    monkeypatch.setattr(worker_jobs, "append_audit_event", lambda *args: events.append(args))
    monkeypatch.setattr(worker_jobs, "daily_hash_chain", lambda: None)

    assert worker_jobs.full_resync_sheets() == {"ok": False}
    assert events == [("FULL_RESYNC_SHEETS", None, {"ok": False})]
