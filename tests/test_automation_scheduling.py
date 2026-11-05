from types import SimpleNamespace

import pytest

import scripts.run_automation_daemon as automation


def _run_loop(monkeypatch, *, filing_results, duration, status_sync_seconds=0, filing_idle_seconds=None):
    clock = [0.0]
    filing_calls = []
    status_calls = []
    heartbeats = []
    results = iter(filing_results)
    args = SimpleNamespace(
        headful=False,
        skip_lookup=False,
        poll_seconds=None,
        filing_idle_seconds=filing_idle_seconds,
        error_sleep_seconds=None,
        status_sync_seconds=status_sync_seconds,
        public_record_sync_seconds=0,
        public_tenant_log_audit_seconds=0,
        burst_size=2,
        between_jobs_seconds=5,
        startup_catchup_limit=0,
    )

    def sleep(seconds):
        clock[0] += seconds
        if clock[0] > duration:
            raise KeyboardInterrupt

    def filing(**_kwargs):
        filing_calls.append(clock[0])
        result = next(results, {"ok": True, "job": None})
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setenv("AUTOMATION_POLL_SECONDS", "60")
    monkeypatch.delenv("AUTOMATION_FILING_IDLE_SECONDS", raising=False)
    monkeypatch.setattr(automation, "load_local_env_file", lambda: None)
    monkeypatch.setattr(automation, "parse_args", lambda: args)
    monkeypatch.setattr(automation.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(automation.time, "sleep", sleep)
    monkeypatch.setattr(automation, "_record_audit_event", lambda *_args: None)
    monkeypatch.setattr(automation, "update_watchdog_status", lambda **_kwargs: None)
    monkeypatch.setattr(automation, "_write_automation_status", lambda **kwargs: heartbeats.append((clock[0], kwargs)))
    monkeypatch.setattr(automation, "run_portal_filing_once", filing)
    monkeypatch.setattr(automation, "sync_311_statuses", lambda: status_calls.append(clock[0]) or {})

    with pytest.raises(KeyboardInterrupt):
        automation.main()
    return filing_calls, status_calls, heartbeats


def test_idle_filing_scans_do_not_follow_heartbeat_or_periodic_maintenance(monkeypatch):
    filings, status_syncs, heartbeats = _run_loop(
        monkeypatch, filing_results=[], duration=960, status_sync_seconds=120,
    )

    assert len(filings) == 2
    assert filings[0] == 0
    assert 900 <= filings[1] <= 960
    assert len(status_syncs) >= 7
    ready_times = [stamp for stamp, status in heartbeats if status["state"] == "ready"]
    assert len(ready_times) > 15
    assert max(b - a for a, b in zip(ready_times, ready_times[1:])) <= 60


def test_active_filing_batches_drain_quickly_then_return_to_idle_schedule(monkeypatch):
    jobs = [{"ok": True, "job_id": job_id} for job_id in range(1, 4)]

    filings, _, _ = _run_loop(monkeypatch, filing_results=jobs, duration=100)

    assert filings == [0, 5, 10, 15]


def test_failed_filing_scan_does_not_trigger_empty_queue_backoff(monkeypatch):
    filings, _, _ = _run_loop(
        monkeypatch, filing_results=[RuntimeError("database temporarily unavailable")], duration=100,
    )

    assert filings == [0, 60]


def test_idle_filing_interval_remains_configurable(monkeypatch):
    filings, _, _ = _run_loop(
        monkeypatch, filing_results=[], duration=360, filing_idle_seconds=180,
    )

    assert filings == [0, 180, 360]


def test_session_probe_schedule_and_short_failure_retry(monkeypatch):
    checked = []
    released = []
    states = iter(['ready', 'login_required', 'ready'])
    monkeypatch.setenv('AUTO_FILE_ENABLED', '1')
    monkeypatch.setattr(automation.session_health, 'configured', lambda: True)
    monkeypatch.setattr(automation.session_health, 'check_interval_seconds', lambda: 1800)
    def probe():
        checked.append(automation.time.monotonic())
        return {'state': next(states, 'ready')}
    monkeypatch.setattr(automation.session_health, 'check_saved_session', probe)
    monkeypatch.setattr(automation, 'release_verified_sign_in_holds', lambda: released.append(automation.time.monotonic()) or 0)
    filings, _, _ = _run_loop(monkeypatch, filing_results=[], duration=2160)
    assert checked == [0, 1800, 2100]
    assert released == [0, 2100]
    assert filings == [0, 900, 1800]


def test_failed_review_recovery_is_periodic_and_backs_off_errors(monkeypatch):
    calls=[]
    states=iter([True,False,True])
    monkeypatch.setattr(automation,'llm_enabled',lambda:True)
    monkeypatch.setattr(automation.session_health,'configured',lambda:False)
    def retry():
        calls.append(automation.time.monotonic())
        return {'ok':next(states,True),'recovered':0,'queued_jobs':0}
    monkeypatch.setattr(automation,'retry_failed_model_reviews',retry)
    _run_loop(monkeypatch,filing_results=[],duration=4560)
    assert calls==[0,900,4500]
