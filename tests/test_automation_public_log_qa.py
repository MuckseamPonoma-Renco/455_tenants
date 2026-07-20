import scripts.run_automation_daemon as automation


def test_public_log_qa_does_not_resync_after_a_live_read_error(monkeypatch):
    calls = []
    events = []

    def fake_audit(**kwargs):
        calls.append(kwargs)
        return {
            "ok": False,
            "days": kwargs["days"],
            "expected_recent_rows": 0,
            "live_recent_rows": 0,
            "source_recent_rows": 0,
            "missing_recent_rows": [],
            "unexpected_recent_rows": [],
            "missing_source_rows": [],
            "expected_latest_update": "Both elevators working",
            "live_latest_update": "",
            "live_read_error": "Google Sheets read timed out",
        }

    monkeypatch.setattr(automation, "run_public_tenant_log_audit", fake_audit)
    monkeypatch.setattr(automation, "append_audit_event", lambda event, _incident_id, meta: events.append((event, meta)))
    monkeypatch.setattr(automation, "daily_hash_chain", lambda: None)

    result = automation._public_tenant_log_qa()

    assert len(calls) == 1
    assert calls[0]["resync"] is False
    assert result["repair_skipped"] == "live_read_error"
    assert events[0][0] == "PUBLIC_TENANT_LOG_QA_DEFERRED"


def test_run_step_survives_audit_persistence_failure(monkeypatch):
    monkeypatch.setattr(automation, "append_audit_event", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk unavailable")))
    monkeypatch.setattr(automation, "daily_hash_chain", lambda: None)

    result = automation._run_step("test", lambda: (_ for _ in ()).throw(RuntimeError("worker failed")))

    assert result is None
