import datetime as dt

from scripts.check_public_health import validate_health


def _payload():
    return {
        "ok": True,
        "database_configured": True,
        "database_ready": True,
        "sheets_disabled": False,
        "sheets_configured": True,
        "storage": {"state": "ready", "low_disk": False},
        "automation": {
            "state": "ready",
            "last_cycle_at": "2026-07-20T02:00:00Z",
            "has_error": False,
        },
        "whatsapp_capture": {
            "state": "ready",
            "login_required": False,
            "last_cycle_at": "2026-07-20T02:00:00Z",
            "has_error": False,
        },
        "chat_export_sync": {
            "state": "ready",
            "last_checked_at": "2026-07-20T01:45:00Z",
            "has_error": False,
        },
    }


def test_validate_health_accepts_fresh_operational_state():
    failures, details = validate_health(
        _payload(),
        now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
        max_capture_age_seconds=600,
        max_automation_age_seconds=1200,
        max_import_age_seconds=3600,
    )

    assert failures == []
    assert details == {
        "storage_state": "ready",
        "automation_age_seconds": 0,
        "whatsapp_capture_age_seconds": 0,
        "chat_export_sync_age_seconds": 900,
    }


def test_validate_health_accepts_fresh_startup_and_working_automation():
    for state in ("starting", "working"):
        payload = _payload()
        payload["automation"]["state"] = state

        failures, _ = validate_health(
            payload,
            now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
            max_capture_age_seconds=600,
            max_automation_age_seconds=1200,
            max_import_age_seconds=3600,
        )

        assert failures == []


def test_validate_health_rejects_locked_or_stale_services():
    payload = _payload()
    payload["whatsapp_capture"]["state"] = "login_required"
    payload["whatsapp_capture"]["login_required"] = True
    payload["chat_export_sync"]["last_checked_at"] = "2026-07-20T00:00:00Z"

    failures, _ = validate_health(
        payload,
        now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
        max_capture_age_seconds=600,
        max_automation_age_seconds=1200,
        max_import_age_seconds=3600,
    )

    assert "WhatsApp capture is not ready" in failures
    assert "chat export sync is stale (7200s old)" in failures


def test_validate_health_rejects_low_host_storage():
    payload = _payload()
    payload["storage"] = {"state": "low_disk", "low_disk": True}

    failures, details = validate_health(
        payload,
        now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
        max_capture_age_seconds=600,
        max_automation_age_seconds=1200,
        max_import_age_seconds=3600,
    )

    assert "host storage is low_disk" in failures
    assert details["storage_state"] == "low_disk"


def test_validate_health_rejects_incomplete_host_storage_state():
    payload = _payload()
    payload["storage"] = {"state": "ready"}

    failures, _ = validate_health(
        payload,
        now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
        max_capture_age_seconds=600,
        max_automation_age_seconds=1200,
        max_import_age_seconds=3600,
    )

    assert "host storage is ready" in failures


def test_validate_health_rejects_unreachable_database_and_stale_automation():
    payload = _payload()
    payload["database_ready"] = False
    payload["automation"]["last_cycle_at"] = "2026-07-20T01:00:00Z"

    failures, _ = validate_health(
        payload,
        now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
        max_capture_age_seconds=600,
        max_automation_age_seconds=1200,
        max_import_age_seconds=3600,
    )

    assert "database is not reachable" in failures
    assert "automation is stale (3600s old)" in failures
