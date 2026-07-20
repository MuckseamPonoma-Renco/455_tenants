import datetime as dt

from scripts.check_public_health import validate_health


def _payload():
    return {
        "ok": True,
        "database_configured": True,
        "sheets_disabled": False,
        "sheets_configured": True,
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
        max_import_age_seconds=3600,
    )

    assert failures == []
    assert details == {"whatsapp_capture_age_seconds": 0, "chat_export_sync_age_seconds": 900}


def test_validate_health_rejects_locked_or_stale_services():
    payload = _payload()
    payload["whatsapp_capture"]["state"] = "login_required"
    payload["whatsapp_capture"]["login_required"] = True
    payload["chat_export_sync"]["last_checked_at"] = "2026-07-20T00:00:00Z"

    failures, _ = validate_health(
        payload,
        now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
        max_capture_age_seconds=600,
        max_import_age_seconds=3600,
    )

    assert "WhatsApp capture is not ready" in failures
    assert "chat export sync is stale (7200s old)" in failures
