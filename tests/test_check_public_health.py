import datetime as dt
import pytest

from scripts.check_public_health import validate_health
from packages.public_records.config import source_configs


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
        "watchdog": {
            "state": "ready", "has_error": False, "source_errors": 0, "sheet_errors": 0, "sheet_readback_ok": True,
            "last_success_at": "2026-07-20T00:00:00Z", "interval_seconds": 21600,
            "sources": [
                {"source_key": source.key, "state": "ready", "source_errors": 0, "last_success_at": "2026-07-20T00:00:00Z"}
                for source in source_configs()
            ],
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
        "cloud_export_receiver": {
            "state": "not_configured",
            "configured": False,
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
        "watchdog_max_age_seconds": 23400,
        "watchdog_state": "ready",
        "watchdog_age_seconds": 7200,
        "watchdog_source_count": len(source_configs()),
        "whatsapp_capture_age_seconds": 0,
        "chat_export_sync_age_seconds": 900,
        "cloud_export_receiver_state": "not_configured",
        "cloud_export_receiver_configured": False,
    }


def test_validate_health_accepts_missing_cloud_receiver_when_not_required():
    payload = _payload()

    failures, details = validate_health(
        payload,
        now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
        max_capture_age_seconds=600,
        max_automation_age_seconds=1200,
        max_import_age_seconds=3600,
    )

    assert failures == []
    assert details["cloud_export_receiver_configured"] is False


def test_validate_health_rejects_missing_cloud_receiver_when_required():
    payload = _payload()

    failures, details = validate_health(
        payload,
        now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
        max_capture_age_seconds=600,
        max_automation_age_seconds=1200,
        max_import_age_seconds=3600,
        require_cloud_export_receiver=True,
    )

    assert "cloud export receiver is not_configured" in failures
    assert details["cloud_export_receiver_configured"] is False


def test_validate_health_accepts_required_configured_cloud_receiver():
    payload = _payload()
    payload["cloud_export_receiver"] = {"state": "configured", "configured": True}

    failures, details = validate_health(
        payload,
        now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
        max_capture_age_seconds=600,
        max_automation_age_seconds=1200,
        max_import_age_seconds=3600,
        require_cloud_export_receiver=True,
    )

    assert failures == []
    assert details["cloud_export_receiver_configured"] is True


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


def test_validate_health_can_report_quota_block_as_warning_only():
    payload = _payload()
    payload["chat_export_sync"].update(
        {
            "state": "blocked_model_review",
            "has_error": True,
        }
    )

    failures, details = validate_health(
        payload,
        now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
        max_capture_age_seconds=600,
        max_automation_age_seconds=1200,
        max_import_age_seconds=3600,
        allow_blocked_model_review=True,
    )

    assert failures == []
    assert details["chat_export_sync_warning"] == "blocked_model_review"


def test_validate_health_still_rejects_quota_block_without_explicit_allowance():
    payload = _payload()
    payload["chat_export_sync"].update(
        {
            "state": "blocked_model_review",
            "has_error": True,
        }
    )

    failures, _ = validate_health(
        payload,
        now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
        max_capture_age_seconds=600,
        max_automation_age_seconds=1200,
        max_import_age_seconds=3600,
    )

    assert "chat export sync is blocked_model_review" in failures
    assert "chat export sync has an error" in failures


def test_validate_health_reports_fresh_cloud_processing_as_in_progress():
    payload = _payload()
    payload["chat_export_sync"].update(
        {
            "state": "sheet_readback_pending",
            "latest_export": {
                "source": "cloud_receiver",
                "status": "sheet_readback_pending",
                "stages": {"sheet_sync": "complete", "sheet_readback": "pending"},
            },
        }
    )

    failures, details = validate_health(
        payload,
        now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
        max_capture_age_seconds=600,
        max_automation_age_seconds=1200,
        max_import_age_seconds=3600,
    )

    assert failures == []
    assert details["chat_export_sync_warning"] == "pipeline_in_progress"
    assert details["chat_export_latest_status"] == "sheet_readback_pending"
    assert details["chat_export_latest_stages"] == {
        "sheet_sync": "complete",
        "sheet_readback": "pending",
    }


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


@pytest.mark.parametrize("change", [
    {"state": "degraded"}, {"has_error": True}, {"source_errors": 1}, {"sheet_errors": 1}, {"sheet_readback_ok": False}, {"interval_seconds": 0},
    {"last_success_at": None}, {"last_success_at": "2026-07-19T18:00:00Z"},
    {"last_success_at": "2026-07-20T02:00:01Z"}, {"sources": []}, {"sources": None},
])
def test_public_monitor_rejects_unverified_watchdog_despite_fresh_heartbeat(change):
    payload = _payload()
    payload["watchdog"].update(change)
    failures, _ = validate_health(payload, now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
        max_capture_age_seconds=600, max_automation_age_seconds=1200, max_import_age_seconds=3600)
    assert any("watchdog" in failure for failure in failures)


def test_public_monitor_requires_every_source_and_checks_source_freshness():
    for mutation in ("missing", "duplicate", "stale", "partial"):
        payload = _payload()
        sources = payload["watchdog"]["sources"]
        if mutation == "missing":
            sources.pop()
        elif mutation == "duplicate":
            sources[-1] = dict(sources[0])
        elif mutation == "stale":
            sources[-1]["last_success_at"] = "2026-07-19T18:00:00Z"
        else:
            sources[-1]["state"] = "partial"
        failures, _ = validate_health(payload, now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
            max_capture_age_seconds=600, max_automation_age_seconds=1200, max_import_age_seconds=3600)
        assert "watchdog per-source completion evidence is incomplete, failed, or stale" in failures


def test_public_monitor_accepts_five_hour_sources_independently_of_heartbeat_limit():
    payload = _payload()
    payload["watchdog"]["last_success_at"] = "2026-07-19T21:00:00Z"
    for source in payload["watchdog"]["sources"]:
        source["last_success_at"] = "2026-07-19T21:00:00Z"
    failures, details = validate_health(payload, now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
        max_capture_age_seconds=600, max_automation_age_seconds=1200, max_import_age_seconds=3600)
    assert failures == []
    assert details["watchdog_age_seconds"] == 18000


def test_public_monitor_rejects_missing_watchdog_field():
    payload = _payload()
    payload.pop("watchdog")
    failures, _ = validate_health(payload, now=dt.datetime(2026, 7, 20, 2, 0, tzinfo=dt.UTC),
        max_capture_age_seconds=600, max_automation_age_seconds=1200, max_import_age_seconds=3600)
    assert "watchdog health is missing" in failures
