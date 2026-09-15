from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from scripts import audit_public_watchdog_tabs as audit


NOW = datetime(2026, 9, 14, 23, 55, tzinfo=timezone.utc)
CURRENT = "2026-09-14T23:54:00Z"
OLD = "2026-09-06T12:00:00Z"


def _check(expected_timestamp="", live_timestamp="", *, checked_by=None,
           live_checked_by=None, topic=None, system_timestamp=CURRENT):
    spec = next(row for row in audit._resolved_tab_specs() if row.logical_name == "ElevatorWatch")
    checked_by = checked_by or audit.HUMAN_ONLY_PHYSICAL_CHECK
    expected = [
        list(spec.headers),
        [topic or audit.LOBBY_POSTING_EVIDENCE_TOPIC, "Resident check needed", "Cannot inspect lobby remotely",
         checked_by, expected_timestamp, "Photo or note if observed", ""],
        [audit.SYSTEM_WATCHDOG_FRESHNESS_TOPIC, "Report observations", "Automatic source checks",
         "System policy", CURRENT, "Only observations", ""],
    ]
    live = [list(row) for row in expected]
    live[1][4] = live_timestamp
    live[2][4] = system_timestamp
    if live_checked_by is not None:
        live[1][3] = live_checked_by
    metadata = {"sheets": [{"properties": {
        "title": "ElevatorWatch", "hidden": False,
        "gridProperties": {"rowCount": 1000, "columnCount": 26},
    }}]}
    return audit._audit_tab(
        spec, audit.ExpectedTab(expected, "USER_ENTERED"), audit.LiveTab(live, live),
        metadata=metadata, workbook_tz=ZoneInfo("America/New_York"), now=NOW,
        max_age_seconds=6.5 * 3600, max_drift_seconds=90 * 60, limit=20,
    )


def test_unperformed_lobby_check_is_not_a_failed_automatic_check():
    result = _check()
    assert result["ok"] is True
    check = result["volatile_last_checked"][0]
    assert check["timestamp_semantics"] == "manual_physical_evidence"
    assert check["evidence_timestamp_present"] is False
    assert check["freshness_required"] is False
    assert result["system_watchdog_freshness"]["ok"] is True


def test_old_manual_observation_is_preserved_not_freshened():
    result = _check(OLD, OLD)
    assert result["ok"] is True
    assert result["volatile_last_checked"][0]["expected_iso"] == OLD
    assert result["volatile_last_checked"][0]["age_seconds"] > 7 * 86400


@pytest.mark.parametrize("expected,live", [
    ("", CURRENT), (CURRENT, ""), (OLD, CURRENT),
    ("2026-09-20T12:00:00Z", "2026-09-20T12:00:00Z"),
    ("2026-09-14", "2026-09-14"),
])
def test_manual_timestamp_still_must_match_real_evidence(expected, live):
    assert _check(expected, live)["ok"] is False


@pytest.mark.parametrize("timestamp", ["", OLD])
def test_automatic_lobby_permit_rule_still_requires_fresh_timestamp(timestamp):
    result = _check(timestamp, timestamp, checked_by="Automatic rule from permit status")
    assert result["ok"] is False
    assert result["volatile_last_checked"][0]["timestamp_semantics"] == "automatic_check"


def test_live_label_cannot_turn_expected_automatic_check_into_manual_exemption():
    result = _check(checked_by="Automatic rule from permit status",
                    live_checked_by=audit.HUMAN_ONLY_PHYSICAL_CHECK)
    assert result["ok"] is False


def test_unrelated_topic_does_not_gain_manual_timestamp_exemption():
    assert _check(topic="Full elevator replacement permit")["ok"] is False


def test_manual_check_cannot_hide_stale_system_heartbeat():
    result = _check(system_timestamp=OLD)
    assert result["ok"] is False
    assert result["system_watchdog_freshness"]["ok"] is False
