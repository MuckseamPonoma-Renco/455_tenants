import datetime as dt
import json
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    ("database", "running", "expected_state", "expected_repair", "reason_fragment"),
    [
        ({"database_configured": True, "database_check": "not_requested", "database_ready": None}, "true", "healthy", "false", "reachability not probed"),
        ({"database_configured": True, "database_check": "not_requested", "database_ready": None}, "false", "orphaned", "true", "tracked api pid is missing"),
        ({"database_configured": True, "database_check": "performed", "database_ready": False}, "true", "blocked", "false", "database health is false"),
        ({"database_configured": False, "database_check": "not_requested", "database_ready": None}, "true", "blocked", "false", "database health is unknown"),
    ],
)
def test_mac_service_health_distinguishes_unprobed_database_from_failure(
    tmp_path, database, running, expected_state, expected_repair, reason_fragment,
):
    row = _service_row(tmp_path, database=database, running=running)

    assert row[6] == expected_state
    assert row[7] == expected_repair
    assert reason_fragment in row[8]


@pytest.mark.parametrize(
    ("database_ready", "database_check", "heartbeat_age", "expected_state", "expected_repair", "reason_fragment"),
    [
        (None, "not_requested", 0, "healthy", "false", "heartbeat is fresh"),
        (None, "not_requested", 1200, "stale", "true", "last_cycle_age="),
        (False, "performed", 0, "blocked", "false", "database health=false"),
    ],
)
def test_automation_health_evaluates_heartbeat_when_database_probe_was_not_requested(
    tmp_path, database_ready, database_check, heartbeat_age, expected_state, expected_repair, reason_fragment,
):
    row = _service_row(
        tmp_path,
        database={"database_configured": True, "database_ready": database_ready, "database_check": database_check},
        running="true",
        service="automation",
        heartbeat_age=heartbeat_age,
    )

    assert row[6] == expected_state
    assert row[7] == expected_repair
    assert reason_fragment in row[8]


def _service_row(tmp_path, *, database, running, service="api", heartbeat_age=0):
    # Execute the actual parser and status decision with fake process helpers;
    # never run the script's service probes or repair entrypoint in a test.
    source = (Path(__file__).resolve().parents[1] / "scripts" / "check_mac_services.sh").read_text()
    database_parser = source.split("refresh_database_health() {", 1)[1].split("\nrefresh_automation_freshness()", 1)[0]
    automation_parser = source.split("refresh_automation_freshness() {", 1)[1].split("\nrefresh_storage_health()", 1)[0]
    status_function = source.split("service_status_row() {", 1)[1].split("\ncollect_statuses()", 1)[0]
    payload = tmp_path / "health.json"
    payload.write_text(json.dumps({
        **database,
        "automation": {
            "state": "ready",
            "has_error": False,
            "poll_seconds": 60,
            "last_cycle_at": (dt.datetime.now(dt.UTC) - dt.timedelta(seconds=heartbeat_age)).isoformat(),
        },
    }))
    shell = """
PYTHON_BIN="$1"
LOCAL_BODY_FILE="$2"
MOCK_RUNNING="$3"
MOCK_SERVICE="$4"
LOCAL_API_HEALTHY=1
LOCAL_HEALTH_CODE=200
STORAGE_STATE=ready
STORAGE_LOW_DISK=false
export MAC_SERVICE_AUTOMATION_MAX_AGE_SECONDS=900
mac_service_runtime_python() { printf '%s\n' "$PYTHON_BIN"; }
mac_service_launchd_loaded() { return 0; }
mac_service_service_pid() { printf '42\n'; }
mac_service_service_pid_source() { printf 'test\n'; }
mac_service_pid_alive() { [[ "$MOCK_RUNNING" == "true" ]]; }
sanitize_field() { printf '%s\n' "$1"; }
"""
    shell += "\nrefresh_database_health() {" + database_parser
    shell += "\nrefresh_automation_freshness() {" + automation_parser
    shell += "\nservice_status_row() {" + status_function
    shell += '\nrefresh_database_health\nrefresh_automation_freshness\nservice_status_row "$MOCK_SERVICE"\n'

    result = subprocess.run(
        ["bash", "-c", shell, "health-test", sys.executable, str(payload), running, service],
        capture_output=True, text=True, check=True,
    )

    return result.stdout.strip().split("\t")
