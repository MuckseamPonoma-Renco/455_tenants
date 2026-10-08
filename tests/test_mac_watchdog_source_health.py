import json
import shlex
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("watchdog,expected_state", [
    ({"state": "ready", "has_error": False, "source_errors": 0, "sheet_errors": 0, "last_success_at": "2026-09-06T13:00:00Z", "retry_seconds": 300}, "healthy"),
    ({"state": "degraded", "has_error": True, "source_errors": 1, "sheet_errors": 0, "last_success_at": "2026-09-06T08:00:00Z", "retry_seconds": 300}, "degraded"),
    ({"state": "stale", "has_error": True, "source_errors": 0, "sheet_errors": 0, "last_success_at": "2026-09-05T08:00:00Z", "retry_seconds": 300}, "degraded"),
    ({"state": "ready", "has_error": False, "source_errors": 0, "sheet_errors": 1, "last_success_at": "2026-09-06T13:00:00Z", "retry_seconds": 300}, "degraded"),
    ({}, "degraded"),
])
def test_mac_monitor_reports_watchdog_failure_without_restarting_services(tmp_path, watchdog, expected_state):
    script = (Path(__file__).resolve().parents[1] / "scripts/check_mac_services.sh").read_text()
    # Load the real functions without running launchd, network probes or repairs.
    definitions = script[script.index("sanitize_field() {"):script.index("while [[ $# -gt 0 ]]; do")]
    payload_path = tmp_path / "health.json"
    payload_path.write_text(json.dumps({"watchdog": watchdog}))
    status_path = tmp_path / "status.tsv"
    shell = "\n".join([
        "set -euo pipefail", definitions,
        f"mac_service_runtime_python() {{ printf '%s\\n' {shlex.quote(sys.executable)}; }}",
        f"LOCAL_BODY_FILE={shlex.quote(str(payload_path))}",
        f"STATUS_FILE={shlex.quote(str(status_path))}",
        "refresh_watchdog_freshness",
        'service_status_row public_records > "$STATUS_FILE"',
        'cat "$STATUS_FILE"',
        'printf "repair_targets=%s\\n" "$(pending_repairs)"',
        'if status_needs_attention; then printf "attention=yes\\n"; else printf "attention=no\\n"; fi',
    ])
    completed = subprocess.run(["bash"], input=shell, text=True, capture_output=True, check=True)
    lines = completed.stdout.splitlines()
    row = lines[0].split("\t")
    assert row[0] == "public_records"
    assert row[6] == expected_state
    assert row[7] == "false"
    assert lines[1] == "repair_targets="
    assert lines[2] == ("attention=no" if expected_state == "healthy" else "attention=yes")
