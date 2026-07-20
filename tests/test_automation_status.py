from packages.automation_status import read_automation_status, write_automation_status


def test_automation_status_round_trip_is_atomic_and_preserves_fields(tmp_path):
    path = tmp_path / "automation.json"

    first = write_automation_status(path, state="starting", poll_seconds=60, last_cycle_at="2026-07-20T02:00:00Z")
    second = write_automation_status(path, state="ready", last_error="")

    assert first["state"] == "starting"
    assert second["state"] == "ready"
    assert read_automation_status(path)["poll_seconds"] == 60
    assert read_automation_status(path)["last_error"] == ""
    assert not list(tmp_path.glob("*.tmp"))
