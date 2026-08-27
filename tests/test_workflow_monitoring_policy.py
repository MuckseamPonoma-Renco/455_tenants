from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _trigger_block(workflow_name: str) -> str:
    workflow = (REPO_ROOT / ".github" / "workflows" / workflow_name).read_text()
    return workflow.split("permissions:", maxsplit=1)[0]


def test_mac_dependent_public_health_is_not_scheduled() -> None:
    triggers = _trigger_block("public-service-health.yml")

    assert "push:" in triggers
    assert "workflow_dispatch:" in triggers
    assert "schedule:" not in triggers


def test_cloud_capable_recovery_remains_scheduled() -> None:
    triggers = _trigger_block("cloud-recovery.yml")

    assert "schedule:" in triggers
    assert "cron:" in triggers
