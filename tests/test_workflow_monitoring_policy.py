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


def test_cloud_recovery_refuses_stale_renderer_revisions() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "cloud-recovery.yml").read_text()

    assert "cancel-in-progress: ${{ github.event_name == 'push' }}" in workflow
    assert "ref: main" in workflow
    assert "git fetch --no-tags --depth=1 origin" in workflow
    assert 'git rev-parse HEAD' in workflow
    assert 'git rev-parse origin/main' in workflow
    assert workflow.index('exit 3') < workflow.index('python scripts/run_cloud_recovery_cycle.py --env-file "$env_file" --mode "$MODE"')


def test_workflows_use_node_24_action_releases() -> None:
    public_health = (REPO_ROOT / ".github" / "workflows" / "public-service-health.yml").read_text()
    cloud_recovery = (REPO_ROOT / ".github" / "workflows" / "cloud-recovery.yml").read_text()

    assert "actions/checkout@v7" in public_health
    assert "actions/checkout@v7" in cloud_recovery
    assert "actions/setup-python@v7" in cloud_recovery
