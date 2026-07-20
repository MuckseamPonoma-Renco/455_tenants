from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_runtime_staging_preserves_runtime_configuration_and_state():
    expected_excludes = {
        "--exclude '.env'",
        "--exclude '.venv/'",
        "--exclude 'secrets/'",
        "--exclude 'incoming/'",
        "--exclude 'whatsapp_capture/'",
    }
    for name in ("install_mac_launch_agents.sh", "install_chat_export_sync_launch_agent.sh"):
        script = (ROOT / "scripts" / name).read_text(encoding="utf-8")
        assert 'Missing runtime configuration: $RUNTIME_ROOT/.env' in script
        for expected in expected_excludes:
            assert expected in script
