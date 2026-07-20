import os
import subprocess
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


def test_chat_export_launcher_runs_icloud_fallback_after_cloud_failure(tmp_path):
    call_log = tmp_path / "calls.log"
    fake_python = tmp_path / "fake-python"
    fake_python.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "printf '%s\\n' \"$1\" >> \"$CALL_LOG\"\n"
        "case \"$1\" in\n"
        "  *sync_cloud_chat_export_inbox.py) exit 7 ;;\n"
        "  *sync_chat_export_inbox.py) exit 0 ;;\n"
        "  *) exit 8 ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)

    result = subprocess.run(
        [str(ROOT / "scripts" / "run_chat_export_inbox_sync.sh")],
        env={
            **os.environ,
            "CHAT_EXPORT_SYNC_PYTHON_BIN": str(fake_python),
            "CALL_LOG": str(call_log),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "continuing with the iCloud fallback" in result.stderr
    assert call_log.read_text(encoding="utf-8").splitlines() == [
        str(ROOT / "scripts" / "sync_cloud_chat_export_inbox.py"),
        str(ROOT / "scripts" / "sync_chat_export_inbox.py"),
    ]
