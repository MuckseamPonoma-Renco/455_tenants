"""Run real installer rollback functions with all service controls mocked."""
import re
import shlex
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("prior_loaded", [("api", "tunnel"), ("api",), ()])
@pytest.mark.parametrize("manual_chat", [True, False])
def test_failed_install_restores_tunnel_definition_and_previous_service_ownership(tmp_path, prior_loaded, manual_chat):
    script = (ROOT / "scripts/install_mac_launch_agents.sh").read_text()
    consumers = re.search(r"^RUNTIME_CONSUMERS=.*$", script, re.MULTILINE).group()
    definitions = script[script.index("prepare_runtime_backup() {"):script.index("trap finish_install EXIT")]
    runtime = tmp_path / "runtime"
    plists = tmp_path / "plists"
    runtime.mkdir()
    plists.mkdir()
    (runtime / "version").write_text("prior source")
    (plists / "api.plist").write_text("prior api")
    prior_tunnel = "tunnel" in prior_loaded
    if prior_tunnel:
        (plists / "tunnel.plist").write_text("prior tunnel")
    log = tmp_path / "service-calls.log"
    shell = "\n".join([
        "set -euo pipefail", consumers, definitions,
        f"STAGING_BASE={shlex.quote(str(tmp_path))}",
        f"RUNTIME_ROOT={shlex.quote(str(runtime))}",
        f"PLISTS={shlex.quote(str(plists))}",
        f"CALL_LOG={shlex.quote(str(log))}",
        f"INSTALL_LOCK_DIR={shlex.quote(str(tmp_path / 'install-lock'))}",
        "RUNTIME_BACKUP_ROOT=''", "RUNTIME_COPY_STARTED=0", "RUNTIME_QUIESCED=0",
        "PREVIOUS_LOADED_SERVICES=()", "PREVIOUS_MANUAL_SERVICES=()",
        'copy_runtime_tree() { cp -a "$1/." "$2/"; }',
        'mac_service_service_plist_path() { printf "%s/%s.plist\\n" "$PLISTS" "$1"; }',
        'mac_service_bootout_launch_agent() { printf "stop:%s\\n" "$1" >> "$CALL_LOG"; }',
        'mac_service_stop_manual_service() { :; }',
        'mac_service_stop_residual_processes() { :; }',
        ('mac_service_launchd_loaded() { [[ ' + ' || '.join(f'"$1" == {name}' for name in prior_loaded) + ' ]]; }'
         if prior_loaded else 'mac_service_launchd_loaded() { return 1; }'),
        ('mac_service_service_pid() { [[ "$1" == chat_export_sync ]] && printf "123\\n"; }'
         if manual_chat else 'mac_service_service_pid() { return 1; }'),
        'mac_service_pid_alive() { [[ "$1" == 123 ]]; }',
        'bootstrap_agent() { printf "bootstrap:%s\\n" "$1" >> "$CALL_LOG"; }',
        'mac_service_start_manual_service() { printf "manual:%s\\n" "$1" >> "$CALL_LOG"; }',
        'mkdir "$INSTALL_LOCK_DIR"',
        "trap finish_install EXIT", "prepare_runtime_backup", "quiesce_runtime_consumers",
        "RUNTIME_COPY_STARTED=1",
        'printf "new source" > "$RUNTIME_ROOT/version"',
        'printf "new api" > "$PLISTS/api.plist"',
        'printf "new tunnel" > "$PLISTS/tunnel.plist"',
        "false",
    ])
    result = subprocess.run(["bash"], input=shell, text=True, capture_output=True)
    assert result.returncode == 1
    assert "restoring the prior runtime" in result.stderr
    assert (runtime / "version").read_text() == "prior source"
    assert (plists / "api.plist").read_text() == "prior api"
    if prior_tunnel:
        assert (plists / "tunnel.plist").read_text() == "prior tunnel"
    else:
        assert not (plists / "tunnel.plist").exists()
    calls = log.read_text().splitlines()
    assert "stop:tunnel" in calls
    assert ("bootstrap:api" in calls) is ("api" in prior_loaded)
    assert ("bootstrap:tunnel" in calls) is prior_tunnel
    assert ("manual:chat_export_sync" in calls) is manual_chat
    assert "bootstrap:automation" not in calls
    assert not (tmp_path / "install-lock").exists()
