#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$REPO_ROOT/scripts/mac_service_helpers.sh"

TEMPLATE_DIR="$REPO_ROOT/launchd"
PYTHON_BIN="$(mac_service_runtime_python)"
STAGING_BASE="$HOME/.local/share/tenant-issue-os"
RUNTIME_ROOT="$STAGING_BASE/runtime"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/install_mac_launch_agents.sh

Installs or updates these per-user LaunchAgents:
  tenant-issue-os.api
  tenant-issue-os.automation
  tenant-issue-os.whatsapp_capture  (only when WHATSAPP_CAPTURE_CHAT_NAMES is configured)
  tenant-issue-os.tunnel      (only when tunnel auth is configured)
  tenant-issue-os.watchdog
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "install_mac_launch_agents.sh only supports macOS" >&2
  exit 1
fi

if [[ -z "$PYTHON_BIN" ]]; then
  echo "Missing python runtime for plist rendering" >&2
  exit 1
fi

mac_service_ensure_dirs

if ! command -v rsync >/dev/null 2>&1; then
  echo "install_mac_launch_agents.sh requires rsync" >&2
  exit 1
fi

mkdir -p "$STAGING_BASE"

stage_runtime_copy() {
  rsync -a --delete \
    --exclude '.git/' \
    --exclude '.DS_Store' \
    --exclude '.pytest_cache/' \
    --exclude '.test_audit/' \
    --exclude '.audit/' \
    --exclude '.local/' \
    --exclude '.vscode/' \
    --exclude '__pycache__/' \
    --exclude 'exports/' \
    --exclude 'e2e_test.sqlite3' \
    --exclude 'smoke_test.sqlite3' \
    --exclude 'tmp_debug.sqlite3' \
    --exclude 'test_app.sqlite3' \
    --exclude 'local.db' \
    --exclude 'WhatsApp Chat - *.zip' \
    "$REPO_ROOT/" "$RUNTIME_ROOT/"
}

render_template() {
  local template_name="$1"
  local destination="$2"

  "$PYTHON_BIN" - "$TEMPLATE_DIR/$template_name" "$destination" "$RUNTIME_ROOT" "$MAC_SERVICE_LOG_DIR" <<'PY'
import html
import pathlib
import sys

template_path = pathlib.Path(sys.argv[1])
destination = pathlib.Path(sys.argv[2])
runtime_root = html.escape(sys.argv[3], quote=False)
log_dir = html.escape(sys.argv[4], quote=False)

text = template_path.read_text(encoding="utf-8")
text = text.replace("__REPO_ROOT__", runtime_root)
text = text.replace("__LOG_DIR__", log_dir)
destination.write_text(text, encoding="utf-8")
PY

  chmod 644 "$destination"
  plutil -lint "$destination" >/dev/null
}

bootstrap_agent() {
  local name="$1"
  local plist_path
  plist_path="$(mac_service_service_plist_path "$name")"

  if [[ "$name" != "watchdog" ]]; then
    mac_service_stop_manual_service "$name"
  fi

  mac_service_bootout_launch_agent "$name"
  launchctl bootstrap "gui/$MAC_SERVICE_UID" "$plist_path"
  launchctl enable "$(mac_service_service_target "$name")"
}

print_agent_summary() {
  local name="$1"
  local label target
  label="$(mac_service_service_label "$name")"
  target="$(mac_service_service_target "$name")"
  echo "Loaded ${label}"
  echo "  plist: $(mac_service_service_plist_path "$name")"
  launchctl print "$target" | awk '
    /program = / {print "  " $0; next}
    /stdout path = / {print "  " $0; next}
    /stderr path = / {print "  " $0; next}
    /state = / {print "  " $0; exit}
  '
}

echo "Staging launchd runtime copy at $RUNTIME_ROOT"
stage_runtime_copy

render_template "tenant-issue-os.api.plist.template" "$(mac_service_service_plist_path api)"
render_template "tenant-issue-os.automation.plist.template" "$(mac_service_service_plist_path automation)"
render_template "tenant-issue-os.watchdog.plist.template" "$(mac_service_service_plist_path watchdog)"

bootstrap_agent api
bootstrap_agent automation
bootstrap_agent watchdog

if mac_service_whatsapp_capture_configured; then
  render_template "tenant-issue-os.whatsapp_capture.plist.template" "$(mac_service_service_plist_path whatsapp_capture)"
  bootstrap_agent whatsapp_capture
else
  mac_service_bootout_launch_agent whatsapp_capture
  rm -f "$(mac_service_service_plist_path whatsapp_capture)"
  echo "Skipped tenant-issue-os.whatsapp_capture because WHATSAPP_CAPTURE_CHAT_NAMES is not configured."
fi

if mac_service_tunnel_configured; then
  render_template "tenant-issue-os.tunnel.plist.template" "$(mac_service_service_plist_path tunnel)"
  bootstrap_agent tunnel
else
  mac_service_bootout_launch_agent tunnel
  rm -f "$(mac_service_service_plist_path tunnel)"
  echo "Skipped tenant-issue-os.tunnel because tunnel auth is not configured."
fi

print_agent_summary api
print_agent_summary automation
if mac_service_whatsapp_capture_configured; then
  print_agent_summary whatsapp_capture
fi
if mac_service_tunnel_configured; then
  print_agent_summary tunnel
fi
print_agent_summary watchdog

echo "Watchdog logs:"
echo "  stdout: $(mac_service_service_stdout_log watchdog)"
echo "  stderr: $(mac_service_service_stderr_log watchdog)"
echo "Launchd runtime root: $RUNTIME_ROOT"
