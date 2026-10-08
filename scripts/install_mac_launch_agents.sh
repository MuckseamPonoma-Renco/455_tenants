#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$REPO_ROOT/scripts/mac_service_helpers.sh"

TEMPLATE_DIR="$REPO_ROOT/launchd"
PYTHON_BIN="$(mac_service_runtime_python)"
STAGING_BASE="$HOME/.local/share/tenant-issue-os"
RUNTIME_ROOT="$STAGING_BASE/runtime"
INSTALL_LOCK_DIR="$(mac_service_install_lock_path)"
RUNTIME_BACKUP_ROOT=""
RUNTIME_COPY_STARTED=0
RUNTIME_QUIESCED=0
# Every service definition this installer may replace or remove must take part
# in backup, quiescing and rollback, including the runtime-backed tunnel wrapper.
RUNTIME_CONSUMERS=(api automation whatsapp_capture chat_export_sync tunnel watchdog)
PREVIOUS_LOADED_SERVICES=()
PREVIOUS_MANUAL_SERVICES=()

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

if ! mkdir "$INSTALL_LOCK_DIR" 2>/dev/null; then
  echo "Another macOS launchd install/update is already running: $INSTALL_LOCK_DIR" >&2
  exit 1
fi
trap 'rmdir "$INSTALL_LOCK_DIR" >/dev/null 2>&1 || true' EXIT

if ! command -v rsync >/dev/null 2>&1; then
  echo "install_mac_launch_agents.sh requires rsync" >&2
  exit 1
fi

mkdir -p "$STAGING_BASE"

if [[ ! -f "$RUNTIME_ROOT/.env" ]]; then
  echo "Missing runtime configuration: $RUNTIME_ROOT/.env" >&2
  echo "Refusing to copy .env from the working tree. Provision the runtime config once, then rerun this installer." >&2
  exit 1
fi
chmod 600 "$RUNTIME_ROOT/.env"

# Verify correction preservation before staging code or touching any service.
"$(mac_service_runtime_python)" "$REPO_ROOT/scripts/check_runtime_migration.py" --runtime-root "$RUNTIME_ROOT" --source-root "$REPO_ROOT"

copy_runtime_tree() {
  local source_root="$1" destination_root="$2"
  rsync -a --delete \
    --exclude '.git/' \
    --exclude '.git' \
    --exclude '.wrangler/' \
    --exclude '.DS_Store' \
    --exclude '.pytest_cache/' \
    --exclude '.test_audit/' \
    --exclude '.audit/' \
    --exclude '.local/' \
    --exclude '.env' \
    --exclude '.venv/' \
    --exclude '.venv.badmove/' \
    --exclude 'secrets/' \
    --exclude '.vscode/' \
    --exclude '__pycache__/' \
    --exclude 'exports/' \
    --exclude 'incoming/' \
    --exclude 'whatsapp_capture/' \
    --exclude 'e2e_test.sqlite3' \
    --exclude 'smoke_test.sqlite3' \
    --exclude 'tmp_debug.sqlite3' \
    --exclude 'test_app.sqlite3' \
    --exclude 'local.db' \
    --exclude 'WhatsApp Chat - *.zip' \
    "$source_root/" "$destination_root/"
}

stage_runtime_copy() {
  RUNTIME_COPY_STARTED=1
  copy_runtime_tree "$REPO_ROOT" "$RUNTIME_ROOT"
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

bootstrap_with_retry() {
  local name="$1"
  local plist_path="$2"
  local attempt output status

  for attempt in 1 2 3 4 5; do
    output="$(launchctl bootstrap "gui/$MAC_SERVICE_UID" "$plist_path" 2>&1)" && return 0
    status=$?
    mac_service_bootout_launch_agent "$name"
    sleep 1
  done

  printf '%s\n' "$output" >&2
  return "$status"
}

bootstrap_agent() {
  local name="$1"
  local plist_path
  plist_path="$(mac_service_service_plist_path "$name")"

  if [[ "$name" != "watchdog" ]]; then
    mac_service_stop_manual_service "$name"
    mac_service_stop_residual_processes "$name"
  fi

  mac_service_bootout_launch_agent "$name"
  bootstrap_with_retry "$name" "$plist_path"
  launchctl enable "$(mac_service_service_target "$name")"
  sleep 1
}

print_agent_summary() {
  local name="$1"
  local label target
  local printed
  label="$(mac_service_service_label "$name")"
  target="$(mac_service_service_target "$name")"
  echo "Loaded ${label}"
  echo "  plist: $(mac_service_service_plist_path "$name")"
  printed="$(launchctl print "$target" 2>&1 || true)"
  if [[ "$printed" == *"Could not find service"* ]]; then
    echo "  state = missing"
    return 1
  fi
  printf '%s\n' "$printed" | awk '
    /program = / {print "  " $0; next}
    /stdout path = / {print "  " $0; next}
    /stderr path = / {print "  " $0; next}
    /state = / {print "  " $0; exit}
  '
}

prepare_runtime_backup() {
  local name plist
  RUNTIME_BACKUP_ROOT="$(mktemp -d "$STAGING_BASE/runtime-install.XXXXXX")"
  mkdir -p "$RUNTIME_BACKUP_ROOT/runtime" "$RUNTIME_BACKUP_ROOT/plists"
  copy_runtime_tree "$RUNTIME_ROOT" "$RUNTIME_BACKUP_ROOT/runtime"
  for name in "${RUNTIME_CONSUMERS[@]}"; do
    plist="$(mac_service_service_plist_path "$name")"
    if [[ -f "$plist" ]]; then
      cp -p "$plist" "$RUNTIME_BACKUP_ROOT/plists/$name.plist"
    fi
  done
}

stop_runtime_consumers() {
  local name
  mac_service_bootout_launch_agent watchdog
  for name in "${RUNTIME_CONSUMERS[@]}"; do
    [[ "$name" == "watchdog" ]] && continue
    mac_service_bootout_launch_agent "$name"
    mac_service_stop_manual_service "$name"
    mac_service_stop_residual_processes "$name"
  done
}

quiesce_runtime_consumers() {
  local name pid
  for name in "${RUNTIME_CONSUMERS[@]}"; do
    if mac_service_launchd_loaded "$name"; then
      PREVIOUS_LOADED_SERVICES+=("$name")
    else
      pid="$(mac_service_service_pid "$name" 2>/dev/null || true)"
      if mac_service_pid_alive "$pid"; then
        PREVIOUS_MANUAL_SERVICES+=("$name")
      fi
    fi
  done
  RUNTIME_QUIESCED=1
  stop_runtime_consumers
}

restore_prior_runtime() {
  local name plist
  if [[ "$RUNTIME_COPY_STARTED" -eq 1 ]]; then
    if ! copy_runtime_tree "$RUNTIME_BACKUP_ROOT/runtime" "$RUNTIME_ROOT"; then
      echo "Runtime restore failed; consumers remain stopped. Recovery copy: $RUNTIME_BACKUP_ROOT" >&2
      return 1
    fi
    for name in "${RUNTIME_CONSUMERS[@]}"; do
      plist="$(mac_service_service_plist_path "$name")"
      if [[ -f "$RUNTIME_BACKUP_ROOT/plists/$name.plist" ]]; then
        cp -p "$RUNTIME_BACKUP_ROOT/plists/$name.plist" "$plist" || return 1
      else
        # This install created the plist; no previous service definition existed.
        rm -f "$plist"
      fi
    done
  fi
  # macOS Bash 3.2 treats an empty array as unset under nounset.
  for name in ${PREVIOUS_LOADED_SERVICES[@]+"${PREVIOUS_LOADED_SERVICES[@]}"}; do
    bootstrap_agent "$name" || return 1
  done
  for name in ${PREVIOUS_MANUAL_SERVICES[@]+"${PREVIOUS_MANUAL_SERVICES[@]}"}; do
    mac_service_start_manual_service "$name" || return 1
  done
}

finish_install() {
  local exit_status=$?
  trap - EXIT
  if [[ "$exit_status" -ne 0 && "$RUNTIME_QUIESCED" -eq 1 ]]; then
    set +e
    echo "Install failed; restoring the prior runtime and service definitions." >&2
    stop_runtime_consumers
    restore_prior_runtime || echo "Some previous services could not be restored; recovery copy: $RUNTIME_BACKUP_ROOT" >&2
  fi
  rmdir "$INSTALL_LOCK_DIR" >/dev/null 2>&1 || true
  exit "$exit_status"
}

trap finish_install EXIT
prepare_runtime_backup
quiesce_runtime_consumers
echo "Staging launchd runtime copy at $RUNTIME_ROOT"
stage_runtime_copy

render_template "tenant-issue-os.api.plist.template" "$(mac_service_service_plist_path api)"
render_template "tenant-issue-os.automation.plist.template" "$(mac_service_service_plist_path automation)"
render_template "tenant-issue-os.watchdog.plist.template" "$(mac_service_service_plist_path watchdog)"

bootstrap_agent api
bootstrap_agent automation

# This separately installed periodic importer also consumes runtime Python.
for service in ${PREVIOUS_LOADED_SERVICES[@]+"${PREVIOUS_LOADED_SERVICES[@]}"}; do
  if [[ "$service" == "chat_export_sync" ]]; then
    bootstrap_agent chat_export_sync
  fi
done
for service in ${PREVIOUS_MANUAL_SERVICES[@]+"${PREVIOUS_MANUAL_SERVICES[@]}"}; do
  if [[ "$service" == "chat_export_sync" ]]; then
    mac_service_start_manual_service chat_export_sync
  fi
done

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

bootstrap_agent watchdog

print_agent_summary api
print_agent_summary automation
if mac_service_whatsapp_capture_configured; then
  print_agent_summary whatsapp_capture
fi
if mac_service_tunnel_configured; then
  print_agent_summary tunnel
fi
print_agent_summary watchdog
echo "Previous runtime source and service definitions retained at $RUNTIME_BACKUP_ROOT"

echo "Watchdog logs:"
echo "  stdout: $(mac_service_service_stdout_log watchdog)"
echo "  stderr: $(mac_service_service_stderr_log watchdog)"
echo "Launchd runtime root: $RUNTIME_ROOT"
