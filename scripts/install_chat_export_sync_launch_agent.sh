#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$REPO_ROOT/scripts/mac_service_helpers.sh"

LABEL="tenant-issue-os.chat-export-sync"
PLIST_PATH="$HOME/Library/LaunchAgents/${LABEL}.plist"
STAGING_BASE="$HOME/.local/share/tenant-issue-os"
RUNTIME_ROOT="$STAGING_BASE/runtime"
PROGRAM="$RUNTIME_ROOT/scripts/run_chat_export_inbox_sync.sh"
STDOUT_LOG="$MAC_SERVICE_LOG_DIR/chat-export-sync.out.log"
STDERR_LOG="$MAC_SERVICE_LOG_DIR/chat-export-sync.err.log"
START_INTERVAL_SECONDS="${CHAT_EXPORT_SYNC_INTERVAL_SECONDS:-900}"
THROTTLE_INTERVAL_SECONDS="${CHAT_EXPORT_SYNC_THROTTLE_INTERVAL_SECONDS:-120}"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "install_chat_export_sync_launch_agent.sh only supports macOS" >&2
  exit 1
fi

mac_service_ensure_dirs
mkdir -p "$HOME/Library/Mobile Documents/com~apple~CloudDocs/455 Tenant Chat Exports"
mkdir -p "$STAGING_BASE"

if ! command -v rsync >/dev/null 2>&1; then
  echo "install_chat_export_sync_launch_agent.sh requires rsync" >&2
  exit 1
fi

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
  --exclude 'incoming/' \
  --exclude 'e2e_test.sqlite3' \
  --exclude 'smoke_test.sqlite3' \
  --exclude 'tmp_debug.sqlite3' \
  --exclude 'test_app.sqlite3' \
  --exclude 'local.db' \
  --exclude 'WhatsApp Chat - *.zip' \
  "$REPO_ROOT/" "$RUNTIME_ROOT/"

chmod +x "$PROGRAM"

"$(mac_service_runtime_python)" - "$PLIST_PATH" "$LABEL" "$PROGRAM" "$RUNTIME_ROOT" "$STDOUT_LOG" "$STDERR_LOG" "$START_INTERVAL_SECONDS" "$THROTTLE_INTERVAL_SECONDS" <<'PY'
import pathlib
import plistlib
import sys

plist_path, label, program, repo_root, stdout_log, stderr_log, interval, throttle_interval = sys.argv[1:9]
icloud_root = pathlib.Path.home() / "Library/Mobile Documents/com~apple~CloudDocs"
icloud_inbox = icloud_root / "455 Tenant Chat Exports"
body = {
    "Label": label,
    "ProgramArguments": [program],
    "WorkingDirectory": repo_root,
    "RunAtLoad": True,
    "StartInterval": int(interval),
    "ThrottleInterval": int(throttle_interval),
    # iOS saves may land in either the dedicated inbox or iCloud Drive's root.
    # ThrottleInterval prevents unrelated iCloud churn from invoking the audit
    # more than once every two minutes.
    "WatchPaths": [str(icloud_inbox), str(icloud_root)],
    "StandardOutPath": stdout_log,
    "StandardErrorPath": stderr_log,
}
path = pathlib.Path(plist_path)
path.parent.mkdir(parents=True, exist_ok=True)
with path.open("wb") as handle:
    plistlib.dump(body, handle)
PY

chmod 644 "$PLIST_PATH"
plutil -lint "$PLIST_PATH" >/dev/null

launchctl bootout "gui/$MAC_SERVICE_UID/$LABEL" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$MAC_SERVICE_UID" "$PLIST_PATH"
launchctl enable "gui/$MAC_SERVICE_UID/$LABEL"

echo "Loaded $LABEL"
echo "  plist: $PLIST_PATH"
echo "  interval_seconds: $START_INTERVAL_SECONDS"
echo "  throttle_interval_seconds: $THROTTLE_INTERVAL_SECONDS"
echo "  runtime: $RUNTIME_ROOT"
echo "  iCloud scan sources: $HOME/Library/Mobile Documents/com~apple~CloudDocs and $HOME/Library/Mobile Documents/com~apple~CloudDocs/455 Tenant Chat Exports"
echo "  immediate watch paths: $HOME/Library/Mobile Documents/com~apple~CloudDocs/455 Tenant Chat Exports and $HOME/Library/Mobile Documents/com~apple~CloudDocs"
echo "  stdout: $STDOUT_LOG"
echo "  stderr: $STDERR_LOG"
