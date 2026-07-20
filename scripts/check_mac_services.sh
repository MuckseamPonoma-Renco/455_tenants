#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$REPO_ROOT/scripts/mac_service_helpers.sh"

EXIT_HEALTHY=0
EXIT_REPAIRED=10
EXIT_BROKEN=20

TMP_DIR="$(mktemp -d)"
STATUS_FILE="$TMP_DIR/status.tsv"
ACTIONS_FILE="$TMP_DIR/actions.log"
LOCAL_BODY_FILE="$TMP_DIR/local_health.txt"
PUBLIC_BODY_FILE="$TMP_DIR/public_health.txt"
trap 'rm -rf "$TMP_DIR"' EXIT
: >"$ACTIONS_FILE"

JSON_MODE=0
REPAIR_MODE=0
QUIET_MODE=0
PUBLIC_BASE_URL=""
LOCAL_HEALTH_CODE="000"
PUBLIC_HEALTH_CODE=""
LOCAL_API_HEALTHY=0
PUBLIC_HEALTHY=0
STARTUP_GRACE_SECONDS="${MAC_SERVICE_STARTUP_GRACE_SECONDS:-20}"
WHATSAPP_CAPTURE_STATE=""
WHATSAPP_CAPTURE_AGE_SECONDS=""
WHATSAPP_CAPTURE_MAX_AGE_SECONDS=""
WHATSAPP_CAPTURE_DETAIL=""
STORAGE_STATE=""
STORAGE_LOW_DISK=""
DATABASE_READY=""
AUTOMATION_STATE=""
AUTOMATION_AGE_SECONDS=""
AUTOMATION_MAX_AGE_SECONDS=""
AUTOMATION_HAS_ERROR=""

usage() {
  cat <<'EOF'
Usage:
  ./scripts/check_mac_services.sh
  ./scripts/check_mac_services.sh --json
  ./scripts/check_mac_services.sh --repair
  ./scripts/check_mac_services.sh --json --repair
  ./scripts/check_mac_services.sh --repair --quiet

Exit codes:
  0   Healthy, no repair needed
  10  Repair mode restarted one or more services and the final state is healthy
  20  One or more services are still unhealthy
EOF
}

sanitize_field() {
  printf '%s' "${1:-}" | tr '\t\r\n' ' ' | sed 's/[[:space:]]\+/ /g; s/^ //; s/ $//'
}

pid_within_startup_grace() {
  local pid="${1:-}"
  local elapsed

  elapsed="$(mac_service_pid_elapsed_seconds "$pid" 2>/dev/null || true)"
  [[ "$elapsed" =~ ^[0-9]+$ ]] || return 1
  (( elapsed < STARTUP_GRACE_SECONDS ))
}

refresh_endpoint_health() {
  PUBLIC_BASE_URL="$(mac_service_public_base_url)"

  LOCAL_HEALTH_CODE="$(curl -sS -o "$LOCAL_BODY_FILE" -w '%{http_code}' http://127.0.0.1:8000/health || true)"
  LOCAL_API_HEALTHY=0
  if [[ "$LOCAL_HEALTH_CODE" == "200" ]]; then
    LOCAL_API_HEALTHY=1
  fi
  refresh_whatsapp_capture_freshness
  refresh_storage_health
  refresh_database_health
  refresh_automation_freshness

  : >"$PUBLIC_BODY_FILE"
  PUBLIC_HEALTH_CODE=""
  PUBLIC_HEALTHY=0
  if [[ -n "$PUBLIC_BASE_URL" ]]; then
    PUBLIC_HEALTH_CODE="$(curl -sS -o "$PUBLIC_BODY_FILE" -w '%{http_code}' "${PUBLIC_BASE_URL%/}/health" || true)"
    if [[ "$PUBLIC_HEALTH_CODE" == "200" ]]; then
      PUBLIC_HEALTHY=1
    fi
  fi
}

refresh_database_health() {
  DATABASE_READY=""

  [[ -s "$LOCAL_BODY_FILE" ]] || return 0
  local python_bin parsed
  python_bin="$(mac_service_runtime_python)" || return 0
  parsed="$("$python_bin" - "$LOCAL_BODY_FILE" <<'PY'
import json
import pathlib
import sys

try:
    payload = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
except Exception:
    payload = {}
if payload.get("database_ready") is True:
    print("true")
elif payload.get("database_ready") is False:
    print("false")
else:
    print("unknown")
PY
)" || return 0
  DATABASE_READY="$parsed"
}

refresh_automation_freshness() {
  AUTOMATION_STATE=""
  AUTOMATION_AGE_SECONDS=""
  AUTOMATION_MAX_AGE_SECONDS=""
  AUTOMATION_HAS_ERROR=""

  [[ -s "$LOCAL_BODY_FILE" ]] || return 0
  local python_bin parsed
  python_bin="$(mac_service_runtime_python)" || return 0
  parsed="$("$python_bin" - "$LOCAL_BODY_FILE" <<'PY'
import datetime as dt
import json
import os
import pathlib
import sys

try:
    payload = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
except Exception:
    payload = {}
automation = payload.get("automation") or {}
state = str(automation.get("state") or "missing")
if automation.get("has_error") is True:
    has_error = "true"
elif automation.get("has_error") is False:
    has_error = "false"
else:
    has_error = "unknown"
try:
    poll_seconds = max(10, int(automation.get("poll_seconds") or 60))
except Exception:
    poll_seconds = 60
try:
    max_age = max(300, int(os.environ.get("MAC_SERVICE_AUTOMATION_MAX_AGE_SECONDS") or max(900, poll_seconds * 15)))
except Exception:
    max_age = max(900, poll_seconds * 15)
age = ""
stamp = str(automation.get("last_cycle_at") or "")
if stamp:
    try:
        parsed_stamp = dt.datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        if parsed_stamp.tzinfo is None:
            parsed_stamp = parsed_stamp.replace(tzinfo=dt.timezone.utc)
        age = str(max(0, int((dt.datetime.now(dt.timezone.utc) - parsed_stamp).total_seconds())))
    except Exception:
        pass
print("\t".join((state, age, str(max_age), has_error)))
PY
)" || return 0
  IFS=$'\t' read -r AUTOMATION_STATE AUTOMATION_AGE_SECONDS AUTOMATION_MAX_AGE_SECONDS AUTOMATION_HAS_ERROR <<<"$parsed"
}

refresh_storage_health() {
  STORAGE_STATE=""
  STORAGE_LOW_DISK=""

  [[ -s "$LOCAL_BODY_FILE" ]] || return 0
  local python_bin parsed
  python_bin="$(mac_service_runtime_python)" || return 0
  parsed="$("$python_bin" - "$LOCAL_BODY_FILE" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    payload = {}
storage = payload.get("storage") or {}
state = str(storage.get("state") or "missing")
if storage.get("low_disk") is True:
    low_disk = "true"
elif storage.get("low_disk") is False:
    low_disk = "false"
else:
    low_disk = "unknown"
print("\t".join((state, low_disk)))
PY
)" || return 0
  IFS=$'\t' read -r STORAGE_STATE STORAGE_LOW_DISK <<<"$parsed"
}

refresh_whatsapp_capture_freshness() {
  WHATSAPP_CAPTURE_STATE=""
  WHATSAPP_CAPTURE_AGE_SECONDS=""
  WHATSAPP_CAPTURE_MAX_AGE_SECONDS=""
  WHATSAPP_CAPTURE_DETAIL=""

  [[ -s "$LOCAL_BODY_FILE" ]] || return 0
  local python_bin parsed
  python_bin="$(mac_service_runtime_python)" || return 0
  parsed="$("$python_bin" - "$LOCAL_BODY_FILE" <<'PY'
import datetime as dt
import json
import os
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    payload = {}
capture = payload.get("whatsapp_capture") or {}
state = str(capture.get("state") or "missing")
try:
    poll_seconds = max(5, int(capture.get("poll_seconds") or 30))
except Exception:
    poll_seconds = 30
try:
    max_age = max(60, int(os.environ.get("MAC_SERVICE_WHATSAPP_CAPTURE_MAX_AGE_SECONDS") or max(300, poll_seconds * 5)))
except Exception:
    max_age = max(300, poll_seconds * 5)
age = ""
stamp = str(capture.get("last_cycle_at") or "")
if stamp:
    try:
        parsed_stamp = dt.datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        if parsed_stamp.tzinfo is None:
            parsed_stamp = parsed_stamp.replace(tzinfo=dt.timezone.utc)
        age = str(max(0, int((dt.datetime.now(dt.timezone.utc) - parsed_stamp).total_seconds())))
    except Exception:
        pass
detail = str(capture.get("last_error") or "").replace("\t", " ").replace("\n", " ")
print("\t".join((state, age, str(max_age), detail)))
PY
)" || return 0
  IFS=$'\t' read -r WHATSAPP_CAPTURE_STATE WHATSAPP_CAPTURE_AGE_SECONDS WHATSAPP_CAPTURE_MAX_AGE_SECONDS WHATSAPP_CAPTURE_DETAIL <<<"$parsed"
}

chat_export_sync_probe() {
  local python_bin state_path interval_seconds
  python_bin="$(mac_service_runtime_python)" || return 1
  state_path="$MAC_SERVICE_STATE_DIR/chat-export-sync.json"
  interval_seconds="${CHAT_EXPORT_SYNC_INTERVAL_SECONDS:-900}"
  "$python_bin" - "$state_path" "$interval_seconds" <<'PY'
import datetime as dt
import json
import pathlib
import sys

state_path = pathlib.Path(sys.argv[1])
try:
    interval = max(60, int(sys.argv[2]))
except Exception:
    interval = 900
try:
    state = json.loads(state_path.read_text(encoding="utf-8"))
except Exception:
    state = {}

checked_at = str(state.get("last_checked_at") or "")
age_seconds = None
if checked_at:
    try:
        checked = dt.datetime.fromisoformat(checked_at.replace("Z", "+00:00"))
        if checked.tzinfo is None:
            checked = checked.replace(tzinfo=dt.timezone.utc)
        age_seconds = max(0, int((dt.datetime.now(dt.timezone.utc) - checked).total_seconds()))
    except Exception:
        pass

source = ""
for key in ("last_pending_fingerprint", "last_seen_fingerprint", "last_processed_fingerprint"):
    value = state.get(key)
    if isinstance(value, dict) and value.get("path"):
        source = str(value["path"])
        break

if age_seconds is None or age_seconds > max(interval * 3, 1800):
    action = "stale"
elif str(state.get("last_error") or "").startswith("waiting for complete iCloud export:"):
    action = "waiting_for_download"
elif state.get("last_error"):
    action = "error"
elif state.get("last_processed_fingerprint"):
    action = "processed"
else:
    action = "no_export_found"

print(json.dumps({"action": action, "source": source, "age_seconds": age_seconds}, sort_keys=True))
PY
}

cloud_export_receiver_probe() {
  local python_bin
  python_bin="$(mac_service_runtime_python)" || return 1
  "$python_bin" "$REPO_ROOT/scripts/sync_cloud_chat_export_inbox.py" --probe
}

service_status_row() {
  local name="$1"
  local configured="true"
  local launchd_loaded="false"
  local pid=""
  local pid_source="none"
  local running="false"
  local state=""
  local needs_repair="false"
  local reason=""

  if [[ "$name" == "tunnel" ]] && ! mac_service_tunnel_configured; then
    configured="false"
    state="not_configured"
    reason="$(sanitize_field "$(mac_service_tunnel_check_message)")"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$name" "$configured" "$launchd_loaded" "$pid" "$pid_source" "$running" "$state" "$needs_repair" "$reason"
    return 0
  fi
  if [[ "$name" == "whatsapp_capture" ]] && ! mac_service_whatsapp_capture_configured; then
    configured="false"
    state="not_configured"
    reason="$(sanitize_field "$(mac_service_whatsapp_capture_check_message)")"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$name" "$configured" "$launchd_loaded" "$pid" "$pid_source" "$running" "$state" "$needs_repair" "$reason"
    return 0
  fi

  if [[ "$name" == "chat_export_sync" ]]; then
    if mac_service_launchd_loaded "$name"; then
      launchd_loaded="true"
    fi
    if [[ "$launchd_loaded" != "true" ]]; then
      state="unhealthy"
      needs_repair="true"
      reason="chat-export-sync LaunchAgent is not loaded"
    else
      local probe parsed action source
      if probe="$(chat_export_sync_probe 2>&1)"; then
        parsed="$(printf '%s' "$probe" | "$(mac_service_runtime_python)" -c '
import json
import sys

try:
    payload = json.load(sys.stdin)
except Exception:
    payload = {}
print("\t".join((str(payload.get("action") or ""), str(payload.get("source") or ""))))
' 2>/dev/null || true)"
        IFS=$'\t' read -r action source <<<"$parsed"
        case "$action" in
          unchanged_skip|no_export_found|processed)
            state="healthy"
            reason="chat-export-sync action=${action}"
            ;;
          would_process)
            state="pending"
            needs_repair="true"
            reason="new chat export is waiting to be imported: ${source:-unknown source}"
            ;;
          waiting_for_download)
            state="pending"
            reason="chat export is waiting for iCloud to download: ${source:-unknown source}"
            ;;
          *)
            state="unhealthy"
            needs_repair="true"
            reason="chat-export-sync returned an unexpected result: ${probe:0:300}"
            ;;
        esac
      else
        state="unhealthy"
        needs_repair="true"
        reason="chat-export-sync probe failed: ${probe:0:300}"
      fi
    fi
    reason="$(sanitize_field "$reason")"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$name" "$configured" "$launchd_loaded" "$pid" "$pid_source" "$running" "$state" "$needs_repair" "$reason"
    return 0
  fi

  if [[ "$name" == "cloud_export_receiver" ]]; then
    local probe parsed action pending
    if probe="$(cloud_export_receiver_probe 2>&1)"; then
      parsed="$(printf '%s' "$probe" | "$(mac_service_runtime_python)" -c '
import json
import sys

try:
    payload = json.load(sys.stdin)
except Exception:
    payload = {}
print("\t".join((str(payload.get("action") or ""), str(payload.get("pending_exports") or ""))))
' 2>/dev/null || true)"
      IFS=$'\t' read -r action pending <<<"$parsed"
      case "$action" in
        not_configured)
          configured="false"
          state="not_configured"
          reason="private cloud chat-export receiver is not configured"
          ;;
        ready)
          state="healthy"
          reason="private cloud receiver and authenticated export listing are ready; pending_exports=${pending:-0}"
          ;;
        *)
          state="unhealthy"
          reason="cloud export receiver returned an unexpected result: ${probe:0:300}"
          ;;
      esac
    else
      state="unhealthy"
      reason="cloud export receiver probe failed: ${probe:0:300}"
    fi
    reason="$(sanitize_field "$reason")"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$name" "$configured" "$launchd_loaded" "$pid" "$pid_source" "$running" "$state" "$needs_repair" "$reason"
    return 0
  fi

  if [[ "$name" == "storage" ]]; then
    if [[ "$STORAGE_STATE" == "ready" && "$STORAGE_LOW_DISK" == "false" ]]; then
      state="healthy"
      reason="host free storage is above the configured safety threshold"
    elif [[ "$STORAGE_STATE" == "low_disk" || "$STORAGE_LOW_DISK" == "true" ]]; then
      state="low_disk"
      reason="host free storage is below the configured safety threshold; no automatic evidence or WhatsApp-session deletion is performed"
    else
      state="unhealthy"
      reason="host storage health is ${STORAGE_STATE:-missing}"
    fi
    reason="$(sanitize_field "$reason")"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$name" "$configured" "$launchd_loaded" "$pid" "$pid_source" "$running" "$state" "$needs_repair" "$reason"
    return 0
  fi

  if mac_service_launchd_loaded "$name"; then
    launchd_loaded="true"
  fi

  pid="$(mac_service_service_pid "$name" 2>/dev/null || true)"
  pid_source="$(mac_service_service_pid_source "$name")"
  if mac_service_pid_alive "$pid"; then
    running="true"
  fi

  case "$name" in
    api)
      if [[ "$running" == "true" && "$LOCAL_API_HEALTHY" -eq 1 && "$DATABASE_READY" == "true" ]]; then
        state="healthy"
        reason="local /health returned 200 and database is reachable"
      elif [[ "$LOCAL_API_HEALTHY" -eq 1 && "$DATABASE_READY" != "true" ]]; then
        state="blocked"
        reason="local /health returned 200 but database health is ${DATABASE_READY:-unknown}"
      elif [[ "$running" == "false" && "$LOCAL_API_HEALTHY" -eq 1 ]]; then
        state="orphaned"
        needs_repair="true"
        reason="local /health returned 200 but tracked api pid is missing"
      elif [[ "$running" == "true" ]] && pid_within_startup_grace "$pid"; then
        state="starting"
        reason="api pid is running but local /health returned ${LOCAL_HEALTH_CODE}; within ${STARTUP_GRACE_SECONDS}s startup grace"
      elif [[ "$running" == "true" ]]; then
        state="unhealthy"
        needs_repair="true"
        reason="api pid is running but local /health returned ${LOCAL_HEALTH_CODE}"
      else
        state="unhealthy"
        needs_repair="true"
        reason="api pid is not running and local /health returned ${LOCAL_HEALTH_CODE}"
      fi
      ;;
    automation)
      if [[ "$running" == "true" ]]; then
        if [[ "$DATABASE_READY" != "true" ]]; then
          state="blocked"
          reason="automation is waiting for database health=${DATABASE_READY:-unknown}"
        elif [[ "$STORAGE_STATE" == "low_disk" || "$STORAGE_LOW_DISK" == "true" ]]; then
          state="blocked"
          reason="automation is waiting for host storage recovery"
        elif [[ ( "$AUTOMATION_STATE" == "ready" || "$AUTOMATION_STATE" == "working" ) \
          && "$AUTOMATION_HAS_ERROR" == "false" \
          && "$AUTOMATION_AGE_SECONDS" =~ ^[0-9]+$ \
          && "$AUTOMATION_MAX_AGE_SECONDS" =~ ^[0-9]+$ \
          && "$AUTOMATION_AGE_SECONDS" -le "$AUTOMATION_MAX_AGE_SECONDS" ]]; then
          state="healthy"
          reason="automation ${AUTOMATION_STATE} heartbeat is fresh (${AUTOMATION_AGE_SECONDS}s old)"
        elif [[ "$AUTOMATION_STATE" == "starting" \
          && "$AUTOMATION_AGE_SECONDS" =~ ^[0-9]+$ \
          && "$AUTOMATION_MAX_AGE_SECONDS" =~ ^[0-9]+$ \
          && "$AUTOMATION_AGE_SECONDS" -le "$AUTOMATION_MAX_AGE_SECONDS" ]]; then
          state="starting"
          reason="automation startup heartbeat is fresh (${AUTOMATION_AGE_SECONDS}s old)"
        else
          state="stale"
          needs_repair="true"
          reason="automation pid is running but state=${AUTOMATION_STATE:-missing} last_cycle_age=${AUTOMATION_AGE_SECONDS:-unknown}s"
        fi
      else
        state="unhealthy"
        needs_repair="true"
        reason="automation pid is not running"
      fi
      ;;
    whatsapp_capture)
      if [[ "$running" == "true" ]]; then
        if [[ "$WHATSAPP_CAPTURE_STATE" == "login_required" ]]; then
          state="blocked"
          reason="WhatsApp Web login is required; scan the QR code in the capture Chrome profile"
        elif [[ "$WHATSAPP_CAPTURE_STATE" == "not_ready" ]]; then
          state="blocked"
          reason="WhatsApp Web is not ready; resolve the visible Chrome prompt in the capture profile"
        elif [[ "$WHATSAPP_CAPTURE_STATE" == "ready" \
          && "$WHATSAPP_CAPTURE_AGE_SECONDS" =~ ^[0-9]+$ \
          && "$WHATSAPP_CAPTURE_MAX_AGE_SECONDS" =~ ^[0-9]+$ \
          && "$WHATSAPP_CAPTURE_AGE_SECONDS" -le "$WHATSAPP_CAPTURE_MAX_AGE_SECONDS" ]]; then
          state="healthy"
          reason="whatsapp_capture cycle is fresh (${WHATSAPP_CAPTURE_AGE_SECONDS}s old)"
        elif [[ "$WHATSAPP_CAPTURE_STATE" == "starting" ]] && pid_within_startup_grace "$pid"; then
          state="starting"
          reason="whatsapp_capture is within ${STARTUP_GRACE_SECONDS}s startup grace"
        else
          state="stale"
          needs_repair="true"
          reason="whatsapp_capture pid is running but capture state=${WHATSAPP_CAPTURE_STATE:-missing} last_cycle_age=${WHATSAPP_CAPTURE_AGE_SECONDS:-unknown}s"
          if [[ -n "$WHATSAPP_CAPTURE_DETAIL" ]]; then
            reason+=" error=${WHATSAPP_CAPTURE_DETAIL}"
          fi
        fi
      else
        state="unhealthy"
        needs_repair="true"
        reason="whatsapp_capture pid is not running"
      fi
      ;;
    tunnel)
      if [[ -z "$PUBLIC_BASE_URL" ]]; then
        if [[ "$running" == "true" ]]; then
          state="healthy"
          reason="PUBLIC_BASE_URL is not set; tunnel pid is running"
        else
          state="unhealthy"
          needs_repair="true"
          reason="PUBLIC_BASE_URL is not set and tunnel pid is not running"
        fi
      elif [[ "$PUBLIC_HEALTHY" -eq 1 ]]; then
        state="healthy"
        reason="public /health returned 200"
      elif [[ "$LOCAL_API_HEALTHY" -ne 1 ]]; then
        state="blocked"
        reason="local api is unhealthy, so public /health ${PUBLIC_HEALTH_CODE:-unavailable} is treated as an api problem first"
      elif [[ "$running" == "true" ]] && pid_within_startup_grace "$pid"; then
        state="starting"
        reason="tunnel pid is running but public /health returned ${PUBLIC_HEALTH_CODE:-unavailable}; within ${STARTUP_GRACE_SECONDS}s startup grace"
      elif [[ "$running" == "true" ]]; then
        state="unhealthy"
        needs_repair="true"
        reason="tunnel pid is running but public /health returned ${PUBLIC_HEALTH_CODE:-unavailable}"
      else
        state="unhealthy"
        needs_repair="true"
        reason="tunnel pid is not running and public /health returned ${PUBLIC_HEALTH_CODE:-unavailable}"
      fi
      ;;
    *)
      state="unknown"
      needs_repair="true"
      reason="unknown service"
      ;;
  esac

  reason="$(sanitize_field "$reason")"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$configured" "$launchd_loaded" "$pid" "$pid_source" "$running" "$state" "$needs_repair" "$reason"
}

collect_statuses() {
  : >"$STATUS_FILE"
  service_status_row api >>"$STATUS_FILE"
  service_status_row storage >>"$STATUS_FILE"
  service_status_row automation >>"$STATUS_FILE"
  service_status_row chat_export_sync >>"$STATUS_FILE"
  service_status_row cloud_export_receiver >>"$STATUS_FILE"
  service_status_row whatsapp_capture >>"$STATUS_FILE"
  service_status_row tunnel >>"$STATUS_FILE"
}

status_needs_attention() {
  awk -F'\t' '$7 != "healthy" && $7 != "not_configured" && $7 != "starting" && $7 != "pending" {found=1} END{exit(found ? 0 : 1)}' "$STATUS_FILE"
}

pending_repairs() {
  awk -F'\t' '$8=="true"{print $1}' "$STATUS_FILE"
}

rotate_service_logs() {
  local python_bin output
  python_bin="$(mac_service_runtime_python)" || return 0
  if ! output="$("$python_bin" "$REPO_ROOT/scripts/rotate_mac_service_logs.py" \
    --log-dir "$MAC_SERVICE_LOG_DIR" \
    --max-bytes "${MAC_SERVICE_LOG_MAX_BYTES:-25165824}" \
    --retain-bytes "${MAC_SERVICE_LOG_RETAIN_BYTES:-8388608}" \
    --quiet 2>&1)"; then
    printf 'Service-log rotation failed: %s\n' "${output:0:500}" >>"$ACTIONS_FILE"
    return 0
  fi
  if [[ -n "$output" ]]; then
    printf 'Service logs rotated: %s\n' "$output" >>"$ACTIONS_FILE"
  fi
}

render_human() {
  while IFS=$'\t' read -r name configured launchd_loaded pid pid_source running state _needs_repair reason; do
    local line
    line="${name}: ${state}"
    if [[ "$configured" == "false" ]]; then
      echo "$line"
      if [[ -n "$reason" ]]; then
        echo "$reason"
      fi
      continue
    fi
    if [[ -n "$pid" ]]; then
      line="${line} pid=${pid}"
    fi
    line="${line} source=${pid_source} launchd=${launchd_loaded}"
    echo "$line"
    if [[ -n "$reason" ]]; then
      echo "$reason"
    fi
  done <"$STATUS_FILE"

  echo "local /health: ${LOCAL_HEALTH_CODE:-unavailable}"
  if [[ -s "$LOCAL_BODY_FILE" ]]; then
    sed -n '1,2p' "$LOCAL_BODY_FILE"
    echo
  fi

  if [[ -n "$PUBLIC_BASE_URL" ]]; then
    echo "public /health: ${PUBLIC_HEALTH_CODE:-unavailable}"
    if [[ -s "$PUBLIC_BODY_FILE" ]]; then
      sed -n '1,2p' "$PUBLIC_BODY_FILE"
      echo
    fi
  fi

  if [[ -s "$ACTIONS_FILE" ]]; then
    echo "repair actions:"
    sed -n '1,40p' "$ACTIONS_FILE"
  fi
}

render_json() {
  local python_bin
  python_bin="$(mac_service_runtime_python)" || {
    echo "JSON mode requires python3 or .venv/bin/python" >&2
    return 1
  }

  "$python_bin" - "$STATUS_FILE" "$ACTIONS_FILE" "$LOCAL_HEALTH_CODE" "$LOCAL_BODY_FILE" "$PUBLIC_BASE_URL" "$PUBLIC_HEALTH_CODE" "$PUBLIC_BODY_FILE" "$1" "$2" <<'PY'
import json
import pathlib
import sys

status_path = pathlib.Path(sys.argv[1])
actions_path = pathlib.Path(sys.argv[2])
local_code = sys.argv[3]
local_body_path = pathlib.Path(sys.argv[4])
public_base_url = sys.argv[5]
public_code = sys.argv[6]
public_body_path = pathlib.Path(sys.argv[7])
outcome = sys.argv[8]
exit_code = int(sys.argv[9])


def preview(path: pathlib.Path) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="ignore").strip()
    return text[:500]


services = []
for raw in status_path.read_text(encoding="utf-8", errors="ignore").splitlines():
    parts = raw.split("\t")
    if len(parts) != 9:
        continue
    name, configured, launchd_loaded, pid, pid_source, running, state, needs_repair, reason = parts
    services.append(
        {
            "name": name,
            "configured": configured == "true",
            "launchd_loaded": launchd_loaded == "true",
            "pid": int(pid) if pid.isdigit() else None,
            "pid_source": pid_source,
            "running": running == "true",
            "state": state,
            "needs_repair": needs_repair == "true",
            "reason": reason,
        }
    )

actions = [line for line in actions_path.read_text(encoding="utf-8", errors="ignore").splitlines() if line]

payload = {
    "outcome": outcome,
    "exit_code": exit_code,
    "local_health": {
        "url": "http://127.0.0.1:8000/health",
        "status_code": local_code,
        "body_preview": preview(local_body_path),
    },
    "public_health": {
        "url": f"{public_base_url.rstrip('/')}/health" if public_base_url else None,
        "status_code": public_code or None,
        "body_preview": preview(public_body_path),
    },
    "repairs_attempted": actions,
    "services": services,
}
print(json.dumps(payload, ensure_ascii=False, indent=2))
PY
}

run_repairs() {
  local repaired=0
  local failed=0
  local service output

  while IFS= read -r service; do
    [[ -n "$service" ]] || continue
    if output="$(mac_service_restart_service "$service" 2>&1)"; then
      printf '%s\n' "$output" >>"$ACTIONS_FILE"
      repaired=1
    else
      printf '%s\n' "$output" >>"$ACTIONS_FILE"
      failed=1
    fi
  done < <(pending_repairs)

  if [[ "$failed" -ne 0 ]]; then
    return 1
  fi
  if [[ "$repaired" -eq 0 ]]; then
    status_needs_attention && return 1
    return 0
  fi

  local _i
  for _i in 1 2 3 4 5 6; do
    refresh_endpoint_health
    collect_statuses
    if ! status_needs_attention; then
      break
    fi
    sleep 2
  done

  if [[ "$failed" -ne 0 ]] || status_needs_attention; then
    return 1
  fi
  if [[ "$repaired" -ne 0 ]]; then
    return 10
  fi
  return 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --json)
      JSON_MODE=1
      ;;
    --repair)
      REPAIR_MODE=1
      ;;
    --quiet)
      QUIET_MODE=1
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

mac_service_ensure_dirs

if [[ "$REPAIR_MODE" -eq 1 ]]; then
  rotate_service_logs
fi

if [[ "$REPAIR_MODE" -eq 1 ]] && mac_service_install_in_progress; then
  if [[ "$JSON_MODE" -eq 1 ]]; then
    python_bin="$(mac_service_runtime_python)" || {
      echo '{"outcome":"install_in_progress","exit_code":0}' 
      exit 0
    }
    "$python_bin" - <<'PY'
import json
print(json.dumps({"outcome": "install_in_progress", "exit_code": 0, "message": "install_mac_launch_agents.sh is updating LaunchAgents; repairs skipped"}))
PY
  else
    echo "install_mac_launch_agents.sh is updating LaunchAgents; skipping repairs."
  fi
  exit 0
fi

refresh_endpoint_health
collect_statuses

REPAIR_RESULT=0
if [[ "$REPAIR_MODE" -eq 1 ]] && status_needs_attention; then
  if run_repairs; then
    REPAIR_RESULT=0
  else
    REPAIR_RESULT=$?
  fi
fi

OUTCOME="healthy"
EXIT_CODE="$EXIT_HEALTHY"
if status_needs_attention; then
  OUTCOME="unhealthy"
  EXIT_CODE="$EXIT_BROKEN"
elif [[ "$REPAIR_MODE" -eq 1 && "$REPAIR_RESULT" -eq 10 ]]; then
  OUTCOME="repaired"
  EXIT_CODE="$EXIT_REPAIRED"
fi

if [[ "$JSON_MODE" -eq 1 ]]; then
  render_json "$OUTCOME" "$EXIT_CODE"
elif [[ "$QUIET_MODE" -eq 1 ]]; then
  if [[ "$OUTCOME" != "healthy" || -s "$ACTIONS_FILE" ]]; then
    printf 'watchdog outcome=%s\n' "$OUTCOME"
    sed -n '1,40p' "$ACTIONS_FILE"
  fi
else
  render_human
fi

exit "$EXIT_CODE"
