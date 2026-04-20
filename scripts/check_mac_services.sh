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
PUBLIC_BASE_URL=""
LOCAL_HEALTH_CODE="000"
PUBLIC_HEALTH_CODE=""
LOCAL_API_HEALTHY=0
PUBLIC_HEALTHY=0
STARTUP_GRACE_SECONDS="${MAC_SERVICE_STARTUP_GRACE_SECONDS:-20}"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/check_mac_services.sh
  ./scripts/check_mac_services.sh --json
  ./scripts/check_mac_services.sh --repair
  ./scripts/check_mac_services.sh --json --repair

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
      if [[ "$running" == "true" && "$LOCAL_API_HEALTHY" -eq 1 ]]; then
        state="healthy"
        reason="local /health returned 200"
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
        state="healthy"
        reason="automation pid is running"
      else
        state="unhealthy"
        needs_repair="true"
        reason="automation pid is not running"
      fi
      ;;
    whatsapp_capture)
      if [[ "$running" == "true" ]]; then
        state="healthy"
        reason="whatsapp_capture pid is running"
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
  service_status_row automation >>"$STATUS_FILE"
  service_status_row whatsapp_capture >>"$STATUS_FILE"
  service_status_row tunnel >>"$STATUS_FILE"
}

status_needs_attention() {
  awk -F'\t' '$8=="true"{found=1} END{exit(found ? 0 : 1)}' "$STATUS_FILE"
}

pending_repairs() {
  awk -F'\t' '$8=="true"{print $1}' "$STATUS_FILE"
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

  local _i
  for _i in 1 2 3 4 5 6; do
    refresh_endpoint_health
    collect_statuses
    if ! status_needs_attention; then
      break
    fi
    sleep 2
  done

  if [[ "$failed" -ne 0 ]]; then
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
else
  render_human
fi

exit "$EXIT_CODE"
