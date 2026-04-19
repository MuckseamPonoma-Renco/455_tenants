#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$REPO_ROOT/scripts/mac_service_helpers.sh"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/start_mac_services.sh
  ./scripts/start_mac_services.sh api automation
  ./scripts/start_mac_services.sh --restart api
  ./scripts/start_mac_services.sh --restart

Behavior:
  - No args starts api + automation and starts tunnel only if tunnel auth is configured.
  - Named services limit the action to those services.
  - --restart performs targeted restarts instead of a start-if-missing action.
EOF
}

MODE="start"
REQUESTED_SERVICES=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --restart)
      MODE="restart"
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    api|automation|tunnel)
      REQUESTED_SERVICES+=("$1")
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

if [[ ${#REQUESTED_SERVICES[@]} -eq 0 ]]; then
  REQUESTED_SERVICES=(api automation tunnel)
fi

mac_service_ensure_dirs

for name in "${REQUESTED_SERVICES[@]}"; do
  if [[ "$name" == "tunnel" ]] && ! mac_service_tunnel_configured; then
    echo "Skipped tunnel:"
    mac_service_tunnel_check_message
    continue
  fi

  if [[ "$MODE" == "restart" ]]; then
    mac_service_restart_service "$name"
    continue
  fi

  if mac_service_launchd_loaded "$name"; then
    pid="$(mac_service_launchd_pid "$name" 2>/dev/null || true)"
    if mac_service_pid_alive "$pid"; then
      echo "${name} managed by launchd (pid ${pid})"
      continue
    fi
    mac_service_start_service "$name"
    continue
  fi

  mac_service_start_manual_service "$name"
done
