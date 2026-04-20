#!/usr/bin/env bash

MAC_SERVICE_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MAC_SERVICE_LOG_DIR="${HOME}/.local/var/log/tenant-issue-os"
MAC_SERVICE_PID_DIR="${HOME}/.local/var/run/tenant-issue-os"
MAC_SERVICE_LAUNCH_AGENTS_DIR="${HOME}/Library/LaunchAgents"
MAC_SERVICE_LABEL_PREFIX="tenant-issue-os"
MAC_SERVICE_UID="$(id -u)"

mac_service_runtime_python() {
  if [[ -x "$MAC_SERVICE_REPO_ROOT/.venv/bin/python" ]]; then
    printf '%s\n' "$MAC_SERVICE_REPO_ROOT/.venv/bin/python"
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return 0
  fi
  return 1
}

mac_service_ensure_dirs() {
  mkdir -p "$MAC_SERVICE_LOG_DIR" "$MAC_SERVICE_PID_DIR" "$MAC_SERVICE_LAUNCH_AGENTS_DIR"
}

mac_service_service_label() {
  printf '%s.%s\n' "$MAC_SERVICE_LABEL_PREFIX" "$1"
}

mac_service_service_target() {
  printf 'gui/%s/%s\n' "$MAC_SERVICE_UID" "$(mac_service_service_label "$1")"
}

mac_service_service_script() {
  case "$1" in
    api)
      printf '%s/scripts/run_api.sh\n' "$MAC_SERVICE_REPO_ROOT"
      ;;
    automation)
      printf '%s/scripts/run_automation.sh\n' "$MAC_SERVICE_REPO_ROOT"
      ;;
    whatsapp_capture)
      printf '%s/scripts/run_whatsapp_capture.sh\n' "$MAC_SERVICE_REPO_ROOT"
      ;;
    tunnel)
      printf '%s/scripts/run_cloudflared.sh\n' "$MAC_SERVICE_REPO_ROOT"
      ;;
    watchdog)
      printf '%s/scripts/check_mac_services.sh\n' "$MAC_SERVICE_REPO_ROOT"
      ;;
    *)
      return 1
      ;;
  esac
}

mac_service_service_stdout_log() {
  printf '%s/%s.out.log\n' "$MAC_SERVICE_LOG_DIR" "$1"
}

mac_service_service_stderr_log() {
  printf '%s/%s.err.log\n' "$MAC_SERVICE_LOG_DIR" "$1"
}

mac_service_service_pid_file() {
  printf '%s/%s.pid\n' "$MAC_SERVICE_PID_DIR" "$1"
}

mac_service_service_plist_path() {
  printf '%s/%s.plist\n' "$MAC_SERVICE_LAUNCH_AGENTS_DIR" "$(mac_service_service_label "$1")"
}

mac_service_valid_pid() {
  [[ "${1:-}" =~ ^[0-9]+$ ]]
}

mac_service_pid_alive() {
  local pid="${1:-}"
  mac_service_valid_pid "$pid" && kill -0 "$pid" 2>/dev/null
}

mac_service_pid_elapsed_seconds() {
  local pid="${1:-}"
  local elapsed

  mac_service_valid_pid "$pid" || return 1
  elapsed="$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d '[:space:]')"
  mac_service_valid_pid "$elapsed" || return 1
  printf '%s\n' "$elapsed"
}

mac_service_read_pidfile() {
  local pid_file
  pid_file="$(mac_service_service_pid_file "$1")"
  if [[ ! -f "$pid_file" ]]; then
    return 1
  fi

  local pid
  pid="$(tr -d '[:space:]' <"$pid_file" 2>/dev/null || true)"
  mac_service_valid_pid "$pid" || return 1
  printf '%s\n' "$pid"
}

mac_service_launchd_print() {
  launchctl print "$(mac_service_service_target "$1")" 2>/dev/null
}

mac_service_launchd_loaded() {
  mac_service_launchd_print "$1" >/dev/null
}

mac_service_launchd_pid() {
  local printed pid
  printed="$(mac_service_launchd_print "$1" || true)"
  if [[ -z "$printed" ]]; then
    return 1
  fi

  pid="$(printf '%s\n' "$printed" | awk '/^[[:space:]]*pid = / {print $3; exit}' | tr -dc '0-9')"
  mac_service_valid_pid "$pid" || return 1
  printf '%s\n' "$pid"
}

mac_service_service_pid_source() {
  if mac_service_launchd_pid "$1" >/dev/null 2>&1; then
    printf 'launchd\n'
    return 0
  fi
  if mac_service_read_pidfile "$1" >/dev/null 2>&1; then
    printf 'pidfile\n'
    return 0
  fi
  printf 'none\n'
}

mac_service_service_pid() {
  local pid
  if pid="$(mac_service_launchd_pid "$1" 2>/dev/null)"; then
    printf '%s\n' "$pid"
    return 0
  fi
  if pid="$(mac_service_read_pidfile "$1" 2>/dev/null)"; then
    printf '%s\n' "$pid"
    return 0
  fi
  return 1
}

mac_service_public_base_url() {
  if [[ ! -f "$MAC_SERVICE_REPO_ROOT/.env" ]]; then
    return 0
  fi
  awk -F= '/^PUBLIC_BASE_URL=/{print $2; exit}' "$MAC_SERVICE_REPO_ROOT/.env" | tr -d '"' | tr -d "'"
}

mac_service_tunnel_configured() {
  "$MAC_SERVICE_REPO_ROOT/scripts/run_cloudflared.sh" --check >/dev/null 2>&1
}

mac_service_tunnel_check_message() {
  "$MAC_SERVICE_REPO_ROOT/scripts/run_cloudflared.sh" --check 2>&1 >/dev/null || true
}

mac_service_whatsapp_capture_configured() {
  if [[ ! -f "$MAC_SERVICE_REPO_ROOT/.env" ]]; then
    return 1
  fi
  local raw
  raw="$(awk -F= '/^WHATSAPP_CAPTURE_CHAT_NAMES=/{print $2; exit}' "$MAC_SERVICE_REPO_ROOT/.env" | tr -d '"' | tr -d "'")"
  [[ -n "${raw//[[:space:],]/}" ]]
}

mac_service_whatsapp_capture_check_message() {
  echo "Set WHATSAPP_CAPTURE_CHAT_NAMES in $MAC_SERVICE_REPO_ROOT/.env to enable the optional Chrome/Playwright WhatsApp capture service."
}

mac_service_stop_manual_service() {
  local name="$1"
  local pid
  if ! pid="$(mac_service_read_pidfile "$name" 2>/dev/null)"; then
    rm -f "$(mac_service_service_pid_file "$name")"
    return 0
  fi

  if mac_service_pid_alive "$pid"; then
    kill "$pid" 2>/dev/null || true
    local _i
    for _i in 1 2 3 4 5 6 7 8 9 10; do
      if ! mac_service_pid_alive "$pid"; then
        break
      fi
      sleep 1
    done
    if mac_service_pid_alive "$pid"; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  fi

  rm -f "$(mac_service_service_pid_file "$name")"
}

mac_service_start_manual_service() {
  local name="$1"
  local script_path pid_file python_bin pid

  if [[ "$name" == "tunnel" ]] && ! mac_service_tunnel_configured; then
    echo "Skipped tunnel:"
    mac_service_tunnel_check_message
    return 0
  fi
  if [[ "$name" == "whatsapp_capture" ]] && ! mac_service_whatsapp_capture_configured; then
    echo "Skipped whatsapp_capture:"
    mac_service_whatsapp_capture_check_message
    return 0
  fi

  pid_file="$(mac_service_service_pid_file "$name")"
  if pid="$(mac_service_read_pidfile "$name" 2>/dev/null)"; then
    if mac_service_pid_alive "$pid"; then
      echo "${name} already running (pid ${pid})"
      return 0
    fi
    rm -f "$pid_file"
  fi

  script_path="$(mac_service_service_script "$name")"
  python_bin="$(mac_service_runtime_python)" || {
    echo "Missing Python runtime for ${name}" >&2
    return 1
  }

  pid="$("$python_bin" - "$script_path" "$MAC_SERVICE_REPO_ROOT" "$(mac_service_service_stdout_log "$name")" "$(mac_service_service_stderr_log "$name")" <<'PY'
import os
import subprocess
import sys

script_path, repo_root, stdout_path, stderr_path = sys.argv[1:5]

with open(os.devnull, "rb") as devnull:
    with open(stdout_path, "ab", buffering=0) as stdout:
        with open(stderr_path, "ab", buffering=0) as stderr:
            proc = subprocess.Popen(
                [script_path],
                cwd=repo_root,
                stdin=devnull,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
                close_fds=True,
            )

print(proc.pid)
PY
)"
  printf '%s\n' "$pid" >"$pid_file"
  sleep 1

  if mac_service_pid_alive "$pid"; then
    echo "Started ${name} (pid ${pid})"
    return 0
  fi

  echo "Failed to start ${name}. See $(mac_service_service_stderr_log "$name")" >&2
  return 1
}

mac_service_start_service() {
  local name="$1"

  if mac_service_launchd_loaded "$name"; then
    if launchctl kickstart -k "$(mac_service_service_target "$name")" >/dev/null 2>&1; then
      echo "Restarted ${name} via launchd"
      return 0
    fi
    echo "Failed to kickstart ${name} via launchd" >&2
    return 1
  fi

  mac_service_start_manual_service "$name"
}

mac_service_restart_service() {
  local name="$1"

  if mac_service_launchd_loaded "$name"; then
    if launchctl kickstart -k "$(mac_service_service_target "$name")" >/dev/null 2>&1; then
      echo "Restarted ${name} via launchd"
      return 0
    fi
    echo "Failed to restart ${name} via launchd" >&2
    return 1
  fi

  mac_service_stop_manual_service "$name"
  mac_service_start_manual_service "$name"
}

mac_service_bootout_launch_agent() {
  local name="$1"
  local target
  target="$(mac_service_service_target "$name")"
  launchctl bootout "$target" >/dev/null 2>&1 || true
}
