#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$HOME/.local/var/log/tenant-issue-os"
PID_DIR="$HOME/.local/var/run/tenant-issue-os"
PYTHON_BIN="$REPO_ROOT/.venv/bin/python"

mkdir -p "$LOG_DIR" "$PID_DIR"

start_service() {
  local name="$1"
  local script_path="$2"
  local pid_file="$PID_DIR/${name}.pid"

  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "$pid" && "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
      echo "${name} already running (pid ${pid})"
      return 0
    fi
    rm -f "$pid_file"
  fi

  if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "Missing $PYTHON_BIN" >&2
    return 1
  fi

  local pid
  pid="$("$PYTHON_BIN" - "$script_path" "$REPO_ROOT" "$LOG_DIR/${name}.out.log" "$LOG_DIR/${name}.err.log" <<'PY'
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
  echo "$pid" >"$pid_file"
  sleep 1

  if kill -0 "$pid" 2>/dev/null; then
    echo "Started ${name} (pid ${pid})"
    return 0
  fi

  echo "Failed to start ${name}. See ${LOG_DIR}/${name}.err.log" >&2
  return 1
}

start_service "api" "$REPO_ROOT/scripts/run_api.sh"
start_service "automation" "$REPO_ROOT/scripts/run_automation.sh"

if reason="$("$REPO_ROOT/scripts/run_cloudflared.sh" --check 2>&1 >/dev/null)"; then
  start_service "tunnel" "$REPO_ROOT/scripts/run_cloudflared.sh"
else
  echo "Skipped tunnel:"
  echo "$reason"
fi
