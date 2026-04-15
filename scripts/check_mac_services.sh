#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="$HOME/.local/var/run/tenant-issue-os"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

print_status() {
  local name="$1"
  local pid_file="$PID_DIR/${name}.pid"
  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "$pid" && "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
      echo "${name}: running pid=${pid}"
      return 0
    fi
    echo "${name}: stale pid file (${pid:-unknown})"
  else
    echo "${name}: not started"
  fi
}

print_status "api"
print_status "automation"
if reason="$("$REPO_ROOT/scripts/run_cloudflared.sh" --check 2>&1 >/dev/null)"; then
  print_status "tunnel"
else
  echo "tunnel: not started"
  echo "$reason"
fi

if curl -fsS http://127.0.0.1:8000/health >"$TMP_DIR/local_health.json"; then
  echo "local /health: ok"
  cat "$TMP_DIR/local_health.json"
  echo
else
  echo "local /health: unavailable"
fi

PUBLIC_BASE_URL="$(awk -F= '/^PUBLIC_BASE_URL=/{print $2; exit}' "$REPO_ROOT/.env" | tr -d '"' | tr -d "'")"
if [[ -n "$PUBLIC_BASE_URL" ]]; then
  PUBLIC_STATUS="$(curl -s -o "$TMP_DIR/public_health.txt" -w '%{http_code}' "${PUBLIC_BASE_URL%/}/health" || true)"
  echo "public /health: ${PUBLIC_STATUS:-unavailable}"
  if [[ -s "$TMP_DIR/public_health.txt" ]]; then
    sed -n '1,2p' "$TMP_DIR/public_health.txt"
  fi
fi
