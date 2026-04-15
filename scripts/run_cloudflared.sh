#!/usr/bin/env bash
set -euo pipefail

CHECK_ONLY=0
if [[ "${1:-}" == "--check" ]]; then
  CHECK_ONLY=1
  shift
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

find_cloudflared_bin() {
  if [[ -n "${CLOUDFLARED_BIN:-}" && -x "${CLOUDFLARED_BIN}" ]]; then
    printf '%s\n' "$CLOUDFLARED_BIN"
    return 0
  fi
  if command -v cloudflared >/dev/null 2>&1; then
    command -v cloudflared
    return 0
  fi
  for candidate in \
    "$HOME/.local/bin/cloudflared" \
    "/opt/homebrew/bin/cloudflared" \
    "/usr/local/bin/cloudflared"
  do
    if [[ -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}

CLOUDFLARED_BIN="${CLOUDFLARED_BIN:-$(find_cloudflared_bin || true)}"
TOKEN_FILE="${CLOUDFLARED_TOKEN_FILE:-$HOME/.cloudflared/tenant-issue-os.token}"
CONFIG_FILE="${CLOUDFLARED_CONFIG_FILE:-$REPO_ROOT/cloudflare/config.yml}"
TUNNEL_ID="${CLOUDFLARED_TUNNEL_ID:-$(awk '/^tunnel:/{print $2; exit}' "$CONFIG_FILE" 2>/dev/null || true)}"
TUNNEL_NAME="${CLOUDFLARED_TUNNEL_NAME:-${TUNNEL_ID:-tenant-issue-os}}"
CREDENTIALS_FILE="${CLOUDFLARED_CREDENTIALS_FILE:-${TUNNEL_ID:+$HOME/.cloudflared/${TUNNEL_ID}.json}}"

if [[ -z "$CLOUDFLARED_BIN" || ! -x "$CLOUDFLARED_BIN" ]]; then
  echo "Missing cloudflared binary at $CLOUDFLARED_BIN" >&2
  exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Missing tunnel config at $CONFIG_FILE" >&2
  exit 1
fi

if [[ -n "${CLOUDFLARED_TOKEN:-}" ]]; then
  if [[ "$CHECK_ONLY" -eq 1 ]]; then
    echo "cloudflared ready via CLOUDFLARED_TOKEN"
    exit 0
  fi
  exec "$CLOUDFLARED_BIN" tunnel --config "$CONFIG_FILE" run --token "$CLOUDFLARED_TOKEN"
fi

if [[ -f "$TOKEN_FILE" ]]; then
  if [[ "$CHECK_ONLY" -eq 1 ]]; then
    echo "cloudflared ready via token file"
    exit 0
  fi
  exec "$CLOUDFLARED_BIN" tunnel --config "$CONFIG_FILE" run --token-file "$TOKEN_FILE"
fi

if [[ -n "$CREDENTIALS_FILE" && -f "$CREDENTIALS_FILE" ]]; then
  if [[ "$CHECK_ONLY" -eq 1 ]]; then
    echo "cloudflared ready via credentials file"
    exit 0
  fi
  exec "$CLOUDFLARED_BIN" tunnel --credentials-file "$CREDENTIALS_FILE" --config "$CONFIG_FILE" run "$TUNNEL_NAME"
fi

echo "Missing Cloudflare tunnel auth. Expected one of:" >&2
echo "  - CLOUDFLARED_TOKEN in the environment" >&2
echo "  - token file at $TOKEN_FILE" >&2
if [[ -n "$CREDENTIALS_FILE" ]]; then
  echo "  - credentials file at $CREDENTIALS_FILE" >&2
fi
exit 1
