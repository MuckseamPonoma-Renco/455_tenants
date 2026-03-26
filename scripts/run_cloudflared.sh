#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

CLOUDFLARED_BIN="${CLOUDFLARED_BIN:-$HOME/.local/bin/cloudflared}"
TOKEN_FILE="${CLOUDFLARED_TOKEN_FILE:-$HOME/.cloudflared/tenant-issue-os.token}"
CONFIG_FILE="${CLOUDFLARED_CONFIG_FILE:-$REPO_ROOT/cloudflare/config.yml}"

if [[ ! -x "$CLOUDFLARED_BIN" ]]; then
  echo "Missing cloudflared binary at $CLOUDFLARED_BIN" >&2
  exit 1
fi

if [[ ! -f "$TOKEN_FILE" ]]; then
  echo "Missing tunnel token file at $TOKEN_FILE" >&2
  exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Missing tunnel config at $CONFIG_FILE" >&2
  exit 1
fi

exec "$CLOUDFLARED_BIN" tunnel --config "$CONFIG_FILE" run --token-file "$TOKEN_FILE"
