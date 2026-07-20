#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${CHAT_EXPORT_SYNC_PYTHON_BIN:-$REPO_ROOT/.venv/bin/python}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

cd "$REPO_ROOT"
if ! "$PYTHON_BIN" "$REPO_ROOT/scripts/sync_cloud_chat_export_inbox.py"; then
  echo "Cloud export recovery failed; continuing with the iCloud fallback." >&2
fi
exec "$PYTHON_BIN" "$REPO_ROOT/scripts/sync_chat_export_inbox.py" "$@"
