#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="$REPO_ROOT/.venv/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

cd "$REPO_ROOT"
"$PYTHON_BIN" "$REPO_ROOT/scripts/sync_cloud_chat_export_inbox.py"
exec "$PYTHON_BIN" "$REPO_ROOT/scripts/sync_chat_export_inbox.py" "$@"
