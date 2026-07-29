#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -x "$REPO_ROOT/.venv/bin/python" ]]; then
  echo "Missing $REPO_ROOT/.venv/bin/python. Create the virtualenv and install requirements first." >&2
  exit 1
fi

if [[ ! -f "$REPO_ROOT/.env" ]]; then
  echo "Missing $REPO_ROOT/.env" >&2
  exit 1
fi

exec "$REPO_ROOT/.venv/bin/python" "$REPO_ROOT/scripts/run_whatsapp_capture.py" "$@"
