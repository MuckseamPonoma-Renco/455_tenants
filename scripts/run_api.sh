#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -x "$REPO_ROOT/.venv/bin/uvicorn" ]]; then
  echo "Missing $REPO_ROOT/.venv/bin/uvicorn. Create the virtualenv and install requirements first." >&2
  exit 1
fi

if [[ ! -f "$REPO_ROOT/.env" ]]; then
  echo "Missing $REPO_ROOT/.env" >&2
  exit 1
fi

exec "$REPO_ROOT/.venv/bin/uvicorn" \
  --env-file "$REPO_ROOT/.env" \
  apps.api.main:app \
  --host 127.0.0.1 \
  --port 8000
