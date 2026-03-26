from __future__ import annotations

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient
from apps.api.main import app


def auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {os.environ['INGEST_TOKEN']}"}


def mobile_headers() -> dict[str, str]:
    token = os.environ.get('MOBILE_FILER_TOKEN') or os.environ['INGEST_TOKEN']
    return {"Authorization": f"Bearer {token}"}


def main() -> None:
    ts_epoch = int(time.time())
    with TestClient(app) as client:
        health = client.get('/health')
        print('health', health.status_code, health.json())

        ingest = client.post('/ingest/tasker', headers=auth_headers(), json={
            'chat_name': '455 Tenants',
            'text': 'Both elevators are out again',
            'sender': 'Smoke Test',
            'ts_epoch': ts_epoch,
        })
        print('ingest', ingest.status_code, ingest.json())

        queue = client.get('/api/queue', headers=auth_headers())
        print('queue', queue.status_code, queue.json())

        claim = client.post('/mobile/filings/claim_next', headers=mobile_headers())
        print('claim', claim.status_code, claim.json())


if __name__ == '__main__':
    main()
