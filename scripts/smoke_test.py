import os
from fastapi.testclient import TestClient
from apps.api.main import app


def auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {os.environ['INGEST_TOKEN']}"}


def mobile_headers() -> dict[str, str]:
    token = os.environ.get('MOBILE_FILER_TOKEN') or os.environ['INGEST_TOKEN']
    return {"Authorization": f"Bearer {token}"}


def main() -> None:
    with TestClient(app) as client:
        health = client.get('/health')
        print('health', health.status_code, health.json())

        ingest = client.post('/ingest/tasker', headers=auth_headers(), json={
            'chat_name': '455 Tenants',
            'text': 'Both elevators are out again',
            'sender': 'Smoke Test',
            'ts_epoch': 1770000200,
        })
        print('ingest', ingest.status_code, ingest.json())

        queue = client.get('/api/queue', headers=auth_headers())
        print('queue', queue.status_code, queue.json())

        claim = client.post('/mobile/filings/claim_next', headers=mobile_headers())
        print('claim', claim.status_code, claim.json())


if __name__ == '__main__':
    main()
