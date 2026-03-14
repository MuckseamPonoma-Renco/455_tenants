import os
import sys
from pathlib import Path
import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load_local_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key, value)


load_local_env()

BASE_URL = os.environ.get("BASE_URL", "").strip().rstrip("/")

if not BASE_URL:
    from fastapi.testclient import TestClient
    from apps.api.main import app


def auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {os.environ['INGEST_TOKEN']}"}


def mobile_headers() -> dict[str, str]:
    token = os.environ.get('MOBILE_FILER_TOKEN') or os.environ['INGEST_TOKEN']
    return {"Authorization": f"Bearer {token}"}


def run_checks(client) -> None:
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


def main() -> None:
    if BASE_URL:
        with httpx.Client(base_url=BASE_URL, timeout=30.0) as client:
            run_checks(client)
        return

    with TestClient(app) as client:
        run_checks(client)


if __name__ == '__main__':
    main()
