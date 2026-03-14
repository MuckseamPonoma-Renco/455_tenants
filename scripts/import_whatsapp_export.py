import argparse
import os
from pathlib import Path
from fastapi.testclient import TestClient
from apps.api.main import app


def main() -> None:
    parser = argparse.ArgumentParser(description='Import a WhatsApp export zip/txt into Tenant Issue OS.')
    parser.add_argument('export_path', help='Path to the WhatsApp export .zip or .txt file')
    args = parser.parse_args()

    export_path = Path(args.export_path)
    if not export_path.exists():
        raise SystemExit(f'File not found: {export_path}')

    token = os.environ['INGEST_TOKEN']
    with TestClient(app) as client, export_path.open('rb') as f:
        resp = client.post(
            '/ingest/export',
            headers={'Authorization': f'Bearer {token}'},
            files={'file': (export_path.name, f, 'application/octet-stream')},
        )
    print(resp.status_code)
    print(resp.json())


if __name__ == '__main__':
    main()
