import os
from pathlib import Path
from fastapi import APIRouter
from packages.llm.openai_client import llm_enabled
from packages.whatsapp.status import read_capture_status

router = APIRouter()


def _truthy(name: str, default: str = '0') -> bool:
    return os.environ.get(name, default).strip().lower() in {'1', 'true', 'yes', 'on'}


def _sheets_creds_present() -> bool:
    candidates = [
        os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'),
        '/run/secrets/gcp_sa.json',
        '/run/secrets/gcp_sa_json',
        '/etc/secrets/gcp_sa.json',
        'secrets/gcp_sa.json',
    ]
    return any(path and Path(path).exists() for path in candidates)


@router.get('/health')
def health():
    whatsapp_capture = read_capture_status()
    return {
        'ok': True,
        'process_inline': _truthy('PROCESS_INLINE'),
        'llm_enabled': llm_enabled(),
        'sheets_disabled': _truthy('DISABLE_SHEETS_SYNC'),
        'database_configured': bool((os.environ.get('DATABASE_URL') or '').strip()),
        'redis_configured': bool((os.environ.get('REDIS_URL') or '').strip()),
        'sheets_configured': bool((os.environ.get('GOOGLE_SHEETS_SPREADSHEET_ID') or '').strip()) and _sheets_creds_present(),
        'whatsapp_capture': whatsapp_capture or None,
    }
