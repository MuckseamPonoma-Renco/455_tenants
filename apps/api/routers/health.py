import os
from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from packages.db import engine, init_db

router = APIRouter()


@router.get('/health')
def health():
    try:
        init_db()
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f'database unavailable: {exc}') from exc

    return {
        'ok': True,
        'db': 'ok',
        'process_inline': os.environ.get('PROCESS_INLINE', '0').strip().lower() in {'1', 'true', 'yes', 'on'},
        'sheets_sync_disabled': os.environ.get('DISABLE_SHEETS_SYNC', '0').strip().lower() in {'1', 'true', 'yes', 'on'},
        'llm_mode': os.environ.get('LLM_MODE', 'off'),
    }
