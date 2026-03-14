import os
import shutil
import sys
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
DB_PATH = ROOT / 'test_app.sqlite3'
AUX_AUDIT_DIR = ROOT / '.local' / 'test_audit'
if DB_PATH.exists():
    DB_PATH.unlink()
if AUX_AUDIT_DIR.exists():
    shutil.rmtree(AUX_AUDIT_DIR)

os.environ['DATABASE_URL'] = f'sqlite:///{DB_PATH}'
os.environ['PROCESS_INLINE'] = '1'
os.environ['DISABLE_SHEETS_SYNC'] = '1'
os.environ['AUDIT_DIR'] = str(AUX_AUDIT_DIR)
os.environ['INGEST_TOKEN'] = 'test-token'
os.environ['MOBILE_FILER_TOKEN'] = 'mobile-token'
os.environ['AUTO_FILE_ENABLED'] = '1'
os.environ['AUTO_FILE_ELEVATOR_ONLY'] = '1'
os.environ['AUTO_FILE_MIN_WITNESSES'] = '1'
os.environ['AUTO_FILE_MIN_REPORTS'] = '1'
os.environ['LLM_MODE'] = 'off'
os.environ['BUILDING_NAME'] = '455 Ocean Parkway'
os.environ['BUILDING_STREET_ADDRESS'] = '455 Ocean Pkwy'
os.environ['BUILDING_CITY'] = 'Brooklyn'
os.environ['BUILDING_STATE'] = 'NY'
os.environ['BUILDING_ZIP'] = '11218'
os.environ['BUILDING_BOROUGH'] = 'Brooklyn'
os.environ['NYC311_CONTACT_NAME'] = 'Test Tenant'
os.environ['NYC311_CONTACT_PHONE'] = '5555555555'
os.environ['NYC311_CONTACT_EMAIL'] = 'test@example.com'

from apps.api.main import app  # noqa: E402
from packages.db import Base, engine  # noqa: E402

Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)


@pytest.fixture()
def client():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    if AUX_AUDIT_DIR.exists():
        shutil.rmtree(AUX_AUDIT_DIR)
    with TestClient(app) as c:
        yield c
