import os
import sys
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
DB_PATH = ROOT / 'test_app.sqlite3'
if DB_PATH.exists():
    DB_PATH.unlink()

os.environ['DATABASE_URL'] = f'sqlite:///{DB_PATH}'
os.environ['PROCESS_INLINE'] = '1'
os.environ['DISABLE_SHEETS_SYNC'] = '1'
os.environ['INGEST_TOKEN'] = 'test-token'
os.environ['MOBILE_FILER_TOKEN'] = 'mobile-token'
os.environ['AUTO_FILE_ENABLED'] = '1'
os.environ['AUTO_FILE_ELEVATOR_ONLY'] = '1'
os.environ['AUTO_FILE_MIN_WITNESSES'] = '1'
os.environ['AUTO_FILE_MIN_REPORTS'] = '1'
os.environ['LLM_MODE'] = 'off'
os.environ['AUDIT_DIR'] = str(ROOT / '.test_audit')
os.environ['BUILDING_NAME'] = '455 Ocean Parkway'
os.environ['BUILDING_FULL_ADDRESS'] = '455 OCEAN PARKWAY, BROOKLYN, NY, 11218'
os.environ['BUILDING_STREET_ADDRESS'] = '455 OCEAN PARKWAY'
os.environ['BUILDING_CITY'] = 'BROOKLYN'
os.environ['BUILDING_STATE'] = 'NY'
os.environ['BUILDING_ZIP'] = '11218'
os.environ['BUILDING_BOROUGH'] = 'BROOKLYN'
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
    with TestClient(app) as c:
        yield c
os.environ['AUTO_FILE_MAX_INCIDENT_AGE_HOURS'] = '0'
