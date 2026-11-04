import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from packages.db import Base
from packages.public_records import sync


@pytest.mark.parametrize('operation', [sync.sync_public_records, sync.sync_replacement_watchdog])
def test_external_fetch_precedes_database_transaction(monkeypatch, operation):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        calls = []
        def fetch():
            calls.append(True)
            assert not session.in_transaction(), 'Network waits must not hold a DB transaction'
            return []
        monkeypatch.setattr(sync, 'fetch_public_record_rows', fetch)
        operation(session)
        assert calls == [True]
    engine.dispose()
