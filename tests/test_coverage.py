from contextlib import contextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker

from packages.db import RawMessage
from packages.verification import coverage


@pytest.fixture
def coverage_database(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    RawMessage.__table__.create(engine)
    sessions = sessionmaker(bind=engine)

    @contextmanager
    def get_session():
        with sessions() as session:
            yield session

    monkeypatch.setattr(coverage, "get_session", get_session)
    yield engine, sessions
    engine.dispose()


def seed_messages(sessions, timestamps):
    with sessions.begin() as session:
        session.add_all(
            RawMessage(
                message_id=str(index),
                sender_hash="tenant",
                ts_epoch=timestamp,
                text="Private message body " * 100,
                attachments='["private-attachment"]',
            )
            for index, timestamp in enumerate(timestamps)
        )


def test_coverage_counts_and_utc_boundaries(coverage_database):
    _, sessions = coverage_database
    midnight = int(datetime(2026, 10, 25, tzinfo=timezone.utc).timestamp())
    seed_messages(
        sessions,
        [None, 0, -86401, -86400, -1, 1, 86399, 86400,
         midnight - 1, midnight, midnight, midnight + 86399, midnight + 86400],
    )

    assert coverage.compute_daily_coverage() == [
        coverage.CoverageRow("1969-12-30", 1, -86401, -86401),
        coverage.CoverageRow("1969-12-31", 2, -86400, -1),
        coverage.CoverageRow("1970-01-01", 2, 1, 86399),
        coverage.CoverageRow("1970-01-02", 1, 86400, 86400),
        coverage.CoverageRow("2026-10-24", 1, midnight - 1, midnight - 1),
        coverage.CoverageRow("2026-10-25", 3, midnight, midnight + 86399),
        coverage.CoverageRow("2026-10-26", 1, midnight + 86400, midnight + 86400),
    ]


@pytest.mark.parametrize("limit_days", [1, 2, 3, 60, 0, -1, -2, -3, -60])
def test_coverage_preserves_day_limit_and_order(coverage_database, limit_days):
    _, sessions = coverage_database
    seed_messages(sessions, [86400, 172800, 259200])
    expected = [
        coverage.CoverageRow("1970-01-02", 1, 86400, 86400),
        coverage.CoverageRow("1970-01-03", 1, 172800, 172800),
        coverage.CoverageRow("1970-01-04", 1, 259200, 259200),
    ]

    assert coverage.compute_daily_coverage(limit_days) == expected[-limit_days:]


@pytest.mark.parametrize("timestamps", [[], [None, 0]])
def test_coverage_empty_or_missing_timestamps(coverage_database, timestamps):
    _, sessions = coverage_database
    seed_messages(sessions, timestamps)

    assert coverage.compute_daily_coverage() == []


def test_coverage_fetches_only_bounded_aggregates(coverage_database):
    engine, sessions = coverage_database
    seed_messages(sessions, [day * 86400 + second for day in range(1, 5) for second in range(10)])
    statements = []
    loaded_messages = []

    def record_statement(connection, cursor, statement, parameters, context, executemany):
        statements.append((statement, context.compiled.statement))

    def record_instance(session, instance):
        loaded_messages.append(instance)

    event.listen(engine, "before_cursor_execute", record_statement)
    event.listen(sessions, "loaded_as_persistent", record_instance)
    try:
        result = coverage.compute_daily_coverage(limit_days=2)
    finally:
        event.remove(engine, "before_cursor_execute", record_statement)
        event.remove(sessions, "loaded_as_persistent", record_instance)

    assert [row.messages for row in result] == [10, 10]
    assert len(statements) == 1
    assert loaded_messages == []
    sql, statement = statements[0]
    projection = sql.lower().split("from", 1)[0]
    assert "count(" in projection
    assert "min(raw_messages.ts_epoch)" in projection
    assert "max(raw_messages.ts_epoch)" in projection
    for column in ("text", "attachments", "sender", "message_id", "chat_name"):
        assert f"raw_messages.{column}" not in sql.lower()
    assert "group by" in sql.lower()
    assert "limit" in sql.lower()

    # The same arithmetic uses integer division on PostgreSQL, independent
    # of the connection's time zone and without SQLite date functions.
    postgres_sql = str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert "CAST(raw_messages.ts_epoch AS BIGINT)" in postgres_sql
    assert "END / 86400" in postgres_sql
    assert "LIMIT 2" in postgres_sql
