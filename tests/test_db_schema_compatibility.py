from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from packages import db
from packages.incident.extractor import _with_rule_state_lock


def test_chosen_source_column_fits_longest_composed_provenance_label():
    rule_choice = {
        "is_issue": True,
        "event_type": "outage",
        "close_incident": False,
    }
    model_choice = {
        "is_issue": True,
        "event_type": "restore",
        "close_incident": True,
    }

    _choice, source = _with_rule_state_lock(
        rule_choice,
        model_choice,
        "hybrid_open_incident_context",
    )

    assert source == "hybrid_open_incident_context_rule_state"
    assert len(source) > 32
    assert db.MessageDecision.__table__.c.chosen_source.type.length >= len(source)


def test_schema_compatibility_widens_existing_postgres_chosen_source(monkeypatch):
    executed: list[str] = []

    class FakeInspector:
        def get_table_names(self):
            return ["message_decisions"]

        def get_columns(self, table_name):
            assert table_name == "message_decisions"
            return [{"name": "chosen_source", "type": SimpleNamespace(length=32)}]

    class FakeConnection:
        def execute(self, statement):
            executed.append(str(statement))

    class FakeEngine:
        dialect = SimpleNamespace(name="postgresql")

        @contextmanager
        def begin(self):
            yield FakeConnection()

    fake_engine = FakeEngine()
    monkeypatch.setattr(db, "engine", fake_engine)
    monkeypatch.setattr(db, "inspect", lambda engine: FakeInspector())

    db._ensure_added_columns()

    assert executed == [
        "ALTER TABLE message_decisions ALTER COLUMN chosen_source TYPE VARCHAR(64)"
    ]
