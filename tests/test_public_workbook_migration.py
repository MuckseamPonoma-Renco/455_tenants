import pytest

from scripts import migrate_public_workbook as migration


class NoMutationService:
    def spreadsheets(self):
        raise AssertionError("No mutation may occur before verification")


def test_requires_separate_workbooks():
    with pytest.raises(ValueError, match="distinct"):
        migration.plan(NoMutationService(), "same", "same")


def test_plan_archives_only_known_obsolete_tabs(monkeypatch):
    monkeypatch.setattr(migration, "_metadata", lambda service, sid: {
        "Tenant Log": 1, "ProjectStatus": 2, "QA Draft old": 3, "My notes": 4,
    } if sid == "public" else {})
    monkeypatch.setattr(migration, "_values", lambda *args: [["original"]])
    result = migration.plan(None, "private", "public")
    assert [item["title"] for item in result] == ["ProjectStatus", "QA Draft old"]
    assert not any(item["verified"] for item in result)


def test_retire_refuses_unverified_archive(monkeypatch):
    monkeypatch.setattr(migration, "plan", lambda *args: [{"verified": False}])
    with pytest.raises(RuntimeError, match="Archive every"):
        migration.retire(NoMutationService(), "private", "public", audit=lambda: {"ok": True})


def test_retire_requires_raw_operator_tables(monkeypatch):
    monkeypatch.setattr(migration, "plan", lambda *args: [{"verified": True}])
    monkeypatch.setattr(migration, "_values", lambda *args: [["wrong schema"]])
    with pytest.raises(RuntimeError, match="operator table"):
        migration.retire(NoMutationService(), "private", "public", audit=lambda: {"ok": True})


def test_retire_refuses_failed_public_readback(monkeypatch):
    monkeypatch.setattr(migration, "plan", lambda *args: [{"verified": True}])
    monkeypatch.setattr(migration, "_values", lambda service, sid, name: [migration.OPERATOR_HEADERS[name]])
    with pytest.raises(RuntimeError, match="audit did not pass"):
        migration.retire(NoMutationService(), "private", "public", audit=lambda: {"ok": False})


def test_retire_refuses_source_change_after_audit(monkeypatch):
    reads = iter([[{"verified": True, "archive": "old"}], [{"verified": False, "archive": "new"}]])
    monkeypatch.setattr(migration, "plan", lambda *args: next(reads))
    monkeypatch.setattr(migration, "_values", lambda service, sid, name: [migration.OPERATOR_HEADERS[name]])
    with pytest.raises(RuntimeError, match="changed during"):
        migration.retire(NoMutationService(), "private", "public", audit=lambda: {"ok": True})


def test_archive_name_binds_full_source_content():
    assert migration.archive_title("QA Draft old", [["one"]]) != migration.archive_title("QA Draft old", [["two"]])
    assert len(migration.archive_title("x" * 100, [["one"]])) <= 100


def test_retirement_audit_requires_each_audience_and_tenant_log(monkeypatch):
    from scripts import audit_public_tenant_log, audit_public_watchdog_tabs
    calls = []

    def watchdog(**kwargs):
        calls.append(kwargs["audience"])
        return {"ok": kwargs["audience"] != "operator"}

    def tenant_log(**kwargs):
        calls.append("tenant_log")
        assert kwargs["resync"] is False
        return {"ok": True}

    monkeypatch.setattr(audit_public_watchdog_tabs, "run_audit", watchdog)
    monkeypatch.setattr(audit_public_tenant_log, "run_audit", tenant_log)
    assert migration.audit_all_views()["ok"] is False
    assert calls == ["operator", "public", "tenant_log"]


def test_verified_retirement_preserves_archive_and_unknown_tabs(monkeypatch):
    public_tabs = {"Tenant Log": 1, "ProjectStatus": 2, "My notes": 3}
    original = [["section", "detail"], ["claim", "Original statement"]]
    saved_title = migration.archive_title("ProjectStatus", original)
    private_tabs = {saved_title: 90}
    private_values = {saved_title: original}
    for index, name in enumerate(migration.WATCHDOG_TAB_ENV, 100):
        private_tabs[name] = index
        private_values[name] = [migration.OPERATOR_HEADERS[name]]
    monkeypatch.setattr(migration, "_metadata", lambda service, sid: dict(public_tabs if sid == "public" else private_tabs))
    monkeypatch.setattr(migration, "_values", lambda service, sid, name: original if sid == "public" else private_values[name])

    class Request:
        def execute(self):
            return {}

    class Service:
        def spreadsheets(self):
            return self

        def batchUpdate(self, *, spreadsheetId, body):
            assert spreadsheetId == "public"
            assert body == {"requests": [{"deleteSheet": {"sheetId": 2}}]}
            public_tabs.pop("ProjectStatus")
            return Request()

    result = migration.retire(Service(), "private", "public", audit=lambda: {"ok": True})
    assert [row["title"] for row in result] == ["ProjectStatus"]
    assert public_tabs == {"Tenant Log": 1, "My notes": 3}
    assert private_values[saved_title] == original
