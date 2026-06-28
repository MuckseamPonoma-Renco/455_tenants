import zipfile

from scripts.sync_chat_export_inbox import newest_export, sync_once


def test_newest_export_chooses_latest_file(tmp_path):
    older = tmp_path / "older.zip"
    newer = tmp_path / "newer.zip"
    older.write_text("old", encoding="utf-8")
    newer.write_text("new", encoding="utf-8")

    assert newest_export([tmp_path]) == newer


def test_sync_once_skips_unchanged_after_processing(tmp_path, monkeypatch):
    source_dir = tmp_path / "icloud"
    dest_dir = tmp_path / "incoming"
    state_path = tmp_path / "state.json"
    source_dir.mkdir()
    export_path = source_dir / "WhatsApp Chat - 455 Tenants.zip"
    with zipfile.ZipFile(export_path, "w") as archive:
        archive.writestr("WhatsApp Chat - 455 Tenants.txt", "[6/5/26, 9:00:00 AM] Karen: North lift dead\n")

    calls = []

    def fake_run(export_path_arg, *, since):
        calls.append((str(export_path_arg), since))
        return {"export": str(export_path_arg)}

    monkeypatch.setattr("scripts.sync_chat_export_inbox.run_import_and_audit", fake_run)

    first = sync_once(
        source_dirs=[source_dir],
        dest_dir=dest_dir,
        state_path=state_path,
        since="2026-06-05",
    )
    second = sync_once(
        source_dirs=[source_dir],
        dest_dir=dest_dir,
        state_path=state_path,
        since="2026-06-05",
    )

    assert first["action"] == "processed"
    assert second["action"] == "unchanged_skip"
    assert len(calls) == 1
    assert (dest_dir / export_path.name).exists()
