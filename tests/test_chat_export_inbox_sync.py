import json
import zipfile

import scripts.sync_chat_export_inbox as inbox_sync
from scripts.sync_chat_export_inbox import newest_export, sync_once


def test_newest_export_chooses_latest_file(tmp_path):
    older = tmp_path / "older.txt"
    newer = tmp_path / "newer.txt"
    older.write_text("old", encoding="utf-8")
    newer.write_text("new", encoding="utf-8")

    assert newest_export([tmp_path]) == newer


def test_newest_export_accepts_root_level_whatsapp_export_without_scanning_other_zips(tmp_path, monkeypatch):
    icloud_root = tmp_path / "icloud-root"
    inbox = icloud_root / "455 Tenant Chat Exports"
    icloud_root.mkdir()
    inbox.mkdir()
    older = inbox / "WhatsApp Chat - 455 Tenants 10.txt"
    older.write_text("old", encoding="utf-8")
    unrelated = icloud_root / "unrelated-newest.zip"
    unrelated.write_text("not a WhatsApp export", encoding="utf-8")
    latest = icloud_root / "WhatsApp Chat - 455 Tenants 11.zip"
    with zipfile.ZipFile(latest, "w") as archive:
        archive.writestr("WhatsApp Chat - 455 Tenants.txt", "new")

    monkeypatch.setattr(inbox_sync, "ICLOUD_DRIVE_ROOT", icloud_root)

    assert newest_export([inbox, icloud_root]) == latest


def test_sync_waits_for_a_newer_zero_byte_icloud_placeholder(tmp_path, monkeypatch):
    icloud_root = tmp_path / "icloud-root"
    inbox = icloud_root / "455 Tenant Chat Exports"
    dest_dir = tmp_path / "incoming"
    state_path = tmp_path / "state.json"
    icloud_root.mkdir()
    inbox.mkdir()
    older = inbox / "WhatsApp Chat - 455 Tenants 10.txt"
    older.write_text("old", encoding="utf-8")
    pending = icloud_root / "WhatsApp Chat - 455 Tenants 11.zip"
    pending.touch()
    state_path.write_text(
        json.dumps(
            {
                "last_processed_fingerprint": {
                    "name": pending.name,
                    "path": str(dest_dir / pending.name),
                    "size": 0,
                },
                "last_result": {"export": str(dest_dir / pending.name)},
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(inbox_sync, "ICLOUD_DRIVE_ROOT", icloud_root)
    result = sync_once(
        source_dirs=[inbox, icloud_root],
        dest_dir=dest_dir,
        state_path=state_path,
        since="2026-06-05",
    )

    assert result["action"] == "waiting_for_download"
    assert result["source"] == str(pending)
    assert not dest_dir.exists()
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert "last_processed_fingerprint" not in state


def test_sync_replaces_an_empty_staged_file_after_icloud_finishes(tmp_path, monkeypatch):
    icloud_root = tmp_path / "icloud-root"
    dest_dir = tmp_path / "incoming"
    state_path = tmp_path / "state.json"
    icloud_root.mkdir()
    source = icloud_root / "WhatsApp Chat - 455 Tenants 11.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("WhatsApp Chat - 455 Tenants.txt", "north elevator update")
    dest_dir.mkdir()
    staged = dest_dir / source.name
    staged.touch()
    calls = []

    monkeypatch.setattr(inbox_sync, "ICLOUD_DRIVE_ROOT", icloud_root)
    monkeypatch.setattr(
        inbox_sync,
        "run_import_and_audit",
        lambda export_path, *, since: calls.append((export_path, since)) or {"export": str(export_path)},
    )
    result = sync_once(
        source_dirs=[icloud_root, dest_dir],
        dest_dir=dest_dir,
        state_path=state_path,
        since="2026-06-05",
    )

    assert result["action"] == "processed"
    assert staged.stat().st_size == source.stat().st_size
    assert zipfile.is_zipfile(staged)
    assert calls == [(staged, "2026-06-05")]


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
