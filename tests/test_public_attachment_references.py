import copy
import json
import os

import pytest

from packages.db import FilingJob, RawMessage, get_session
from packages.whatsapp import media
from packages.whatsapp.attachments import attachment_items, build_attachment_manifest
from scripts.backfill_public_attachment_references import prepare_backfill, run_backfill


def _manifest(tmp_path, monkeypatch, *, count=1):
    root = tmp_path / "media"
    root.mkdir()
    monkeypatch.setenv("WHATSAPP_CAPTURE_MEDIA_DIR", str(root))
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://tenant.example")
    items = []
    for index in range(count):
        path = root / f"photo-{index}.png"
        path.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\rIHDR" + (1200).to_bytes(4, "big") + (1600).to_bytes(4, "big"))
        os.utime(path, (1777000000 + index, 1777000000 + index))
        items.append({"kind": "image", "status": "downloaded", "path": str(path), "filename": path.name})
    return build_attachment_manifest(items=items, source="export_media")


def _remote(monkeypatch, tmp_path):
    monkeypatch.setattr(media, "media_roots", lambda: (tmp_path / "linux-runner-media",))


def test_reference_preserves_exact_url_and_has_host_independent_rendering(monkeypatch, tmp_path):
    manifest = _manifest(tmp_path, monkeypatch)
    old = media.public_attachment_entries("msg", manifest)
    enriched = media.enrich_public_attachment_references("msg", manifest)
    local = media.public_attachment_entries("msg", enriched)
    assert local[0]["public_url"] == old[0]["public_url"] == "https://tenant.example/media/whatsapp/msg/0?v=1777000000"
    assert media.enrich_public_attachment_references("msg", enriched) == enriched
    os.utime(attachment_items(manifest)[0]["path"], (1888000000, 1888000000))
    assert media.public_attachment_entries("msg", enriched) == local
    _remote(monkeypatch, tmp_path)
    assert media.public_attachment_entries("msg", enriched) == local
    assert media.public_attachment_entries("msg", manifest) == []


def test_physical_duplicate_identity_suffix_is_supported(monkeypatch, tmp_path):
    manifest = _manifest(tmp_path, monkeypatch)
    message_id = "a" * 60 + "~2"
    enriched = media.enrich_public_attachment_references(message_id, manifest)
    _remote(monkeypatch, tmp_path)
    assert media.public_attachment_entries(message_id, enriched)[0]["public_url"].endswith(f"/{message_id}/0?v=1777000000")


@pytest.mark.parametrize("change", ["message", "index", "path", "kind", "status", "source_url", "version", "source_hash"])
def test_reference_rejects_copied_or_changed_source(monkeypatch, tmp_path, change):
    manifest = _manifest(tmp_path, monkeypatch)
    enriched = json.loads(media.enrich_public_attachment_references("msg", manifest))
    item = enriched["items"][0]
    message_id = "msg"
    if change == "message":
        message_id = "different"
    elif change == "index":
        enriched["items"].insert(0, {"kind": "message_screenshot"})
    elif change in {"path", "kind", "status", "source_url"}:
        item[change] = "changed"
    elif change == "version":
        item["public_reference"]["version"] = True
    else:
        item["public_reference"]["source_sha256"] = "0" * 64
    _remote(monkeypatch, tmp_path)
    assert media.public_attachment_entries(message_id, json.dumps(enriched)) == []


@pytest.mark.parametrize("url", [
    "https://evil.example/media/whatsapp/msg/0?v=1777000000",
    "https://tenant.example/media/whatsapp/msg/1?v=1777000000",
    "https://tenant.example/media/whatsapp/msg/0?other=1",
    "https://tenant.example/media/whatsapp/msg/0?v=1&v=2",
    "https://tenant.example/media/whatsapp/msg/0?v=1#fragment",
    "file:///etc/passwd", "javascript:alert(1)",
])
def test_remote_reference_accepts_only_exact_canonical_origin_path_and_version(monkeypatch, tmp_path, url):
    manifest = _manifest(tmp_path, monkeypatch)
    enriched = json.loads(media.enrich_public_attachment_references("msg", manifest))
    enriched["items"][0]["public_reference"]["url"] = url
    _remote(monkeypatch, tmp_path)
    assert media.public_attachment_entries("msg", json.dumps(enriched)) == []


@pytest.mark.parametrize("kind,source_url,status", [
    ("message_screenshot", "", "captured"),
    ("image", "https://static.whatsapp.net/rsrc/profile.png", "downloaded"),
    ("image", "", "metadata_only"),
    ("image", "", "download_error"),
    ("legacy_reference", "", "downloaded"),
])
def test_private_or_unavailable_media_never_gets_reference(monkeypatch, tmp_path, kind, source_url, status):
    manifest = json.loads(_manifest(tmp_path, monkeypatch))
    manifest["items"][0].update(kind=kind, source_url=source_url, status=status)
    raw = json.dumps(manifest)
    assert media.enrich_public_attachment_references("msg", raw) == raw


def test_outside_root_symlink_and_missing_file_never_get_reference(monkeypatch, tmp_path):
    manifest = json.loads(_manifest(tmp_path, monkeypatch))
    external = tmp_path / "outside.txt"
    external.write_text("private")
    for path in (external, tmp_path / "media" / "missing.png"):
        manifest["items"][0]["path"] = str(path)
        raw = json.dumps(manifest)
        assert media.enrich_public_attachment_references("msg", raw) == raw
    link = tmp_path / "media" / "escape.png"
    link.symlink_to(external)
    manifest["items"][0]["path"] = str(link)
    raw = json.dumps(manifest)
    assert media.enrich_public_attachment_references("msg", raw) == raw


def test_existing_noncanonical_link_keeps_local_behavior_without_remote_fallback(monkeypatch, tmp_path):
    manifest = json.loads(_manifest(tmp_path, monkeypatch))
    manifest["items"][0]["sheet_image_url"] = "https://cdn.example/photo.png"
    raw = json.dumps(manifest)
    assert media.enrich_public_attachment_references("msg", raw) == raw
    assert media.public_attachment_entries("msg", raw)[0]["public_url"] == "https://cdn.example/photo.png"
    _remote(monkeypatch, tmp_path)
    assert media.public_attachment_entries("msg", raw) == []


def test_reference_and_legacy_alias_keep_path_deduplication(monkeypatch, tmp_path):
    manifest = json.loads(_manifest(tmp_path, monkeypatch))
    manifest["items"].append(copy.deepcopy(manifest["items"][0]))
    enriched = media.enrich_public_attachment_references("msg", json.dumps(manifest), attachment_indices={0})
    local = media.public_attachment_entries("msg", enriched)
    assert len(local) == 1
    _remote(monkeypatch, tmp_path)
    assert media.public_attachment_entries("msg", enriched) == local


def test_reference_does_not_relax_media_serving_path_boundary(client, monkeypatch, tmp_path):
    manifest = _manifest(tmp_path, monkeypatch)
    enriched = media.enrich_public_attachment_references("msg", manifest)
    with get_session() as session:
        session.add(RawMessage(message_id="msg", chat_name="455 Tenants", sender_hash="sender", text="Photo", attachments=enriched))
        session.commit()
    assert client.get("/media/whatsapp/msg/0").status_code == 200
    _remote(monkeypatch, tmp_path)
    assert len(media.public_attachment_entries("msg", enriched)) == 1
    assert client.get("/media/whatsapp/msg/0").status_code == 404


def test_backfill_is_exactly_scoped_hash_bound_and_idempotent(client, monkeypatch, tmp_path):
    manifest = _manifest(tmp_path, monkeypatch, count=2)
    urls = media.public_attachment_entries("msg", manifest)
    whitelist = [{"message_id": "msg", "attachment_index": 0, "public_url": urls[0]["public_url"]}]
    with get_session() as session:
        session.add_all([
            RawMessage(message_id="msg", sender_hash="sender", text="Photo", attachments=manifest),
            RawMessage(message_id="unselected", sender_hash="sender", text="Private", attachments=manifest),
        ])
        session.commit()
    preview = run_backfill(whitelist)
    assert preview["ok"] and preview["references_added"] == 1
    assert run_backfill(whitelist, apply=True, expected_preview_sha256="bad")["action"] == "blocked"
    with get_session() as session:
        assert session.get(RawMessage, "msg").attachments == manifest
    applied = run_backfill(whitelist, apply=True, expected_preview_sha256=preview["preview_sha256"])
    assert applied["action"] == "applied"
    with get_session() as session:
        enriched = session.get(RawMessage, "msg").attachments
        assert "public_reference" in attachment_items(enriched)[0]
        assert attachment_items(enriched)[1] == attachment_items(manifest)[1]
        assert session.get(RawMessage, "unselected").attachments == manifest
        assert session.query(FilingJob).count() == 0
    repeated = run_backfill(whitelist)
    assert repeated["ok"] and repeated["changed_messages"] == 0 and repeated["references_added"] == 0


def test_backfill_rejects_stale_url_whitelist_and_changed_source(client, monkeypatch, tmp_path):
    manifest = _manifest(tmp_path, monkeypatch)
    url = media.public_attachment_entries("msg", manifest)[0]["public_url"]
    whitelist = [{"message_id": "msg", "attachment_index": 0, "public_url": url}]
    preview, _ = prepare_backfill({"msg": manifest}, whitelist)
    assert preview["ok"]
    changed = json.loads(manifest)
    changed["items"][0]["label"] = "changed after preview"
    with get_session() as session:
        session.add(RawMessage(message_id="msg", sender_hash="sender", text="Photo", attachments=json.dumps(changed)))
        session.commit()
    assert run_backfill(whitelist, apply=True, expected_preview_sha256=preview["preview_sha256"])["action"] == "blocked"
    bad_whitelist = [{**whitelist[0], "public_url": url + "1"}]
    bad, _ = prepare_backfill({"msg": manifest}, bad_whitelist)
    assert bad["ok"] is False
