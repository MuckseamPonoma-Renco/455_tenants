from __future__ import annotations

import json
import os

import pytest

from packages.sheets.public_semantic_overrides import (
    DEFAULT_OVERRIDE_PATH,
    OVERRIDE_PATH_ENV,
    PublicSemanticOverrideError,
    PublicSemanticOverrideHashMismatch,
    get_public_semantic_override,
    load_public_semantic_overrides,
    raw_text_sha256,
)


SYNTHETIC_MESSAGE_ID = "a" * 64
SYNTHETIC_SOURCE = "The fictional north elevator is working again."


@pytest.fixture(autouse=True)
def isolated_override_configuration(monkeypatch):
    monkeypatch.delenv(OVERRIDE_PATH_ENV, raising=False)


def _entry(**updates):
    value = {
        "message_id": "a" * 64,
        "raw_text_sha256": raw_text_sha256("source text"),
        "include": True,
        "issue_label": "Issue",
        "category_label": "Category",
        "summary": "Neutral public summary.",
        "show_evidence": False,
        "reason": "Audited correction.",
    }
    value.update(updates)
    return value


def _write_manifest(tmp_path, entries, **root_updates):
    payload = {"schema_version": 1, "overrides": entries}
    payload.update(root_updates)
    path = tmp_path / "overrides.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_bundled_default_contains_no_operational_records():
    assert dict(load_public_semantic_overrides(DEFAULT_OVERRIDE_PATH)) == {}
    assert dict(load_public_semantic_overrides()) == {}


def test_explicit_private_file_preserves_exact_source_binding(tmp_path, monkeypatch):
    entry = _entry(raw_text_sha256=raw_text_sha256(SYNTHETIC_SOURCE))
    path = _write_manifest(tmp_path, [entry])
    monkeypatch.setenv(OVERRIDE_PATH_ENV, str(path))

    override = get_public_semantic_override(SYNTHETIC_MESSAGE_ID, SYNTHETIC_SOURCE)
    assert override is not None
    assert override.summary == entry["summary"]
    assert override.show_evidence is False
    with pytest.raises(PublicSemanticOverrideHashMismatch, match=SYNTHETIC_MESSAGE_ID):
        get_public_semantic_override(SYNTHETIC_MESSAGE_ID, SYNTHETIC_SOURCE + " Changed.")
    assert get_public_semantic_override("f" * 64, "Unrelated fictional message") is None


def test_private_exclusion_remains_excluded(tmp_path, monkeypatch):
    entry = _entry(include=False, issue_label="", category_label="", summary="", show_evidence=False)
    path = _write_manifest(tmp_path, [entry])
    monkeypatch.setenv(OVERRIDE_PATH_ENV, str(path))
    override = get_public_semantic_override(SYNTHETIC_MESSAGE_ID, "source text")
    assert override is not None and not override.include and not override.show_evidence


def test_missing_configured_file_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setenv(OVERRIDE_PATH_ENV, str(tmp_path / "missing.json"))
    with pytest.raises(PublicSemanticOverrideError, match="unable to load"):
        load_public_semantic_overrides()
    with pytest.raises(PublicSemanticOverrideError, match="unable to load"):
        get_public_semantic_override(SYNTHETIC_MESSAGE_ID, SYNTHETIC_SOURCE)


def test_invalid_configured_file_fails_closed(tmp_path, monkeypatch):
    path = tmp_path / "invalid.json"
    path.write_text("not JSON", encoding="utf-8")
    monkeypatch.setenv(OVERRIDE_PATH_ENV, str(path))
    with pytest.raises(PublicSemanticOverrideError, match="unable to load"):
        load_public_semantic_overrides()


def test_explicit_argument_takes_precedence_over_configuration(tmp_path, monkeypatch):
    monkeypatch.setenv(OVERRIDE_PATH_ENV, str(tmp_path / "missing.json"))
    path = _write_manifest(tmp_path, [_entry()])
    assert list(load_public_semantic_overrides(path)) == [SYNTHETIC_MESSAGE_ID]


def test_configuration_change_is_not_hidden_by_cache(tmp_path, monkeypatch):
    assert not load_public_semantic_overrides()
    path = _write_manifest(tmp_path, [_entry()])
    monkeypatch.setenv(OVERRIDE_PATH_ENV, str(path))
    assert SYNTHETIC_MESSAGE_ID in load_public_semantic_overrides()
    monkeypatch.delenv(OVERRIDE_PATH_ENV)
    assert not load_public_semantic_overrides()


def test_private_file_replacement_or_removal_does_not_use_stale_cache(tmp_path, monkeypatch):
    path = _write_manifest(tmp_path, [_entry()])
    monkeypatch.setenv(OVERRIDE_PATH_ENV, str(path))
    assert SYNTHETIC_MESSAGE_ID in load_public_semantic_overrides()
    previous = path.stat()
    path.write_text(json.dumps({"schema_version": 1, "overrides": []}), encoding="utf-8")
    os.utime(path, ns=(previous.st_atime_ns, previous.st_mtime_ns + 1_000_000_000))
    assert not load_public_semantic_overrides()
    path.unlink()
    with pytest.raises(PublicSemanticOverrideError, match="unable to load"):
        load_public_semantic_overrides()



def test_loader_rejects_duplicate_message_ids(tmp_path):
    entry = _entry()
    path = _write_manifest(tmp_path, [entry, dict(entry)])

    with pytest.raises(PublicSemanticOverrideError, match="duplicate override message_id"):
        load_public_semantic_overrides(path)


@pytest.mark.parametrize(
    "entry, match",
    [
        (_entry(raw_text_sha256="not-a-digest"), "raw_text_sha256"),
        (_entry(include=1), "include must be a boolean"),
        (
            _entry(
                include=False,
                issue_label="Should be blank",
                category_label="",
                summary="",
                show_evidence=False,
            ),
            "excluded rows must have blank public text",
        ),
        (
            _entry(
                include=False,
                issue_label="",
                category_label="",
                summary="",
                show_evidence=True,
            ),
            "excluded rows must have blank public text",
        ),
    ],
)
def test_loader_rejects_unsafe_or_malformed_entries(tmp_path, entry, match):
    path = _write_manifest(tmp_path, [entry])

    with pytest.raises(PublicSemanticOverrideError, match=match):
        load_public_semantic_overrides(path)


def test_loader_rejects_unknown_fields_and_schema_versions(tmp_path):
    entry = _entry(extra="typo")
    bad_field_path = _write_manifest(tmp_path, [entry])

    with pytest.raises(PublicSemanticOverrideError, match=r"unexpected=\['extra'\]"):
        load_public_semantic_overrides(bad_field_path)

    bad_schema_path = _write_manifest(tmp_path, [], schema_version=2)
    with pytest.raises(PublicSemanticOverrideError, match="unsupported.*schema_version"):
        load_public_semantic_overrides(bad_schema_path)
