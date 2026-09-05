from __future__ import annotations

import json

import pytest

from packages.sheets.public_semantic_overrides import (
    PublicSemanticOverrideError,
    PublicSemanticOverrideHashMismatch,
    get_public_semantic_override,
    load_public_semantic_overrides,
    raw_text_sha256,
)


RESTORE_MESSAGE_ID = "a1f33f3c5ea919e042d082a0a25768ffafe85230ce57490f155c16b1971086be"
TRANSIT_MESSAGE_ID = "d2abe256aac02ed84ffd4a7926ae5f8c500fdf7562cd14d842a3799275cf5c38"


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


def test_default_manifest_is_complete_and_privacy_conservative():
    overrides = load_public_semantic_overrides()

    assert len(overrides) == 134
    assert sum(item.include for item in overrides.values()) == 97
    assert sum(not item.include for item in overrides.values()) == 37

    restore = overrides[RESTORE_MESSAGE_ID]
    assert restore.include is True
    assert restore.issue_label == "Both elevators working"
    assert restore.summary == "Both elevators were reported working."

    transit = overrides[TRANSIT_MESSAGE_ID]
    assert transit.include is False
    assert transit.show_evidence is False
    assert transit.summary == ""

    # Rescue photos show residents, and the garage photo contains readable
    # vehicle plates, so neither may be linked from the public sheet.
    assert overrides[
        "82830e6f0ecb201ad6a3cad9c125647047b69a05a5d382885bdba08821679b36"
    ].show_evidence is False
    assert overrides[
        "5e14fe204def6391e047d33a647802b5a6bb1251d8e01e67b4f38018a68839c9"
    ].show_evidence is False


def test_exact_message_id_and_raw_text_resolve_audited_override():
    override = get_public_semantic_override(RESTORE_MESSAGE_ID, "Both currently working")

    assert override is not None
    assert override.include is True
    assert override.summary == "Both elevators were reported working."


def test_unknown_message_id_has_no_override():
    assert get_public_semantic_override("f" * 64, "anything") is None


def test_known_message_id_with_changed_text_fails_closed():
    with pytest.raises(PublicSemanticOverrideHashMismatch, match=RESTORE_MESSAGE_ID):
        get_public_semantic_override(RESTORE_MESSAGE_ID, "Both currently working.")


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
