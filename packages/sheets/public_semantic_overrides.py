"""Audited, fail-closed semantic overrides for the public Tenant Log.

These overrides are deliberately keyed by both the immutable message ID and a
SHA-256 digest of the exact raw message text.  A matching ID with changed text
raises instead of silently applying a stale public-facing decision.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType
from typing import Mapping


DEFAULT_OVERRIDE_PATH = Path(__file__).with_name("public_semantic_overrides.json")
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_ROOT_FIELDS = frozenset({"schema_version", "overrides"})
_ENTRY_FIELDS = frozenset(
    {
        "message_id",
        "raw_text_sha256",
        "include",
        "issue_label",
        "category_label",
        "summary",
        "show_evidence",
        "reason",
    }
)


class PublicSemanticOverrideError(RuntimeError):
    """Base error for invalid or stale public semantic override data."""


class PublicSemanticOverrideHashMismatch(PublicSemanticOverrideError):
    """The source text changed for an audited message ID."""


@dataclass(frozen=True, slots=True)
class PublicSemanticOverride:
    message_id: str
    raw_text_sha256: str
    include: bool
    issue_label: str
    category_label: str
    summary: str
    show_evidence: bool
    reason: str


def raw_text_sha256(raw_text: str) -> str:
    """Return the digest used by the audited override manifest."""

    if not isinstance(raw_text, str):
        raise TypeError("raw_text must be a string")
    return hashlib.sha256(raw_text.encode("utf-8")).hexdigest()


def _require_exact_fields(payload: Mapping[str, object], expected: frozenset[str], *, location: str) -> None:
    actual = frozenset(payload)
    if actual == expected:
        return
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    raise PublicSemanticOverrideError(
        f"invalid fields at {location}: missing={missing}, unexpected={unexpected}"
    )


def _parse_entry(payload: object, *, index: int) -> PublicSemanticOverride:
    location = f"overrides[{index}]"
    if not isinstance(payload, dict):
        raise PublicSemanticOverrideError(f"{location} must be an object")
    _require_exact_fields(payload, _ENTRY_FIELDS, location=location)

    string_fields = (
        "message_id",
        "raw_text_sha256",
        "issue_label",
        "category_label",
        "summary",
        "reason",
    )
    for field in string_fields:
        if not isinstance(payload[field], str):
            raise PublicSemanticOverrideError(f"{location}.{field} must be a string")
    for field in ("include", "show_evidence"):
        if not isinstance(payload[field], bool):
            raise PublicSemanticOverrideError(f"{location}.{field} must be a boolean")

    message_id = payload["message_id"]
    digest = payload["raw_text_sha256"]
    include = payload["include"]
    issue_label = payload["issue_label"]
    category_label = payload["category_label"]
    summary = payload["summary"]
    show_evidence = payload["show_evidence"]
    reason = payload["reason"]

    if not _HEX_64.fullmatch(message_id):
        raise PublicSemanticOverrideError(f"{location}.message_id must be 64 lowercase hex characters")
    if not _HEX_64.fullmatch(digest):
        raise PublicSemanticOverrideError(
            f"{location}.raw_text_sha256 must be 64 lowercase hex characters"
        )
    if not reason.strip():
        raise PublicSemanticOverrideError(f"{location}.reason must not be blank")
    if include:
        if not issue_label.strip() or not category_label.strip() or not summary.strip():
            raise PublicSemanticOverrideError(
                f"{location} included rows require issue_label, category_label, and summary"
            )
    elif issue_label or category_label or summary or show_evidence:
        raise PublicSemanticOverrideError(
            f"{location} excluded rows must have blank public text and show_evidence=false"
        )

    return PublicSemanticOverride(
        message_id=message_id,
        raw_text_sha256=digest,
        include=include,
        issue_label=issue_label,
        category_label=category_label,
        summary=summary,
        show_evidence=show_evidence,
        reason=reason,
    )


def _load_override_file(path: Path) -> Mapping[str, PublicSemanticOverride]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PublicSemanticOverrideError(f"unable to load public semantic overrides from {path}") from exc

    if not isinstance(payload, dict):
        raise PublicSemanticOverrideError("public semantic override root must be an object")
    _require_exact_fields(payload, _ROOT_FIELDS, location="root")
    if type(payload["schema_version"]) is not int or payload["schema_version"] != 1:
        raise PublicSemanticOverrideError("unsupported public semantic override schema_version")
    entries = payload["overrides"]
    if not isinstance(entries, list):
        raise PublicSemanticOverrideError("root.overrides must be an array")

    parsed: dict[str, PublicSemanticOverride] = {}
    for index, entry in enumerate(entries):
        override = _parse_entry(entry, index=index)
        if override.message_id in parsed:
            raise PublicSemanticOverrideError(f"duplicate override message_id: {override.message_id}")
        parsed[override.message_id] = override
    return MappingProxyType(parsed)


@lru_cache(maxsize=1)
def _load_default_overrides() -> Mapping[str, PublicSemanticOverride]:
    return _load_override_file(DEFAULT_OVERRIDE_PATH)


def load_public_semantic_overrides(
    path: str | Path | None = None,
) -> Mapping[str, PublicSemanticOverride]:
    """Load and strictly validate the audited override manifest."""

    if path is None:
        return _load_default_overrides()
    return _load_override_file(Path(path))


def get_public_semantic_override(
    message_id: str,
    raw_text: str,
    *,
    overrides: Mapping[str, PublicSemanticOverride] | None = None,
) -> PublicSemanticOverride | None:
    """Resolve an override and verify its exact source text before returning it.

    Unknown message IDs return ``None``.  Known IDs whose raw text has drifted
    raise ``PublicSemanticOverrideHashMismatch`` so a public sync can fail
    closed rather than publish a stale semantic correction.
    """

    if not isinstance(message_id, str):
        raise TypeError("message_id must be a string")
    selected = (overrides if overrides is not None else _load_default_overrides()).get(message_id)
    if selected is None:
        return None
    actual_digest = raw_text_sha256(raw_text)
    if not hmac.compare_digest(actual_digest, selected.raw_text_sha256):
        raise PublicSemanticOverrideHashMismatch(
            f"raw text hash mismatch for audited message_id {message_id}: "
            f"expected {selected.raw_text_sha256}, got {actual_digest}"
        )
    return selected


__all__ = [
    "DEFAULT_OVERRIDE_PATH",
    "PublicSemanticOverride",
    "PublicSemanticOverrideError",
    "PublicSemanticOverrideHashMismatch",
    "get_public_semantic_override",
    "load_public_semantic_overrides",
    "raw_text_sha256",
]
