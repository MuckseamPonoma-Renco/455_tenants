#!/usr/bin/env python3
"""Preview or apply exact, explicitly whitelisted existing public references.

This is a metadata-only backfill. It does not upload files, synchronize Sheets,
classify messages, or create/submit filing jobs. Apply requires an unchanged
preview hash and revalidates every existing canonical URL under row locks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file
from packages.whatsapp.attachments import attachment_items
from packages.whatsapp.media import enrich_public_attachment_references, public_attachment_entries


def _sha(value: str | None) -> str:
    return hashlib.sha256((value or "").encode()).hexdigest()


def normalize_whitelist(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError("a nonempty exact attachment whitelist is required")
    normalized = []
    seen = set()
    for row in value:
        if (
            not isinstance(row, dict)
            or not isinstance(row.get("message_id"), str)
            or not row["message_id"]
            or type(row.get("attachment_index")) is not int
            or row["attachment_index"] < 0
            or not isinstance(row.get("public_url"), str)
            or not row["public_url"]
        ):
            raise ValueError("invalid attachment whitelist entry")
        key = (row["message_id"], row["attachment_index"])
        if key in seen:
            raise ValueError("duplicate attachment whitelist entry")
        seen.add(key)
        normalized.append({name: row[name] for name in ("message_id", "attachment_index", "public_url")})
    return sorted(normalized, key=lambda row: (row["message_id"], row["attachment_index"]))


def _public_urls(message_id: str, manifest: str | None) -> dict[int, str]:
    return {int(item["attachment_index"]): str(item["public_url"]) for item in public_attachment_entries(message_id, manifest)}


def prepare_backfill(
    manifests: dict[str, str | None], whitelist: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, str | None]]:
    whitelist = normalize_whitelist(whitelist)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in whitelist:
        grouped.setdefault(item["message_id"], []).append(item)
    updates = {}
    errors = []
    bindings = []
    references_added = 0
    for message_id, requested in grouped.items():
        if message_id not in manifests:
            errors.append({"message_id": message_id, "reason": "message_missing"})
            continue
        original = manifests[message_id]
        before = _public_urls(message_id, original)
        indices = {item["attachment_index"] for item in requested}
        enriched = enrich_public_attachment_references(message_id, original, attachment_indices=indices)
        after = _public_urls(message_id, enriched)
        before_items, after_items = attachment_items(original), attachment_items(enriched)
        if before != after:
            errors.append({"message_id": message_id, "reason": "public_urls_changed"})
        for item in requested:
            index, expected = item["attachment_index"], item["public_url"]
            reference = after_items[index].get("public_reference") if index < len(after_items) else None
            if before.get(index) != expected or after.get(index) != expected:
                errors.append({"message_id": message_id, "attachment_index": index, "reason": "whitelisted_url_changed"})
            elif not isinstance(reference, dict) or reference.get("version") != 1 or reference.get("url") != expected:
                errors.append({"message_id": message_id, "attachment_index": index, "reason": "reference_unavailable"})
            elif before_items[index].get("public_reference") != reference:
                references_added += 1
        # Enrichment of a selected message may never touch an unselected item.
        if len(before_items) != len(after_items) or any(
            old != new for index, (old, new) in enumerate(zip(before_items, after_items)) if index not in indices
        ):
            errors.append({"message_id": message_id, "reason": "unselected_attachment_changed"})
        bindings.append({"message_id": message_id, "before_sha256": _sha(original), "after_sha256": _sha(enriched)})
        if enriched != original:
            updates[message_id] = enriched
    bound_preview = {
        "version": 1, "whitelist": whitelist, "bindings": bindings,
        "public_base_url": os.environ.get("PUBLIC_BASE_URL", ""), "errors": errors,
    }
    preview = {
        "ok": not errors,
        "action": "preview",
        "selected_messages": len(grouped),
        "selected_attachments": len(whitelist),
        "changed_messages": len(updates),
        "references_added": references_added,
        "public_urls_unchanged": not errors,
        "errors": errors,
        "preview_sha256": _sha(json.dumps(bound_preview, ensure_ascii=False, sort_keys=True, separators=(",", ":"))),
    }
    return preview, updates


def run_backfill(
    whitelist: list[dict[str, Any]], *, apply: bool = False, expected_preview_sha256: str | None = None,
) -> dict[str, Any]:
    from sqlalchemy import select
    from packages.db import RawMessage, get_session

    whitelist = normalize_whitelist(whitelist)
    ids = {item["message_id"] for item in whitelist}
    with get_session() as session:
        query = select(RawMessage).where(RawMessage.message_id.in_(ids)).order_by(RawMessage.message_id)
        if apply:
            query = query.with_for_update()
        rows = {row.message_id: row for row in session.scalars(query).all()}
        preview, updates = prepare_backfill({key: row.attachments for key, row in rows.items()}, whitelist)
        if not apply:
            return preview
        if not preview["ok"]:
            return {**preview, "action": "blocked"}
        if not expected_preview_sha256 or expected_preview_sha256 != preview["preview_sha256"]:
            return {**preview, "ok": False, "action": "blocked", "error": "preview_changed_or_missing"}
        for message_id, manifest in updates.items():
            rows[message_id].attachments = manifest
        session.commit()
        return {**preview, "action": "applied"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", help="Private runtime environment file")
    parser.add_argument("--whitelist", required=True, help="JSON array of exact message_id, attachment_index, public_url entries")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-preview-sha256")
    args = parser.parse_args()
    load_local_env_file(Path(args.env_file).expanduser() if args.env_file else ROOT / ".env")
    try:
        whitelist = normalize_whitelist(json.loads(Path(args.whitelist).expanduser().read_text()))
        result = run_backfill(whitelist, apply=args.apply, expected_preview_sha256=args.expected_preview_sha256)
    except Exception:
        # SQL errors can contain connection or attachment details.
        result = {"ok": False, "action": "blocked", "error": "public_reference_backfill_failed"}
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("ok") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
