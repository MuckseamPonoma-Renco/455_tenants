from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _clean(value: str | None) -> str:
    return (value or "").replace("\u202f", " ").replace("\u200e", "").replace("\u200f", "").strip()


def make_attachment_item(
    *,
    kind: str,
    label: str | None = None,
    status: str = "captured",
    path: str | Path | None = None,
    content_type: str | None = None,
    filename: str | None = None,
    source_url: str | None = None,
    error: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "kind": _clean(kind),
        "status": _clean(status) or "captured",
    }
    if label:
        item["label"] = _clean(label)
    if path:
        item["path"] = str(Path(path).expanduser())
    if content_type:
        item["content_type"] = _clean(content_type)
    if filename:
        item["filename"] = _clean(filename)
    if source_url:
        item["source_url"] = _clean(source_url)
    if error:
        item["error"] = _clean(error)
    if extra:
        for key, value in extra.items():
            if value is None:
                continue
            item[key] = value
    return item


def build_attachment_manifest(
    *,
    items: list[dict[str, Any]] | None = None,
    message_context: dict[str, Any] | None = None,
    links: list[str] | None = None,
    source: str = "unknown",
) -> str | None:
    normalized_items = [item for item in (items or []) if item]
    normalized_links = [_clean(link) for link in (links or []) if _clean(link)]
    context = {key: value for key, value in (message_context or {}).items() if value not in (None, "", [], {})}
    if not normalized_items and not normalized_links and not context:
        return None
    manifest = {
        "version": 1,
        "source": _clean(source) or "unknown",
        "items": normalized_items,
    }
    if normalized_links:
        manifest["links"] = normalized_links
    if context:
        manifest["message_context"] = context
    return json.dumps(manifest, ensure_ascii=False, sort_keys=True)


def parse_attachment_manifest(raw: str | None) -> dict[str, Any]:
    text = _clean(raw)
    if not text:
        return {"version": 1, "source": "unknown", "items": []}

    if text.startswith("{") or text.startswith("["):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                items = parsed.get("items")
                if isinstance(items, list):
                    parsed["items"] = [item for item in items if isinstance(item, dict)]
                else:
                    parsed["items"] = []
                return parsed
            if isinstance(parsed, list):
                return {"version": 1, "source": "legacy_json", "items": [item for item in parsed if isinstance(item, dict)]}
        except Exception:
            pass

    items: list[dict[str, Any]] = []
    for token in [_clean(part) for part in text.split(",") if _clean(part)]:
        if token.startswith("omitted:"):
            items.append(make_attachment_item(kind=token.split(":", 1)[1] or "media", label=token, status="placeholder"))
            continue
        items.append(make_attachment_item(kind="legacy_reference", label=token, path=token, status="legacy"))
    return {"version": 0, "source": "legacy_string", "items": items}


def attachment_items(raw: str | None) -> list[dict[str, Any]]:
    parsed = parse_attachment_manifest(raw)
    items = parsed.get("items")
    return [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []

