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


def merge_attachment_manifests(existing: str | None, incoming: str | None) -> str | None:
    if not incoming:
        return existing
    if not existing:
        return incoming

    old = parse_attachment_manifest(existing)
    new = parse_attachment_manifest(incoming)
    merged_items: list[dict[str, object]] = []
    seen_items: set[str] = set()
    for item in list(old.get("items") or []) + list(new.get("items") or []):
        if not isinstance(item, dict):
            continue
        if item.get("path"):
            key = "|".join(str(item.get(part) or "") for part in ("path", "kind"))
        else:
            key = "|".join(str(item.get(part) or "") for part in ("kind", "label", "status", "source_url", "filename"))
        if key in seen_items:
            continue
        seen_items.add(key)
        merged_items.append(item)

    merged_links: list[str] = []
    seen_links: set[str] = set()
    for link in list(old.get("links") or []) + list(new.get("links") or []):
        if not isinstance(link, str):
            continue
        clean = _clean(link)
        if not clean or clean in seen_links:
            continue
        seen_links.add(clean)
        merged_links.append(clean)

    context: dict[str, object] = {}
    if isinstance(old.get("message_context"), dict):
        context.update(old["message_context"])
    if isinstance(new.get("message_context"), dict):
        context.update(new["message_context"])

    return build_attachment_manifest(
        items=merged_items,
        links=merged_links,
        message_context=context,
        source=str(new.get("source") or old.get("source") or "unknown"),
    )


def _dedupe_repeated_lines(value: str | None) -> str:
    lines = [_clean(line) for line in _clean(value).splitlines() if _clean(line)]
    if not lines:
        return ""
    count = len(lines)
    if count % 2 == 0:
        half = count // 2
        if lines[:half] == lines[half:]:
            return "\n".join(lines[:half])
    return "\n".join(lines)


def strip_reply_context_from_text(text: str | None, attachments: str | None) -> str:
    clean_text = _clean(text)
    if not clean_text or not attachments:
        return clean_text

    parsed = parse_attachment_manifest(attachments)
    context = parsed.get("message_context") if isinstance(parsed.get("message_context"), dict) else {}
    reply_text = _clean(str((context or {}).get("reply_text") or ""))
    if not reply_text:
        return clean_text

    candidates: list[str] = []
    for candidate in (reply_text, _dedupe_repeated_lines(reply_text)):
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    reply_lines = [_clean(line) for line in reply_text.splitlines() if _clean(line)]
    for end in range(len(reply_lines) - 1, 0, -1):
        candidate = "\n".join(reply_lines[:end])
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    for candidate in sorted(candidates, key=len, reverse=True):
        if clean_text == candidate:
            return clean_text
        if clean_text.startswith(candidate + "\n"):
            stripped = _clean(clean_text[len(candidate) :])
            if stripped:
                return stripped
    return clean_text
