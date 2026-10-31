from __future__ import annotations

import os
import hashlib
import json
from pathlib import Path
import re
import struct
from collections.abc import Collection
from typing import Any
from urllib.parse import parse_qsl, urlsplit

from packages.whatsapp.attachments import attachment_items, parse_attachment_manifest

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MEDIA_DIR = Path(".local/whatsapp_media")
STAGED_RUNTIME_ROOT = Path.home() / ".local" / "share" / "tenant-issue-os" / "runtime"
WHATSAPP_UI_IMAGE_URL_RE = re.compile(
    r"^(?:https?:)?//(?:static\.whatsapp\.net|pps\.whatsapp\.net|web\.whatsapp\.com)/(?:rsrc|.*profile)",
    re.IGNORECASE,
)
PUBLIC_REFERENCE_VERSION = 1
PUBLIC_REFERENCE_KINDS = frozenset({"image", "video", "audio", "voice", "document", "file"})


def _clean(value: str | None) -> str:
    return (value or "").replace("\u202f", " ").replace("\u200e", "").replace("\u200f", "").strip()


def media_root() -> Path:
    configured = _clean(os.environ.get("WHATSAPP_CAPTURE_MEDIA_DIR"))
    root = Path(configured).expanduser() if configured else DEFAULT_MEDIA_DIR
    if not root.is_absolute():
        root = REPO_ROOT / root
    return root.resolve()


def media_roots() -> tuple[Path, ...]:
    configured = _clean(os.environ.get("WHATSAPP_CAPTURE_MEDIA_DIR"))
    if configured:
        configured_path = Path(configured).expanduser()
        if configured_path.is_absolute():
            return (configured_path.resolve(),)
        return tuple(
            candidate.resolve()
            for candidate in (
                REPO_ROOT / configured_path,
                STAGED_RUNTIME_ROOT / configured_path,
            )
        )
    return (
        (REPO_ROOT / DEFAULT_MEDIA_DIR).resolve(),
        (STAGED_RUNTIME_ROOT / DEFAULT_MEDIA_DIR).resolve(),
    )


def resolve_allowed_media_path(path: str | Path | None) -> Path | None:
    raw = _clean(str(path)) if path is not None else ""
    if not raw:
        return None
    try:
        candidate = Path(raw).expanduser().resolve()
    except Exception:
        return None
    for root in media_roots():
        if candidate == root or root in candidate.parents:
            return candidate
    return None


def public_attachment_url(
    message_id: str,
    attachment_index: int,
    *,
    base_url: str | None = None,
    version: str | int | None = None,
) -> str | None:
    root = _clean(base_url or os.environ.get("PUBLIC_BASE_URL"))
    if not root:
        return None
    url = f"{root.rstrip('/')}/media/whatsapp/{message_id}/{attachment_index}"
    clean_version = _clean(str(version)) if version is not None else ""
    if clean_version:
        url = f"{url}?v={clean_version}"
    return url


def media_dimensions(path: str | Path | None) -> tuple[int, int] | None:
    resolved = resolve_allowed_media_path(path)
    if resolved is None:
        return None
    try:
        data = resolved.read_bytes()[:4096]
    except Exception:
        return None
    if data.startswith(b"\x89PNG\r\n\x1a\n") and len(data) >= 24:
        try:
            return struct.unpack(">II", data[16:24])
        except Exception:
            return None
    if data.startswith(b"\xff\xd8"):
        idx = 2
        while idx + 9 < len(data):
            if data[idx] != 0xFF:
                idx += 1
                continue
            marker = data[idx + 1]
            idx += 2
            if marker in {0xD8, 0xD9}:
                continue
            if idx + 2 > len(data):
                return None
            segment_length = int.from_bytes(data[idx : idx + 2], "big")
            if segment_length < 2:
                return None
            if marker in {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}:
                if idx + 7 > len(data):
                    return None
                height = int.from_bytes(data[idx + 3 : idx + 5], "big")
                width = int.from_bytes(data[idx + 5 : idx + 7], "big")
                return width, height
            idx += segment_length
    return None


def attachment_preview_eligible(item: dict[str, Any]) -> bool:
    return attachment_public_image_eligible(item)


def attachment_public_image_eligible(item: dict[str, Any]) -> bool:
    kind = _clean(str(item.get("kind") or "")).casefold()
    if kind != "image":
        return False
    status = _clean(str(item.get("status") or "")).casefold()
    if status in {"metadata_only", "placeholder", "download_error"}:
        return False
    label = _clean(str(item.get("label") or "")).casefold()
    if label in {"metadata_only", "download_error"}:
        return False
    source_url = _clean(str(item.get("source_url") or ""))
    if not source_url:
        return True
    lowered = source_url.casefold()
    if lowered.startswith(("blob:", "data:image/")):
        return True
    if WHATSAPP_UI_IMAGE_URL_RE.search(source_url):
        return False
    if _clean(str(item.get("capture_method") or "")).casefold() == "inline_image":
        return False
    return True


def _public_entry_sort_key(item: dict[str, Any]) -> tuple[int, int]:
    kind = _clean(str(item.get("kind") or "")).casefold()
    if kind == "image":
        return (0, int(item.get("attachment_index") or 0))
    if kind in {"video", "audio", "voice", "document", "file"}:
        return (1, int(item.get("attachment_index") or 0))
    return (3, int(item.get("attachment_index") or 0))


def _reference_source_hash(message_id: str, index: int, item: dict[str, Any]) -> str:
    source = {key: value for key, value in item.items() if key != "public_reference"}
    payload = {"message_id": message_id, "attachment_index": index, "item": source}
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _reference_kind_allowed(item: dict[str, Any]) -> bool:
    kind = _clean(str(item.get("kind") or "")).casefold()
    return (
        kind in PUBLIC_REFERENCE_KINDS
        and _clean(str(item.get("status") or "")).casefold() in {"captured", "downloaded"}
        and (kind != "image" or attachment_public_image_eligible(item))
    )


def _canonical_reference_url(message_id: str, index: int, url: object, *, base_url: str | None) -> bool:
    expected = public_attachment_url(message_id, index, base_url=base_url)
    if (
        not expected or not isinstance(url, str) or url != url.strip()
        or any(ord(char) < 32 for char in url)
        or not re.fullmatch(r"[A-Za-z0-9_~-]{1,128}", message_id)
    ):
        return False
    try:
        target, canonical = urlsplit(url), urlsplit(expected)
        query = parse_qsl(target.query, keep_blank_values=True, strict_parsing=True)
    except ValueError:
        return False
    return (
        target.scheme == canonical.scheme == "https"
        and target.netloc == canonical.netloc
        and target.path == canonical.path
        and bool(target.hostname)
        and target.username is None
        and target.password is None
        and not target.fragment
        and not canonical.query
        and not canonical.fragment
        and (not query or (len(query) == 1 and query[0][0] == "v" and re.fullmatch(r"[0-9]+", query[0][1]) is not None))
    )


def _valid_public_reference(
    message_id: str, index: int, item: dict[str, Any], *, base_url: str | None = None,
) -> dict[str, Any] | None:
    reference = item.get("public_reference")
    if not _reference_kind_allowed(item) or not isinstance(reference, dict):
        return None
    if (
        type(reference.get("version")) is not int
        or reference["version"] != PUBLIC_REFERENCE_VERSION
        or reference.get("message_id") != message_id
        or type(reference.get("attachment_index")) is not int
        or reference["attachment_index"] != index
        or reference.get("source_sha256") != _reference_source_hash(message_id, index, item)
        or not isinstance(reference.get("path_key"), str)
        or re.fullmatch(r"[0-9a-f]{64}", reference["path_key"]) is None
        or not _canonical_reference_url(message_id, index, reference.get("url"), base_url=base_url)
    ):
        return None
    return reference


def enrich_public_attachment_references(
    message_id: str, raw_attachments: str | None, *, base_url: str | None = None,
    attachment_indices: Collection[int] | None = None,
) -> str | None:
    """Persist stable render metadata only for locally allowed, existing files.

    A reference preserves an already-generated canonical URL; it grants no file
    access. Serving still requires resolve_allowed_media_path on the API host.
    Existing noncanonical CDN links are left untouched, without remote fallback.
    """
    parsed = parse_attachment_manifest(raw_attachments)
    items = parsed.get("items") or []
    changed = False
    for index, item in enumerate(items):
        if attachment_indices is not None and index not in attachment_indices:
            continue
        if not _reference_kind_allowed(item) or _valid_public_reference(message_id, index, item, base_url=base_url):
            continue
        path = resolve_allowed_media_path(item.get("path"))
        if path is None:
            continue
        try:
            if not path.is_file():
                continue
            version = int(path.stat().st_mtime)
        except OSError:
            continue
        url = _clean(str(item.get("sheet_image_url") or item.get("public_url") or ""))
        if not url:
            url = public_attachment_url(message_id, index, base_url=base_url, version=version)
        if not _canonical_reference_url(message_id, index, url, base_url=base_url):
            continue
        item["public_reference"] = {
            "version": PUBLIC_REFERENCE_VERSION,
            "message_id": message_id,
            "attachment_index": index,
            "source_sha256": _reference_source_hash(message_id, index, item),
            "url": url,
            "path_key": hashlib.sha256(str(path).encode()).hexdigest(),
        }
        changed = True
    if not changed:
        return raw_attachments
    return json.dumps(parsed, ensure_ascii=False, sort_keys=True)


def public_attachment_entries(
    message_id: str,
    raw_attachments: str | None,
    *,
    base_url: str | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    items = attachment_items(raw_attachments)
    seen_paths: set[str] = set()
    for index, item in enumerate(items):
        kind = _clean(str(item.get("kind") or "")).casefold()
        if kind == "message_screenshot":
            continue
        if kind == "image" and not attachment_public_image_eligible(item):
            continue
        reference = _valid_public_reference(message_id, index, item, base_url=base_url)
        if reference is not None:
            # Rendering depends on persisted publication metadata, not which
            # machine currently has the bytes. File serving remains separate.
            path_key = reference["path_key"]
            if path_key in seen_paths:
                continue
            seen_paths.add(path_key)
            row = dict(item)
            row["attachment_index"] = index
            row["public_url"] = reference["url"]
            out.append(row)
            continue
        path = resolve_allowed_media_path(item.get("path"))
        version = None
        if path is not None:
            try:
                version = int(path.stat().st_mtime)
            except Exception:
                version = None
        url = _clean(str(item.get("sheet_image_url") or item.get("public_url") or ""))
        if not url:
            url = public_attachment_url(message_id, index, base_url=base_url, version=version)
        if path is None or not url:
            continue
        path_key = hashlib.sha256(str(path).encode()).hexdigest()
        if path_key in seen_paths:
            continue
        seen_paths.add(path_key)
        row = dict(item)
        row["attachment_index"] = index
        row["path"] = str(path)
        row["public_url"] = url
        dimensions = media_dimensions(path)
        if dimensions:
            row["width"], row["height"] = dimensions
            row["preview_eligible"] = attachment_preview_eligible(row)
        out.append(row)
    return sorted(out, key=_public_entry_sort_key)


def attachment_context(raw_attachments: str | None) -> dict[str, Any]:
    parsed = parse_attachment_manifest(raw_attachments)
    return {
        "links": [str(link) for link in (parsed.get("links") or []) if isinstance(link, str) and _clean(link)],
        "message_context": parsed.get("message_context") if isinstance(parsed.get("message_context"), dict) else {},
    }
