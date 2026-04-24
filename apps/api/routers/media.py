from __future__ import annotations

import json
import mimetypes
from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import FileResponse

from packages.auth import require_bearer_token
from packages.db import RawMessage, get_session
from packages.whatsapp.attachments import attachment_items, parse_attachment_manifest
from packages.whatsapp.media import public_attachment_entries, resolve_allowed_media_path

router = APIRouter()


def _raw_message_or_404(message_id: str) -> RawMessage:
    with get_session() as session:
        row = session.get(RawMessage, message_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Unknown message")
        return row


@router.get("/api/messages/{message_id}/attachments")
def list_message_attachments(message_id: str, authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    row = _raw_message_or_404(message_id)
    parsed = parse_attachment_manifest(row.attachments)
    shareable = {item["attachment_index"]: item for item in public_attachment_entries(row.message_id, row.attachments)}
    items = []
    for index, item in enumerate(attachment_items(row.attachments)):
        rendered = dict(item)
        if index in shareable:
            rendered["public_url"] = shareable[index]["public_url"]
        items.append(rendered)
    return {
        "ok": True,
        "message_id": row.message_id,
        "source": row.source,
        "links": [str(link) for link in (parsed.get("links") or []) if isinstance(link, str)],
        "message_context": parsed.get("message_context") if isinstance(parsed.get("message_context"), dict) else {},
        "items": items,
    }


@router.get("/media/whatsapp/{message_id}/{attachment_index}")
def get_whatsapp_media(message_id: str, attachment_index: int):
    row = _raw_message_or_404(message_id)
    items = attachment_items(row.attachments)
    if attachment_index < 0 or attachment_index >= len(items):
        raise HTTPException(status_code=404, detail="Unknown attachment")
    item = items[attachment_index]
    path = resolve_allowed_media_path(item.get("path"))
    if path is None or not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Attachment file is not available")
    filename = str(item.get("filename") or path.name)
    media_type = str(item.get("content_type") or "").strip() or mimetypes.guess_type(filename or path.name)[0] or None
    disposition = "inline" if (media_type or "").startswith("image/") else "attachment"
    return FileResponse(path=str(path), filename=filename, media_type=media_type, content_disposition_type=disposition)
