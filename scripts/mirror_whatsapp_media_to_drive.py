from __future__ import annotations

import argparse
import json
import mimetypes
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from packages.local_env import load_local_env_file

load_local_env_file()

from packages.db import RawMessage, get_session
from packages.sheets.sync import _creds_path
from packages.whatsapp.attachments import attachment_items, build_attachment_manifest, parse_attachment_manifest
from packages.whatsapp.media import resolve_allowed_media_path

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.file"]
PUBLIC_IMAGE_KINDS = {"image", "message_screenshot"}


def _drive_service():
    creds = Credentials.from_service_account_file(_creds_path(), scopes=DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds)


def _clean(value: object) -> str:
    return str(value or "").replace("\u202f", " ").replace("\u200e", "").replace("\u200f", "").strip()


def _folder_id_from_env() -> str:
    import os

    return _clean(os.environ.get("GOOGLE_PUBLIC_MEDIA_DRIVE_FOLDER_ID") or os.environ.get("GOOGLE_DRIVE_MEDIA_FOLDER_ID"))


def _ensure_folder(service, *, dry_run: bool) -> str | None:
    folder_id = _folder_id_from_env()
    if folder_id:
        return folder_id
    if dry_run:
        return None
    metadata = {
        "name": "455 Tenants Log Media",
        "mimeType": "application/vnd.google-apps.folder",
    }
    folder = service.files().create(body=metadata, fields="id").execute()
    return str(folder["id"])


def _public_url(file_id: str) -> str:
    return f"https://drive.google.com/thumbnail?id={file_id}&sz=w600"


def _find_existing(service, *, message_id: str, attachment_index: int) -> str | None:
    query = (
        "trashed = false and "
        f"appProperties has {{ key='tenant_message_id' and value='{message_id}' }} and "
        f"appProperties has {{ key='tenant_attachment_index' and value='{attachment_index}' }}"
    )
    result = service.files().list(q=query, spaces="drive", fields="files(id)", pageSize=1).execute()
    files = result.get("files") or []
    return str(files[0]["id"]) if files else None


def _upload_file(service, *, path: Path, folder_id: str | None, message_id: str, attachment_index: int, content_type: str | None) -> str:
    existing_id = _find_existing(service, message_id=message_id, attachment_index=attachment_index)
    metadata: dict[str, Any] = {
        "name": f"{message_id[:12]}_{attachment_index}_{path.name}",
        "appProperties": {
            "tenant_message_id": message_id,
            "tenant_attachment_index": str(attachment_index),
        },
    }
    if folder_id:
        metadata["parents"] = [folder_id]
    media = MediaFileUpload(str(path), mimetype=content_type or mimetypes.guess_type(path.name)[0] or "application/octet-stream", resumable=False)
    if existing_id:
        updated = service.files().update(fileId=existing_id, body=metadata, media_body=media, fields="id").execute()
        return str(updated["id"])
    created = service.files().create(body=metadata, media_body=media, fields="id").execute()
    return str(created["id"])


def _make_public(service, file_id: str) -> None:
    service.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": "reader"},
        fields="id",
    ).execute()


def _manifest_with_updated_items(raw_attachments: str | None, items: list[dict[str, Any]]) -> str | None:
    parsed = parse_attachment_manifest(raw_attachments)
    links = [str(link) for link in (parsed.get("links") or []) if isinstance(link, str)]
    context = parsed.get("message_context") if isinstance(parsed.get("message_context"), dict) else {}
    return build_attachment_manifest(
        items=items,
        links=links,
        message_context=context,
        source=str(parsed.get("source") or "whatsapp_web"),
    )


def mirror_media(*, dry_run: bool, limit: int | None = None, refresh: bool = False) -> dict[str, Any]:
    service = None if dry_run else _drive_service()
    folder_id = _ensure_folder(service, dry_run=dry_run) if service is not None else None
    inspected = 0
    upload_candidates = 0
    updated_rows = 0
    skipped = 0
    details: list[dict[str, Any]] = []

    with get_session() as session:
        rows = session.query(RawMessage).filter(RawMessage.attachments.isnot(None)).order_by(RawMessage.ts_epoch.desc().nullslast()).all()
        for row in rows:
            items = attachment_items(row.attachments)
            if not items:
                continue
            changed = False
            new_items = [dict(item) for item in items]
            for index, item in enumerate(new_items):
                if limit is not None and upload_candidates >= limit:
                    break
                kind = _clean(item.get("kind")).casefold()
                if kind not in PUBLIC_IMAGE_KINDS:
                    skipped += 1
                    continue
                path = resolve_allowed_media_path(item.get("path"))
                if path is None or not path.exists():
                    skipped += 1
                    continue
                inspected += 1
                if item.get("sheet_image_url") and not refresh:
                    skipped += 1
                    continue
                upload_candidates += 1
                detail = {
                    "message_id": row.message_id,
                    "attachment_index": index,
                    "path": str(path),
                    "kind": kind,
                }
                if dry_run:
                    details.append(detail)
                    continue
                assert service is not None
                file_id = _upload_file(
                    service,
                    path=path,
                    folder_id=folder_id,
                    message_id=row.message_id,
                    attachment_index=index,
                    content_type=_clean(item.get("content_type")) or mimetypes.guess_type(path.name)[0],
                )
                _make_public(service, file_id)
                item["drive_file_id"] = file_id
                item["sheet_image_url"] = _public_url(file_id)
                changed = True
                detail["drive_file_id"] = file_id
                detail["sheet_image_url"] = item["sheet_image_url"]
                details.append(detail)
            if changed:
                row.attachments = _manifest_with_updated_items(row.attachments, new_items)
                updated_rows += 1
            if limit is not None and upload_candidates >= limit:
                break
        if not dry_run:
            session.commit()

    return {
        "dry_run": dry_run,
        "inspected_existing_images": inspected,
        "upload_candidates": upload_candidates,
        "updated_rows": updated_rows,
        "skipped": skipped,
        "folder_id": folder_id,
        "details": details,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Mirror WhatsApp evidence images to Google Drive URLs that Google Sheets can render reliably.")
    parser.add_argument("--apply", action="store_true", help="Upload files, make them link-readable, and update attachment manifests.")
    parser.add_argument("--limit", type=int, help="Limit the number of candidate images to process.")
    parser.add_argument("--refresh", action="store_true", help="Re-upload media that already has a sheet_image_url.")
    args = parser.parse_args()

    result = mirror_media(dry_run=not args.apply, limit=args.limit, refresh=args.refresh)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
