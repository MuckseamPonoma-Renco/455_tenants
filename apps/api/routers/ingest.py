from __future__ import annotations

import io
import zipfile
from fastapi import APIRouter, File, Header, HTTPException, UploadFile
from pydantic import BaseModel
from packages.audit import append_audit_event, compute_message_id, sender_hash
from packages.auth import require_bearer_token
from packages.db import RawMessage, get_session
from packages.queue import enqueue_full_resync, enqueue_process_message
from packages.tasker_capture import (
    find_recent_duplicate,
    find_recent_live_capture_duplicate,
    is_noise_tasker_capture,
    normalize_tasker_capture,
    tasker_duplicate_window_seconds,
)
from packages.timeutil import epoch_to_iso, parse_ts_to_epoch
from packages.whatsapp.parser import parse_export_text

router = APIRouter()


class CapturePayload(BaseModel):
    chat_name: str | None = None
    text: str
    sender: str | None = None
    ts_iso: str | None = None
    ts_epoch: int | float | str | None = None
    attachments: str | None = None


class CaptureBatchPayload(BaseModel):
    items: list[CapturePayload]


TaskerPayload = CapturePayload
TaskerBatchPayload = CaptureBatchPayload


def _prepare_tasker_row(payload: CapturePayload) -> tuple[dict[str, object], tuple[str, str, str]]:
    source_ts = payload.ts_epoch if payload.ts_epoch is not None else payload.ts_iso
    resolved_epoch = parse_ts_to_epoch(source_ts)
    resolved_ts = payload.ts_iso or epoch_to_iso(source_ts)
    normalized = normalize_tasker_capture(payload.chat_name, payload.sender, payload.text)
    stored_chat_name = normalized.chat_name or payload.chat_name or ''
    stored_sender = normalized.sender or payload.sender or ''
    stored_text = normalized.text or payload.text
    mid = compute_message_id(stored_chat_name, stored_sender, resolved_ts or '', stored_text)
    return {
        'message_id': mid,
        'chat_name': stored_chat_name,
        'sender': stored_sender,
        'sender_hash': sender_hash(stored_sender),
        'ts_iso': resolved_ts,
        'ts_epoch': resolved_epoch,
        'text': stored_text,
        'attachments': payload.attachments,
    }, normalized.signature


def _batch_recent_duplicate(signature: tuple[str, str, str], ts_epoch: int | None, recent_rows: list[tuple[tuple[str, str, str], int | None, str]]) -> str | None:
    if ts_epoch is None:
        return None
    window = tasker_duplicate_window_seconds()
    if window <= 0:
        return None
    for recent_signature, recent_epoch, recent_mid in reversed(recent_rows):
        if recent_epoch is None:
            continue
        if recent_signature == signature and abs(int(recent_epoch) - int(ts_epoch)) <= window:
            return recent_mid
    return None


def _store_capture_payload(
    session,
    payload: CapturePayload,
    *,
    source: str,
    recent_rows: list[tuple[tuple[str, str, str], int | None, str]] | None = None,
) -> tuple[dict[str, object], bool]:
    prepared, signature = _prepare_tasker_row(payload)
    mid = str(prepared['message_id'])

    if is_noise_tasker_capture(prepared['chat_name'], prepared['sender'], prepared['text']):
        return prepared, False

    if session.get(RawMessage, mid):
        return prepared, False

    if recent_rows is not None:
        recent_mid = _batch_recent_duplicate(signature, prepared['ts_epoch'], recent_rows)
        if recent_mid:
            prepared['message_id'] = recent_mid
            return prepared, False

    recent_duplicate = find_recent_live_capture_duplicate(
        session,
        chat_name=prepared['chat_name'],
        sender=prepared['sender'],
        text=prepared['text'],
        ts_epoch=prepared['ts_epoch'],
    )
    if recent_duplicate:
        prepared['message_id'] = recent_duplicate.message_id
        return prepared, False

    session.add(RawMessage(
        message_id=mid,
        chat_name=prepared['chat_name'],
        sender=prepared['sender'],
        sender_hash=prepared['sender_hash'],
        ts_iso=prepared['ts_iso'],
        ts_epoch=prepared['ts_epoch'],
        text=prepared['text'],
        attachments=prepared['attachments'],
        source=source,
    ))
    session.flush()
    if recent_rows is not None:
        recent_rows.append((signature, prepared['ts_epoch'], mid))
    return prepared, True


def _ingest_single_capture(payload: CapturePayload, *, authorization: str | None, source: str):
    require_bearer_token(authorization)
    with get_session() as session:
        prepared, inserted = _store_capture_payload(session, payload, source=source)
        session.commit()

    if not inserted:
        return {'ok': True, 'deduped': True, 'message_id': prepared['message_id']}

    message_id = str(prepared['message_id'])
    append_audit_event('INGEST_RAW', message_id, {'source': source})
    job_id = enqueue_process_message(message_id)
    return {'ok': True, 'deduped': False, 'message_id': message_id, 'job_id': job_id}


def _ingest_batch_capture(payload: CaptureBatchPayload, *, authorization: str | None, source: str):
    require_bearer_token(authorization)
    if not payload.items:
        raise HTTPException(status_code=400, detail='Provide at least one capture message')

    inserted_rows: list[dict[str, object]] = []
    deduped = 0
    recent_rows: list[tuple[tuple[str, str, str], int | None, str]] = []

    with get_session() as session:
        for item in payload.items:
            prepared, inserted = _store_capture_payload(session, item, source=source, recent_rows=recent_rows)
            if inserted:
                inserted_rows.append(prepared)
            else:
                deduped += 1
        session.commit()

    job_ids = [enqueue_process_message(str(row['message_id']), sync_sheets=False) for row in inserted_rows]
    if inserted_rows:
        enqueue_full_resync()
    append_audit_event('INGEST_RAW_BATCH', None, {
        'source': source,
        'received': len(payload.items),
        'inserted': len(inserted_rows),
        'deduped': deduped,
    })
    return {
        'ok': True,
        'received': len(payload.items),
        'inserted': len(inserted_rows),
        'deduped': deduped,
        'message_ids': [str(row['message_id']) for row in inserted_rows],
        'job_ids': job_ids,
    }


@router.post('/tasker')
def ingest_tasker(payload: TaskerPayload, authorization: str | None = Header(default=None)):
    return _ingest_single_capture(payload, authorization=authorization, source='tasker')


@router.post('/tasker_batch')
def ingest_tasker_batch(payload: TaskerBatchPayload, authorization: str | None = Header(default=None)):
    return _ingest_batch_capture(payload, authorization=authorization, source='tasker')


@router.post('/whatsapp_web')
def ingest_whatsapp_web(payload: CapturePayload, authorization: str | None = Header(default=None)):
    return _ingest_single_capture(payload, authorization=authorization, source='whatsapp_web')


@router.post('/whatsapp_web_batch')
def ingest_whatsapp_web_batch(payload: CaptureBatchPayload, authorization: str | None = Header(default=None)):
    return _ingest_batch_capture(payload, authorization=authorization, source='whatsapp_web')


@router.post('/export')
async def ingest_export(file: UploadFile = File(...), authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    raw = await file.read()
    is_zip = raw[:4] == b'PK\x03\x04'

    if is_zip:
        zf = zipfile.ZipFile(io.BytesIO(raw))
        txt_name = '_chat.txt' if '_chat.txt' in zf.namelist() else None
        if not txt_name:
            for name in zf.namelist():
                if name.lower().endswith('.txt'):
                    txt_name = name
                    break
        if not txt_name:
            raise HTTPException(status_code=400, detail='ZIP does not contain a .txt chat export')
        content = zf.read(txt_name).decode('utf-8', errors='replace')
    else:
        content = raw.decode('utf-8', errors='replace')

    parsed = parse_export_text(content)
    inserted = 0
    deduped = 0
    job_ids = []
    seen_mids = set()
    with get_session() as session:
        for msg in parsed:
            ts_epoch = parse_ts_to_epoch(msg.ts_iso)
            mid = compute_message_id(msg.chat_name, msg.sender, msg.ts_iso or '', msg.text)
            if mid in seen_mids or session.get(RawMessage, mid):
                deduped += 1
                continue
            duplicate = find_recent_duplicate(
                session,
                chat_name=msg.chat_name,
                sender=msg.sender,
                text=msg.text,
                ts_epoch=ts_epoch,
                require_chat_match=False,
            )
            if duplicate:
                deduped += 1
                continue
            seen_mids.add(mid)
            session.add(RawMessage(
                message_id=mid,
                chat_name=msg.chat_name,
                sender=msg.sender,
                sender_hash=sender_hash(msg.sender),
                ts_iso=msg.ts_iso,
                ts_epoch=ts_epoch,
                text=msg.text,
                attachments=msg.attachments,
                source='export',
            ))
            inserted += 1
            job_ids.append(mid)
        session.commit()

    for mid in job_ids:
        enqueue_process_message(mid, sync_sheets=False)
    if job_ids:
        enqueue_full_resync()

    append_audit_event('INGEST_EXPORT', None, {
        'inserted': inserted,
        'parsed': len(parsed),
        'deduped': deduped,
        'zip': bool(is_zip),
    })
    return {
        'ok': True,
        'inserted': inserted,
        'parsed': len(parsed),
        'deduped': deduped,
        'zip': bool(is_zip),
        'enqueued': len(job_ids),
    }
