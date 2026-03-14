import io
import zipfile
from fastapi import APIRouter, File, Header, HTTPException, UploadFile
from pydantic import BaseModel
from packages.audit import append_audit_event, compute_message_id, sender_hash
from packages.auth import require_bearer_token
from packages.db import RawMessage, get_session
from packages.queue import enqueue_process_message
from packages.timeutil import parse_ts_to_epoch
from packages.whatsapp.parser import parse_export_text

router = APIRouter()


class TaskerPayload(BaseModel):
    chat_name: str | None = None
    text: str
    sender: str | None = None
    ts_iso: str | None = None
    ts_epoch: int | float | str | None = None


@router.post('/tasker')
def ingest_tasker(payload: TaskerPayload, authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    resolved_epoch = parse_ts_to_epoch(payload.ts_epoch if payload.ts_epoch is not None else payload.ts_iso)
    resolved_ts = payload.ts_iso or str(payload.ts_epoch or '') or None
    mid = compute_message_id(payload.chat_name or '', payload.sender or '', resolved_ts or '', payload.text)

    with get_session() as session:
        if session.get(RawMessage, mid):
            return {'ok': True, 'deduped': True, 'message_id': mid}
        session.add(RawMessage(
            message_id=mid,
            chat_name=payload.chat_name,
            sender=payload.sender,
            sender_hash=sender_hash(payload.sender or ''),
            ts_iso=resolved_ts,
            ts_epoch=resolved_epoch,
            text=payload.text,
            attachments=None,
            source='tasker',
        ))
        session.commit()

    append_audit_event('INGEST_RAW', mid, {'source': 'tasker'})
    job_id = enqueue_process_message(mid)
    return {'ok': True, 'deduped': False, 'message_id': mid, 'job_id': job_id}


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
    job_ids = []
    seen_mids = set()
    with get_session() as session:
        for msg in parsed:
            mid = compute_message_id(msg.chat_name, msg.sender, msg.ts_iso or '', msg.text)
            if mid in seen_mids or session.get(RawMessage, mid):
                continue
            seen_mids.add(mid)
            session.add(RawMessage(
                message_id=mid,
                chat_name=msg.chat_name,
                sender=msg.sender,
                sender_hash=sender_hash(msg.sender),
                ts_iso=msg.ts_iso,
                ts_epoch=parse_ts_to_epoch(msg.ts_iso),
                text=msg.text,
                attachments=msg.attachments,
                source='export',
            ))
            inserted += 1
            job_ids.append(mid)
        session.commit()

    for mid in job_ids:
        enqueue_process_message(mid)

    append_audit_event('INGEST_EXPORT', None, {'inserted': inserted, 'parsed': len(parsed), 'zip': bool(is_zip)})
    return {'ok': True, 'inserted': inserted, 'parsed': len(parsed), 'zip': bool(is_zip), 'enqueued': len(job_ids)}
