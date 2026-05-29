from __future__ import annotations

from fastapi import APIRouter, Body, Header, HTTPException
from packages.auth import require_bearer_token
from packages.db import get_session
from packages.public_records.sync import add_watchdog_check as add_watchdog_check_impl
from packages.public_records.sync import public_record_payload, verify_public_record as verify_public_record_impl
from packages.queue import (
    enqueue_export_elevator_replacement_bundle,
    enqueue_export_legal_bundle,
    enqueue_full_resync,
    enqueue_queue_311_jobs,
    enqueue_resync_replacement_watchdog,
    enqueue_reprocess_last_n,
    enqueue_sync_public_records,
    enqueue_sync_311_statuses,
)

router = APIRouter()


@router.post('/resync_sheets')
def resync_sheets(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    return {'ok': True, 'job_id': enqueue_full_resync()}


@router.post('/reprocess_last/{n}')
def reprocess_last(n: int, authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    return {'ok': True, 'job_id': enqueue_reprocess_last_n(n)}


@router.post('/queue_311_jobs')
def queue_311_jobs(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    return {'ok': True, 'job_id': enqueue_queue_311_jobs()}


@router.post('/sync_311_statuses')
def sync_311_statuses(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    return {'ok': True, 'job_id': enqueue_sync_311_statuses()}


@router.post('/sync_public_records')
def sync_public_records(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    return {'ok': True, 'job_id': enqueue_sync_public_records()}


@router.post('/resync_replacement_watchdog')
def resync_replacement_watchdog(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    return {'ok': True, 'job_id': enqueue_resync_replacement_watchdog()}


@router.post('/verify_public_record/{record_id}')
def verify_public_record(
    record_id: int,
    authorization: str | None = Header(default=None),
    payload: dict | None = Body(default=None),
):
    require_bearer_token(authorization)
    with get_session() as session:
        record = verify_public_record_impl(session, record_id, verified_by=(payload or {}).get('verified_by'))
        if not record:
            raise HTTPException(status_code=404, detail='public record not found')
        session.commit()
        return {'ok': True, 'record': public_record_payload(record)}


@router.post('/add_watchdog_check')
def add_watchdog_check(
    authorization: str | None = Header(default=None),
    payload: dict = Body(...),
):
    require_bearer_token(authorization)
    with get_session() as session:
        check = add_watchdog_check_impl(
            session,
            check_type=str(payload.get('check_type') or '').strip(),
            status=str(payload.get('status') or 'pending').strip(),
            checked_by=(payload.get('checked_by') or None),
            photo_url=(payload.get('photo_url') or None),
            source_url=(payload.get('source_url') or None),
            notes=(payload.get('notes') or None),
        )
        if not check.check_type:
            raise HTTPException(status_code=400, detail='check_type required')
        session.commit()
        return {'ok': True, 'check_id': check.id}


@router.post('/export_legal_bundle')
def export_legal_bundle(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    return {'ok': True, 'job_id': enqueue_export_legal_bundle()}


@router.post('/export_elevator_replacement_bundle')
def export_elevator_replacement_bundle(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    return {'ok': True, 'job_id': enqueue_export_elevator_replacement_bundle()}
