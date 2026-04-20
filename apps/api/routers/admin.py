from __future__ import annotations

from fastapi import APIRouter, Header
from packages.auth import require_bearer_token
from packages.queue import (
    enqueue_export_elevator_replacement_bundle,
    enqueue_export_legal_bundle,
    enqueue_full_resync,
    enqueue_queue_311_jobs,
    enqueue_reprocess_last_n,
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


@router.post('/export_legal_bundle')
def export_legal_bundle(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    return {'ok': True, 'job_id': enqueue_export_legal_bundle()}


@router.post('/export_elevator_replacement_bundle')
def export_elevator_replacement_bundle(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    return {'ok': True, 'job_id': enqueue_export_elevator_replacement_bundle()}
