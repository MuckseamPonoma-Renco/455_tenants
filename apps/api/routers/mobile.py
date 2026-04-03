from __future__ import annotations

import json
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from packages.auth import require_bearer_token
from packages.db import FilingJob, ServiceRequestCase, get_session
from packages.nyc311.planner import claim_next_job
from packages.nyc311.tracker import create_case_from_filing_job, normalize_sr_number, upsert_service_request_case
from packages.queue import enqueue_full_resync
from packages.worker_jobs import sync_311_statuses as sync_311_statuses_now

router = APIRouter()


class FilingSubmittedPayload(BaseModel):
    service_request_number: str
    app_status: str | None = None
    notes: str | None = None


class FilingFailedPayload(BaseModel):
    error: str
    notes: str | None = None


class ServiceRequestUpdatePayload(BaseModel):
    service_request_number: str
    status: str
    agency: str | None = None
    complaint_type: str | None = None
    resolution_description: str | None = None
    raw_status: dict | None = None


def _schedule_sheet_refresh() -> None:
    try:
        enqueue_full_resync()
    except Exception:
        return


@router.post('/filings/claim_next')
def mobile_claim_next(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization, kind='mobile')
    with get_session() as session:
        job, skipped = claim_next_job(session)
        session.commit()
        if skipped:
            _schedule_sheet_refresh()
        if not job:
            return {'ok': True, 'job': None}
        payload = json.loads(job.payload_json or '{}')
    _schedule_sheet_refresh()
    return {
        'ok': True,
        'job': {
                'job_id': job.job_id,
                'incident_id': job.incident_id,
                'state': job.state,
                'complaint_type': job.complaint_type,
                'form_target': job.form_target,
                'payload': payload,
                'notes': job.notes,
            },
        }


@router.post('/filings/{job_id}/submitted')
def mobile_mark_submitted(job_id: int, payload: FilingSubmittedPayload, authorization: str | None = Header(default=None)):
    require_bearer_token(authorization, kind='mobile')
    sr_number = normalize_sr_number(payload.service_request_number)
    if not sr_number:
        raise HTTPException(status_code=400, detail='Invalid NYC311 service request number')
    with get_session() as session:
        job = session.get(FilingJob, job_id)
        if not job:
            raise HTTPException(status_code=404, detail='Unknown filing job')
        try:
            case = create_case_from_filing_job(session, job=job, sr_number=sr_number)
            if payload.notes:
                job.notes = ((job.notes or '') + ' | ' + payload.notes)[:2000]
            if payload.app_status:
                case.status = payload.app_status
            session.commit()
            _schedule_sheet_refresh()
            return {'ok': True, 'service_request_number': case.service_request_number, 'case_id': case.id}
        except IntegrityError:
            # Another request may have inserted the SR case between our initial lookup and commit.
            session.rollback()
            job = session.get(FilingJob, job_id)
            if not job:
                raise HTTPException(status_code=404, detail='Unknown filing job')
            case = create_case_from_filing_job(session, job=job, sr_number=sr_number)
            if payload.notes:
                job.notes = ((job.notes or '') + ' | ' + payload.notes)[:2000]
            if payload.app_status:
                case.status = payload.app_status
            session.commit()
            _schedule_sheet_refresh()
            return {'ok': True, 'service_request_number': case.service_request_number, 'case_id': case.id}


@router.post('/filings/{job_id}/failed')
def mobile_mark_failed(job_id: int, payload: FilingFailedPayload, authorization: str | None = Header(default=None)):
    require_bearer_token(authorization, kind='mobile')
    with get_session() as session:
        job = session.get(FilingJob, job_id)
        if not job:
            raise HTTPException(status_code=404, detail='Unknown filing job')
        job.state = 'failed'
        job.last_error = payload.error[:2000]
        if payload.notes:
            job.notes = ((job.notes or '') + ' | ' + payload.notes)[:2000]
        session.commit()
        result = {'ok': True, 'job_id': job.job_id, 'state': job.state}
    _schedule_sheet_refresh()
    return result


@router.post('/sr_updates')
def mobile_sr_update(payload: ServiceRequestUpdatePayload, authorization: str | None = Header(default=None)):
    require_bearer_token(authorization, kind='mobile')
    sr_number = normalize_sr_number(payload.service_request_number)
    if not sr_number:
        raise HTTPException(status_code=400, detail='Invalid NYC311 service request number')
    with get_session() as session:
        case = upsert_service_request_case(
            session,
            sr_number=sr_number,
            source='mobile_status_update',
            complaint_type=payload.complaint_type,
            status=payload.status,
            agency=payload.agency,
            resolution_description=payload.resolution_description,
            raw_status=payload.raw_status,
        )
        session.commit()
        result = {'ok': True, 'case_id': case.id, 'service_request_number': case.service_request_number}
    _schedule_sheet_refresh()
    return result


@router.post('/sr_updates/sync_now')
def mobile_sync_now(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization, kind='mobile')
    return sync_311_statuses_now()
