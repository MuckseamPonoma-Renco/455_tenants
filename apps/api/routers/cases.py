import json
from fastapi import APIRouter, Header
from sqlalchemy import select
from packages.auth import require_bearer_token
from packages.db import FilingJob, Incident, ServiceRequestCase, get_session

router = APIRouter()


@router.get('/cases')
def list_cases(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    with get_session() as session:
        cases = session.scalars(select(ServiceRequestCase).order_by(ServiceRequestCase.submitted_at.desc().nullslast())).all()
        return {
            'ok': True,
            'cases': [
                {
                    'service_request_number': case.service_request_number,
                    'incident_id': case.incident_id,
                    'status': case.status,
                    'agency': case.agency,
                    'complaint_type': case.complaint_type,
                    'submitted_at': case.submitted_at,
                    'closed_at': case.closed_at,
                    'resolution_description': case.resolution_description,
                }
                for case in cases
            ],
        }


@router.get('/queue')
def list_queue(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    with get_session() as session:
        jobs = session.scalars(select(FilingJob).order_by(FilingJob.created_at.desc().nullslast())).all()
        return {
            'ok': True,
            'jobs': [
                {
                    'job_id': job.job_id,
                    'incident_id': job.incident_id,
                    'state': job.state,
                    'complaint_type': job.complaint_type,
                    'form_target': job.form_target,
                    'payload': json.loads(job.payload_json or '{}'),
                    'attempts': job.attempts,
                    'last_error': job.last_error,
                }
                for job in jobs
            ],
        }


@router.get('/incidents')
def list_incidents(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    with get_session() as session:
        rows = session.scalars(select(Incident).order_by(Incident.last_ts_epoch.desc().nullslast())).all()
        return {
            'ok': True,
            'incidents': [
                {
                    'incident_id': row.incident_id,
                    'category': row.category,
                    'asset': row.asset,
                    'status': row.status,
                    'severity': row.severity,
                    'witness_count': row.witness_count,
                    'report_count': row.report_count,
                    'start_ts': row.start_ts,
                    'end_ts': row.end_ts,
                    'title': row.title,
                    'summary': row.summary,
                }
                for row in rows
            ],
        }
