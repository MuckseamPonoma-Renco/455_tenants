from __future__ import annotations

import json
import os
from fastapi import APIRouter, Header
from sqlalchemy import select
from packages.auth import require_bearer_token
from packages.db import FilingJob, Incident, MessageDecision, RawMessage, ServiceRequestCase, get_session
from packages.llm.briefing import generate_briefing
from packages.ops_summary import build_ops_summary
from packages.timeutil import normalize_timestamp, normalize_timestamp_fields

router = APIRouter()


def _spreadsheet_url() -> str | None:
    sid = (os.environ.get('GOOGLE_SHEETS_SPREADSHEET_ID') or '').strip()
    if not sid:
        return None
    return f'https://docs.google.com/spreadsheets/d/{sid}/edit'


@router.get('/cases')
def list_cases(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    with get_session() as session:
        cases = session.scalars(select(ServiceRequestCase).order_by(ServiceRequestCase.submitted_at.desc().nullslast())).all()
        return {
            'ok': True,
            'spreadsheet_url': _spreadsheet_url(),
            'cases': [
                {
                    'service_request_number': case.service_request_number,
                    'incident_id': case.incident_id,
                    'status': case.status,
                    'agency': case.agency,
                    'complaint_type': case.complaint_type,
                    'submitted_at': normalize_timestamp(case.submitted_at),
                    'closed_at': normalize_timestamp(case.closed_at),
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
            'spreadsheet_url': _spreadsheet_url(),
            'jobs': [
                {
                    'job_id': job.job_id,
                    'incident_id': job.incident_id,
                    'state': job.state,
                    'complaint_type': job.complaint_type,
                    'form_target': job.form_target,
                    'payload': normalize_timestamp_fields(json.loads(job.payload_json or '{}')),
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
            'spreadsheet_url': _spreadsheet_url(),
            'incidents': [
                {
                    'incident_id': row.incident_id,
                    'category': row.category,
                    'asset': row.asset,
                    'status': row.status,
                    'severity': row.severity,
                    'witness_count': row.witness_count,
                    'report_count': row.report_count,
                    'start_ts': normalize_timestamp(row.start_ts, fallback=row.start_ts_epoch),
                    'end_ts': normalize_timestamp(row.end_ts, fallback=row.end_ts_epoch),
                    'title': row.title,
                    'summary': row.summary,
                }
                for row in rows
            ],
        }


@router.get('/decisions')
def list_decisions(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    with get_session() as session:
        rows = session.scalars(
            select(MessageDecision)
            .outerjoin(RawMessage, RawMessage.message_id == MessageDecision.message_id)
            .order_by(RawMessage.ts_epoch.desc().nullslast(), MessageDecision.created_at.desc().nullslast())
            .limit(200)
        ).all()
        message_ids = [row.message_id for row in rows]
        raw_map = {row.message_id: row for row in session.scalars(select(RawMessage).where(RawMessage.message_id.in_(message_ids))).all()} if message_ids else {}
        return {
            'ok': True,
            'spreadsheet_url': _spreadsheet_url(),
            'decisions': [
                {
                    'message_id': row.message_id,
                    'message_ts': normalize_timestamp(getattr(raw_map.get(row.message_id), 'ts_iso', None), fallback=getattr(raw_map.get(row.message_id), 'ts_epoch', None)),
                    'decision_updated_at': normalize_timestamp(row.created_at),
                    'source': getattr(raw_map.get(row.message_id), 'source', None),
                    'text': getattr(raw_map.get(row.message_id), 'text', None),
                    'chosen_source': row.chosen_source,
                    'is_issue': row.is_issue,
                    'category': row.category,
                    'event_type': row.event_type,
                    'confidence': row.confidence,
                    'needs_review': row.needs_review,
                    'incident_id': row.incident_id,
                    'auto_file_candidate': row.auto_file_candidate,
                    'rules': json.loads(row.rules_json or '{}'),
                    'llm': json.loads(row.llm_json or '{}'),
                    'final': json.loads(row.final_json or '{}'),
                }
                for row in rows
            ],
        }


@router.get('/summary')
def get_summary(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    with get_session() as session:
        return {'ok': True, 'spreadsheet_url': _spreadsheet_url(), **build_ops_summary(session)}


@router.get('/briefing')
def get_briefing(authorization: str | None = Header(default=None)):
    require_bearer_token(authorization)
    with get_session() as session:
        summary = build_ops_summary(session)
    briefing = generate_briefing(summary)
    return {'ok': True, 'spreadsheet_url': _spreadsheet_url(), 'summary': summary, 'briefing': briefing}
