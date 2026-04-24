from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select

from packages.db import FilingJob, Incident, RawMessage, ServiceRequestCase
from packages.llm.openai_client import llm_enabled
from packages.timeutil import normalize_timestamp


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except Exception:
        return None


def _has_llm() -> bool:
    return llm_enabled()


def _humanize_timedelta(value: timedelta) -> str:
    total = int(value.total_seconds())
    if total < 60:
        return f'{max(total, 0)}s'
    minutes = total // 60
    if minutes < 60:
        return f'{minutes}m'
    hours = minutes // 60
    if hours < 48:
        return f'{hours}h'
    days = hours // 24
    return f'{days}d'


def _incident_to_dict(row: Incident) -> dict[str, Any]:
    return {
        'incident_id': row.incident_id,
        'category': row.category,
        'asset': row.asset,
        'status': row.status,
        'severity': int(row.severity or 0),
        'witness_count': int(row.witness_count or 0),
        'report_count': int(row.report_count or 0),
        'title': row.title,
        'summary': row.summary,
        'start_ts': normalize_timestamp(row.start_ts, fallback=row.start_ts_epoch),
        'end_ts': normalize_timestamp(row.end_ts, fallback=row.end_ts_epoch),
        'updated_at': normalize_timestamp(row.updated_at),
    }


def _job_to_dict(row: FilingJob) -> dict[str, Any]:
    return {
        'job_id': row.job_id,
        'incident_id': row.incident_id,
        'state': row.state,
        'complaint_type': row.complaint_type,
        'attempts': int(row.attempts or 0),
        'created_at': normalize_timestamp(row.created_at),
        'updated_at': normalize_timestamp(row.updated_at),
        'last_error': row.last_error,
    }


def _case_to_dict(row: ServiceRequestCase) -> dict[str, Any]:
    return {
        'service_request_number': row.service_request_number,
        'incident_id': row.incident_id,
        'status': row.status,
        'agency': row.agency,
        'complaint_type': row.complaint_type,
        'submitted_at': normalize_timestamp(row.submitted_at),
        'closed_at': normalize_timestamp(row.closed_at),
    }


def build_ops_summary(session) -> dict[str, Any]:
    raw_count = session.scalar(select(func.count(RawMessage.message_id))) or 0
    incident_count = session.scalar(select(func.count(Incident.incident_id))) or 0
    case_count = session.scalar(select(func.count(ServiceRequestCase.id))) or 0
    open_incidents = session.scalars(
        select(Incident).where(Incident.status != 'closed').order_by(Incident.last_ts_epoch.desc().nullslast())
    ).all()
    recent_incidents = session.scalars(
        select(Incident).order_by(Incident.last_ts_epoch.desc().nullslast()).limit(12)
    ).all()
    jobs = session.scalars(select(FilingJob).order_by(FilingJob.created_at.desc().nullslast())).all()
    cases = session.scalars(
        select(ServiceRequestCase).order_by(ServiceRequestCase.submitted_at.desc().nullslast()).limit(12)
    ).all()
    latest_message = session.scalar(select(RawMessage).order_by(RawMessage.ts_epoch.desc().nullslast()).limit(1))

    job_counts = {'pending': 0, 'claimed': 0, 'submitted': 0, 'failed': 0}
    for job in jobs:
        job_counts[job.state] = job_counts.get(job.state, 0) + 1

    repeated_cutoff = int((_now() - timedelta(days=30)).timestamp())
    recent_elevator_count = session.scalar(
        select(func.count(Incident.incident_id)).where(
            Incident.category == 'elevator',
            Incident.start_ts_epoch.is_not(None),
            Incident.start_ts_epoch >= repeated_cutoff,
        )
    ) or 0

    stage = 'bootstrap'
    next_step = 'Import the WhatsApp export or send one trigger message through /ingest/whatsapp_web.'
    if raw_count > 0:
        stage = 'capture_live'
        next_step = 'Keep live WhatsApp capture running so new reports keep entering the system.'
    if case_count > 0:
        stage = 'tracking_live'
        next_step = 'Keep the NYC311 portal worker available for new incidents and run case-status sync daily.'
    if open_incidents and (job_counts.get('pending', 0) or job_counts.get('failed', 0)):
        stage = 'ready_for_portal_worker'
        next_step = 'Run the Playwright NYC311 portal worker and submit one real complaint.'

    alerts: list[dict[str, Any]] = []
    actions: list[dict[str, str]] = []

    if open_incidents:
        top = open_incidents[0]
        age = None
        if top.start_ts_epoch:
            age = max(0, int(_now().timestamp()) - int(top.start_ts_epoch))
        detail = f"{top.title or top.category} is still open"
        if age is not None:
            detail += f" for ~{_humanize_timedelta(timedelta(seconds=age))}"
        alerts.append({
            'level': 'critical' if top.category == 'elevator' else 'warning',
            'code': 'open_incident',
            'title': 'Open building issue needs follow-through',
            'detail': detail,
        })

    if job_counts.get('failed', 0):
        alerts.append({
            'level': 'warning',
            'code': 'failed_jobs',
            'title': '311 filing retries needed',
            'detail': f"{job_counts['failed']} filing job(s) failed and need a portal rerun or selector refresh.",
        })

    if recent_elevator_count >= 3:
        alerts.append({
            'level': 'warning',
            'code': 'repeat_elevator_outages',
            'title': 'Chronic elevator pattern detected',
            'detail': f'{recent_elevator_count} elevator incidents were recorded in the last 30 days.',
        })

    if open_incidents and not case_count and (job_counts.get('pending', 0) or job_counts.get('claimed', 0) or job_counts.get('failed', 0)):
        actions.append({
            'kind': 'do_now',
            'title': 'Submit the first real complaint',
            'detail': 'Run the Playwright portal worker once and complete one end-to-end NYC311 submission.',
        })

    if case_count:
        actions.append({
            'kind': 'routine',
            'title': 'Track service-request status daily',
            'detail': 'Run /admin/sync_311_statuses once per day so tenants see whether DOB/311 actually moved the case.',
        })

    if recent_elevator_count >= 3 or len(open_incidents) >= 2:
        actions.append({
            'kind': 'pressure',
            'title': 'Export the elevator replacement bundle',
            'detail': 'Generate the focused pressure packet with elevator incidents, WhatsApp evidence, 311 cases, and portal screenshots.',
        })
    if recent_elevator_count >= 3 or len(open_incidents) >= 2:
        actions.append({
            'kind': 'pressure',
            'title': 'Export the legal bundle',
            'detail': 'Generate the chronology packet and circulate it to tenant organizers, the attorney, or building leadership when pressure is needed.',
        })

    if not _has_llm():
        actions.append({
            'kind': 'optional',
            'title': 'Turn on the low-cost LLM layer',
            'detail': 'Add OPENAI_API_KEY to enable richer issue classification plus AI-generated tenant and management briefings.',
        })

    metrics = {
        'raw_messages': int(raw_count),
        'incidents_total': int(incident_count),
        'incidents_open': len(open_incidents),
        'filing_jobs_total': len(jobs),
        'filing_jobs_pending': int(job_counts.get('pending', 0)),
        'filing_jobs_failed': int(job_counts.get('failed', 0)),
        'service_requests_total': int(case_count),
        'recent_elevator_incidents_30d': int(recent_elevator_count),
        'llm_enabled': _has_llm(),
    }

    return {
        'stage': stage,
        'next_step': next_step,
        'metrics': metrics,
        'alerts': alerts,
        'actions': actions,
        'latest_message': {
            'message_id': getattr(latest_message, 'message_id', None),
            'ts_iso': normalize_timestamp(getattr(latest_message, 'ts_iso', None), fallback=getattr(latest_message, 'ts_epoch', None)),
            'text': (getattr(latest_message, 'text', '') or '')[:200] or None,
        },
        'open_incidents': [_incident_to_dict(row) for row in open_incidents[:6]],
        'recent_jobs': [_job_to_dict(row) for row in jobs[:6]],
        'recent_cases': [_case_to_dict(row) for row in cases[:6]],
        'recent_incidents': [_incident_to_dict(row) for row in recent_incidents[:6]],
    }
