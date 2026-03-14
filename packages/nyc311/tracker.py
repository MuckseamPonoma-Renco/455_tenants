from __future__ import annotations
import json
import os
import re
from datetime import datetime, timezone
import httpx
from sqlalchemy import select
from packages.db import FilingJob, Incident, ServiceRequestCase

SR_RE = re.compile(r"\b311[-\s]?(\d{8,})\b")


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def normalize_sr_number(value: str | None) -> str | None:
    if not value:
        return None
    match = SR_RE.search(value)
    if not match:
        return None
    return f"311-{match.group(1)}"


def find_sr_numbers(text: str | None) -> list[str]:
    if not text:
        return []
    found = []
    for match in SR_RE.finditer(text):
        sr = f"311-{match.group(1)}"
        if sr not in found:
            found.append(sr)
    return found


def upsert_service_request_case(
    session,
    *,
    sr_number: str,
    incident_id: str | None = None,
    filing_job_id: str | None = None,
    source: str = "manual_chat",
    complaint_type: str | None = None,
    status: str = "submitted",
    agency: str | None = None,
    resolution_description: str | None = None,
    raw_status: dict | None = None,
) -> ServiceRequestCase:
    existing = session.scalar(select(ServiceRequestCase).where(ServiceRequestCase.service_request_number == sr_number))
    if existing:
        if incident_id and not existing.incident_id:
            existing.incident_id = incident_id
        if filing_job_id and not existing.filing_job_id:
            existing.filing_job_id = filing_job_id
        if complaint_type:
            existing.complaint_type = complaint_type
        if agency:
            existing.agency = agency
        if status:
            existing.status = status
        if resolution_description:
            existing.resolution_description = resolution_description[:2000]
        existing.last_checked_at = now_iso()
        if raw_status:
            existing.raw_status_json = json.dumps(raw_status, ensure_ascii=False, sort_keys=True)
        return existing

    case = ServiceRequestCase(
        service_request_number=sr_number,
        incident_id=incident_id,
        filing_job_id=filing_job_id,
        source=source,
        complaint_type=complaint_type,
        status=status,
        agency=agency,
        submitted_at=now_iso(),
        last_checked_at=now_iso(),
        resolution_description=(resolution_description or "")[:2000] or None,
        raw_status_json=json.dumps(raw_status, ensure_ascii=False, sort_keys=True) if raw_status else None,
    )
    session.add(case)
    return case


def create_case_from_filing_job(session, *, job: FilingJob, sr_number: str) -> ServiceRequestCase:
    case = upsert_service_request_case(
        session,
        sr_number=sr_number,
        incident_id=job.incident_id,
        filing_job_id=job.job_id,
        source="android_filer",
        complaint_type=job.complaint_type,
        status="submitted",
    )
    job.state = "submitted"
    job.updated_at = now_iso()
    job.completed_at = now_iso()
    return case


def attach_manual_cases_from_text(session, *, text: str, incident: Incident | None = None) -> list[ServiceRequestCase]:
    out = []
    for sr_number in find_sr_numbers(text):
        case = upsert_service_request_case(
            session,
            sr_number=sr_number,
            incident_id=getattr(incident, "incident_id", None),
            source="whatsapp_message",
            complaint_type="Unknown",
            status="submitted",
        )
        out.append(case)
    return out


def _tracker_endpoint() -> str:
    return os.environ.get("NYC311_TRACKER_ENDPOINT", "https://data.cityofnewyork.us/resource/erm2-nwe9.json")


def fetch_live_status(sr_number: str) -> dict | None:
    endpoint = _tracker_endpoint()
    query = {
        "$limit": 1,
        "$select": "unique_key,status,agency,complaint_type,descriptor,created_date,closed_date,resolution_description,resolution_action_updated_date",
        "unique_key": sr_number,
    }
    with httpx.Client(timeout=20.0) as client:
        response = client.get(endpoint, params=query)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list) and payload:
            return payload[0]
    return None


def sync_all_case_statuses(session) -> list[dict]:
    results = []
    rows = session.scalars(select(ServiceRequestCase).order_by(ServiceRequestCase.submitted_at.desc())).all()
    for case in rows:
        live = fetch_live_status(case.service_request_number)
        if not live:
            continue
        case.status = live.get("status") or case.status
        case.agency = live.get("agency") or case.agency
        case.complaint_type = live.get("complaint_type") or case.complaint_type
        case.resolution_description = (live.get("resolution_description") or case.resolution_description or "")[:2000] or None
        case.last_checked_at = now_iso()
        case.raw_status_json = json.dumps(live, ensure_ascii=False, sort_keys=True)
        if live.get("closed_date"):
            case.closed_at = live.get("closed_date")
        results.append({"service_request_number": case.service_request_number, "status": case.status})
    return results
