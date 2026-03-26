from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from packages.db import FilingJob, Incident, MessageDecision, RawMessage, ServiceRequestCase, get_session
from packages.verification.coverage import compute_daily_coverage, detect_gaps

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
NY = ZoneInfo("America/New_York")


def _disabled() -> bool:
    return os.environ.get("DISABLE_SHEETS_SYNC", "0").strip().lower() in {"1", "true", "yes", "on"}


def _creds_path() -> str:
    candidates = [
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        "/run/secrets/gcp_sa.json",
        "/run/secrets/gcp_sa_json",
        "/etc/secrets/gcp_sa.json",
        "secrets/gcp_sa.json",
    ]
    for path in candidates:
        if path and os.path.exists(path):
            return path
    raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set or missing")


def _env_first(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value is not None and value != "":
            return value
    return default


def _service():
    if _disabled():
        raise RuntimeError("Sheets sync disabled")
    creds_path = _creds_path()
    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def _sheet_id():
    sid = os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not sid:
        raise RuntimeError("GOOGLE_SHEETS_SPREADSHEET_ID not set")
    return sid


def _tab(*names: str, default: str) -> str:
    return _env_first(*names, default=default) or default


def _duration_minutes(inc: Incident) -> int | None:
    if inc.start_ts_epoch and inc.end_ts_epoch and inc.end_ts_epoch >= inc.start_ts_epoch:
        return int((inc.end_ts_epoch - inc.start_ts_epoch) // 60)
    return None


def _fmt_ts(epoch: int | None) -> str:
    if not epoch:
        return ""
    return datetime.fromtimestamp(epoch, tz=NY).strftime("%Y-%m-%d %I:%M %p")


def _spreadsheet_url() -> str:
    sid = os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID", "")
    return f"https://docs.google.com/spreadsheets/d/{sid}/edit" if sid else ""


def sync_incidents_to_sheets():
    svc = _service()
    sheet_id = _sheet_id()
    tab = _tab("SHEETS_INCIDENTS_TAB", default="Incidents")
    with get_session() as session:
        incidents = session.query(Incident).all()
    values = [[
        "incident_id", "category", "asset", "severity", "status",
        "start_ts", "end_ts", "duration_min",
        "title", "summary", "proof_refs",
        "report_count", "witness_count", "confidence", "needs_review", "updated_at",
    ]]
    for inc in sorted(incidents, key=lambda row: row.last_ts_epoch or 0, reverse=True):
        values.append([
            inc.incident_id,
            inc.category,
            inc.asset or "",
            inc.severity,
            inc.status,
            inc.start_ts or "",
            inc.end_ts or "",
            _duration_minutes(inc) or "",
            inc.title,
            (inc.summary or "")[:250],
            inc.proof_refs or "",
            int(inc.report_count or 0),
            int(inc.witness_count or 0),
            int(inc.confidence or 0),
            "YES" if inc.needs_review else "",
            inc.updated_at or "",
        ])
    svc.spreadsheets().values().update(spreadsheetId=sheet_id, range=f"{tab}!A1", valueInputOption="RAW", body={"values": values}).execute()


def _elevator_status_from_incidents(incidents: list[Incident], asset: str) -> dict:
    now_epoch = int(datetime.now(tz=timezone.utc).timestamp())
    relevant = [row for row in incidents if row.category == "elevator" and row.asset in (asset, "elevator_both", None)]
    if not relevant:
        return {"status": "UNKNOWN", "last_evidence": "", "confidence": "Low", "incident_id": ""}
    relevant.sort(key=lambda row: row.last_ts_epoch or 0, reverse=True)
    latest = relevant[0]
    age_sec = now_epoch - int(latest.last_ts_epoch or 0) if latest.last_ts_epoch else 10**9
    age_hours = age_sec / 3600.0
    if age_hours > 6:
        return {"status": "UNKNOWN", "last_evidence": _fmt_ts(latest.last_ts_epoch), "confidence": "Low", "incident_id": latest.incident_id}
    status = "OUT" if latest.status != "closed" else "WORKING"
    wc = int(latest.witness_count or 0)
    confidence = "High" if age_hours <= 2 and wc >= 2 else "Medium" if age_hours <= 6 and wc >= 1 else "Low"
    return {"status": status, "last_evidence": _fmt_ts(latest.last_ts_epoch), "confidence": confidence, "incident_id": latest.incident_id}


def sync_dashboard_to_sheets():
    svc = _service()
    sheet_id = _sheet_id()
    tab = _tab("SHEETS_DASHBOARD_TAB", default="Dashboard")
    with get_session() as session:
        incidents = session.query(Incident).all()
        raw_count = session.query(RawMessage).count()
        last_raw = session.query(RawMessage).order_by(RawMessage.ts_epoch.desc().nullslast()).first()
        open_cases = session.query(ServiceRequestCase).filter(ServiceRequestCase.closed_at.is_(None)).count()
        queue_count = session.query(FilingJob).filter(FilingJob.state.in_(["pending", "claimed", "failed"])).count()
        review_count = session.query(MessageDecision).filter(MessageDecision.needs_review.is_(True)).count()

    total = len(incidents)
    open_incidents = sum(1 for row in incidents if row.status != "closed")
    needs_review = sum(1 for row in incidents if row.needs_review)
    by_cat = {}
    for row in incidents:
        by_cat[row.category] = by_cat.get(row.category, 0) + 1

    north = _elevator_status_from_incidents(incidents, "elevator_north")
    south = _elevator_status_from_incidents(incidents, "elevator_south")
    if north["status"] == "OUT" and south["status"] == "OUT":
        overall = "CRITICAL (both likely out)"
    elif north["status"] == "OUT" or south["status"] == "OUT":
        overall = "DEGRADED (one likely out)"
    elif north["status"] == "WORKING" and south["status"] == "WORKING":
        overall = "OK (both likely working)"
    else:
        overall = "UNKNOWN"

    report_form_url = ((_env_first("PUBLIC_BASE_URL", default="") or "").rstrip("/") + "/report") if _env_first("PUBLIC_BASE_URL", default="") else ""
    values = [
        ["ELEVATOR STATUS NOW", ""],
        ["north_status", north["status"]],
        ["north_last_evidence", north["last_evidence"]],
        ["north_confidence", north["confidence"]],
        ["north_incident_id", north["incident_id"]],
        ["", ""],
        ["south_status", south["status"]],
        ["south_last_evidence", south["last_evidence"]],
        ["south_confidence", south["confidence"]],
        ["south_incident_id", south["incident_id"]],
        ["", ""],
        ["overall", overall],
        ["", ""],
        ["CONTROL", ""],
        ["spreadsheet_url", _spreadsheet_url()],
        ["report_form_url", report_form_url],
        ["llm_mode", os.environ.get("LLM_MODE", "uncertain")],
        ["", ""],
        ["SYSTEM METRICS", ""],
        ["raw_messages_total", raw_count],
        ["raw_last_seen", _fmt_ts(getattr(last_raw, "ts_epoch", None)) if last_raw else ""],
        ["total_incidents", total],
        ["open_incidents", open_incidents],
        ["incidents_needing_review", needs_review],
        ["decision_rows_needing_review", review_count],
        ["open_311_cases", open_cases],
        ["311_queue_depth", queue_count],
        ["", ""],
        ["category", "count"],
    ]
    for key, value in sorted(by_cat.items(), key=lambda item: item[1], reverse=True):
        values.append([key, value])

    svc.spreadsheets().values().update(spreadsheetId=sheet_id, range=f"{tab}!A1", valueInputOption="RAW", body={"values": values}).execute()


def sync_coverage_to_sheets():
    svc = _service()
    sheet_id = _sheet_id()
    tab = _tab("SHEETS_COVERAGE_TAB", default="Coverage")
    coverage = compute_daily_coverage(limit_days=90)
    gaps = detect_gaps(coverage, min_messages_per_day=1)
    values = [["day", "messages", "first_ts_epoch", "last_ts_epoch"]]
    for row in coverage:
        values.append([row.day, row.messages, row.first_ts_epoch or "", row.last_ts_epoch or ""])
    values += [[""], ["gap_days (messages<1)", ", ".join(gaps)]]
    svc.spreadsheets().values().update(spreadsheetId=sheet_id, range=f"{tab}!A1", valueInputOption="RAW", body={"values": values}).execute()


def sync_311_cases_to_sheets():
    svc = _service()
    sheet_id = _sheet_id()
    tab = _tab("SHEETS_311_CASES_TAB", "SHEETS_CASES_TAB", default="Cases311")
    with get_session() as session:
        cases = session.query(ServiceRequestCase).all()
    values = [["service_request_number", "incident_id", "source", "complaint_type", "status", "agency", "submitted_at", "last_checked_at", "closed_at", "resolution_description"]]
    for case in sorted(cases, key=lambda row: row.submitted_at or "", reverse=True):
        values.append([
            case.service_request_number,
            case.incident_id or "",
            case.source,
            case.complaint_type or "",
            case.status,
            case.agency or "",
            case.submitted_at or "",
            case.last_checked_at or "",
            case.closed_at or "",
            (case.resolution_description or "")[:500],
        ])
    svc.spreadsheets().values().update(spreadsheetId=sheet_id, range=f"{tab}!A1", valueInputOption="RAW", body={"values": values}).execute()


def sync_311_queue_to_sheets():
    svc = _service()
    sheet_id = _sheet_id()
    tab = _tab("SHEETS_311_QUEUE_TAB", "SHEETS_QUEUE_TAB", default="Queue311")
    with get_session() as session:
        jobs = session.query(FilingJob).all()
    values = [["job_id", "incident_id", "state", "priority", "complaint_type", "form_target", "attempts", "created_at", "claimed_at", "completed_at", "notes"]]
    for job in sorted(jobs, key=lambda row: row.created_at or "", reverse=True):
        values.append([
            job.job_id,
            job.incident_id or "",
            job.state,
            job.priority,
            job.complaint_type or "",
            job.form_target or "",
            int(job.attempts or 0),
            job.created_at or "",
            job.claimed_at or "",
            job.completed_at or "",
            (job.notes or "")[:500],
        ])
    svc.spreadsheets().values().update(spreadsheetId=sheet_id, range=f"{tab}!A1", valueInputOption="RAW", body={"values": values}).execute()


def sync_decisions_to_sheets():
    svc = _service()
    sheet_id = _sheet_id()
    tab = _tab("SHEETS_DECISIONS_TAB", default="DecisionLog")
    with get_session() as session:
        decisions = session.query(MessageDecision).order_by(MessageDecision.created_at.desc().nullslast()).limit(500).all()
        raw_map = {row.message_id: row for row in session.query(RawMessage).filter(RawMessage.message_id.in_([d.message_id for d in decisions])).all()} if decisions else {}
    values = [[
        "created_at", "message_id", "source", "text", "chosen_source", "is_issue", "category", "event_type",
        "confidence", "needs_review", "incident_id", "auto_file_candidate", "rules_json", "llm_json", "final_json",
    ]]
    for row in decisions:
        raw = raw_map.get(row.message_id)
        values.append([
            row.created_at or "",
            row.message_id,
            getattr(raw, "source", ""),
            ((getattr(raw, "text", "") or "")[:250]),
            row.chosen_source,
            "YES" if row.is_issue else "",
            row.category or "",
            row.event_type or "",
            int(row.confidence or 0),
            "YES" if row.needs_review else "",
            row.incident_id or "",
            "YES" if row.auto_file_candidate else "",
            row.rules_json or "",
            row.llm_json or "",
            row.final_json or "",
        ])
    svc.spreadsheets().values().update(spreadsheetId=sheet_id, range=f"{tab}!A1", valueInputOption="RAW", body={"values": values}).execute()
