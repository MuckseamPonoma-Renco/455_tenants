from __future__ import annotations
from typing import Any
import json
import os
import re
import time
from datetime import datetime, timezone
import httpx
from sqlalchemy import select
from packages.audit import append_audit_event
from packages.db import FilingJob, Incident, MessageDecision, RawMessage, ServiceRequestCase
from packages.timeutil import normalize_timestamp

SR_NORMALIZE_RE = re.compile(r"(?<![A-Za-z0-9])(?:311[-\s]?)?(\d{8})(?!\d)")
SR_TEXT_RE = re.compile(r"(?<![A-Za-z0-9])311[-\s]?(\d{8})(?!\d)")


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _json_dumps(value: dict) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def normalize_sr_number(value: str | None) -> str | None:
    if not value:
        return None
    match = SR_NORMALIZE_RE.search(value)
    if not match:
        return None
    return f"311-{match.group(1)}"


def find_sr_numbers(text: str | None) -> list[str]:
    if not text:
        return []
    found = []
    for match in SR_TEXT_RE.finditer(text):
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
    submitted_at: str | None = None,
) -> ServiceRequestCase:
    existing = session.scalar(select(ServiceRequestCase).where(ServiceRequestCase.service_request_number == sr_number))
    if existing:
        if incident_id and not existing.incident_id:
            existing.incident_id = incident_id
        if filing_job_id and not existing.filing_job_id:
            existing.filing_job_id = filing_job_id
        if complaint_type and (complaint_type.casefold() != "unknown" or not existing.complaint_type):
            existing.complaint_type = complaint_type
        if agency:
            existing.agency = agency
        # Re-ingesting a chat mention is not a new official status observation.
        if status and (raw_status or not existing.last_checked_at):
            existing.status = status
        if resolution_description:
            existing.resolution_description = resolution_description[:2000]
        if submitted_at and not existing.submitted_at:
            existing.submitted_at = submitted_at
        if raw_status:
            existing.last_checked_at = now_iso()
            existing.raw_status_json = _json_dumps(raw_status)
        return existing

    case = ServiceRequestCase(
        service_request_number=sr_number,
        incident_id=incident_id,
        filing_job_id=filing_job_id,
        source=source,
        complaint_type=complaint_type,
        status=status,
        agency=agency,
        submitted_at=submitted_at or now_iso(),
        last_checked_at=now_iso() if raw_status else None,
        resolution_description=(resolution_description or "")[:2000] or None,
        raw_status_json=_json_dumps(raw_status) if raw_status else None,
    )
    session.add(case)
    return case


def create_case_from_filing_job(session, *, job: FilingJob, sr_number: str) -> ServiceRequestCase:
    case = upsert_service_request_case(
        session,
        sr_number=sr_number,
        incident_id=job.incident_id,
        filing_job_id=job.job_id,
        source=job.filing_channel or "portal_playwright",
        complaint_type=job.complaint_type,
        status="submitted",
    )
    job.state = "submitted"
    job.updated_at = now_iso()
    job.completed_at = now_iso()
    return case


def _infer_incident_for_manual_case(session, *, raw_message: RawMessage) -> Incident | None:
    if raw_message.ts_epoch is None:
        return None

    window_start = int(raw_message.ts_epoch) - (6 * 3600)
    rows = session.execute(
        select(MessageDecision, RawMessage, Incident)
        .join(RawMessage, RawMessage.message_id == MessageDecision.message_id)
        .join(Incident, Incident.incident_id == MessageDecision.incident_id)
        .where(
            MessageDecision.incident_id.is_not(None),
            RawMessage.ts_epoch.is_not(None),
            RawMessage.ts_epoch >= window_start,
            RawMessage.ts_epoch <= raw_message.ts_epoch,
        )
        .order_by(RawMessage.ts_epoch.desc())
    ).all()

    best: Incident | None = None
    best_score: int | None = None
    seen_incidents: set[str] = set()
    current_sender = (raw_message.sender or "").strip().lower()
    for _, prior_raw, incident in rows:
        if incident.incident_id in seen_incidents:
            continue
        seen_incidents.add(incident.incident_id)
        delta_seconds = max(0, int(raw_message.ts_epoch) - int(prior_raw.ts_epoch or raw_message.ts_epoch))
        score = 0
        if current_sender and (prior_raw.sender or "").strip().lower() == current_sender:
            score += 200
        if incident.status != "closed":
            score += 25
        score += max(0, 180 - (delta_seconds // 60))
        score += int(incident.severity or 0) * 10
        score += int(incident.report_count or 0)
        if best_score is None or score > best_score:
            best = incident
            best_score = score
    return best


def attach_manual_cases_from_text(
    session,
    *,
    text: str,
    incident: Incident | None = None,
    raw_message: RawMessage | None = None,
) -> list[ServiceRequestCase]:
    out = []
    linked_incident = incident or (_infer_incident_for_manual_case(session, raw_message=raw_message) if raw_message else None)
    message_submitted_at = None
    if raw_message:
        message_submitted_at = normalize_timestamp(raw_message.ts_iso, fallback=raw_message.ts_epoch)
    for sr_number in find_sr_numbers(text):
        case = upsert_service_request_case(
            session,
            sr_number=sr_number,
            incident_id=getattr(linked_incident, "incident_id", None),
            source="whatsapp_message",
            complaint_type="Unknown",
            status="submitted",
            submitted_at=message_submitted_at,
        )
        out.append(case)
    return out


def _tracker_endpoint() -> str:
    return os.environ.get("NYC311_TRACKER_ENDPOINT", "https://data.cityofnewyork.us/resource/erm2-nwe9.json")


def _tracker_retries() -> int:
    try:
        return max(1, int(os.environ.get("NYC311_TRACKER_RETRIES", "3")))
    except ValueError:
        return 3


def fetch_live_status(sr_number: str) -> dict | None:
    normalized = normalize_sr_number(sr_number)
    if not normalized:
        return None
    endpoint = _tracker_endpoint()
    query = {
        "$limit": 1,
        "$select": "unique_key,status,agency,complaint_type,descriptor,created_date,closed_date,resolution_description,resolution_action_updated_date",
        "unique_key": normalized.split("-", 1)[1],
    }
    attempts = _tracker_retries()
    with httpx.Client(timeout=20.0) as client:
        for attempt in range(1, attempts + 1):
            try:
                response = client.get(endpoint, params=query)
                response.raise_for_status()
                payload = response.json()
                break
            except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                retryable_status = status is not None and (status >= 500 or status == 429)
                if attempt >= attempts or (status is not None and not retryable_status):
                    raise
                time.sleep(min(2.0, 0.25 * attempt))
        if isinstance(payload, list) and payload:
            return payload[0]
    return None


PORTAL_FIELD_LABELS = {
    "updated_on": "Updated On",
    "date_reported": "Date Reported",
    "date_closed": "Date Closed",
    "problem": "Problem",
    "problem_details": "Problem Details",
    "additional_details": "Additional Details",
    "sr_address": "SR Address",
    "time_to_next_update": "Time To Next Update",
}
INVALID_PORTAL_STATUS_LABELS = {"sign in | sign up", "subscribe", "service request status"}
VALID_STATUS_LABELS = {
    "closed", "completed", "in progress", "in process", "open", "submitted", "pending",
    "assigned", "started", "cancelled", "canceled", "resolved", "reopened", "duplicate",
}
CLOSED_STATUS_LABELS = {"closed", "completed", "cancelled", "canceled", "resolved", "duplicate"}


def _clean_portal_value(value: str | None) -> str:
    clean = (value or "").replace("\u202f", " ").replace("\u200e", "").replace("\u200f", "").strip()
    return "" if clean in {"", "-"} else clean


def _clean_portal_status(value: str | None) -> str:
    clean = _clean_portal_value(value)
    if clean.casefold() not in VALID_STATUS_LABELS:
        return ""
    return clean


def _portal_field(page_text: str, label: str) -> str:
    lines = [(line or "").replace("\u202f", " ").replace("\u200e", "").replace("\u200f", "").strip() for line in (page_text or "").splitlines()]
    wanted = label.casefold()
    for idx, line in enumerate(lines):
        if line.casefold() == wanted and idx + 1 < len(lines):
            return _clean_portal_value(lines[idx + 1])
    return ""


def _portal_ts(value: str | None) -> str | None:
    clean = _clean_portal_value(value)
    if not clean:
        return None
    clean = re.sub(r"(\d{1,2}/\d{1,2}/\d{2,4}),\s*", r"\1 ", clean)
    return normalize_timestamp(clean)


def portal_lookup_to_raw_status(lookup: Any) -> dict | None:
    if not lookup:
        return None
    if isinstance(lookup, dict):
        if lookup.get("error"):
            return None
        payload = dict(lookup)
    else:
        payload = dict(getattr(lookup, "__dict__", {}) or {})
    sr_number = normalize_sr_number(str(payload.get("service_request_number") or ""))
    if not sr_number:
        return None

    page_text = str(payload.get("page_text") or "")
    fields = {key: _portal_field(page_text, label) for key, label in PORTAL_FIELD_LABELS.items()}
    raw = {
        "source": "nyc311_portal",
        "checked_at": now_iso(),
        "service_request_number": sr_number,
        "found": bool(payload.get("found")),
        "status": _clean_portal_status(str(payload.get("status") or "")),
        "final_url": _clean_portal_value(str(payload.get("final_url") or "")),
        "observed_service_request_number": normalize_sr_number(
            _portal_field(page_text, "SR Number") or _portal_field(page_text, "Service Request Number")
        ),
    }
    for key, value in fields.items():
        clean = _clean_portal_value(value)
        if clean:
            raw[key] = clean
    for source_key, target_key in (
        ("updated_on", "updated_on_normalized"),
        ("date_reported", "date_reported_normalized"),
        ("date_closed", "date_closed_normalized"),
    ):
        normalized = _portal_ts(raw.get(source_key))
        if normalized:
            raw[target_key] = normalized
    if "Department of Buildings" in page_text:
        raw["agency"] = "DOB"
    if not raw["status"] and raw.get("date_closed_normalized"):
        raw["status"] = "Closed"
        raw["status_inferred_from"] = "date_closed"
    return raw


def _portal_validation_error(case: ServiceRequestCase, raw: dict | None) -> str | None:
    if not raw:
        return "invalid_lookup"
    expected = normalize_sr_number(case.service_request_number)
    if raw.get("service_request_number") != expected or raw.get("observed_service_request_number") != expected:
        return "service_request_number_mismatch_or_missing"
    if not raw.get("found"):
        return "not_found"
    if not raw.get("status"):
        return "missing_verified_status"
    if raw.get("date_closed_normalized") and str(raw["status"]).casefold() not in CLOSED_STATUS_LABELS:
        return "open_status_with_closed_date"
    return None


def _open_data_validation_error(case: ServiceRequestCase, live: dict | None) -> str | None:
    if not isinstance(live, dict):
        return "missing_record"
    if normalize_sr_number(str(live.get("unique_key") or "")) != normalize_sr_number(case.service_request_number):
        return "service_request_number_mismatch_or_missing"
    status = _clean_portal_status(live.get("status"))
    if not status and not normalize_timestamp(live.get("closed_date")):
        return "missing_verified_status"
    if status and live.get("closed_date") and status.casefold() not in CLOSED_STATUS_LABELS:
        return "open_status_with_closed_date"
    return None


def apply_open_data_status(case: ServiceRequestCase, live: dict) -> bool:
    if _open_data_validation_error(case, live):
        return False
    case.status = _clean_portal_status(live.get("status")) or "Closed"
    case.agency = live.get("agency") or case.agency
    case.complaint_type = live.get("complaint_type") or case.complaint_type
    case.resolution_description = (live.get("resolution_description") or case.resolution_description or "")[:2000] or None
    case.last_checked_at = now_iso()
    case.raw_status_json = _json_dumps({"source": "nyc_open_data", **live})
    if live.get("created_date"):
        case.submitted_at = normalize_timestamp(live.get("created_date")) or case.submitted_at
    if live.get("closed_date"):
        case.closed_at = normalize_timestamp(live.get("closed_date")) or live.get("closed_date")
    elif case.status.casefold() not in CLOSED_STATUS_LABELS:
        case.closed_at = None
    return True


def apply_portal_lookup_status(case: ServiceRequestCase, lookup: Any) -> bool:
    raw = portal_lookup_to_raw_status(lookup)
    if _portal_validation_error(case, raw):
        return False
    case.last_checked_at = raw.get("checked_at") or now_iso()
    case.raw_status_json = _json_dumps(raw)
    case.status = str(raw["status"])
    case.complaint_type = case.complaint_type or raw.get("problem")
    case.agency = raw.get("agency") or case.agency
    if raw.get("date_reported_normalized"):
        case.submitted_at = raw["date_reported_normalized"]
    if raw.get("date_closed_normalized"):
        case.closed_at = raw["date_closed_normalized"]
    elif case.status.casefold() not in CLOSED_STATUS_LABELS:
        case.closed_at = None
    # "Problem Details: Not Working" describes the complaint, not its resolution.
    return True


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return default


class StatusSyncResults(list):
    """Backwards-compatible case rows plus an explicit, non-silent cycle receipt."""

    def __init__(self):
        super().__init__()
        self.summary = {
            "ok": True, "total": 0, "attempted": 0, "updated": 0,
            "not_found": 0, "unverified": 0, "deferred": 0, "errors": 0,
            "error_details": [], "coverage_complete": False,
        }


def _last_status_attempt(case: ServiceRequestCase) -> str:
    try:
        raw = json.loads(case.raw_status_json or "{}")
        attempt = raw.get("status_check_attempt") or {}
        return max(str(attempt.get("checked_at") or ""), str(case.last_checked_at or ""))
    except (ValueError, TypeError, AttributeError):
        return str(case.last_checked_at or "")


def _record_unsuccessful_attempt(case: ServiceRequestCase, outcome: str) -> None:
    try:
        raw = json.loads(case.raw_status_json or "{}")
        if not isinstance(raw, dict):
            raw = {"prior_raw_status": raw}
    except (ValueError, TypeError):
        raw = {"prior_raw_status": case.raw_status_json}
    raw["status_check_attempt"] = {"checked_at": now_iso(), "outcome": outcome}
    case.raw_status_json = _json_dumps(raw)


def sync_all_case_statuses(session, *, portal_fallback: bool | None = None, headless: bool = True, commit_each: bool = False) -> StatusSyncResults:
    use_portal_fallback = _env_bool("NYC311_STATUS_SYNC_PORTAL_FALLBACK", True) if portal_fallback is None else portal_fallback
    portal_max_cases = max(0, _env_int("NYC311_STATUS_SYNC_PORTAL_MAX_CASES", 25))
    results = StatusSyncResults()
    rows = list(session.scalars(select(ServiceRequestCase)).all())
    # Failed/not-found attempts rotate too, without falsely refreshing last_checked_at.
    rows.sort(key=lambda case: (_last_status_attempt(case), case.id or 0))
    results.summary["total"] = len(rows)
    portal_checked = 0
    for case in rows:
        results.summary["attempted"] += 1
        source = "nyc_open_data"
        problems = []
        outcome = "not_found"
        try:
            live = fetch_live_status(case.service_request_number)
            updated = apply_open_data_status(case, live) if live else False
            if live and not updated:
                outcome = "unverified"
                problems.append({"source": source, "error": _open_data_validation_error(case, live)})
        except Exception as exc:
            updated = False
            outcome = "error"
            problems.append({"source": source, "error": str(exc)[:500]})
            append_audit_event(
                "NYC311_STATUS_OPEN_DATA_ERROR",
                None,
                {"service_request_number": case.service_request_number, "error": str(exc)[:500]},
            )
        if not updated and use_portal_fallback and portal_checked < portal_max_cases:
            portal_checked += 1
            try:
                from packages.nyc311.portal import lookup_service_request_status

                lookup = lookup_service_request_status(case.service_request_number, headless=headless)
                updated = apply_portal_lookup_status(case, lookup)
                source = "nyc311_portal"
                if not updated:
                    raw = portal_lookup_to_raw_status(lookup)
                    outcome = "not_found" if raw and not raw.get("found") else "unverified"
                    problems.append({"source": source, "error": _portal_validation_error(case, raw)})
            except Exception as exc:
                outcome = "error"
                problems.append({"source": "nyc311_portal", "error": str(exc)[:500]})
                append_audit_event(
                    "NYC311_STATUS_PORTAL_ERROR",
                    None,
                    {"service_request_number": case.service_request_number, "error": str(exc)[:500]},
                )
        elif not updated and use_portal_fallback:
            if outcome == "not_found":
                outcome = "deferred"
        if updated:
            results.append({"service_request_number": case.service_request_number, "status": case.status, "source": source})
            results.summary["updated"] += 1
        else:
            key = "errors" if outcome == "error" else outcome
            results.summary[key] += 1
            if outcome != "deferred":
                _record_unsuccessful_attempt(case, outcome)
            if problems:
                results.summary["error_details"].append({"service_request_number": case.service_request_number, "problems": problems})
        if commit_each:
            # Browser/network work can take minutes. Preserve completed checks
            # and release database locks between cases, not after the whole queue.
            session.commit()
    results.summary["ok"] = not any(results.summary[key] for key in ("errors", "not_found", "unverified"))
    results.summary["coverage_complete"] = results.summary["updated"] == results.summary["total"]
    return results
