from __future__ import annotations
import os
from zoneinfo import ZoneInfo
from sqlalchemy import select
from packages.db import Incident, RawMessage, get_session
from packages.local_env import load_local_env_file
from packages.nyc311.address import canonicalize_building_address
from packages.nyc311.models import FilingDraft
from packages.timeutil import normalize_timestamp

NY = ZoneInfo("America/New_York")


def _env(name: str, default: str = "") -> str:
    value = os.environ.get(name)
    return value if value is not None and value != "" else default


def _asset_label(asset: str | None) -> str:
    return {
        "elevator_north": "north elevator",
        "elevator_south": "south elevator",
        "elevator_both": "both elevators",
        None: "building elevator service",
        "": "building elevator service",
    }.get(asset, asset or "building condition")


def _public_subject(asset: str | None) -> str:
    return {
        "elevator_north": "North elevator",
        "elevator_south": "South elevator",
        "elevator_both": "Both elevators",
        None: "Elevator",
        "": "Elevator",
    }.get(asset, "Elevator")


def _short_description(inc: Incident) -> str:
    subject = _public_subject(inc.asset)
    summary = " ".join((inc.summary or "").split()).lower()
    raw_text = ""
    refs = [ref.strip() for ref in (inc.proof_refs or "").split(",") if ref.strip()]
    if refs:
        with get_session() as session:
            rows = session.scalars(select(RawMessage).where(RawMessage.message_id.in_(refs))).all()
        rows = sorted(rows, key=lambda row: int(row.ts_epoch or 0), reverse=True)
        if rows:
            raw_text = " ".join((rows[0].text or "").split()).lower()

    text = raw_text or summary

    if "stopping on each floor" in text or "stopping on every floor" in text:
        return f"{subject} stopping on every floor."
    if "trapped a passenger" in text and "stuck" in text:
        return f"{subject} stuck and trapped a passenger."
    if "trapped a passenger" in text:
        return f"{subject} trapped a passenger."
    if "problematic ride" in text or "rough ride" in text or "behaving badly" in text:
        return f"{subject} acting up and stopping on random floors."
    if "stop" in text and "each floor" in text:
        return f"{subject} stopping on each floor."
    if "one working elevator" in text or "down to one working elevator" in text:
        return "Only one elevator working."
    if "stuck" in text:
        return f"{subject} stuck."
    if "reduced or not working" in text or "not working" in text or "down" in text or "dead" in text:
        if inc.asset == "elevator_both":
            return "Both elevators down."
        return f"{subject} dead."
    return f"{subject} not working."


def build_filing_draft(inc: Incident) -> FilingDraft | None:
    load_local_env_file()
    building_name = _env("BUILDING_NAME", "Building")
    address = canonicalize_building_address()
    notes = _env("BUILDING_NOTES", "")

    if inc.category != "elevator":
        return None

    subject = _asset_label(inc.asset)
    description = _short_description(inc)

    payload = {
        "incident_id": inc.incident_id,
        "complaint_type": "Elevator or Escalator Complaint",
        "problem": "Not Working or Defective",
        "title": f"{subject.title()} outage at {building_name}",
        "description": description.strip(),
        "building": {
            "name": building_name,
            **address,
        },
        "contact": {
            "name": _env("NYC311_CONTACT_NAME"),
            "phone": _env("NYC311_CONTACT_PHONE"),
            "email": _env("NYC311_CONTACT_EMAIL"),
        },
        "incident": {
            "category": inc.category,
            "asset": inc.asset,
            "severity": int(inc.severity or 0),
            "start_ts": normalize_timestamp(inc.start_ts, fallback=inc.start_ts_epoch),
            "end_ts": normalize_timestamp(inc.end_ts, fallback=inc.end_ts_epoch),
            "proof_refs": inc.proof_refs,
            "witness_count": int(inc.witness_count or 0),
            "report_count": int(inc.report_count or 0),
        },
        "portal_filing_notes": [
            "Open the NYC311 portal elevator complaint flow.",
            "Use Additional Details = Bldg w/ Multiple Devices.",
            "Paste the generated description exactly.",
            "Resolve the building address and submit anonymously.",
            "Capture the service request number and post it back to /mobile/filings/{job_id}/submitted.",
        ],
    }

    return FilingDraft(
        complaint_type="Elevator or Escalator Complaint",
        form_target="elevator_not_working",
        title=f"Auto-file elevator complaint for {subject}",
        description=description.strip(),
        category=inc.category,
        incident_id=inc.incident_id,
        payload=payload,
    )
