from __future__ import annotations
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from packages.db import Incident
from packages.nyc311.models import FilingDraft

NY = ZoneInfo("America/New_York")


def _env(name: str, default: str = "") -> str:
    value = os.environ.get(name)
    return value if value is not None and value != "" else default


def _fmt_epoch(epoch: int | None) -> str:
    if not epoch:
        return "unknown time"
    return datetime.fromtimestamp(epoch, tz=NY).strftime("%Y-%m-%d %I:%M %p %Z")


def _asset_label(asset: str | None) -> str:
    return {
        "elevator_north": "north elevator",
        "elevator_south": "south elevator",
        "elevator_both": "both elevators",
        None: "building elevator service",
        "": "building elevator service",
    }.get(asset, asset or "building condition")


def build_filing_draft(inc: Incident) -> FilingDraft | None:
    building_name = _env("BUILDING_NAME", "Building")
    address = _env("BUILDING_STREET_ADDRESS", "")
    city = _env("BUILDING_CITY", "Brooklyn")
    state = _env("BUILDING_STATE", "NY")
    zip_code = _env("BUILDING_ZIP", "")
    borough = _env("BUILDING_BOROUGH", "Brooklyn")
    notes = _env("BUILDING_NOTES", "")

    if inc.category != "elevator":
        return None

    subject = _asset_label(inc.asset)
    description = (
        f"{building_name} at {address}, {city}, {state} {zip_code}: tenants report {subject} is not working. "
        f"Incident started around {_fmt_epoch(inc.start_ts_epoch)} and latest evidence is {_fmt_epoch(inc.last_ts_epoch)}. "
        f"Witnesses: {int(inc.witness_count or 0)}. Reports: {int(inc.report_count or 0)}. "
        f"Internal incident id: {inc.incident_id}. "
    )
    if notes:
        description += f"Building notes: {notes}. "
    if inc.summary:
        description += f"Summary: {inc.summary[:400]}"

    payload = {
        "incident_id": inc.incident_id,
        "complaint_type": "Elevator or Escalator Complaint",
        "problem": "Not Working or Defective",
        "title": f"{subject.title()} outage at {building_name}",
        "description": description.strip(),
        "building": {
            "name": building_name,
            "street_address": address,
            "city": city,
            "state": state,
            "zip": zip_code,
            "borough": borough,
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
            "start_ts": inc.start_ts,
            "end_ts": inc.end_ts,
            "proof_refs": inc.proof_refs,
            "witness_count": int(inc.witness_count or 0),
            "report_count": int(inc.report_count or 0),
        },
        "android_filing_notes": [
            "Open NYC311 app or mobile site.",
            "Use the elevator complaint path.",
            "Paste the generated description exactly.",
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
