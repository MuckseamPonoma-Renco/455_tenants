from __future__ import annotations
import csv
import os
from pathlib import Path
from datetime import datetime, timezone
from sqlalchemy import select
from packages.db import Incident, ServiceRequestCase
from packages.timeutil import normalize_timestamp


EXPORT_DIR = Path(os.environ.get("EXPORT_DIR", "exports"))


def now_slug() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def export_legal_bundle(session) -> dict:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    slug = now_slug()
    csv_path = EXPORT_DIR / f"tenant_case_bundle_{slug}.csv"
    md_path = EXPORT_DIR / f"tenant_case_bundle_{slug}.md"

    incidents = session.scalars(select(Incident).order_by(Incident.start_ts_epoch.asc().nullsfirst())).all()
    cases = session.scalars(select(ServiceRequestCase).order_by(ServiceRequestCase.submitted_at.asc().nullsfirst())).all()
    cases_by_incident = {}
    for case in cases:
        cases_by_incident.setdefault(case.incident_id or "", []).append(case)

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "incident_id", "category", "asset", "status", "start_ts", "end_ts", "witness_count",
            "report_count", "service_request_numbers", "title", "summary",
        ])
        for inc in incidents:
            sr_numbers = ", ".join(case.service_request_number for case in cases_by_incident.get(inc.incident_id, []))
            start_ts = normalize_timestamp(inc.start_ts, fallback=inc.start_ts_epoch) or ""
            end_ts = normalize_timestamp(inc.end_ts, fallback=inc.end_ts_epoch) or ""
            writer.writerow([
                inc.incident_id, inc.category, inc.asset or "", inc.status, start_ts, end_ts,
                int(inc.witness_count or 0), int(inc.report_count or 0), sr_numbers, inc.title, inc.summary,
            ])

    with md_path.open("w", encoding="utf-8") as f:
        f.write("# Tenant issue chronology\n\n")
        for inc in incidents:
            start_ts = normalize_timestamp(inc.start_ts, fallback=inc.start_ts_epoch) or "unknown"
            end_ts = normalize_timestamp(inc.end_ts, fallback=inc.end_ts_epoch) or "open"
            f.write(f"## {inc.title} ({inc.incident_id})\n")
            f.write(f"- Category: {inc.category}\n")
            f.write(f"- Asset: {inc.asset or 'n/a'}\n")
            f.write(f"- Status: {inc.status}\n")
            f.write(f"- Start: {start_ts}\n")
            f.write(f"- End: {end_ts}\n")
            f.write(f"- Witnesses: {int(inc.witness_count or 0)}\n")
            f.write(f"- Reports: {int(inc.report_count or 0)}\n")
            sr_numbers = [case.service_request_number for case in cases_by_incident.get(inc.incident_id, [])]
            f.write(f"- 311 cases: {', '.join(sr_numbers) if sr_numbers else 'none linked'}\n")
            f.write(f"- Summary: {inc.summary or ''}\n\n")

    return {"csv": str(csv_path), "markdown": str(md_path), "incidents": len(incidents), "cases": len(cases)}
