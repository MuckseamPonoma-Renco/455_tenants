from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select

from packages.db import Incident, RawMessage, ServiceRequestCase
from packages.timeutil import normalize_timestamp
from packages.whatsapp.attachments import attachment_items


def now_slug() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _export_dir() -> Path:
    return Path(os.environ.get("EXPORT_DIR", "exports"))


def _portal_screenshot_dir() -> Path:
    return Path(os.environ.get("NYC311_PORTAL_SCREENSHOT_DIR", ".local/nyc311_portal"))


def _recent_portal_screenshots(limit: int = 20) -> list[str]:
    screenshot_dir = _portal_screenshot_dir()
    if not screenshot_dir.exists():
        return []
    files = [path for path in screenshot_dir.iterdir() if path.is_file()]
    files.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return [str(path.resolve()) for path in files[:limit]]


def _proof_ids(incident: Incident) -> list[str]:
    return [item for item in (incident.proof_refs or "").split(",") if item.strip()]


def _attachment_paths(raw: RawMessage) -> list[str]:
    paths: list[str] = []
    for item in attachment_items(raw.attachments):
        path = item.get("path")
        if isinstance(path, str) and path.strip():
            paths.append(path.strip())
    return paths


def _incident_case_map(session, incidents: list[Incident]) -> dict[str, list[ServiceRequestCase]]:
    incident_ids = [row.incident_id for row in incidents]
    rows = session.scalars(
        select(ServiceRequestCase).where(ServiceRequestCase.incident_id.in_(incident_ids)).order_by(ServiceRequestCase.submitted_at.asc().nullsfirst())
    ).all() if incident_ids else []
    out: dict[str, list[ServiceRequestCase]] = {}
    for row in rows:
        out.setdefault(row.incident_id or "", []).append(row)
    return out


def _incident_raw_map(session, incidents: list[Incident]) -> dict[str, list[RawMessage]]:
    message_ids: list[str] = []
    proof_by_incident: dict[str, list[str]] = {}
    for incident in incidents:
        proof_ids = _proof_ids(incident)
        proof_by_incident[incident.incident_id] = proof_ids
        message_ids.extend(proof_ids)
    raw_rows = session.scalars(select(RawMessage).where(RawMessage.message_id.in_(message_ids))).all() if message_ids else []
    raw_map = {row.message_id: row for row in raw_rows}
    out: dict[str, list[RawMessage]] = {}
    for incident in incidents:
        rows = [raw_map[mid] for mid in proof_by_incident.get(incident.incident_id, []) if mid in raw_map]
        rows.sort(key=lambda row: (row.ts_epoch if row.ts_epoch is not None else 10**18, row.ts_iso or "", row.message_id))
        out[incident.incident_id] = rows
    return out


def export_elevator_replacement_bundle(session) -> dict:
    export_dir = _export_dir()
    export_dir.mkdir(parents=True, exist_ok=True)
    slug = now_slug()
    bundle_dir = export_dir / f"elevator_replacement_bundle_{slug}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    csv_path = bundle_dir / "incidents.csv"
    md_path = bundle_dir / "bundle.md"
    json_path = bundle_dir / "bundle.json"

    incidents = session.scalars(
        select(Incident)
        .where(Incident.category == "elevator")
        .order_by(Incident.start_ts_epoch.asc().nullsfirst())
    ).all()
    case_map = _incident_case_map(session, incidents)
    raw_map = _incident_raw_map(session, incidents)
    recent_cutoff = int((datetime.now(tz=timezone.utc) - timedelta(days=30)).timestamp())
    portal_screenshots = _recent_portal_screenshots()

    bundle_rows: list[dict] = []
    media_paths: set[str] = set()
    for incident in incidents:
        cases = case_map.get(incident.incident_id, [])
        raw_rows = raw_map.get(incident.incident_id, [])
        message_rows = []
        for raw in raw_rows:
            attachment_paths = _attachment_paths(raw)
            media_paths.update(attachment_paths)
            message_rows.append(
                {
                    "message_id": raw.message_id,
                    "source": raw.source,
                    "sender": raw.sender,
                    "ts_iso": normalize_timestamp(raw.ts_iso, fallback=raw.ts_epoch),
                    "text": raw.text,
                    "attachment_paths": attachment_paths,
                    "attachments": attachment_items(raw.attachments),
                }
            )
        bundle_rows.append(
            {
                "incident_id": incident.incident_id,
                "title": incident.title,
                "asset": incident.asset,
                "status": incident.status,
                "severity": int(incident.severity or 0),
                "report_count": int(incident.report_count or 0),
                "witness_count": int(incident.witness_count or 0),
                "confidence": int(incident.confidence or 0),
                "start_ts": normalize_timestamp(incident.start_ts, fallback=incident.start_ts_epoch),
                "end_ts": normalize_timestamp(incident.end_ts, fallback=incident.end_ts_epoch),
                "updated_at": normalize_timestamp(incident.updated_at),
                "recent_30d": bool(incident.start_ts_epoch and int(incident.start_ts_epoch) >= recent_cutoff),
                "summary": incident.summary,
                "service_requests": [
                    {
                        "service_request_number": case.service_request_number,
                        "status": case.status,
                        "agency": case.agency,
                        "complaint_type": case.complaint_type,
                        "submitted_at": normalize_timestamp(case.submitted_at),
                        "closed_at": normalize_timestamp(case.closed_at),
                        "resolution_description": case.resolution_description,
                    }
                    for case in cases
                ],
                "messages": message_rows,
            }
        )

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "incident_id",
                "title",
                "asset",
                "status",
                "severity",
                "report_count",
                "witness_count",
                "start_ts",
                "end_ts",
                "service_request_numbers",
                "media_paths",
                "summary",
            ]
        )
        for row in bundle_rows:
            writer.writerow(
                [
                    row["incident_id"],
                    row["title"],
                    row["asset"] or "",
                    row["status"],
                    row["severity"],
                    row["report_count"],
                    row["witness_count"],
                    row["start_ts"] or "",
                    row["end_ts"] or "",
                    ", ".join(item["service_request_number"] for item in row["service_requests"]),
                    ", ".join(path for message in row["messages"] for path in message["attachment_paths"]),
                    row["summary"],
                ]
            )

    markdown_lines = [
        "# Elevator replacement pressure bundle",
        "",
        f"- Generated at: {slug}",
        f"- Elevator incidents included: {len(bundle_rows)}",
        f"- Recent 30d elevator incidents: {sum(1 for row in bundle_rows if row['recent_30d'])}",
        f"- Linked 311 cases: {sum(len(row['service_requests']) for row in bundle_rows)}",
        f"- Media evidence files: {len(media_paths)}",
        "",
    ]
    if portal_screenshots:
        markdown_lines.append("## Recent portal screenshots")
        markdown_lines.append("")
        for path in portal_screenshots:
            markdown_lines.append(f"- {path}")
        markdown_lines.append("")

    for row in bundle_rows:
        markdown_lines.append(f"## {row['title']} ({row['incident_id']})")
        markdown_lines.append(f"- Asset: {row['asset'] or 'n/a'}")
        markdown_lines.append(f"- Status: {row['status']}")
        markdown_lines.append(f"- Start: {row['start_ts'] or 'unknown'}")
        markdown_lines.append(f"- End: {row['end_ts'] or 'open'}")
        markdown_lines.append(f"- Reports: {row['report_count']}")
        markdown_lines.append(f"- Witnesses: {row['witness_count']}")
        markdown_lines.append(f"- Summary: {row['summary'] or ''}")
        if row["service_requests"]:
            markdown_lines.append("- 311 cases:")
            for case in row["service_requests"]:
                markdown_lines.append(
                    f"  - {case['service_request_number']} | {case['status']} | {case['agency'] or 'agency unknown'} | submitted {case['submitted_at'] or 'unknown'}"
                )
        else:
            markdown_lines.append("- 311 cases: none linked")
        if row["messages"]:
            markdown_lines.append("- WhatsApp evidence:")
            for message in row["messages"]:
                markdown_lines.append(
                    f"  - {message['ts_iso'] or 'unknown'} | {message['sender'] or 'unknown'} | {message['text'][:200]}"
                )
                for path in message["attachment_paths"]:
                    markdown_lines.append(f"    media: {path}")
        markdown_lines.append("")

    md_path.write_text("\n".join(markdown_lines).strip() + "\n", encoding="utf-8")
    json_path.write_text(
        json.dumps(
            {
                "generated_at": slug,
                "bundle_dir": str(bundle_dir.resolve()),
                "portal_screenshots": portal_screenshots,
                "incidents": bundle_rows,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return {
        "bundle_dir": str(bundle_dir.resolve()),
        "markdown": str(md_path.resolve()),
        "json": str(json_path.resolve()),
        "csv": str(csv_path.resolve()),
        "incidents": len(bundle_rows),
        "cases": sum(len(row["service_requests"]) for row in bundle_rows),
        "media_files": len(media_paths),
        "portal_screenshots": len(portal_screenshots),
    }
