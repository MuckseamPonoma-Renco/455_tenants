#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import select

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
load_dotenv(REPO_ROOT / ".env")

from packages.db import FilingJob, Incident, IncidentWitness, MessageDecision, RawMessage, ServiceRequestCase, get_session
from packages.timeutil import parse_ts_to_epoch
from packages.whatsapp.web_capture import CaptureStateStore
from packages.worker_jobs import full_resync_sheets


@dataclass
class CleanupTargets:
    raw_messages: list[RawMessage]
    decisions: list[MessageDecision]
    incidents_to_delete: list[Incident]
    incidents_to_repair: list[Incident]
    filing_jobs: list[FilingJob]
    service_requests: list[ServiceRequestCase]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dry-run or delete test artifacts tied to a message, chat/source filter, incident, job, or service request."
    )
    parser.add_argument("--message-id")
    parser.add_argument("--incident-id")
    parser.add_argument("--job-id", type=int)
    parser.add_argument("--service-request-number")
    parser.add_argument("--chat-name")
    parser.add_argument("--source")
    parser.add_argument("--before-ts", help="Only target messages at or before this timestamp.")
    parser.add_argument("--resync-sheets", action="store_true", help="Run the normal sheet resync after deletion.")
    parser.add_argument("--reset-capture-state", action="store_true", help="Clear capture state for the resolved chat name.")
    parser.add_argument("--apply", action="store_true", help="Delete the discovered records.")
    return parser.parse_args()


def _dedupe_rows(rows: list, key: str) -> list:
    out: list = []
    seen: set = set()
    for row in rows:
        row_key = getattr(row, key)
        if row_key in seen:
            continue
        seen.add(row_key)
        out.append(row)
    return out


def _targeted_raw_messages(session, args: argparse.Namespace) -> list[RawMessage]:
    rows: list[RawMessage] = []
    if args.message_id:
        row = session.get(RawMessage, args.message_id)
        if row:
            rows.append(row)

    if args.chat_name or args.source or args.before_ts:
        query = session.query(RawMessage)
        if args.chat_name:
            query = query.filter(RawMessage.chat_name == args.chat_name)
        if args.source:
            query = query.filter(RawMessage.source == args.source)
        if args.before_ts:
            before_epoch = parse_ts_to_epoch(args.before_ts)
            if before_epoch is None:
                raise SystemExit(f"Could not parse --before-ts value: {args.before_ts}")
            query = query.filter(RawMessage.ts_epoch.is_not(None), RawMessage.ts_epoch <= before_epoch)
        rows.extend(query.order_by(RawMessage.ts_epoch.asc().nullsfirst()).all())

    return _dedupe_rows(rows, "message_id")


def _targeted_decisions(session, raw_messages: list[RawMessage], args: argparse.Namespace) -> list[MessageDecision]:
    message_ids = [row.message_id for row in raw_messages]
    rows: list[MessageDecision] = []
    if message_ids:
        rows.extend(session.scalars(select(MessageDecision).where(MessageDecision.message_id.in_(message_ids))).all())
    if args.message_id:
        row = session.get(MessageDecision, args.message_id)
        if row:
            rows.append(row)
    return _dedupe_rows(rows, "message_id")


def _incidents_for_targets(session, decisions: list[MessageDecision], args: argparse.Namespace) -> list[Incident]:
    incident_ids = {row.incident_id for row in decisions if row.incident_id}
    if args.incident_id:
        incident_ids.add(args.incident_id)
    if args.job_id:
        job = session.get(FilingJob, args.job_id)
        if job and job.incident_id:
            incident_ids.add(job.incident_id)
    if args.service_request_number:
        case = session.scalar(
            select(ServiceRequestCase).where(ServiceRequestCase.service_request_number == args.service_request_number)
        )
        if case and case.incident_id:
            incident_ids.add(case.incident_id)
    return session.scalars(select(Incident).where(Incident.incident_id.in_(sorted(incident_ids)))).all() if incident_ids else []


def _jobs_for_incidents(session, incidents_to_delete: list[Incident], args: argparse.Namespace) -> list[FilingJob]:
    rows: list[FilingJob] = []
    if incidents_to_delete:
        incident_ids = [row.incident_id for row in incidents_to_delete]
        rows.extend(session.scalars(select(FilingJob).where(FilingJob.incident_id.in_(incident_ids))).all())
    if args.job_id:
        row = session.get(FilingJob, args.job_id)
        if row:
            rows.append(row)
    return _dedupe_rows(rows, "job_id")


def _cases_for_targets(
    session,
    incidents_to_delete: list[Incident],
    filing_jobs: list[FilingJob],
    args: argparse.Namespace,
) -> list[ServiceRequestCase]:
    rows: list[ServiceRequestCase] = []
    if incidents_to_delete:
        incident_ids = [row.incident_id for row in incidents_to_delete]
        rows.extend(session.scalars(select(ServiceRequestCase).where(ServiceRequestCase.incident_id.in_(incident_ids))).all())
    if filing_jobs:
        job_ids = [row.job_id for row in filing_jobs]
        rows.extend(session.scalars(select(ServiceRequestCase).where(ServiceRequestCase.filing_job_id.in_(job_ids))).all())
    if args.service_request_number:
        row = session.scalar(
            select(ServiceRequestCase).where(ServiceRequestCase.service_request_number == args.service_request_number)
        )
        if row:
            rows.append(row)
    return _dedupe_rows(rows, "id")


def _split_incidents_for_cleanup(session, incidents: list[Incident], decisions: list[MessageDecision]) -> tuple[list[Incident], list[Incident]]:
    targeted_counts: dict[str, int] = {}
    for row in decisions:
        if row.incident_id:
            targeted_counts[row.incident_id] = targeted_counts.get(row.incident_id, 0) + 1

    delete_rows: list[Incident] = []
    repair_rows: list[Incident] = []
    for incident in incidents:
        total = session.query(MessageDecision).filter(MessageDecision.incident_id == incident.incident_id).count()
        remaining = max(0, int(total) - int(targeted_counts.get(incident.incident_id, 0)))
        if remaining == 0:
            delete_rows.append(incident)
        else:
            repair_rows.append(incident)
    return delete_rows, repair_rows


def collect_targets(args: argparse.Namespace) -> CleanupTargets:
    with get_session() as session:
        raw_messages = _targeted_raw_messages(session, args)
        decisions = _targeted_decisions(session, raw_messages, args)
        incidents = _incidents_for_targets(session, decisions, args)
        incidents_to_delete, incidents_to_repair = _split_incidents_for_cleanup(session, incidents, decisions)
        filing_jobs = _jobs_for_incidents(session, incidents_to_delete, args)
        service_requests = _cases_for_targets(session, incidents_to_delete, filing_jobs, args)
        return CleanupTargets(
            raw_messages=raw_messages,
            decisions=decisions,
            incidents_to_delete=incidents_to_delete,
            incidents_to_repair=incidents_to_repair,
            filing_jobs=filing_jobs,
            service_requests=service_requests,
        )


def render_targets(targets: CleanupTargets) -> dict:
    return {
        "counts": {
            "raw_messages": len(targets.raw_messages),
            "decisions": len(targets.decisions),
            "incidents_to_delete": len(targets.incidents_to_delete),
            "incidents_to_repair": len(targets.incidents_to_repair),
            "filing_jobs": len(targets.filing_jobs),
            "service_requests": len(targets.service_requests),
        },
        "raw_messages": [
            {
                "message_id": row.message_id,
                "chat_name": row.chat_name,
                "sender": row.sender,
                "source": row.source,
                "ts_epoch": row.ts_epoch,
                "text": row.text[:200],
            }
            for row in targets.raw_messages[:25]
        ],
        "decisions": [
            {
                "message_id": row.message_id,
                "incident_id": row.incident_id,
                "category": row.category,
                "event_type": row.event_type,
                "is_issue": row.is_issue,
            }
            for row in targets.decisions[:25]
        ],
        "incidents_to_delete": [
            {
                "incident_id": row.incident_id,
                "status": row.status,
                "title": row.title,
                "report_count": row.report_count,
            }
            for row in targets.incidents_to_delete
        ],
        "incidents_to_repair": [
            {
                "incident_id": row.incident_id,
                "status": row.status,
                "title": row.title,
                "report_count": row.report_count,
            }
            for row in targets.incidents_to_repair
        ],
        "filing_jobs": [
            {
                "job_id": row.job_id,
                "incident_id": row.incident_id,
                "state": row.state,
                "priority": row.priority,
                "dedupe_key": row.dedupe_key,
            }
            for row in targets.filing_jobs
        ],
        "service_requests": [
            {
                "id": row.id,
                "service_request_number": row.service_request_number,
                "incident_id": row.incident_id,
                "filing_job_id": row.filing_job_id,
                "source": row.source,
                "status": row.status,
            }
            for row in targets.service_requests
        ],
    }


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _recompute_incident(session, incident_id: str) -> None:
    incident = session.get(Incident, incident_id)
    if incident is None:
        return

    remaining_decisions = session.scalars(
        select(MessageDecision).where(MessageDecision.incident_id == incident_id)
    ).all()
    if not remaining_decisions:
        session.delete(incident)
        session.flush()
        return

    raw_rows = session.scalars(
        select(RawMessage).where(RawMessage.message_id.in_([row.message_id for row in remaining_decisions]))
    ).all()
    raw_rows.sort(key=lambda row: (row.ts_epoch if row.ts_epoch is not None else 10**18, row.ts_iso or "", row.message_id))
    incident.report_count = len(remaining_decisions)
    incident.proof_refs = ",".join([row.message_id for row in raw_rows[-3:]])
    incident.start_ts_epoch = raw_rows[0].ts_epoch
    incident.start_ts = raw_rows[0].ts_iso
    incident.last_ts_epoch = raw_rows[-1].ts_epoch
    incident.updated_at = _now_iso()
    incident.witness_count = len({row.sender_hash for row in raw_rows if row.sender_hash})

    session.query(IncidentWitness).filter(IncidentWitness.incident_id == incident_id).delete()
    for sender_hash in sorted({row.sender_hash for row in raw_rows if row.sender_hash}):
        session.add(IncidentWitness(incident_id=incident_id, sender_hash=sender_hash))
    session.flush()


def _service_pid_file(name: str) -> Path:
    return Path.home() / ".local" / "var" / "run" / "tenant-issue-os" / f"{name}.pid"


def _service_plist_path(name: str) -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"tenant-issue-os.{name}.plist"


def _service_target(name: str) -> str:
    return f"gui/{os.getuid()}/tenant-issue-os.{name}"


def _is_launchd_loaded(name: str) -> bool:
    return subprocess.run(
        ["launchctl", "print", _service_target(name)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0


def _stop_service(name: str) -> dict[str, bool]:
    was_launchd_loaded = _is_launchd_loaded(name)
    pid_file = _service_pid_file(name)
    pid = None
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text(encoding="utf-8").strip())
        except Exception:
            pid = None

    was_running = False
    if pid:
        try:
            os.kill(pid, 0)
            was_running = True
        except OSError:
            was_running = False

    if was_launchd_loaded:
        subprocess.run(["launchctl", "bootout", _service_target(name)], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if pid:
        try:
            os.kill(pid, signal.SIGTERM)
            was_running = True
        except OSError:
            pass
        time_limit = time.time() + 5
        while time.time() < time_limit:
            try:
                os.kill(pid, 0)
                time.sleep(0.2)
            except OSError:
                break
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass

    if pid_file.exists():
        pid_file.unlink(missing_ok=True)

    return {"was_running": was_running or was_launchd_loaded, "was_launchd_loaded": was_launchd_loaded}


def _restart_service(name: str, prior_state: dict[str, bool]) -> None:
    if not prior_state.get("was_running"):
        return
    if prior_state.get("was_launchd_loaded") and _service_plist_path(name).exists():
        subprocess.run(["launchctl", "bootstrap", f"gui/{os.getuid()}", str(_service_plist_path(name))], check=False)
        subprocess.run(["launchctl", "enable", _service_target(name)], check=False)
        subprocess.run(["launchctl", "kickstart", "-k", _service_target(name)], check=False)
        if _is_launchd_loaded(name):
            return
    subprocess.run([str(REPO_ROOT / "scripts" / "start_mac_services.sh"), name], cwd=str(REPO_ROOT), check=False)


def _reset_capture_state(chat_name: str) -> bool:
    configured = os.environ.get("WHATSAPP_CAPTURE_STATE_PATH", "")
    path = Path(configured).expanduser() if configured else (REPO_ROOT / ".local/share/tenant-issue-os/whatsapp_capture/state.json")
    state = CaptureStateStore(path)
    return state.reset_chat(chat_name)


def apply_cleanup(args: argparse.Namespace) -> dict[str, object]:
    service_state: dict[str, bool] | None = None
    if args.reset_capture_state:
        if not args.chat_name:
            raise SystemExit("--reset-capture-state requires --chat-name")
        service_state = _stop_service("whatsapp_capture")

    with get_session() as session:
        targets = collect_targets(args)
        target_message_ids = [row.message_id for row in targets.raw_messages]
        repaired_incident_ids = [row.incident_id for row in targets.incidents_to_repair]

        for row in targets.service_requests:
            db_row = session.get(ServiceRequestCase, row.id)
            if db_row is not None:
                session.delete(db_row)
        session.flush()

        for row in targets.filing_jobs:
            db_row = session.get(FilingJob, row.job_id)
            if db_row is not None:
                session.delete(db_row)
        session.flush()

        for row in targets.decisions:
            db_row = session.get(MessageDecision, row.message_id)
            if db_row is not None:
                session.delete(db_row)
        session.flush()

        for row in targets.raw_messages:
            db_row = session.get(RawMessage, row.message_id)
            if db_row is not None:
                session.delete(db_row)
        session.flush()

        for row in targets.incidents_to_delete:
            db_row = session.get(Incident, row.incident_id)
            if db_row is not None:
                session.delete(db_row)
        session.flush()

        for incident_id in repaired_incident_ids:
            _recompute_incident(session, incident_id)

        session.commit()

    state_reset = False
    if args.reset_capture_state and args.chat_name:
        state_reset = _reset_capture_state(args.chat_name)
    if args.resync_sheets:
        full_resync_sheets()
    if service_state is not None:
        _restart_service("whatsapp_capture", service_state)

    return {
        "deleted_message_ids": target_message_ids,
        "state_reset": state_reset,
        "resynced_sheets": bool(args.resync_sheets),
    }


def main() -> int:
    args = _parse_args()
    targets = collect_targets(args)
    print(json.dumps(render_targets(targets), indent=2, sort_keys=True))

    if not args.apply:
        return 0

    result = apply_cleanup(args)
    print("\nApplied cleanup:")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
