#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import select
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
load_dotenv(REPO_ROOT / ".env")

from packages.db import FilingJob, Incident, MessageDecision, RawMessage, ServiceRequestCase, get_session


@dataclass
class CleanupTargets:
    raw_message: RawMessage | None
    decision: MessageDecision | None
    incident: Incident | None
    filing_jobs: list[FilingJob]
    service_requests: list[ServiceRequestCase]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dry-run or delete test artifacts tied to a message, incident, job, or service request."
    )
    parser.add_argument("--message-id")
    parser.add_argument("--incident-id")
    parser.add_argument("--job-id", type=int)
    parser.add_argument("--service-request-number")
    parser.add_argument("--apply", action="store_true", help="Delete the discovered records.")
    return parser.parse_args()


def _dedupe_cases(rows: list[ServiceRequestCase]) -> list[ServiceRequestCase]:
    out: list[ServiceRequestCase] = []
    seen: set[int] = set()
    for row in rows:
        if row.id in seen:
            continue
        seen.add(row.id)
        out.append(row)
    return out


def _collect_targets(args: argparse.Namespace) -> CleanupTargets:
    with get_session() as session:
        raw_message = session.get(RawMessage, args.message_id) if args.message_id else None
        decision = session.get(MessageDecision, args.message_id) if args.message_id else None

        incident_id = args.incident_id or (decision.incident_id if decision else None)
        if args.job_id:
            job = session.get(FilingJob, args.job_id)
            if job and not incident_id:
                incident_id = job.incident_id

        incident = session.get(Incident, incident_id) if incident_id else None

        filing_jobs: list[FilingJob] = []
        if incident_id:
            filing_jobs.extend(session.scalars(select(FilingJob).where(FilingJob.incident_id == incident_id)).all())
        if args.job_id:
            explicit_job = session.get(FilingJob, args.job_id)
            if explicit_job and all(explicit_job.job_id != row.job_id for row in filing_jobs):
                filing_jobs.append(explicit_job)

        service_requests: list[ServiceRequestCase] = []
        if incident_id:
            service_requests.extend(
                session.scalars(select(ServiceRequestCase).where(ServiceRequestCase.incident_id == incident_id)).all()
            )
        if filing_jobs:
            job_ids = [row.job_id for row in filing_jobs]
            service_requests.extend(
                session.scalars(select(ServiceRequestCase).where(ServiceRequestCase.filing_job_id.in_(job_ids))).all()
            )
        if args.service_request_number:
            case = session.scalar(
                select(ServiceRequestCase).where(ServiceRequestCase.service_request_number == args.service_request_number)
            )
            if case:
                service_requests.append(case)

        return CleanupTargets(
            raw_message=raw_message,
            decision=decision,
            incident=incident,
            filing_jobs=filing_jobs,
            service_requests=_dedupe_cases(service_requests),
        )


def _render(targets: CleanupTargets) -> dict:
    return {
        "raw_message": None
        if not targets.raw_message
        else {
            "message_id": targets.raw_message.message_id,
            "sender": targets.raw_message.sender,
            "source": targets.raw_message.source,
            "ts_epoch": targets.raw_message.ts_epoch,
            "text": targets.raw_message.text,
        },
        "decision": None
        if not targets.decision
        else {
            "message_id": targets.decision.message_id,
            "incident_id": targets.decision.incident_id,
            "category": targets.decision.category,
            "event_type": targets.decision.event_type,
        },
        "incident": None
        if not targets.incident
        else {
            "incident_id": targets.incident.incident_id,
            "status": targets.incident.status,
            "title": targets.incident.title,
            "report_count": targets.incident.report_count,
        },
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


def _delete(args: argparse.Namespace) -> None:
    with get_session() as session:
        targets = _collect_targets(args)

        for row in targets.service_requests:
            session.delete(session.get(ServiceRequestCase, row.id))
        session.flush()

        for row in targets.filing_jobs:
            session.delete(session.get(FilingJob, row.job_id))
        session.flush()

        if targets.decision:
            session.delete(session.get(MessageDecision, targets.decision.message_id))
            session.flush()

        if targets.incident:
            session.delete(session.get(Incident, targets.incident.incident_id))
            session.flush()

        if targets.raw_message:
            session.delete(session.get(RawMessage, targets.raw_message.message_id))
            session.flush()

        session.commit()


def main() -> int:
    args = _parse_args()
    targets = _collect_targets(args)
    print(json.dumps(_render(targets), indent=2, sort_keys=True))

    if not args.apply:
        return 0

    _delete(args)
    print("\nDeleted the records above.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
