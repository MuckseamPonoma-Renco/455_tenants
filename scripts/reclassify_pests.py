#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import delete, select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file()

from packages.db import FilingJob, Incident, IncidentWitness, MessageDecision, RawMessage, ServiceRequestCase, get_session
from packages.incident.extractor import classify_and_upsert_incident
from packages.worker_jobs import full_resync_sheets


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild all rows currently classified as pests after a classifier fix, then optionally resync Sheets."
    )
    parser.add_argument("--apply", action="store_true", help="Actually mutate the database.")
    parser.add_argument("--resync-sheets", action="store_true", help="Run a full Sheets resync after rebuilding.")
    return parser.parse_args()


def targeted_rows() -> tuple[list[RawMessage], list[str]]:
    with get_session() as session:
        incident_ids = [
            row[0]
            for row in session.execute(select(Incident.incident_id).where(Incident.category == "pests")).all()
        ]
        message_ids = {
            row[0]
            for row in session.execute(select(MessageDecision.message_id).where(MessageDecision.category == "pests")).all()
        }
        if incident_ids:
            message_ids.update(
                row[0]
                for row in session.execute(
                    select(MessageDecision.message_id).where(MessageDecision.incident_id.in_(incident_ids))
                ).all()
            )
        raws = (
            session.scalars(
                select(RawMessage)
                .where(RawMessage.message_id.in_(sorted(message_ids)))
                .order_by(RawMessage.ts_epoch.asc().nullsfirst())
            ).all()
            if message_ids
            else []
        )
    return raws, sorted(set(incident_ids))


def apply_rebuild(raws: list[RawMessage], incident_ids: list[str]) -> None:
    message_ids = [row.message_id for row in raws]
    with get_session() as session:
        if message_ids:
            session.execute(delete(MessageDecision).where(MessageDecision.message_id.in_(message_ids)))
        if incident_ids:
            session.execute(delete(IncidentWitness).where(IncidentWitness.incident_id.in_(incident_ids)))
            session.execute(delete(ServiceRequestCase).where(ServiceRequestCase.incident_id.in_(incident_ids)))
            session.execute(delete(FilingJob).where(FilingJob.incident_id.in_(incident_ids)))
            session.execute(delete(Incident).where(Incident.incident_id.in_(incident_ids)))
        session.commit()

    with get_session() as session:
        for raw in raws:
            db_raw = session.get(RawMessage, raw.message_id)
            if db_raw is None:
                continue
            classify_and_upsert_incident(session, db_raw)
        session.commit()


def main() -> int:
    args = parse_args()
    raws, incident_ids = targeted_rows()
    print(f"targeted_raw_messages={len(raws)}")
    print(f"targeted_incidents={len(incident_ids)}")
    if raws:
        for row in raws[:10]:
            text = (row.text or "").replace("\n", " | ")
            print(f"- {row.message_id} {row.ts_iso or row.ts_epoch or ''} {text[:160]}")
    if not args.apply:
        print("dry_run=1")
        return 0

    apply_rebuild(raws, incident_ids)
    if args.resync_sheets:
        full_resync_sheets()
    print("applied=1")
    print(f"resynced_sheets={1 if args.resync_sheets else 0}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
