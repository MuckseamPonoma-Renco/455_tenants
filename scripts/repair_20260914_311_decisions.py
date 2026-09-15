"""Guarded correction of two current outage decisions; never files a complaint.

Default is read-only. --apply writes a private pre-change snapshot before making
the exact corrections. Original source/model evidence and downstream jobs stay.
"""
import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from packages.local_env import load_local_env_file
load_local_env_file()
from packages.db import (get_session, RawMessage, MessageDecision, Incident,
                         IncidentWitness, FilingJob, ServiceRequestCase)

REPAIR = '20260914-311-context-v1'
INCIDENT = '5ddb46ecba7728f7dd4191c578a4e42b'
TARGETS = (
    ('52a3339d032e906fc66de1284dda3b3cba58ce4be183d997b8b6257c706bcecd',
     'only one working at 2:20', '2026-09-14T18:20:00Z', None),
    ('1b2c4a8d4ca64c89e8d48d21b0546f70b32e696add372352e407395d9cb59635',
     'yeah. south lift is out', '2026-09-14T18:38:00Z', 'elevator_south'),
)


def snapshot(row):
    return {col.name: getattr(row, col.name) for col in row.__table__.columns}


def repair(session, *, apply=False, backup=None):
    inc = session.query(Incident).filter_by(incident_id=INCIDENT).with_for_update().one()
    rows = []
    for mid, text, stamp, asset in TARGETS:
        raw = session.query(RawMessage).filter_by(message_id=mid).with_for_update().one_or_none()
        decision = session.query(MessageDecision).filter_by(message_id=mid).with_for_update().one()
        if not raw or hashlib.sha256(raw.text.encode()).hexdigest() != hashlib.sha256(text.encode()).hexdigest() or raw.ts_iso != stamp:
            raise ValueError('Source identity changed; review required')
        final = json.loads(decision.final_json or '{}')
        if not isinstance(final, dict):
            raise ValueError('Malformed decision requires review')
        if final.get('repair_311_context', {}).get('id') == REPAIR:
            if decision.event_type != 'outage' or decision.incident_id != INCIDENT:
                raise ValueError('Previously repaired decision changed')
            continue
        if not decision.is_issue or decision.incident_id != INCIDENT or decision.event_type != 'status_update' or decision.chosen_source != 'rules_context':
            raise ValueError('Decision starting state changed')
        rows.append((decision, final, asset))
    if not rows:
        return {'ok': True, 'decision_updates': 0, 'applied': False}
    if inc.status != 'open' or inc.asset is not None or inc.report_count != 3:
        raise ValueError('Incident changed; review current condition before correcting')
    if session.query(FilingJob).filter_by(incident_id=INCIDENT).count() or session.query(ServiceRequestCase).filter_by(incident_id=INCIDENT).count():
        raise ValueError('An existing filing/receipt needs reconciliation first')
    result = {'ok': True, 'decision_updates': len(rows), 'incident_updates': 1, 'applied': False}
    if not apply:
        return result
    if not backup:
        raise ValueError('Private backup path required')
    target = Path(backup)
    if target.parent.stat().st_mode & 0o077:
        raise ValueError('Backup directory must be private')
    tables = {model.__tablename__: [snapshot(row) for row in session.query(model).all()]
              for model in (RawMessage, MessageDecision, Incident, IncidentWitness, FilingJob, ServiceRequestCase)}
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w') as handle:
        json.dump({'repair': REPAIR, 'tables': tables}, handle, ensure_ascii=False)
        handle.flush()
        os.fsync(handle.fileno())
    stamp = datetime.now(timezone.utc).isoformat()
    for decision, final, asset in rows:
        final['repair_311_context'] = {'id': REPAIR, 'at': stamp, 'prior_decision': snapshot(decision),
                                     'reason': 'Explicit outage was downgraded by contextual update handling'}
        final.update(event_type='outage', asset=asset, close_incident=False)
        decision.event_type = 'outage'
        decision.final_json = json.dumps(final, ensure_ascii=False)
        decision.auto_file_candidate = True
    inc.asset = 'elevator_south'
    inc.title = 'South elevator outage'
    inc.summary = 'Reports described reduced elevator service and identified the south elevator as out. Mechanic presence was later reported, not a confirmed restoration.'
    inc.updated_at = stamp
    session.flush()
    result['applied'] = True
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--backup')
    args = parser.parse_args()
    with get_session() as session:
        result = repair(session, apply=args.apply, backup=args.backup)
        session.commit() if result.get('applied') else session.rollback()
        print(json.dumps(result))
