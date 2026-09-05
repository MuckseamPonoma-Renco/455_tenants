from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file(ROOT / ".env")

from packages.whatsapp.export import parse_export_path
from packages.db import get_session, RawMessage
from packages.audit import sender_hash
from packages.tasker_capture import LIVE_CAPTURE_SOURCES, find_recent_cross_source_duplicate, find_recent_duplicate
from packages.timeutil import parse_ts_to_epoch
from packages.whatsapp.identity import iter_physical_message_identities
from packages.queue import enqueue_full_resync, enqueue_process_message


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--zip', required=True, help='WhatsApp export ZIP (contains _chat.txt)')
    ap.add_argument(
        '--llm-mode',
        default='all',
        choices=('off', 'uncertain', 'assist', 'all', 'supervised'),
        help='LLM_MODE to use while processing imported messages. Default: all.',
    )
    args = ap.parse_args()

    print(f"Importing {Path(args.zip).name}...", flush=True)
    export = parse_export_path(args.zip)
    parsed = export.messages

    inserted = 0
    deduped = 0
    queued = 0
    seen_mids: set[str] = set()
    mids_to_queue: list[str] = []
    with get_session() as s:
        for identity in iter_physical_message_identities(parsed):
            m = identity.message
            ts_epoch = parse_ts_to_epoch(m.ts_iso)
            mid = identity.message_id
            if mid in seen_mids or s.get(RawMessage, mid):
                deduped += 1
                continue
            duplicate = None
            if identity.occurrence == 1:
                duplicate = find_recent_duplicate(
                    s,
                    chat_name=m.chat_name,
                    sender=m.sender,
                    text=m.text,
                    ts_epoch=ts_epoch,
                    require_chat_match=False,
                )
                if duplicate is None:
                    duplicate = find_recent_cross_source_duplicate(
                        s,
                        text=m.text,
                        ts_epoch=ts_epoch,
                        sources=LIVE_CAPTURE_SOURCES,
                    )
            if duplicate:
                deduped += 1
                continue
            seen_mids.add(mid)
            s.add(RawMessage(
                message_id=mid,
                chat_name=m.chat_name,
                sender=m.sender,
                sender_hash=sender_hash(m.sender),
                ts_iso=m.ts_iso,
                ts_epoch=ts_epoch,
                text=m.text,
                attachments=m.attachments,
                source='zip_import',
            ))
            inserted += 1
            mids_to_queue.append(mid)
        s.commit()

    if args.llm_mode:
        os.environ["LLM_MODE"] = args.llm_mode

    for mid in mids_to_queue:
        enqueue_process_message(mid, sync_sheets=False)
        queued += 1
    if mids_to_queue:
        enqueue_full_resync()

    print({
        'parsed': len(parsed),
        'inserted': inserted,
        'deduped': deduped,
        'queued': queued,
        'chat_files': export.chat_files,
    })


if __name__ == '__main__':
    main()
