from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.local_env import load_local_env_file

load_local_env_file(ROOT / ".env")

from packages.whatsapp.parser import parse_export_text
from packages.db import get_session, RawMessage
from packages.audit import compute_message_id, sender_hash
from packages.tasker_capture import find_recent_duplicate
from packages.timeutil import parse_ts_to_epoch
from packages.queue import enqueue_full_resync, enqueue_process_message


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--zip', required=True, help='WhatsApp export ZIP (contains _chat.txt)')
    args = ap.parse_args()

    z = zipfile.ZipFile(args.zip)
    txt = '_chat.txt' if '_chat.txt' in z.namelist() else None
    if not txt:
        for n in z.namelist():
            if n.lower().endswith('.txt'):
                txt = n
                break
    if not txt:
        raise SystemExit('No .txt chat file found in ZIP')

    print(f"Importing {Path(args.zip).name}...", flush=True)
    content = z.read(txt).decode('utf-8', errors='replace')
    parsed = parse_export_text(content)

    inserted = 0
    deduped = 0
    queued = 0
    seen_mids: set[str] = set()
    mids_to_queue: list[str] = []
    with get_session() as s:
        for m in parsed:
            ts_epoch = parse_ts_to_epoch(m.ts_iso)
            mid = compute_message_id(m.chat_name, m.sender, m.ts_iso or '', m.text)
            if mid in seen_mids or s.get(RawMessage, mid):
                deduped += 1
                continue
            duplicate = find_recent_duplicate(
                s,
                chat_name=m.chat_name,
                sender=m.sender,
                text=m.text,
                ts_epoch=ts_epoch,
                require_chat_match=False,
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

    for mid in mids_to_queue:
        enqueue_process_message(mid, sync_sheets=False)
        queued += 1
    if mids_to_queue:
        enqueue_full_resync()

    print({'parsed': len(parsed), 'inserted': inserted, 'deduped': deduped, 'queued': queued, 'chat_file': txt})


if __name__ == '__main__':
    main()
