from __future__ import annotations

import argparse
import zipfile
from packages.whatsapp.parser import parse_export_text
from packages.db import get_session, RawMessage
from packages.audit import compute_message_id, sender_hash
from packages.timeutil import parse_ts_to_epoch
from packages.queue import enqueue_process_message


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

    content = z.read(txt).decode('utf-8', errors='replace')
    parsed = parse_export_text(content)

    inserted = 0
    queued = 0
    seen_mids: set[str] = set()
    mids_to_queue: list[str] = []
    with get_session() as s:
        for m in parsed:
            mid = compute_message_id(m.chat_name, m.sender, m.ts_iso or '', m.text)
            if mid in seen_mids or s.get(RawMessage, mid):
                continue
            seen_mids.add(mid)
            s.add(RawMessage(
                message_id=mid,
                chat_name=m.chat_name,
                sender=m.sender,
                sender_hash=sender_hash(m.sender),
                ts_iso=m.ts_iso,
                ts_epoch=parse_ts_to_epoch(m.ts_iso),
                text=m.text,
                attachments=m.attachments,
                source='zip_import',
            ))
            inserted += 1
            mids_to_queue.append(mid)
        s.commit()

    for mid in mids_to_queue:
        enqueue_process_message(mid)
        queued += 1

    print({'parsed': len(parsed), 'inserted': inserted, 'queued': queued, 'chat_file': txt})


if __name__ == '__main__':
    main()
