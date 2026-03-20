from html import escape
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from packages.audit import append_audit_event, compute_message_id, sender_hash
from packages.db import RawMessage, get_session
from packages.queue import enqueue_process_message
from packages.timeutil import epoch_to_iso

router = APIRouter()


def _render_form(message: str = "") -> str:
    notice = f"<p style='color:#0a7a0a'>{escape(message)}</p>" if message else ""
    return f"""
<!doctype html>
<html>
<head>
  <meta name='viewport' content='width=device-width, initial-scale=1' />
  <title>455 building report</title>
  <style>
    body {{ font-family: sans-serif; max-width: 560px; margin: 24px auto; padding: 0 16px; }}
    label {{ display:block; margin-top: 14px; font-weight: 600; }}
    select, textarea, input, button {{ width: 100%; padding: 12px; margin-top: 6px; box-sizing: border-box; }}
    button {{ margin-top: 20px; font-weight: 700; }}
    .hint {{ color:#555; font-size: 14px; }}
  </style>
</head>
<body>
  <h2>Report a building issue</h2>
  <p class='hint'>Fastest path: pick the elevator status and add a short note only if useful.</p>
  {notice}
  <form method='post' action='/report/submit'>
    <label>Your name or apartment (optional)</label>
    <input name='reporter' placeholder='Optional' />

    <label>What are you reporting?</label>
    <select name='kind'>
      <option value='elevator_out'>Elevator outage</option>
      <option value='elevator_restore'>Elevator restored</option>
      <option value='heat_hot_water'>Heat / hot water problem</option>
      <option value='security_access'>Door / lock / intercom problem</option>
      <option value='other'>Other building issue</option>
    </select>

    <label>Which elevator?</label>
    <select name='asset'>
      <option value='elevator_both'>Both elevators</option>
      <option value='elevator_north'>North elevator</option>
      <option value='elevator_south'>South elevator</option>
      <option value='unspecified'>Not sure / not elevator</option>
    </select>

    <label>Short note</label>
    <textarea name='note' rows='4' placeholder='Optional details'></textarea>

    <button type='submit'>Submit report</button>
  </form>
</body>
</html>
"""


@router.get('/report', response_class=HTMLResponse)
def report_form():
    return HTMLResponse(_render_form())


@router.post('/report/submit', response_class=HTMLResponse)
def report_submit(
    request: Request,
    reporter: str = Form(default=''),
    kind: str = Form(...),
    asset: str = Form(default='unspecified'),
    note: str = Form(default=''),
):
    client_ip = request.client.host if request.client else ''
    reporter_label = reporter.strip() or 'QR report'
    note_clean = (note or '').strip()

    if kind == 'elevator_out':
        subject = {
            'elevator_both': 'Both elevators are out',
            'elevator_north': 'North elevator is out',
            'elevator_south': 'South elevator is out',
        }.get(asset, 'Elevator is out')
    elif kind == 'elevator_restore':
        subject = {
            'elevator_both': 'Both elevators are working now',
            'elevator_north': 'North elevator is working now',
            'elevator_south': 'South elevator is working now',
        }.get(asset, 'Elevator is working now')
    elif kind == 'heat_hot_water':
        subject = 'Heat or hot water is not working'
    elif kind == 'security_access':
        subject = 'Door / lock / intercom problem'
    else:
        subject = 'Building issue reported'

    text = subject
    if note_clean:
        text += f'. Note: {note_clean}'

    import time
    ts_epoch = int(time.time())
    ts_iso = epoch_to_iso(ts_epoch)
    mid = compute_message_id('455 Report Form', reporter_label, ts_iso or '', text + client_ip)
    with get_session() as session:
        if not session.get(RawMessage, mid):
            session.add(RawMessage(
                message_id=mid,
                chat_name='455 Report Form',
                sender=reporter_label,
                sender_hash=sender_hash(reporter_label + client_ip),
                ts_iso=ts_iso,
                ts_epoch=ts_epoch,
                text=text,
                attachments=None,
                source='report_form',
            ))
            session.commit()
            enqueue_process_message(mid)
            append_audit_event('INGEST_REPORT_FORM', mid, {'kind': kind, 'asset': asset})

    return HTMLResponse(_render_form('Thank you. Your report was captured and sent into the building issue system.'))
