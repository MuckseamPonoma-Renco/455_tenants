# API reference

## POST /ingest/whatsapp_web
Accepts one WhatsApp Web message captured from Chrome/Playwright on the Mac mini.

Payload shape matches the legacy Tasker payload, plus optional `attachments` JSON for downloaded media and reply/context metadata.

## POST /ingest/whatsapp_web_batch
Accepts multiple WhatsApp Web messages in one request.

Payload shape is the same as `/ingest/whatsapp_web`.

Use this for the Chrome/Playwright live watcher. Duplicate messages are deduped against both legacy Android Tasker and Chrome live capture. Captured media can be opened through `/media/whatsapp/{message_id}/{attachment_index}` and is surfaced into Sheets when `PUBLIC_BASE_URL` is configured.

## POST /ingest/tasker
Legacy compatibility endpoint for the retired Android Tasker capture flow.

Accepts one WhatsApp message.

```json
{
  "chat_name": "455 Tenants",
  "text": "Both elevators are out again",
  "sender": "Tibor Simon",
  "ts_epoch": 1770000000
}
```

`ts_epoch` is accepted in seconds or milliseconds and is normalized to ISO time internally.

## POST /ingest/tasker_batch
Legacy compatibility endpoint for replaying multiple Android Tasker notification messages in one request.

Accepts multiple WhatsApp notification messages in one request.

```json
{
  "items": [
    {
      "chat_name": "455 Tenants",
      "text": "Both elevators are out again",
      "sender": "Tibor Simon",
      "ts_epoch": 1770000000
    }
  ]
}
```

Use this for replaying a phone-side backlog after downtime. Duplicate messages are ignored safely.

## GET /api/messages/{message_id}/attachments
Returns the parsed attachment manifest for one stored message plus any tenant-openable public media URLs.

## GET /media/whatsapp/{message_id}/{attachment_index}
Public file route used by the spreadsheet for captured WhatsApp screenshots and downloaded media.

## POST /ingest/export
Multipart form upload of TXT or ZIP containing `_chat.txt`.

## GET /api/incidents
Returns classified incidents.

## GET /api/queue
Returns filing queue jobs.

## GET /api/cases
Returns stored 311 service requests.

## GET /api/summary
Returns the current operating stage, key metrics, alerts, and the next best action.

Useful for answering: “What stage is the project at right now?”

## GET /api/briefing
Returns:
- the full structured summary
- a tenant-ready update draft
- a management-ready follow-up draft
- the current next-best action

Works without an API key via deterministic fallback text, and improves automatically once `OPENAI_API_KEY` is set.

## POST /mobile/filings/claim_next
Claims the next pending job.

## POST /mobile/filings/{job_id}/submitted
Stores the SR number and marks the job submitted.

## POST /mobile/filings/{job_id}/failed
Marks a filing job failed and stores the failure reason.

## POST /mobile/sr_updates
Stores a status update from the portal worker or another trusted source.

## POST /mobile/sr_updates/sync_now
Runs the 311 case tracker sync immediately.

## POST /admin/export_elevator_replacement_bundle
Builds the focused elevator pressure bundle with elevator incidents, WhatsApp evidence, linked 311 cases, and recent portal screenshots.
