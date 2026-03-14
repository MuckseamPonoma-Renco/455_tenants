# API reference

## POST /ingest/tasker
Accepts one WhatsApp message.

```json
{
  "chat_name": "455 Tenants",
  "text": "Both elevators are out again",
  "sender": "Tibor Simon",
  "ts_epoch": 1770000000
}
```

## POST /ingest/export
Multipart form upload of TXT or ZIP containing `_chat.txt`.

## GET /api/incidents
Returns classified incidents.

## GET /api/queue
Returns filing queue jobs.

## GET /api/cases
Returns stored 311 service requests.

## POST /mobile/filings/claim_next
Claims the next pending job.

## POST /mobile/filings/{job_id}/submitted
Stores the SR number and marks the job submitted.

## POST /mobile/sr_updates
Stores a status update from Android or another trusted source.
