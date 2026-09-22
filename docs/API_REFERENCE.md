# API reference

Intake, admin, read API, filing callbacks, and attachment manifests require bearer authentication. The health/report routes and eligible media downloads are public. See [sharing boundaries](OPERATIONS.md#resident-sharing-and-private-data) before exposing a deployment. JSON examples below are fictional.

## POST /ingest/whatsapp_web
Accepts one WhatsApp Web message captured from Chrome/Playwright on the Mac mini.

Payload shape matches the legacy Tasker payload, plus optional `attachments` JSON for downloaded media and reply/context metadata.

## POST /ingest/whatsapp_web_batch
Accepts multiple WhatsApp Web messages in one request.

Wrap single-message payloads in an `items` array: `{"items": [<message>, ...]}`.

Use this for the Chrome/Playwright live watcher. Duplicate messages are deduped against both legacy Android Tasker and Chrome live capture. Captured media can be opened through `/media/whatsapp/{message_id}/{attachment_index}` and is surfaced into Sheets when `PUBLIC_BASE_URL` is configured.

## POST /ingest/tasker
Legacy compatibility endpoint for the retired Android Tasker capture flow.

Accepts one WhatsApp message.

```json
{
  "chat_name": "Example Tenant Chat",
  "text": "Both elevators are out again",
  "sender": "Example Resident",
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
      "chat_name": "Example Tenant Chat",
      "text": "Both elevators are out again",
      "sender": "Example Resident",
      "ts_epoch": 1770000000
    }
  ]
}
```

Use this for replaying a phone-side backlog after downtime. Duplicate messages are ignored safely.

## GET /api/messages/{message_id}/attachments
Returns the parsed attachment manifest for one stored message plus any tenant-openable public media URLs.

## GET /media/whatsapp/{message_id}/{attachment_index}
Public file route for eligible downloaded attachments. It excludes `message_screenshot` attachments and filtered images, and enforces allowed media paths. Content filtering is not a privacy guarantee; inspect eligible attachments before sharing.

## POST /ingest/export
Multipart form upload of a WhatsApp TXT export or ZIP. The shared parser reads every supported text chat file in the archive. Disable live filing and use the isolated environment described in [verification](VERIFY.md) when replaying history.

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

Works without a model API key via deterministic fallback text. Optional model-assisted briefing requires configured API access; generated drafts still require review.

## GET /api/project
Returns the resident-oriented elevator replacement watchdog state split into management claims, trusted official public records, and tenant-observed reality. Weak or conflicting official-record matches are kept internal until verified.

## GET /api/project/records
Returns tenant-visible trusted elevator/replacement-relevant public records. Rows include `machine_verification_status`, `machine_confidence`, and `machine_verified_at`. Machine verification is official-source corroboration, not a human confirmation.

## GET /api/project/actions
Returns tenant-visible replacement-watchdog action queue items. Internal operator/system monitoring is not exposed here.

## GET /api/project/briefing
Returns structured project state, tenant-ready update draft, management follow-up draft, and next best action. Uses deterministic fallback text.

## GET /mobile/filings/{job_id}/preview
Returns the current filing payload and its SHA-256 digest.

## POST /mobile/filings/{job_id}/approve
Legacy/manual compatibility endpoint. It moves an unchanged legacy filing
preview to `approved` when supplied its current `payload_sha256` and the exact
approval phrase `APPROVED — GO LIVE`. Normal filing does not require this call.

## POST /mobile/filings/claim_next
Claims the next current `pending` job or retryable `failed` job. Claiming binds
the exact payload hash; the portal worker independently rebuilds and compares
the current draft before the final submit click.

## POST /mobile/filings/{job_id}/submitted
Stores the SR number and marks the job submitted.

## POST /mobile/filings/{job_id}/failed
Marks a retryable filing job failed and stores the failure reason. Returns HTTP 409 for `submitting`, `submission_unknown`, `submitted`, or `skipped` jobs; reconcile uncertain receipts instead of using this callback to reset them.

## POST /mobile/sr_updates
Stores a status update from the portal worker or another trusted source.

## POST /mobile/sr_updates/sync_now
Runs the 311 case tracker sync immediately.

## POST /admin/sync_public_records
Imports the configured NYC Open Data sources into `PublicRecordWatch`; record verification is tracked separately.

## POST /admin/resync_replacement_watchdog
Imports public records, evaluates replacement-watchdog rules, and syncs watchdog sheet tabs.

## POST /admin/verify_public_record/{record_id}
Marks an imported public record human-verified and completes its verification action.

## POST /admin/add_watchdog_check
Adds a volunteer compliance check, such as permit posting, notice posting, barricade safety, or work-hours posting.

## POST /admin/export_elevator_replacement_bundle
Builds the focused elevator pressure bundle with elevator incidents, WhatsApp evidence, linked 311 cases, and recent portal screenshots.
