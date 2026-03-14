# Tenant Issue OS — Android capture → incident detection → automatic 311 queue → case tracking

This repo now covers the full server-side loop for the 455 Ocean Parkway tenant project:

- WhatsApp capture from an Android companion phone
- message dedupe + incident detection
- automatic 311 filing queue for elevator outages
- Android filing worker handshake
- service request storage and status tracking
- Google Sheets sync for incidents, queue, cases, and dashboard
- legal-ready chronology export

## What is finished in code

- Real-time `/ingest/tasker` ingestion with Tasker-friendly timestamps
- Bulk WhatsApp export ingestion with automatic reprocessing
- Elevator outage / restore clustering with witness counting
- Auto-extraction of SR numbers from chat messages like `311-25815998`
- Auto-queue of eligible elevator incidents into a 311 filing queue
- Mobile worker API:
  - `POST /mobile/filings/claim_next`
  - `POST /mobile/filings/{job_id}/submitted`
  - `POST /mobile/filings/{job_id}/failed`
  - `POST /mobile/sr_updates`
- 311 case status sync from the public NYC Open Data endpoint
- Legal bundle export to CSV + Markdown
- Docker build fixed so the API and worker actually include `packages/`
- Inline-processing mode for local/dev so Redis is optional

## Important constraint

This repo does **not** implement CAPTCHA solving or CAPTCHA bypass. The intended automatic filing path is:

1. Android receives filing job from the API.
2. Android automates the NYC311 app or mobile web flow locally.
3. Android posts the resulting SR number back to the API.
4. The API tracks that SR number over time.

That avoids building the project around any CAPTCHA-avoidance service.

## Quick start

### 1. Configure env

```bash
cp .env.example .env
```

Fill in:

- `INGEST_TOKEN`
- `MOBILE_FILER_TOKEN` (optional; defaults to `INGEST_TOKEN`)
- `DATABASE_URL`
- `GOOGLE_SHEETS_SPREADSHEET_ID`
- building/contact fields used in filing drafts

### 2. Local dev without Redis worker

```bash
export PROCESS_INLINE=1
export DISABLE_SHEETS_SYNC=1
uvicorn apps.api.main:app --reload
```

### 3. Full docker run

```bash
docker compose up --build
```

## Main endpoints

### Ingest

- `GET /health`
- `POST /ingest/tasker`
- `POST /ingest/export`

### Admin

- `POST /admin/reprocess_last/{n}`
- `POST /admin/resync_sheets`
- `POST /admin/queue_311_jobs`
- `POST /admin/sync_311_statuses`
- `POST /admin/export_legal_bundle`

### Read API

- `GET /api/incidents`
- `GET /api/queue`
- `GET /api/cases`

### Android filing worker API

- `POST /mobile/filings/claim_next`
- `POST /mobile/filings/{job_id}/submitted`
- `POST /mobile/filings/{job_id}/failed`
- `POST /mobile/sr_updates`
- `POST /mobile/sr_updates/sync_now`

## Files to read next

- `docs/ANDROID_CAPTURE_SETUP.md`
- `docs/ANDROID_FILER_SETUP.md`
- `docs/VERIFY.md`
- `docs/API_REFERENCE.md`

## Recommended initial rollout

1. Run bulk import on the WhatsApp export.
2. Review `/api/incidents` and `/api/queue`.
3. Configure Android capture for WhatsApp notifications.
4. Configure Android filing flow for queued elevator jobs.
5. Submit one real test complaint.
6. Confirm the SR number lands in `/api/cases`.
7. Run status sync the next day.
