# Tenant Issue OS — WhatsApp capture → incident engine → browser 311 filing → Sheets control surface

This repo is the working backend for the 455 Ocean Parkway tenant project.

The core loop is:

1. Android Tasker or Mac Chrome Playwright captures a WhatsApp message.
2. Backend stores the raw message.
3. Backend decides whether it is a real building issue.
4. Backend clusters it into an incident.
5. Backend queues a 311 filing job when eligible.
6. A local Playwright worker claims the job and files NYC311 through the web portal.
7. SR number comes back to the backend.
8. Backend tracks the case and syncs the spreadsheet.

## What is the primary face of the system

The Google Sheet is the primary day-to-day control surface.

It should show:

- Dashboard: current building state and control links
- Incidents: structured issue timeline
- Queue311: what still needs filing
- Cases311: which SRs exist and their status
- DecisionLog: how the message engine decided what each message meant
- Coverage: whether capture is missing days/messages

The API is still useful, but the Sheet is not a side feature. It is the main operator face.

## Core product functions

- WhatsApp capture from Android Tasker
- WhatsApp Web capture from Chrome on the Mac mini via Playwright
- raw message dedupe
- hybrid rules + LLM message classification
- incident clustering
- outage / restore handling
- automatic 311 filing queue for eligible incidents
- Playwright filing worker
- SR number capture from chat or browser worker
- 311 case status tracking
- legal chronology export
- spreadsheet sync for all major state
- optional QR/link report form for tenants (`/report`)

## LLM role

The LLM is not required for transport or persistence.

But it now has a first-class role in the decision engine when enabled:

- `LLM_MODE=assist` → rules handle obvious cases, LLM helps with fuzzy/ambiguous reports
- `LLM_MODE=supervised` → LLM reviews every message and the system logs rule vs LLM vs final choice
- `LLM_MODE=off` → deterministic rules only
- ambiguous or disagreeing rule/LLM outcomes can trigger a stronger review model before the final decision is stored

The final filing queue remains deterministic and still under your control via config.

## What is finished in code

- Real-time `/ingest/tasker` ingestion with Tasker-friendly timestamps
- Real-time `/ingest/tasker_batch` backlog replay for phone-side retry after downtime
- Bulk WhatsApp export ingestion with automatic reprocessing
- Elevator outage / restore clustering with witness counting
- Auto-extraction of SR numbers from chat messages like `311-25815998`
- Auto-queue of eligible incidents into a 311 filing queue
- Filing worker API:
  - `POST /mobile/filings/claim_next`
  - `POST /mobile/filings/{job_id}/submitted`
  - `POST /mobile/filings/{job_id}/failed`
  - `POST /mobile/sr_updates`
- 311 case status sync from the public NYC Open Data endpoint
- Legal bundle export to CSV + Markdown
- Decision log sync so you can audit rules vs LLM vs final result in the spreadsheet
- Simple tenant report form at `/report` for QR/link rollout
- Inline-processing mode for local/dev so Redis is optional

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
- `GOOGLE_APPLICATION_CREDENTIALS`
- building/contact fields used in filing drafts
- optionally `PUBLIC_BASE_URL` so the Dashboard can expose the tenant report form link

For Cloudflare Tunnel + Neon hosting, use the guide in `docs/DEPLOY_CLOUDFLARE_NEON.md`.

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
- `POST /ingest/tasker_batch`
- `POST /ingest/whatsapp_web`
- `POST /ingest/whatsapp_web_batch`
- `POST /ingest/export`
- `GET /report`
- `POST /report/submit`

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
- `GET /api/decisions`
- `GET /api/summary`
- `GET /api/briefing`

### Filing worker API

- `POST /mobile/filings/claim_next`
- `POST /mobile/filings/{job_id}/submitted`
- `POST /mobile/filings/{job_id}/failed`
- `POST /mobile/sr_updates`
- `POST /mobile/sr_updates/sync_now`

## Recommended rollout

1. Initialize and connect the Sheet.
2. Import the WhatsApp export.
3. Review `Dashboard`, `Incidents`, `Queue311`, and `DecisionLog`.
4. Turn on Android capture for WhatsApp notifications.
   Or run the Chrome/Playwright watcher on the Mac mini for the exact tenant chats you want to monitor.
5. Run the Playwright portal worker.
6. Submit one real complaint.
7. Confirm SR appears in `Cases311`.
8. Add a QR or link to `/report` only if the first tenant tests show it is intuitive.

## Files to read next

- `docs/ANDROID_CAPTURE_SETUP.md`
- `docs/WHATSAPP_WEB_CAPTURE_SETUP.md`
- `docs/NYC311_PORTAL_AUTOMATION.md`
- `docs/DEPLOY_CLOUDFLARE_NEON.md`
- `docs/VERIFY.md`
- `docs/API_REFERENCE.md`
