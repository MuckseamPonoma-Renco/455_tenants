# Deploy (Render + Neon) — cheap and practical

## Neon (Postgres)
1) Create a Neon project and get a Postgres connection string.
2) Put it in Render as `DATABASE_URL`.

## Render topology

Use three Render services together:

1) `tenant-issue-os-api` as a Docker web service
2) `tenant-issue-os-worker` as a Docker background worker
3) `tenant-issue-os-redis` as a Render Key Value instance

The checked-in [render.yaml](/media/max/T7/scripts/455-tenants-finalized-v6/455-tenants-finalized/render.yaml) now reflects that topology.

This is the recommended steady-state deployment. Inline mode is okay for local/dev, but it is a poor fit for large backfills because the web process blocks while it classifies messages and syncs Sheets.

## Required env vars

Set these on both the web service and the worker unless noted otherwise:

- `DATABASE_URL` (your Neon connection string)
- `REDIS_URL` (provided by the Render Key Value instance)
- `INGEST_TOKEN` (web only)
- `MOBILE_FILER_TOKEN` (web only; optional but recommended)
- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `OPENAI_API_KEY` if you want `LLM_MODE=assist`
- `OPENAI_MODEL`, `OPENAI_ESCALATE_MODEL`, `LLM_MODE`
- building / filing fields from `.env.example` if you want the 311 filing loop fully live

Recommended values:

- `PROCESS_INLINE=0`
- `DISABLE_SHEETS_SYNC=0`
- `LLM_MODE=assist`

## Google Sheets credentials

Add the service-account JSON as a Render secret file named `gcp_sa.json`.

For Docker services, Render mounts secret files at `/etc/secrets/<filename>` at runtime, so either:

- set `GOOGLE_APPLICATION_CREDENTIALS=/etc/secrets/gcp_sa.json`, or
- rely on the app's built-in fallback to that path

## What "healthy" should look like

After deploy, `GET /health` on the web service should report:

- `process_inline: false`
- `sheets_disabled: false`
- `database_configured: true`
- `redis_configured: true`
- `sheets_configured: true`

## Android reliability
- In Tasker, enable retries (important for cold starts).
- Optional: add a health ping every ~10 minutes to reduce cold-start latency.
