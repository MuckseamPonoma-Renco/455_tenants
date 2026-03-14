# Deploy (Render + Neon) — cheap and practical

## Recommended first deploy

For this project, the fastest reliable path is:

1. deploy the API as a single Render web service
2. use Neon for Postgres
3. run with `PROCESS_INLINE=1` at first so Redis/worker setup is not required
4. point the Android phone at the Render URL, not your laptop

This repo now includes a Render blueprint at [render.yaml](/media/max/T7/scripts/455-tenants-finalized/render.yaml).

## Neon (Postgres)

1. Create a Neon project.
2. Copy the Postgres connection string.
3. Use that value for `DATABASE_URL` in Render.

## Render deploy

### Option A: deploy from the blueprint in this repo

1. Push this repo to GitHub.
2. In Render, create a new Blueprint deployment from that repo.
3. Render will read [render.yaml](/media/max/T7/scripts/455-tenants-finalized/render.yaml).
4. Set `DATABASE_URL` to your Neon connection string.
5. Deploy.

The blueprint intentionally keeps the first deploy simple:

- `PROCESS_INLINE=1`
- `DISABLE_SHEETS_SYNC=1`
- `LLM_MODE=off`
- the API binds to Render's `PORT` automatically

That is enough to get:

- `GET /health`
- `POST /ingest/tasker`
- `GET /api/incidents`
- `GET /api/queue`
- `GET /api/cases`

### Option B: create the Render web service manually

If you do not use the blueprint, create a Render web service with:

- Runtime: `Docker`
- Dockerfile: `apps/api/Dockerfile`
- Health check path: `/health`

Set these env vars at minimum:

- `DATABASE_URL`
- `INGEST_TOKEN`
- `PROCESS_INLINE=1`
- `DISABLE_SHEETS_SYNC=1`
- `LLM_MODE=off`
- optional: `MOBILE_FILER_TOKEN` if you want Android filing auth separate from ingest auth

## What your public base URL will look like

After the Render web service is created, Render assigns a public URL in this form:

```text
https://SERVICE_NAME.onrender.com
```

If you keep the blueprint's default service name, your URL will usually be:

```text
https://tenant-issue-os-api.onrender.com
```

If that name is unavailable, Render will ask for a different service name. In that case, your real base URL becomes:

```text
https://YOUR_ACTUAL_SERVICE_NAME.onrender.com
```

Use that as the base URL in Tasker:

```text
https://YOUR_ACTUAL_SERVICE_NAME.onrender.com/ingest/tasker
```

## Post-deploy verification

Once Render shows the service as live, verify the deployed API before pointing the phone at it.

1. Confirm health:

```bash
curl https://YOUR_ACTUAL_SERVICE_NAME.onrender.com/health
```

You should get JSON with:

- `"ok": true`
- `"db": "ok"`

2. Run the smoke script against the deployed service:

```bash
BASE_URL=https://YOUR_ACTUAL_SERVICE_NAME.onrender.com \
INGEST_TOKEN=your-render-token \
MOBILE_FILER_TOKEN=your-mobile-token \
python3.11 scripts/smoke_test.py
```

That will hit:

- `GET /health`
- `POST /ingest/tasker`
- `GET /api/queue`
- `POST /mobile/filings/claim_next`

If you did not set `MOBILE_FILER_TOKEN` separately in Render, just reuse `INGEST_TOKEN`.

## Adding more features later

Once the public API is working, you can add:

- `MOBILE_FILER_TOKEN` if you want it separate from `INGEST_TOKEN`
- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `GOOGLE_APPLICATION_CREDENTIALS` plus the `gcp_sa.json` secret file
- `OPENAI_API_KEY` and model settings
- `REDIS_URL` plus a worker service if you want background jobs instead of inline processing

## Android reliability

- In Tasker, enable retries because Render may cold-start after idle periods.
- Optional: add a health ping every 10 to 15 minutes to reduce cold-start latency.
- Keep the Tasker base URL on the Render hostname, not a LAN IP from your laptop.
