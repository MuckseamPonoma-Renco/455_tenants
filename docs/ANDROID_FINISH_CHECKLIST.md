# Android finish checklist (superseded)

Use [docs/NYC311_PORTAL_AUTOMATION.md](/media/max/T7/scripts/455-tenants-finalized-v6/455-tenants-finalized/docs/NYC311_PORTAL_AUTOMATION.md) for the current filing path. This checklist is kept only as historical reference.

This is the remaining work only.

## 1. Use the real Cloudflare hostname

Replace `YOUR_DOMAIN` in Tasker and AutoNotification with your live Cloudflare hostname, for example:

`https://api.YOUR_DOMAIN`

Use:

- `/ingest/tasker` on the capture phone
- `/mobile/filings/claim_next` on the filer phone
- `/mobile/filings/{job_id}/submitted` after a successful NYC311 submission
- `/mobile/filings/{job_id}/failed` on every hard failure path

## 2. Run one live capture test from WhatsApp

Do not use an old export for this test.

Send one fresh tenant-group message such as:

`both elevators are out right now`

Then confirm:

1. `/api/incidents` shows a new elevator incident
2. `/api/queue` shows a pending filing job
3. `/api/summary` says `ready_for_android_filer`

If you test only with old imported chat, the queue can stay empty because historical incidents older than `AUTO_FILE_MAX_INCIDENT_AGE_HOURS` are intentionally blocked from auto-filing.

## 3. Finish the filer phone loop

Make sure the spare phone can do this exact loop:

1. `POST /mobile/filings/claim_next`
2. Open NYC311 app
3. Paste the generated description from `job.payload.description`
4. Extract the SR number from the success screen
5. `POST /mobile/filings/{job_id}/submitted`

If anything breaks mid-flow, immediately call:

`POST /mobile/filings/{job_id}/failed`

and include the selector or screen that failed.

## 4. Add the two phone-side safeguards that still matter

1. Save a screenshot before final submit and on every failure branch.
2. Retry claim/poll and submit callbacks at least once before giving up.

## 5. Run one real complaint end to end

After the filer phone submits successfully, confirm:

1. `/api/cases` contains the new `311-########`
2. `/api/summary` moves from `ready_for_android_filer` to `tracking_live`
3. the Google Sheet shows the new case in `Cases311`

## 6. Next-day follow-up

Run:

`POST /admin/sync_311_statuses`

Then confirm the case status updated in `/api/cases` and the sheet.
