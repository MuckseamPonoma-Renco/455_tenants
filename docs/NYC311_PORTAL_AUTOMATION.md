# NYC311 portal automation

This is the current 311 filing path.

The backend queue and `/mobile/*` callbacks stay the same, but the actual complaint filing now runs through Playwright against the NYC311 web portal instead of the Android app.

## Required env

Add these to `.env` if you want the worker to attempt portal login first:

```dotenv
311_EMAIL=you@example.com
311_PASSWORD=replace_me
```

If those vars are missing, the worker still files anonymously, which is enough for the elevator flow this repo needs.

## One-time setup

```bash
./.venv/bin/pip install -r requirements.txt
./.venv/bin/python -m playwright install chromium
```

## Review and approve a filing

Read the current job through `GET /mobile/filings/{job_id}/preview`. Submit its
unchanged `payload_sha256` and the exact phrase `APPROVED — GO LIVE` to
`POST /mobile/filings/{job_id}/approve`.

An approval is bound to that payload hash. A changed payload or stale claimed job
returns to `awaiting_approval`.

## Run the filing worker once

```bash
./.venv/bin/python scripts/run_311_portal_worker.py
```

What it does:

1. Claims the next explicitly approved filing job from the local queue.
2. Opens the elevator complaint portal flow.
3. Sets `Additional Details` to `Bldg w/ Multiple Devices`.
4. Pastes `job.payload.description`.
5. Resolves `455 OCEAN PARKWAY` through the portal lookup service.
6. Marks the filing anonymous.
7. Reaches review, submits, extracts the SR number, and stores the result back in the app.

Screenshots are saved to `.local/nyc311_portal/`.

## Look up a service request in the portal

```bash
./.venv/bin/python scripts/run_311_portal_worker.py --lookup 311-12345678
```

This is a portal-side verification helper only. The app still uses `NYC311_TRACKER_ENDPOINT` for daily status sync because that is simpler and already works.

## Notes

- Archive imports and historical reprocessing cannot create live filing jobs.
- A failed portal attempt requires a new preview approval before retry.
- The low-level portal helper defaults to review-only; only the approval-gated worker enables the final submit click.
- The browser context runs in `America/New_York` so the portal accepts `Date/Time Observed` validation consistently.
- When `311_EMAIL` and `311_PASSWORD` exist in `.env`, the worker signs into NYC311 first and then still submits the elevator flow anonymously if requested by the form step.
- The confirmation page does not always render the SR number in visible text; the worker falls back to the `View Details or Subscribe for Updates` link and extracts `srnum=...` from that URL.
- `/mobile/filings/{job_id}/preview`, `/approve`, `/claim_next`, `/submitted`, and `/failed` define the filing lifecycle.
- `/api/summary` uses `awaiting_filing_approval` before approval and `ready_for_portal_worker` afterward.
