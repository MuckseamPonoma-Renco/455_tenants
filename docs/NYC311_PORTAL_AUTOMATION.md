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

## Automatic filing contract

Eligible current incidents enter the queue as `pending`; no per-case approval is
required. When the worker claims a job, it binds the exact payload SHA-256 in the
job record. At the portal review screen it rebuilds the draft from the current
incident and cancels unless the eligibility, complaint type, form target, and
payload all still match.

Stale claims return to `pending` and can be claimed again. Failed portal attempts
retry automatically up to `AUTO_FILE_MAX_PORTAL_ATTEMPTS` (default `3`).

## Run the filing worker once

```bash
./.venv/bin/python scripts/run_311_portal_worker.py
```

What it does:

1. Claims the next current eligible filing job from the local queue.
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
- Failed portal attempts retry automatically up to the configured limit; a job remains `failed` after that limit for inspection.
- The low-level portal helper defaults to review-only; only the automatic worker explicitly enables the final submit click.
- The browser context runs in `America/New_York` so the portal accepts `Date/Time Observed` validation consistently.
- When `311_EMAIL` and `311_PASSWORD` exist in `.env`, the worker signs into NYC311 first and then still submits the elevator flow anonymously if requested by the form step.
- The confirmation page does not always render the SR number in visible text; the worker falls back to the `View Details or Subscribe for Updates` link and extracts `srnum=...` from that URL.
- `/mobile/filings/{job_id}/preview`, `/claim_next`, `/submitted`, and `/failed` define the normal filing lifecycle. `/approve` remains only for legacy/manual compatibility.
- `/api/summary` uses `ready_for_portal_worker` for queued work and `filing_attention_needed` when failed work needs inspection.
