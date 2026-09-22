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

Stale pre-submit claims return to `pending`. A still-eligible payload that changes
during review is rebuilt and requeued, rather than permanently skipped. Failed
pre-submit attempts retry up to `AUTO_FILE_MAX_PORTAL_ATTEMPTS` (default `3`).

Immediately before the final click, the worker commits `submitting`. A timeout
or persistence error after that boundary becomes `submission_unknown`; neither
state automatically retries. Reconcile the confirmation/receipt first. The
receipt is committed before optional status lookup. A tenant-provided receipt
for the same incident also blocks filing at claim and at final review.

## Run the filing worker once

This can submit a real complaint. Use it only for an authorized live deployment with a genuine current eligible incident, not as a demo or smoke test.

```bash
./.venv/bin/python scripts/run_311_portal_worker.py
```

What it does:

1. Claims the next current eligible filing job from the local queue.
2. Opens the elevator complaint portal flow.
3. Sets `Additional Details` to `Bldg w/ Multiple Devices`.
4. Pastes `job.payload.description`.
5. Resolves the configured building address through the portal lookup service.
6. Marks the filing anonymous.
7. Reaches review, submits, extracts the SR number, and stores the result back in the app.

Screenshots are saved to `.local/nyc311_portal/`.

## Look up a service request in the portal

```bash
./.venv/bin/python scripts/run_311_portal_worker.py --lookup 311-12345678
```

The recurring tracker checks `NYC311_TRACKER_ENDPOINT`, then falls back to the
portal. Bounded portal checks rotate by oldest attempt instead of repeatedly
checking only the newest cases. A matching SR number and valid status are
required before refreshing verified timestamps. Closed DOB pages may omit the
status label; a closure date supplies the closed status, but conflicting explicit
status/date evidence is not silently accepted. Closing a complaint does not
establish that the building condition was repaired.

`/health` exposes sanitized `nyc311_status` counts and freshness separately from
the automation heartbeat. Failed, not-found, and deferred checks remain distinct.
`/api/summary` and the operator queue show uncertain submissions needing receipt
reconciliation. Status refreshes commit per case so slow portal requests cannot
discard an entire batch's progress.

## Notes

- Archive-processing and historical reprocessing suppress new filing creation. Keep automatic filing disabled and workers stopped during replay; later queue operations can consider eligible incidents.
- Only failures known to precede the submission boundary retry automatically.
- The low-level portal helper defaults to review-only; only the automatic worker explicitly enables the final submit click.
- The browser context runs in `America/New_York` so the portal accepts `Date/Time Observed` validation consistently.
- When `311_EMAIL` and `311_PASSWORD` exist in `.env`, the worker signs into NYC311 first and then still submits the elevator flow anonymously if requested by the form step.
- The confirmation page does not always render the SR number in visible text; the worker falls back to the `View Details or Subscribe for Updates` link and extracts `srnum=...` from that URL.
- `/mobile/filings/{job_id}/preview`, `/claim_next`, `/submitted`, and `/failed` define the normal filing lifecycle. `/approve` remains only for legacy/manual compatibility.
- `/api/summary` uses `ready_for_portal_worker` for queued work and `filing_attention_needed` when failed work needs inspection.
