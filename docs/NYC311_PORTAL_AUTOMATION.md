# NYC311 portal automation

This is the current 311 filing path.

The backend queue and `/mobile/*` callbacks stay the same, but the actual complaint filing now runs through Playwright against the NYC311 web portal instead of the Android app.

## Required env

Add these to `.env` if you want the worker to attempt portal login first:

```dotenv
311_EMAIL=you@example.com
311_PASSWORD=replace_me
```

Without credentials or a saved session, the worker can reach the anonymous flow.
NYC311 may require CAPTCHA before it accepts a complaint. A running worker or a
completed draft is not a confirmed filing.

NYC.ID may use an email-first verification flow instead of a password form. To
sign in interactively without creating any complaint, run:

```bash
./.venv/bin/python scripts/run_311_portal_worker.py --sign-in
```

Complete the city's verification yourself in the visible browser. Only a verified
portal sign-in is saved, in an owner-only local file at
`~/.local/state/tenant-issue-os/private/nyc311-session.json`. Set
`NYC311_SESSION_STATE_PATH` to choose another private location. This is a session
created by this app, not an export of your normal browser profile. Keep it out of
source control and shared storage. Expired sessions require sign-in again;
configured authentication failures never silently fall back to anonymous filing.

The visible browser can close after sign-in. The worker reuses the app's saved
session in a fresh browser after restart. With filing enabled, the automation
daemon checks that session at startup and every 30 minutes (configurable with
`NYC311_SESSION_CHECK_SECONDS`, bounded to 5–60 minutes). A check visits only the
portal home; it does not open or submit a complaint. Verified server-issued
renewals are saved atomically with owner-only permissions. A slow older check
cannot overwrite a newer interactive login. The worker also verifies login at
the final review screen.

The city controls expiry and can require verification again; no local setting
can guarantee permanent login. `/health`, the operations summary and the Mac
service check report an expired, unavailable, unverified or stale login
separately from process health. The daemon retries unsuccessful checks after
five minutes without restarting capture or other services. To check directly:

```bash
./.venv/bin/python scripts/run_311_portal_worker.py --check-session
```

If this reports `login_required`, run `--sign-in` again. A fresh verified check
automatically returns only known pre-submit sign-in holds to the normal queue.
It preserves attempt counts, eligibility and duplicate checks. CAPTCHA holds,
completed jobs and uncertain submissions do not receive this recovery.

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

A never-attempted job skipped for ineligibility or a corrected false-positive can
return to `pending` when a newer live WhatsApp/Tasker outage report arrives. The
report must be newer than the skip, no more than 24 hours old, and classified as
an actionable elevator issue. Existing receipts and equivalent filing jobs still
block recovery. Historical replay, bulk queue scans, manual holds, attempted
jobs, and uncertain submissions do not receive this automatic recovery. The old
skip explanation remains in the job history, and the payload is rebuilt from
current evidence before the normal submission checks run.

Immediately before the final click, the worker commits `submitting`. A timeout
or persistence error after that boundary becomes `submission_unknown`; neither
state automatically retries. Reconcile the confirmation/receipt first. The
receipt is committed before optional status lookup. A tenant-provided receipt
for the same incident also blocks filing at claim and at final review.

An unsolved visible CAPTCHA or required sign-in stops before the final click and
holds the job as `awaiting_human_verification`. The operator queue and summary
show the hold. The unattended worker does not solve challenges. Sign-in holds
recover after fresh authentication as described above. To handle a CAPTCHA,
explicitly resume one eligible held job with:

```bash
./.venv/bin/python scripts/run_311_portal_worker.py \
  --headful --resume-human-job JOB_ID --human-verification-wait-seconds 300
```

This waits for the user to complete any visible challenge, revalidates the current
incident and exact payload, and then submits. It refuses uncertain or completed
jobs. Authentication does not guarantee that the city will omit CAPTCHA.

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

Screenshots are saved with owner-only permissions to `.local/nyc311_portal/`.
A post-click error also saves the visible outcome when possible, for receipt
reconciliation; the absence of a screenshot never permits a blind retry.

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
