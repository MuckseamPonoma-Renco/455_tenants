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

## Run the filing worker once

```bash
./.venv/bin/python scripts/run_311_portal_worker.py
```

What it does:

1. Claims the next pending filing job from the local queue.
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

- The worker keeps the existing queue contract intact.
- The browser context runs in `America/New_York` so the portal accepts `Date/Time Observed` validation consistently.
- When `311_EMAIL` and `311_PASSWORD` exist in `.env`, the worker signs into NYC311 first and then still submits the elevator flow anonymously if requested by the form step.
- The confirmation page does not always render the SR number in visible text; the worker falls back to the `View Details or Subscribe for Updates` link and extracts `srnum=...` from that URL.
- `/mobile/filings/claim_next`, `/submitted`, and `/failed` still define the filing lifecycle.
- `/api/summary` still uses the legacy stage name `ready_for_android_filer` for compatibility, even though the filing path is now browser-based.
