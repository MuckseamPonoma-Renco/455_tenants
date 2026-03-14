# Codex runbook: finish the on-device layer fast

The backend is finished enough to run. The main remaining work is on the Android phone itself.

## Highest-value remaining tasks
1. Lock the exact AutoInput selectors for the NYC311 app filing flow.
2. Add Tasker retries and screenshot-on-failure for every critical filing step.
3. Verify the Service Request Number extraction pattern from the final confirmation screen.
4. Run one real elevator complaint end to end and confirm it lands in `/api/cases`.
5. Schedule a daily `/admin/sync_311_statuses` call.
6. Share the Google Sheet with tenants as read-only.

## Best prompts to give Codex
### Selector hardening
> Create a Tasker/AutoInput selector map for the NYC311 Android app filing flow using visible text first, content description second, and coordinate fallback last. Include screenshot checkpoints and a failure branch that posts `/mobile/filings/{job_id}/failed`.

### Health watchdog
> Add a Tasker watchdog that checks `/health` every 15 minutes, reopens WhatsApp if notification access dies, and writes local logs for the last 100 runs.

### SR number parsing
> Add a robust SR-number extraction regex for Tasker that captures both `311-12345678` and bare 8-digit values shown on the success screen, then normalizes them to `311-########` before posting to `/mobile/filings/{job_id}/submitted`.
