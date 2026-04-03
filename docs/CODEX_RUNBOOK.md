# Codex runbook: finish the browser filing layer fast

The backend is finished enough to run. The main remaining work is on the browser filing worker.

## Highest-value remaining tasks
1. Verify the Playwright filing flow still reaches the final confirmation screen.
2. Keep the address lookup selection stable for `455 OCEAN PARKWAY`.
3. Verify the Service Request Number extraction pattern from the final confirmation screen.
4. Run one real elevator complaint end to end and confirm it lands in `/api/cases`.
5. Schedule a daily `/admin/sync_311_statuses` call.
6. Share the Google Sheet with tenants as read-only.

## Best prompts to give Codex
### Portal filing flow
> Build or update the Playwright NYC311 portal worker so it claims the next filing job, reaches the elevator complaint review screen, submits the complaint, extracts the SR number, and stores screenshots for the review and confirmation pages.

### Portal lookup
> Add or update the portal lookup helper so `scripts/run_311_portal_worker.py --lookup 311-########` returns the latest page text and parsed status from https://portal.311.nyc.gov/check-status/.

### Worker verification
> Read docs/NYC311_PORTAL_AUTOMATION.md, docs/VERIFY.md, and docs/FINAL_WORKFLOW.md. Produce the shortest exact checklist to run one live capture and one real portal submission end to end.
