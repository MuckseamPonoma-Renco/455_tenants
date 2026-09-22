# Verification

Use the [README local setup](../README.md#local-setup) in a fresh checkout without live `.env`, credentials, exports, or browser sessions. Keep local application checks separate from deployment checks.

## Automated checks

```bash
.venv/bin/python -m pytest -q
node --test cloudflare/chat_export_receiver/worker.test.mjs
```

The Python fixtures recreate `test_app.sqlite3` in the checkout, use inline processing, disable Sheets and LLM calls, and enable filing for mocked eligibility/lifecycle tests. Do not run this suite in an operational checkout containing data you need to preserve. The receiver suite uses Node's test runner and Web APIs.

These checks cover code paths and regression fixtures. They do not verify external credentials, live capture, public-record freshness, portal availability, or successful live filing.

For an optional syntax check limited to source directories:

```bash
.venv/bin/python -m compileall -q apps packages scripts tests
```

## Isolated smoke check

The existing smoke script inserts a synthetic message into its configured database and attempts to claim a job. It is a mutation test, not a production health probe. Run it with a dedicated SQLite database, filing disabled, and review-local status paths:

```bash
(
  export DATABASE_URL=sqlite:///./smoke-review.sqlite3
  export PROCESS_INLINE=1
  export DISABLE_SHEETS_SYNC=1
  export LLM_MODE=off
  export AUTO_FILE_ENABLED=0
  export INGEST_TOKEN=local-review-only
  export MOBILE_FILER_TOKEN=local-review-only
  export AUDIT_DIR=.local/review/audit
  export WHATSAPP_CAPTURE_STATUS_PATH=.local/review/capture.json
  export AUTOMATION_STATUS_PATH=.local/review/automation.json
  export NYC311_STATUS_STATE_PATH=.local/review/nyc311.json
  export CHAT_EXPORT_SYNC_STATE_PATH=.local/review/export-sync.json
  export CLOUD_CHAT_EXPORT_SYNC_STATE_PATH=.local/review/cloud-sync.json
  .venv/bin/python scripts/smoke_test.py
)
```

Inspect the printed HTTP statuses and response bodies; the script does not turn every failed response into a failing exit code. Intake should succeed and no filing job should be claimed. The smoke script exercises the retained Tasker compatibility endpoint; the regression suite also exercises the current WhatsApp routes.

The review-local status paths prevent a local demo from displaying another installation's heartbeat files. They can also be exported for the README's local API session.

## Read-only checks for a configured deployment

Use only your own configured host and token:

```bash
curl --fail http://127.0.0.1:8000/health
curl --fail -H "Authorization: Bearer $INGEST_TOKEN" \
  http://127.0.0.1:8000/api/summary
```

`/health` returning HTTP 200 or `ok: true` is not proof that every dependency is healthy. Inspect database readiness, storage, capture, automation, export-recovery stages, and NYC311 status freshness separately. `/api/summary` provides operating stage and work needing attention.

For a configured public hostname, check its current DNS/TLS and `/health` response. Do not reuse a historical hard-coded Cloudflare IP as a permanent verification command.

On an existing macOS installation:

```bash
./scripts/check_mac_services.sh
launchctl print "gui/$(id -u)/tenant-issue-os.api"
launchctl print "gui/$(id -u)/tenant-issue-os.automation"
launchctl print "gui/$(id -u)/tenant-issue-os.watchdog"
```

On a Linux installation that actually uses the documented systemd units:

```bash
systemctl --user is-active tenant-issue-os-api.service tenant-issue-os-tunnel.service
```

Installation and repair are separate operational actions. `install_mac_launch_agents.sh` copies code into the staged runtime and installs/restarts services; `check_mac_services.sh --repair` changes service state. Reinstalling after code changes is how the macOS staged runtime is updated. Do not run either as a portfolio check. See the [deployment guide](DEPLOY_CLOUDFLARE_NEON.md).

### Reboot boundary

LaunchAgents recover during normal logged-in operation but do not bypass FileVault. After a full restart, the Mac must be unlocked for user LaunchAgents to run. The optional private export receiver can archive uploads while the Mac is unavailable; Mac capture/import resumes after unlock. A separately configured cloud recovery workflow has its own status and receipts.

## Historical import verification

Keep automatic filing disabled and the portal worker/daemon stopped. The flags must be set on the receiving API process, not merely the shell running `curl`. Use a locally prepared fictional export for evaluation:

```bash
curl --fail -X POST \
  -H "Authorization: Bearer $INGEST_TOKEN" \
  -F "file=@example-chat.zip" \
  http://127.0.0.1:8000/ingest/export
```

Check authenticated `/api/incidents`, `/api/decisions`, `/api/queue`, and `/api/cases` against the input. Verify deduplication and that historical processing creates no live filing jobs. A historical message can mention an existing SR; storing that reference is not a new submission.

Archive-processing/reprocess paths suppress new filing creation; weekly import audit and cloud recovery disable filing for historical processing. Later explicit queue operations can still consider eligible incidents. The incident age limit (`AUTO_FILE_MAX_INCIDENT_AGE_HOURS`, template default 168) is an additional eligibility check, not a substitute for keeping filing off during replay. Weekly audits can sync/read live Sheets and are not offline tests.

## WhatsApp capture verification

Follow the [capture setup guide](WHATSAPP_WEB_CAPTURE_SETUP.md) with an explicitly authorized test chat and fictional messages, a review database, filing disabled, and Sheets disabled.

1. Configure the exact test chat in `WHATSAPP_CAPTURE_CHAT_NAMES`.
2. Run `./scripts/run_whatsapp_capture.sh --headful` and complete the WhatsApp QR login if needed.
3. Let the first pass prime visible messages, then send a clearly fictional message only to that test chat.
4. Inspect stored messages/decisions after a poll cycle. For a deliberate backlog pass, the existing `--once --headful --no-prime` options bypass priming and can ingest visible history; use them only with the isolated test configuration.
5. Inspect attachment output against the current filtering rules. Public media handling must be reviewed separately; capture alone does not establish safe sharing.

Never send a fake outage into a live tenant chat to test a configured filing system.

## Live filing and spreadsheet verification

A real NYC311 complaint is an operational action. File only a genuine current eligible incident after reviewing the live configuration; do not create a complaint solely to test a portfolio demo.

For an authorized live incident, follow the [portal filing contract](NYC311_PORTAL_AUTOMATION.md). Confirm the committed receipt and matching service-request lookup, then check the linked case/status record. If the worker reaches `submitting` or `submission_unknown`, reconcile the receipt before any retry.

Case closure is not proof that the underlying building issue was repaired. Check the incident evidence separately.

For Sheets/watchdog output, compare stored records with rendered rows, inspect source errors and freshness, then read back the actual destination workbook. A running heartbeat or successful sync request alone does not prove current, correct public output. Export the chronology bundle only from the intended dataset and review its contents before sharing.

The [fictional resident-view example](TENANT_LOG_EXAMPLE.md) is the portfolio preview; it is not live service evidence.
