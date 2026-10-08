# Operator guide

This is the canonical setup and operating guide for [Tenant Issue OS](../README.md). Start with the isolated local setup below before connecting a deployment. The repository contains building-specific configuration; adapt and verify it before using it for another building.

## Local setup

Use a fresh checkout with Python 3.11 (the repository pins 3.11.15). This path needs no Google, WhatsApp, NYC311, or OpenAI credentials.

```bash
git clone https://github.com/MuckseamPonoma-Renco/455_tenants.git
cd 455_tenants
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt

export DATABASE_URL=sqlite:///./local-review.sqlite3
export PROCESS_INLINE=1
export DISABLE_SHEETS_SYNC=1
export LLM_MODE=off
export AUTO_FILE_ENABLED=0
export INGEST_TOKEN=local-review-only
export MOBILE_FILER_TOKEN=local-review-only

.venv/bin/python -c "from packages.db import init_db; init_db()"
.venv/bin/python -m uvicorn apps.api.main:app --host 127.0.0.1 --port 8000
```

Open `http://127.0.0.1:8000/docs` for the API schema. The example tokens are only for this loopback review session. Use fictional inputs and do not call live status/public-record sync endpoints during offline review. On a machine with an existing deployment, isolate status-file paths as described in [verification](VERIFY.md#isolated-smoke-check).

The exports apply to the current shell. Bare `uvicorn` does not load `.env` automatically. Production-oriented scripts have their own environment loading; [configure integrations separately](#integration-configuration) after local verification. Copying `.env.example` without reviewing it is not a safe local quick start.

Run the [automated checks and isolated smoke check](VERIFY.md) in the fresh checkout before configuring integrations. Tests use fixtures and mocks; they do not establish live capture, successful city submissions, current public records, or spreadsheet freshness.

## End-to-end workflow

1. Chrome/Playwright captures messages from explicitly configured WhatsApp chats, or an operator imports an export.
2. The backend persists raw messages and deduplicates repeated or cross-source captures.
3. Rules and optional model review classify messages and store the rule/model/final decision.
4. The incident engine groups reports, counts witnesses, and tracks outage/restoration events.
5. Eligible live incidents enter the filing queue when automatic filing is enabled.
6. The Playwright worker claims a job and binds its current payload hash.
7. At portal review, the worker revalidates eligibility and the exact payload before submission.
8. The worker persists the service-request receipt; uncertain submissions remain fenced from automatic retry.
9. Case tracking refreshes status, and spreadsheet sync renders records for operators and residents.

## Filing controls

Live filing is automatic when enabled. The supplied `.env.example` sets `AUTO_FILE_ENABLED=1`; normal eligible jobs do not require per-case approval. `/approve` remains for legacy/manual compatibility. Operators choose sources, building configuration, eligibility thresholds, credentials, and whether to run the worker.

Before the final click, the worker rechecks incident eligibility and the claim-bound payload. It records `submitting` before clicking; `submission_unknown` and `submitting` do not automatically retry. Reconcile the receipt before considering another attempt. See the [filing contract](NYC311_PORTAL_AUTOMATION.md).

Historical archive-processing and reprocessing paths suppress new filing jobs; weekly audit and cloud recovery runners disable automatic filing for their historical processing. Keep filing disabled and the portal worker and automation daemon stopped during review/import. Later queue operations can still consider eligible incidents, and an age threshold alone is not a replay safeguard.

## Operator views

Google Sheets is the primary day-to-day operator interface. The database remains the source of truth; these tabs are generated output, not a two-way command interface.

| View | Purpose |
| --- | --- |
| Dashboard | Building state and navigation/control links |
| Incidents | Structured issue timeline |
| Queue311 | Filing state and work needing attention |
| Cases311 | Service-request numbers and case status |
| DecisionLog | Rules, model output, and final classification |
| Coverage | Capture coverage and missing-message indicators |
| Tenant Log | Resident incident history, evidence references, and 311 follow-up |

With a dedicated public workbook configured, sync writes four resident views: `Tenant Log`, `ElevatorWatch`, `PublicRecords`, and `WeeklyDigest`. They present issue updates, project status, selected official records, and summaries for residents.

The private operator workbook retains all six detailed watchdog views: `ElevatorWatch`, `ProjectStatus`, `PublicRecords`, `WatchdogChecks`, `ActionQueue`, and `WeeklyDigest`. These preserve management claims, source provenance, verification checks, deadlines, and follow-up drafts. See the [replacement watchdog guide](ELEVATOR_REPLACEMENT_WATCHDOG.md) for the underlying rules.

## Day-to-day checks

On an existing configured macOS installation, begin with read-only checks:

```bash
./scripts/check_mac_services.sh --json
```

Inspect local/public API responses, capture readiness, `login_required`, automation state, export processing, and source freshness. An HTTP 200 or a running process does not establish that every stage is current. Authenticated `/api/summary` gives structured work needing attention; `/api/briefing` supplies an update and a management follow-up draft.

If capture requires login, run `./scripts/run_whatsapp_capture.sh --headful` and re-link the configured WhatsApp session. `./scripts/check_mac_services.sh --repair` changes service state; use it only when intentionally repairing the installation. The [capture guide](WHATSAPP_WEB_CAPTURE_SETUP.md) covers installation, restart, and login recovery.

For an intended processing/sync change, check the complete path: stored source and decision, incident, public row, and linked case/status. Confirm a genuine filing by its stored receipt and matching lookup. Compare spreadsheet readback with the database output and inspect evidence previews. A chronology export should come from the intended dataset and be reviewed before sharing. See [verification](VERIFY.md) for exact checks; live complaints are operational actions, not test fixtures.

## LLM modes

Transport and persistence do not require an LLM.

- `LLM_MODE=off`: deterministic rules only.
- `LLM_MODE=assist`: rules handle clear reports and a model helps with ambiguous ones.
- `LLM_MODE=supervised`: model review for every message, with rule/model/final provenance logged.
- Ambiguous or disagreeing outcomes can trigger a stronger review model before the final decision is stored.

Model-review errors are stored explicitly. Weekly supervised audits fail closed when required reviews are missing or the API reports an error. The filing planner and pre-submit validation determine eligibility; model review does not replace those checks.

## Integration configuration

For a deployment, copy the configuration template and review every field:

```bash
cp .env.example .env
```

The template includes building-specific values and enables automatic filing. Keep `AUTO_FILE_ENABLED=0` and the portal worker/automation daemon stopped during preparation and historical import. Do not copy live credentials into a review checkout.

| Setting | Purpose |
| --- | --- |
| `INGEST_TOKEN` | Bearer token for protected intake, read, and admin routes; replace the example |
| `MOBILE_FILER_TOKEN` | Filing-worker token; falls back to `INGEST_TOKEN` when omitted |
| `DATABASE_URL` | SQLAlchemy URL; SQLite locally or configured PostgreSQL |
| `PROCESS_INLINE` / `REDIS_URL` | Inline processing or Redis-backed queue configuration |
| `GOOGLE_SHEETS_SPREADSHEET_ID` | Private operator workbook |
| `GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID` | Dedicated resident workbook; must differ from the operator workbook ID |
| `TENANT_PUBLIC_SEMANTIC_OVERRIDES_PATH` | Private deployment JSON for reviewed Tenant Log corrections; provision before updating an existing installation |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to the locally stored Google service-account credential |
| `DISABLE_SHEETS_SYNC` | Keep `1` until intended workbooks and permissions are configured |
| Building address/identity fields | Address, BIN/BBL, and context for filings and public-record matching |
| `PUBLIC_BASE_URL` | Optional report-form/media-link base; review public exposure first |
| `WHATSAPP_CAPTURE_CHAT_NAMES` | Exact chats selected for live capture |
| `PUBLIC_UPDATES_CHAT_NAMES` | Optional narrower scope for resident evidence output |
| `LLM_MODE` / model settings | Optional model review configuration |
| `AUTO_FILE_*` | Filing enablement, incident eligibility, age limits, bounded retries |

Do not shell-source the template: keys such as `311_EMAIL` are dotenv keys, not valid shell variable assignments. Bare Uvicorn needs explicit exports or `--env-file .env`; deployment/worker scripts document their own loading behavior. Install Playwright Chromium when setting up browser operation:

```bash
.venv/bin/python -m playwright install chromium
```

The checked-in semantic-override manifest is an empty generic schema. Existing deployments with reviewed corrections must preserve their complete private manifest, set `TENANT_PUBLIC_SEMANTIC_OVERRIDES_PATH` to its absolute local path, and verify output before installing an update. An explicitly configured missing, invalid, or source-mismatched manifest fails closed. Keep this file out of public source control.

Public sync requires an explicitly configured manifest, including an explicit empty one for a new deployment. The Mac installers validate that an existing installation's reviewed corrections are preserved exactly in durable private storage before replacing code or restarting services.

Provision the GitHub Actions secret `CLOUD_RECOVERY_SEMANTIC_OVERRIDES_GZIP_BASE64` before publishing this upgrade to a repository with scheduled cloud recovery. It holds the compressed private correction manifest, not credentials or raw chats. The recovery workflow validates it into an owner-only temporary file and removes that file on exit. A missing or invalid secret blocks recovery before it changes the public workbook. The configuration helper can prepare a preview and requires explicit approval before writing repository secrets. Do not enable or reconfigure other recovery settings just to add this secret.

See [WhatsApp capture](WHATSAPP_WEB_CAPTURE_SETUP.md), [NYC311 portal operation](NYC311_PORTAL_AUTOMATION.md), and [Cloudflare/Neon deployment](DEPLOY_CLOUDFLARE_NEON.md) for integration-specific setup.

### Docker and background processing

`docker-compose.yml` defines PostgreSQL, Redis, the API, and an RQ worker. It expects `.env` and a local `secrets/gcp_sa.json` file. Container hostnames (`db`, `redis`) and `/run/secrets/gcp_sa.json` are container paths, not host-shell defaults. It does not start Chrome capture or the NYC311 portal worker.

The deployment command remains:

```bash
docker compose up --build
```

**Known compatibility issue:** `apps/worker/worker.py` imports `rq.Connection`, while `requirements.txt` permits RQ 2.x versions without that export. Validate/fix the worker path before relying on Compose for queued processing. The inline [local setup](#local-setup) avoids the worker entrypoint.

## Main endpoints

Use bearer authentication for protected API calls. The [API reference](API_REFERENCE.md) provides payload and response details; `/docs` exposes the route schema.

| Group | Routes |
| --- | --- |
| Health | `GET /health` |
| Intake | `POST /ingest/whatsapp_web`, `POST /ingest/whatsapp_web_batch`, `POST /ingest/export` |
| Legacy intake | `POST /ingest/tasker`, `POST /ingest/tasker_batch` |
| Optional resident form | `GET /report`, `POST /report/submit` |
| Processing/admin | `POST /admin/reprocess_last/{n}`, `POST /admin/resync_sheets`, `POST /admin/queue_311_jobs` |
| Tracking/export | `POST /admin/sync_311_statuses`, `POST /admin/export_legal_bundle` |
| Read views | `GET /api/incidents`, `GET /api/queue`, `GET /api/cases`, `GET /api/decisions`, `GET /api/summary`, `GET /api/briefing` |
| Filing | `GET /mobile/filings/{job_id}/preview`, `POST /mobile/filings/claim_next`, `POST /mobile/filings/{job_id}/submitted`, `POST /mobile/filings/{job_id}/failed` |
| Compatibility approval | `POST /mobile/filings/{job_id}/approve` (normal filing does not require this call) |
| Status callbacks | `POST /mobile/sr_updates`, `POST /mobile/sr_updates/sync_now` |

The code also supports bulk-export reprocessing, SR extraction from chat, elevator witness clustering, decision-log sync, a QR/link report form, and CSV/Markdown chronology exports. Tasker/Android routes remain available for old stored data and migration compatibility.

## Deployment rollout

1. Complete local verification and configure the intended database and private operator workbook.
2. With filing disabled and workers stopped, import historical exports and review stored messages, incidents, decisions, and spreadsheet output.
3. Configure Chrome capture for the exact intended chats. Leave Android/Tasker capture off unless needed for migration.
4. Inspect current filing previews and building/eligibility settings before enabling live operation. `/approve` is a compatibility endpoint, not an ongoing mandatory gate.
5. Enable filing and start the portal worker only when ready for automatic submissions of eligible current incidents.
6. Confirm receipts in `Cases311` and independently look up status. Reconcile uncertain submissions before retrying.
7. Verify source freshness, sync results, and resident workbook readback separately.
8. Share the dedicated resident workbook after reviewing every visible tab and linked attachment. Introduce `/report` by QR/link after checking the resident experience.

## Resident sharing and private data

Configure the [public resident workbook](PUBLIC_SPREADSHEET.md) with an ID different from the private operator workbook. Public initialization, sync, and audit commands require a dedicated public ID; they fail if it is missing or matches the operator workbook.

Use a separate resident workbook; hiding operator tabs does not isolate their contents from workbook sharing. Publish only the four resident views, and confirm that detailed operator tabs are absent from the shared workbook. Keep raw chats, personal contact details, private access-needs records, credentials, and unreviewed evidence private. Redaction rules are not a privacy guarantee; inspect rendered text and linked media before sharing.

Sync does not automatically remove older tabs. When migrating an existing shared workbook, first verify that all detailed tabs and data are preserved in the private workbook. The migration helper defaults to a read-only plan:

```bash
.venv/bin/python scripts/migrate_public_workbook.py
```

After approval of the plan, `--archive` copies obsolete public detail and QA tabs into the private workbook and verifies their values by readback. Run that step before deploying the new projections. Initialize the four resident workbook views with the command below, replacing `PUBLIC_SPREADSHEET_ID` with the configured resident workbook ID. The initializer reads the local `.env` and checks that the destination differs from the operator workbook. Then run the intended sync and inspect content and evidence links.

```bash
.venv/bin/python scripts/init_sheet.py --spreadsheet-id PUBLIC_SPREADSHEET_ID --public-tabs
```

This initializes workbook tabs without changing Google's publication settings. Verify the intended published scope separately and keep the primary published link on Tenant Log. Audit public watchdog output with `scripts/audit_public_watchdog_tabs.py --audience public`; `--audience operator` checks all six detailed watchdog views. Audit Tenant Log separately with `scripts/audit_public_tenant_log.py`.

Only after the approved migration and passing readback, `scripts/migrate_public_workbook.py --retire` can remove the archived obsolete public copies. It requires matching private archives and value-level readback of all six operator tables, public watchdog views, and Tenant Log. Unknown tabs are left untouched. Read back both workbooks after migration. Sharing permissions and database records are not changed by the helper.

The route `/media/whatsapp/{message_id}/{attachment_index}` is public by design for eligible attachments. It applies path and content filtering, but it is not an authentication boundary or a guarantee that an image contains no identifying details. `PUBLIC_BASE_URL` controls generated links, not route access. Inspect text, images, filenames, and outbound links before sharing them. A public SR number or address can also reveal a real incident's location.

The public tenant workbook is the project's main shareable output after its content and sharing permissions have been reviewed. Recheck all visible tabs and linked evidence when output changes. The [fictional resident-view example](TENANT_LOG_EXAMPLE.md) is available for offline review without real incident data. Keep operator workbooks and live complaint forms out of portfolio previews.

Historical recovery tools and their reviewed data belong in private operational storage. Preserve their exact files and SHA-256 manifest before removing a public copy. To use an archived tool, restore it at its original path in a compatible private checkout, run its paired tests against a disposable database, and inspect its read-only plan before any authorized repair. The retained archive-review certifier requires an explicit private `--ledger` file. These tools are not scheduled tasks or normal setup steps.

## Further guides

- [Verification](VERIFY.md)
- [WhatsApp Web capture](WHATSAPP_WEB_CAPTURE_SETUP.md)
- [Weekly export audit](WEEKLY_CHAT_EXPORT_AUDIT.md)
- [NYC311 portal operation](NYC311_PORTAL_AUTOMATION.md)
- [Public-record sources](PUBLIC_RECORD_SOURCES.md)
- [Replacement watchdog](ELEVATOR_REPLACEMENT_WATCHDOG.md)
- [Cloudflare/Neon deployment](DEPLOY_CLOUDFLARE_NEON.md)
- [iOS export shortcut](IOS_CHAT_EXPORT_SHORTCUT.md)

Legacy references are retained for compatibility, not recommended setup paths: [Android capture](ANDROID_CAPTURE_SETUP.md), [Tasker](TASKER_SETUP.md), and [Android filer](ANDROID_FILER_SETUP.md).
