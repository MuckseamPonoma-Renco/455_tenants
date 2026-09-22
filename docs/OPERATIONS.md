# Operator guide

This guide holds the operational detail behind the [README](../README.md). Start with its isolated local setup before connecting a live deployment. The repository contains building-specific configuration; adapt and verify it before using it for another building.

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

Project-watchdog views add official-record checks, management claims, tenant observations, and follow-up actions. See the [replacement watchdog guide](ELEVATOR_REPLACEMENT_WATCHDOG.md).

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
| `GOOGLE_PUBLIC_SHEETS_SPREADSHEET_ID` | Separate resident workbook; sharing is workbook-wide |
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

See [WhatsApp capture](WHATSAPP_WEB_CAPTURE_SETUP.md), [NYC311 portal operation](NYC311_PORTAL_AUTOMATION.md), and [Cloudflare/Neon deployment](DEPLOY_CLOUDFLARE_NEON.md) for integration-specific setup.

### Docker and background processing

`docker-compose.yml` defines PostgreSQL, Redis, the API, and an RQ worker. It expects `.env` and a local `secrets/gcp_sa.json` file. Container hostnames (`db`, `redis`) and `/run/secrets/gcp_sa.json` are container paths, not host-shell defaults. It does not start Chrome capture or the NYC311 portal worker.

The deployment command remains:

```bash
docker compose up --build
```

**Known compatibility issue:** `apps/worker/worker.py` imports `rq.Connection`, while `requirements.txt` permits RQ 2.x versions without that export. Validate/fix the worker path before relying on Compose for queued processing. The README's inline local path avoids the worker entrypoint. This documentation cleanup does not change dependency versions or worker behavior.

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

The configured [public resident workbook](PUBLIC_SPREADSHEET.md) is separate from the operator workbook.

Use a separate resident workbook; hiding operator tabs does not isolate their contents from workbook sharing. Keep operator tabs, raw chats, personal contact details, private access-needs records, credentials, and unreviewed evidence out of public portfolio material.

The route `/media/whatsapp/{message_id}/{attachment_index}` is public by design for eligible attachments. It applies path and content filtering, but it is not an authentication boundary or a guarantee that an image contains no identifying details. `PUBLIC_BASE_URL` controls generated links, not route access. Inspect text, images, filenames, and outbound links before sharing them. A public SR number or address can also reveal a real incident's location.

For a portfolio, use the [fictional resident-view example](TENANT_LOG_EXAMPLE.md). A live resident workbook serves a different audience and requires its own content and sharing review. Do not use an operator workbook or live complaint form as an employer demo.

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
