# Tenant Issue OS

Building reports, incident updates, and city service requests in one public record.

**[Open the public tenant log](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ9A_Y-07pb6ZHoN4LBDXwyNxdN3XoVcO9dqHl_ibjjfPiu1TZ1PKokKMy5ABorCciI318-6CLdcKtf/pubhtml?gid=0&single=true)**

The published page opens **Tenant Log** for building issues and follow-up. Supporting workbook views cover the elevator replacement, official records, and weekly summaries. [How to read the log](docs/PUBLIC_SPREADSHEET.md).

## What this project does

Built for a single-building tenant workflow, this Python/FastAPI backend connects WhatsApp reports with incident history, evidence, NYC311 cases, and Google Sheets updates. The replacement watchdog keeps management statements, resident observations, and official records distinct.

Reports are deduplicated and grouped into incidents. Rules and optional model review retain the reasons for classification. Eligible current incidents can be filed through a Playwright worker, which checks the payload before submission and preserves uncertain outcomes for receipt reconciliation. Imports, case tracking, evidence links, and chronology exports support follow-up.

The resident workbook shows the result. Detailed decisions, processing history, and operator follow-up remain in the private workbook. The database holds the source records; spreadsheet edits do not change them.

## Explore the code

| Area | Source |
| --- | --- |
| Intake and authenticated API routes | [`apps/api/routers/`](apps/api/routers/) |
| Incident and decision processing | [`packages/incident/`](packages/incident/), [`packages/worker_jobs.py`](packages/worker_jobs.py) |
| Filing eligibility and receipts | [`packages/nyc311/`](packages/nyc311/) |
| Official records and project watchdog | [`packages/public_records/`](packages/public_records/), [`packages/project_watch/`](packages/project_watch/) |
| Spreadsheet output | [`packages/sheets/`](packages/sheets/) |
| Regression tests | [`tests/`](tests/) |

For a credential-free review, follow [local setup](docs/OPERATIONS.md#local-setup) and [verification](docs/VERIFY.md). The [operator guide](docs/OPERATIONS.md) covers configuration, filing controls, sharing, and deployment; the [API reference](docs/API_REFERENCE.md) documents endpoints.

This is an operational project for one building. Tests use fixtures and mocks; current service health, source freshness, and public-sheet correctness require separate checks. The [fictional spreadsheet example](docs/TENANT_LOG_EXAMPLE.md) is available for offline review.
