# Public resident spreadsheet

[Open the resident workbook — start with Tenant Log](https://docs.google.com/spreadsheets/d/1zdbS-MXdHzUu_dOoyOxD1gUGGyqkcKB2_DIFhy5MqP0/edit#gid=0)

This is the resident-facing output of the configured tenant system. It is a separate workbook from the private operator workbook and contains real building-issue history, city service-request references, public-record checks, and selected evidence.

## Where to start

| Tab | What it shows |
| --- | --- |
| Tenant Log | Issue updates, evidence references, and NYC311 case follow-up |
| ProjectStatus | Replacement-project state and the evidence behind it |
| PublicRecords | Building-related records from public sources |
| WatchdogChecks | Observation and verification checks |
| ActionQueue | Follow-up items and draft actions |
| WeeklyDigest | Generated periodic summaries |
| ElevatorWatch | Elevator-related monitoring information |

Start with **Tenant Log** for the resident experience. Use the other tabs to inspect how reports, official records, and follow-up information are kept separate.

## How to interpret it

The database holds the source records; the spreadsheet is generated output. Updates reflect the last completed processing and sync. Check each row's update/check dates and source before treating it as current.

A city service request marked closed does not establish that the underlying building condition was repaired. A management statement, a tenant observation, and a corroborated official record are different kinds of evidence. Generated follow-up text is a draft, not proof that a message was sent or an action completed.

The workbook is an operational example for one building. It does not establish adoption, uptime, a service guarantee, or measured impact. The public source repository documents the implementation; [verification](VERIFY.md) explains the separate checks used for source freshness, receipts, and spreadsheet readback.

## Local example

For a downloadable layout illustration without real incident data, see the [fictional resident-view example](TENANT_LOG_EXAMPLE.md). It is labeled separately and does not replace the operational workbook above.
