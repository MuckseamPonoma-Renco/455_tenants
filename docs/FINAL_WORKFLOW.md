# Final workflow

1. Bulk import the WhatsApp export through `/ingest/export`.
2. Turn on live Android capture to `/ingest/tasker`.
3. Review `/api/incidents` and `/api/queue`.
4. Let the Playwright filing worker claim pending elevator jobs.
5. After submission, store SR numbers in `/api/cases`.
6. Run status sync daily.
7. Export the legal chronology bundle whenever you need a pressure / attorney / tenant-association packet.

## What is automatic now
- message ingest
- incident clustering
- witness counting
- elevator auto-queueing
- manual SR number extraction from chat
- case storage
- case-status sync
- legal bundle export

## What still requires your phone
- only WhatsApp notification capture if you keep using `/ingest/tasker`


## Fast operator view
- `GET /api/summary` for structured state / next step
- `GET /api/briefing` for a tenant-ready update plus a management follow-up draft
