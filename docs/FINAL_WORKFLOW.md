# Final workflow

1. Bulk import the WhatsApp export through `/ingest/export`.
2. Turn on WhatsApp Web capture through Chrome/Playwright.
3. Review `/api/incidents` and `/api/queue`.
4. Review the exact filing preview and approve that payload with `APPROVED — GO LIVE`.
5. Let the Playwright filing worker claim the approved elevator job.
6. After submission, store SR numbers in `/api/cases`.
7. Run status sync daily.
8. Export the legal chronology bundle whenever you need a pressure / attorney / tenant-association packet.

## What is automatic now
- message ingest
- incident clustering
- witness counting
- approval-gated elevator draft preparation
- manual SR number extraction from chat
- case storage
- case-status sync
- legal bundle export

## What still requires your phone
- WhatsApp may occasionally ask you to re-link the Mac Chrome session.
- The retired Android/Tasker path should stay off unless you are replaying old migration data.
- Each consequential NYC311 submission requires review and exact approval of its current preview.


## Fast operator view
- `GET /api/summary` for structured state / next step
- `GET /api/briefing` for a tenant-ready update plus a management follow-up draft
