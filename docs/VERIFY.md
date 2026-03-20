# Verification checklist

## Backend

```bash
pytest -q
python -m py_compile $(find . -name '*.py')
```

## Import live WhatsApp export

```bash
export PROCESS_INLINE=1
export DISABLE_SHEETS_SYNC=1
curl -X POST \
  -H "Authorization: Bearer $INGEST_TOKEN" \
  -F "file=@WhatsApp Chat - 455 Tenants03082026.zip" \
  http://127.0.0.1:8000/ingest/export
```

Then verify:

- `/api/incidents` contains elevator incidents
- `/api/queue` contains filing jobs
- `/api/cases` contains any SR numbers already mentioned in chat
- `/api/summary` says the stage is `ready_for_android_filer` once queue jobs exist

## Android live test

1. Claim a queue job through Tasker.
2. Submit one real complaint in the NYC311 app.
3. Post the SR number back.
4. Confirm `/api/cases` shows the new case.
5. Run `/admin/sync_311_statuses` the next day.
6. Export `/admin/export_legal_bundle`.

## Fastest way to know where you are

Call `GET /api/summary` after every major setup step. It will tell you the current stage and the next best action.

Historical imports are intentionally prevented from auto-filing when they are older than `AUTO_FILE_MAX_INCIDENT_AGE_HOURS` (default 168). This keeps the Android phone focused on current incidents instead of months-old backlog.
