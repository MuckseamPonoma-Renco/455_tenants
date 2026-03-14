# Verification checklist

## Backend

```bash
python3.11 -m pytest -q
python3.11 -m py_compile $(find apps packages scripts tests -name '*.py')
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

## Deployed Render smoke test

```bash
BASE_URL=https://YOUR_ACTUAL_SERVICE_NAME.onrender.com \
INGEST_TOKEN=your-render-token \
MOBILE_FILER_TOKEN=your-mobile-token \
python3.11 scripts/smoke_test.py
```

## Android live test

1. Claim a queue job through Tasker.
2. Submit one real complaint in the NYC311 app.
3. Post the SR number back.
4. Confirm `/api/cases` shows the new case.
5. Run `/admin/sync_311_statuses` the next day.
6. Export `/admin/export_legal_bundle`.
