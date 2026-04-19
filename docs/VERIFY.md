# Verification checklist

## Backend

```bash
./.venv/bin/pytest -q
./.venv/bin/python scripts/smoke_test.py
./.venv/bin/python -m py_compile $(find . -name '*.py')
```

## Runtime status

Fastest way to confirm the API + tunnel stack is up on this machine:

```bash
systemctl --user is-active tenant-issue-os-api.service tenant-issue-os-tunnel.service
curl http://127.0.0.1:8000/health
curl https://api.455tenants.com/health
```

On macOS, install the LaunchAgents once and use the helper scripts:

```bash
./scripts/install_mac_launch_agents.sh
./scripts/check_mac_services.sh
./scripts/check_mac_services.sh --repair
launchctl print "gui/$(id -u)/tenant-issue-os.api"
launchctl print "gui/$(id -u)/tenant-issue-os.automation"
launchctl print "gui/$(id -u)/tenant-issue-os.watchdog"
```

Re-run `./scripts/install_mac_launch_agents.sh` after repo-side code changes so the staged launchd runtime stays in sync.

If the public hostname fails on this machine because local DNS is lagging, force Cloudflare resolution during checks:

```bash
curl --resolve api.455tenants.com:443:104.21.20.97 https://api.455tenants.com/health
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

## NYC311 portal live test

1. Install Chromium for Playwright: `./.venv/bin/python -m playwright install chromium`
2. Run `./.venv/bin/python scripts/run_311_portal_worker.py`
3. Let the worker claim the next filing job and submit one real complaint.
4. Confirm `/api/cases` shows the new case.
5. Run `/admin/sync_311_statuses` the next day.
6. Export `/admin/export_legal_bundle`.

## Fastest way to know where you are

Call `GET /api/summary` after every major setup step. It will tell you the current stage and the next best action.

Historical imports are intentionally prevented from auto-filing when they are older than `AUTO_FILE_MAX_INCIDENT_AGE_HOURS` (default 168). This keeps the portal worker focused on current incidents instead of months-old backlog.
