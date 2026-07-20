# WhatsApp Web capture setup (Chrome on the Mac mini)

Use this when you want the Mac mini itself to watch WhatsApp Web in Chrome and send new messages into Tenant Issue OS without relying on Android notification capture.

## What it does

- opens `https://web.whatsapp.com/` in a persistent Chrome profile
- keeps the WhatsApp login session on disk after the first QR scan
- watches the exact chat names you configure
- posts new visible messages to `POST /ingest/whatsapp_web_batch`
- scrolls upward until it finds an already-seen message fingerprint so short bursts do not get missed
- stores media evidence and message-bubble screenshots under `.local/whatsapp_media/`
- primes the currently visible messages on first run so it starts as a live watcher instead of re-importing the whole screen
- writes capture status to `WHATSAPP_CAPTURE_STATUS_PATH` so `/health` and operators can see when WhatsApp needs a fresh login

## Required env

Add these to `.env`:

```bash
WHATSAPP_CAPTURE_CHAT_NAMES="455 Tenants"
WHATSAPP_CAPTURE_POLL_SECONDS=30
WHATSAPP_CAPTURE_MESSAGE_LIMIT=30
WHATSAPP_CAPTURE_SCROLL_PAGES=8
WHATSAPP_CAPTURE_HEADLESS=0
WHATSAPP_CAPTURE_DISK_CACHE_BYTES=268435456
WHATSAPP_CAPTURE_MEDIA_CACHE_BYTES=67108864
```

Optional:

```bash
WHATSAPP_CAPTURE_API_BASE=http://127.0.0.1:8000
WHATSAPP_CAPTURE_USER_DATA_DIR=~/.local/share/tenant-issue-os/whatsapp_capture/chrome_profile
WHATSAPP_CAPTURE_STATE_PATH=~/.local/share/tenant-issue-os/whatsapp_capture/state.json
WHATSAPP_CAPTURE_STATUS_PATH=~/.local/share/tenant-issue-os/whatsapp_capture/status.json
WHATSAPP_CAPTURE_MEDIA_DIR=.local/whatsapp_media
```

Notes:

- `WHATSAPP_CAPTURE_CHAT_NAMES` must match the chat titles in WhatsApp exactly.
- The first API target is local `http://127.0.0.1:8000`; if that is unavailable, the worker will also try `PUBLIC_BASE_URL`.
- `WHATSAPP_CAPTURE_HEADLESS=0` is the safest starting mode because you may need to scan the QR code once.
- The dedicated Chrome profile caps rebuildable disk/media cache at 256 MB / 64 MB by default. This does not delete the WhatsApp session, IndexedDB, or captured evidence; set either cache value to `0` only if a different cap is required.
- Media downloads are best-effort. When WhatsApp Web does not expose a downloadable asset cleanly, the watcher still stores metadata and a message-bubble screenshot.
- If `PUBLIC_BASE_URL` is set, the synced Sheet can now show inline image previews plus tenant-openable media links for captured WhatsApp evidence.

## First run

1. Make sure Google Chrome is installed on the Mac mini.
2. Start the local API as usual.
3. Run:

```bash
./scripts/run_whatsapp_capture.sh --headful
```

4. If Chrome shows a WhatsApp QR code, scan it once from the phone already logged into WhatsApp.
5. Leave that Chrome profile signed in.
6. The first pass primes the visible messages and does not send them yet.
7. Send one fresh test message in one of the configured chats and wait one poll cycle.

## One-pass test

```bash
./scripts/run_whatsapp_capture.sh --once --headful --no-prime
curl -H "Authorization: Bearer $INGEST_TOKEN" http://127.0.0.1:8000/api/summary
```

Use `--no-prime` only for a deliberate one-time test after you send a fresh message.

If the capture is working, the new test message should appear in the normal incident/decision pipeline and any captured image should get a public spreadsheet link.

## Run it as a Mac service

Once the chat names are set in `.env`:

```bash
./scripts/install_mac_launch_agents.sh
./scripts/check_mac_services.sh --repair
./scripts/check_mac_services.sh
```

The optional `whatsapp_capture` LaunchAgent is only installed when `WHATSAPP_CAPTURE_CHAT_NAMES` is configured. The installer stages a launchd-safe runtime copy under `~/.local/share/tenant-issue-os/runtime`, so re-run `./scripts/install_mac_launch_agents.sh` after repo code changes or `.env` changes.

## Normal Mac runbook

Once the Mac is logged into this user account, the LaunchAgents should start automatically and keep running while the screen is locked or off. Use this to verify and repair them:

```bash
cd /Users/max/Desktop/scripts/455-tenants-finalized-v6/455-tenants-finalized
./scripts/check_mac_services.sh --repair
./scripts/check_mac_services.sh --json
```

The JSON health output should show:

- `local_health.status_code` is `200`
- `public_health.status_code` is `200`
- `sheets_disabled` is `false`
- `whatsapp_capture.state` is `ready`
- `whatsapp_capture.login_required` is `false`
- `chat_export_sync.state` is `ready`, `no_export`, or transiently `waiting_for_download`

Only use the manual starter when the LaunchAgents are not installed yet or you are intentionally starting a fallback process:

```bash
./scripts/start_mac_services.sh
./scripts/check_mac_services.sh
```

If WhatsApp reports `login_required=true`, run the watcher headfully and scan the QR code from the logged-in phone:

```bash
./scripts/run_whatsapp_capture.sh --headful
```

## Notes and limits

- Keep the Mac awake and logged into the user session when you need live WhatsApp capture. The screen can be off or locked, but do not log out if you need continuous capture.
- Current target power shape: computer sleep off, display sleep on. Check it with `pmset -g custom`; on AC power, `sleep` should be `0` and `displaysleep` can be a low value like `5` or `10`.
- If you have admin access and want the display to turn off sooner while the Mac stays awake, run `sudo pmset -c sleep 0 displaysleep 5 disksleep 10 powernap 1 womp 1 autorestart 1`.
- After a full reboot, the per-user LaunchAgents start after the user session logs in. WhatsApp Web capture cannot reliably run at the macOS login screen because it depends on Chrome and the saved WhatsApp Web session.
- The public `/health` endpoint deliberately reports only operational state. It does not expose watched chat names, local paths, or raw capture errors.
- This watcher reads what WhatsApp Web currently exposes in the browser; it is not a historical backfill tool.
- For older history, keep using `POST /ingest/export` with a WhatsApp export ZIP/TXT.
- The old Android Tasker route is now legacy-only. Leave it off unless you need a temporary migration bridge.
