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

## Required env

Add these to `.env`:

```bash
WHATSAPP_CAPTURE_CHAT_NAMES=455 Ocean Parkway Tenants,455 Test Chat
WHATSAPP_CAPTURE_POLL_SECONDS=30
WHATSAPP_CAPTURE_MESSAGE_LIMIT=30
WHATSAPP_CAPTURE_SCROLL_PAGES=8
WHATSAPP_CAPTURE_HEADLESS=0
```

Optional:

```bash
WHATSAPP_CAPTURE_API_BASE=http://127.0.0.1:8000
WHATSAPP_CAPTURE_USER_DATA_DIR=~/.local/share/tenant-issue-os/whatsapp_capture/chrome_profile
WHATSAPP_CAPTURE_STATE_PATH=~/.local/share/tenant-issue-os/whatsapp_capture/state.json
WHATSAPP_CAPTURE_MEDIA_DIR=.local/whatsapp_media
```

Notes:

- `WHATSAPP_CAPTURE_CHAT_NAMES` must match the chat titles in WhatsApp exactly.
- The first API target is local `http://127.0.0.1:8000`; if that is unavailable, the worker will also try `PUBLIC_BASE_URL`.
- `WHATSAPP_CAPTURE_HEADLESS=0` is the safest starting mode because you may need to scan the QR code once.
- Media downloads are best-effort. When WhatsApp Web does not expose a downloadable asset cleanly, the watcher still stores metadata and a message-bubble screenshot.

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

If the capture is working, the new test message should appear in the normal incident/decision pipeline exactly like Android live capture.

## Run it as a Mac service

Once the chat names are set in `.env`:

```bash
./scripts/install_mac_launch_agents.sh
./scripts/start_mac_services.sh whatsapp_capture
./scripts/check_mac_services.sh
```

The optional `whatsapp_capture` LaunchAgent is only installed when `WHATSAPP_CAPTURE_CHAT_NAMES` is configured.

## Notes and limits

- This watcher reads what WhatsApp Web currently exposes in the browser; it is not a historical backfill tool.
- For older history, keep using `POST /ingest/export` with a WhatsApp export ZIP/TXT.
- If both Android Tasker and Chrome capture the same message, the backend now dedupes those live sources against each other.
