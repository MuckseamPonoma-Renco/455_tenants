# Deploy (Cloudflare Tunnel + Neon)

## Recommended production shape

Keep the current Python app intact.

- Neon stays the database.
- Cloudflare Tunnel exposes the API under your domain.
- Run the API in `PROCESS_INLINE=1` mode unless you truly need Redis + the separate worker.

That keeps every existing feature, removes the old Render-specific setup, and stays simple enough for this project.

## 1. Neon

Create or reuse a Neon project and copy the pooled Postgres connection string.

Use it as `DATABASE_URL`, for example:

```env
DATABASE_URL=postgresql+psycopg://USER:PASSWORD@YOUR-NEON-ENDPOINT-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require
```

Neon’s current guidance for Python / SQLAlchemy is to use a secure server-side TCP connection with pooling.

## 2. App env

Set these in `.env` on the machine that will run the API:

```env
PROCESS_INLINE=1
DISABLE_SHEETS_SYNC=0
PUBLIC_BASE_URL=https://api.YOUR_DOMAIN
GOOGLE_APPLICATION_CREDENTIALS=/run/secrets/gcp_sa.json
```

Also fill in the existing required values already used by the app:

- `INGEST_TOKEN`
- `MOBILE_FILER_TOKEN`
- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `OPENAI_API_KEY` if you want LLM assist mode
- building + NYC311 contact fields

Do not remove any current credentials. Just point the runtime at the ones you already have.

## 3. Run the API

Build the image:

```bash
docker build -f apps/api/Dockerfile -t tenant-issue-os-api .
```

Run it:

```bash
docker run -d \
  --name tenant-issue-os-api \
  --restart unless-stopped \
  --env-file .env \
  -p 8000:8000 \
  -v "$(pwd)/secrets/gcp_sa.json:/run/secrets/gcp_sa.json:ro" \
  tenant-issue-os-api
```

Check it:

```bash
curl http://127.0.0.1:8000/health
```

If you want to keep Redis + the separate worker later, the code still supports it. For now, inline mode is the lowest-friction production path.

## 4. Cloudflare Tunnel

Cloudflare’s current tunnel flow is:

1. Install `cloudflared`
2. `cloudflared tunnel login`
3. `cloudflared tunnel create tenant-issue-os`
4. `cloudflared tunnel route dns tenant-issue-os api.YOUR_DOMAIN`
5. Run the tunnel with a config file

Use the example config in `cloudflare/config.example.yml` and replace the placeholders with your real tunnel UUID, credential path, and hostname.

Then start the tunnel:

```bash
cloudflared tunnel --config cloudflare/config.yml run tenant-issue-os
```

If you want it persistent, install `cloudflared` as a service after the first successful run.

## 4A. User-service runtime on this machine

This repo now includes a no-sudo runtime path for the current host:

- API launch script: `scripts/run_api.sh`
- tunnel launch script: `scripts/run_cloudflared.sh`
- automation launch script: `scripts/run_automation.sh`
- user service: `tenant-issue-os-api.service`
- user service: `tenant-issue-os-tunnel.service`
- user service: `tenant-issue-os-automation.service`

Enable and start all three:

```bash
systemctl --user daemon-reload
systemctl --user enable --now tenant-issue-os-api.service tenant-issue-os-tunnel.service tenant-issue-os-automation.service
```

Check status:

```bash
systemctl --user --no-pager --full status tenant-issue-os-api.service tenant-issue-os-tunnel.service tenant-issue-os-automation.service
```

Restart all three:

```bash
systemctl --user restart tenant-issue-os-api.service tenant-issue-os-tunnel.service tenant-issue-os-automation.service
```

Tail logs:

```bash
journalctl --user -u tenant-issue-os-api.service -f
journalctl --user -u tenant-issue-os-tunnel.service -f
journalctl --user -u tenant-issue-os-automation.service -f
```

Important:

- The current host is using user services, not root-level services.
- If `loginctl show-user "$USER" -p Linger` says `Linger=no`, these services will not auto-start after a full reboot until root enables linger:

```bash
sudo loginctl enable-linger "$USER"
```

- Without linger, log in first and then start them manually:

```bash
systemctl --user start tenant-issue-os-api.service tenant-issue-os-tunnel.service tenant-issue-os-automation.service
```

## 4B. macOS runtime on this machine

macOS does not use `systemd`, so this repo now includes simple helper scripts:

- start after login: `./scripts/start_mac_services.sh`
- verify: `./scripts/check_mac_services.sh`

These start the API and automation loop in the background from Terminal after you log in.

Important:

- The tunnel only starts if this Mac has Cloudflare tunnel auth already copied over.
- `scripts/run_cloudflared.sh` now accepts either:
  - `CLOUDFLARED_TOKEN` in the environment
  - `~/.cloudflared/tenant-issue-os.token`
  - `~/.cloudflared/<tunnel-id>.json`
- If tunnel auth is missing, the API still runs locally on `127.0.0.1:8000`, but `https://api.YOUR_DOMAIN` will stay down until the token or credentials file is added on this Mac.

Exact transfer steps from the old machine:

```bash
# On the old machine, see which tunnel auth file exists
ls -l ~/.cloudflared/tenant-issue-os.token ~/.cloudflared/273c3233-ee8a-4ffd-8f7e-d180614938c5.json
```

Copy whichever file exists to the same path on the Mac.

Then on the Mac:

```bash
mkdir -p ~/.cloudflared
chmod 700 ~/.cloudflared
```

If you copied the token file, place it here:

```text
~/.cloudflared/tenant-issue-os.token
```

If you copied the credentials JSON, place it here:

```text
~/.cloudflared/273c3233-ee8a-4ffd-8f7e-d180614938c5.json
```

Then verify and start:

```bash
./scripts/run_cloudflared.sh --check
./scripts/start_mac_services.sh
./scripts/check_mac_services.sh
curl https://api.455tenants.com/health
```

## 5. Final production checks

Use a real fresh message, not an old imported one:

```bash
curl -X POST \
  -H "Authorization: Bearer $INGEST_TOKEN" \
  -H "Content-Type: application/json" \
  https://api.YOUR_DOMAIN/ingest/tasker \
  -d '{"chat_name":"455 Tenants","text":"Both elevators are out right now","sender":"Deployment Test","ts_epoch":'\"$(date +%s)\"'}'
```

Then verify:

- `https://api.YOUR_DOMAIN/health`
- `https://api.YOUR_DOMAIN/api/summary`
- `https://api.YOUR_DOMAIN/api/queue`

If the message is fresh and the rules match, `/api/summary` should move to `ready_for_android_filer`.
