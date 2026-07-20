# Private Chat Export Receiver

This Worker persists WhatsApp chat exports while the Mac is unavailable. It does not publish files, chat content, or credentials.

## Design

1. The iPhone Shortcut sends metadata to `POST /v1/uploads` with `UPLOAD_AUTH_TOKEN`.
2. The Worker returns a 15-minute, single-object R2 `PUT` URL.
3. The Shortcut uploads the ZIP directly to private R2. The Worker never proxies the large file.
4. The Mac agent calls `GET /v1/exports` with `PULL_AUTH_TOKEN`, downloads each unacknowledged export, runs the existing audit, and calls `POST /v1/exports/ack` only after a successful audit.

The original R2 archive is retained. An acknowledgment is a small separate receipt object, so a transient failure cannot erase evidence or cause an export to be silently skipped.

## Required Cloudflare configuration

Create a private R2 bucket, bind it to this Worker as `EXPORTS`, and set the following Worker secrets:

- `UPLOAD_AUTH_TOKEN`: used only by the iPhone Shortcut to ask for a one-time upload URL.
- `PULL_AUTH_TOKEN`: used only by the Mac recovery agent to list, download, and acknowledge exports.
- `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`: an R2 Object Read & Write token scoped only to this bucket, used to sign direct upload and download URLs.

Set `R2_ACCOUNT_ID` and `R2_BUCKET_NAME` as Worker variables. The `wrangler.toml.example` has the non-secret configuration shape. Do not make the bucket public and do not expose the R2 API token in the Shortcut.

## Receiver API

`GET /health` is public and reveals only receiver/R2 readiness.

`POST /v1/uploads` requires `Authorization: Bearer <UPLOAD_AUTH_TOKEN>` and a JSON body:

```json
{
  "filename": "WhatsApp Chat - 455 Tenants 12.zip",
  "size_bytes": 205520896
}
```

It returns `upload_url` and the exact `Content-Type` header required for the direct `PUT`.

`GET /v1/exports` and `POST /v1/exports/ack` require `Authorization: Bearer <PULL_AUTH_TOKEN>` and are used only by `scripts/sync_cloud_chat_export_inbox.py`.
