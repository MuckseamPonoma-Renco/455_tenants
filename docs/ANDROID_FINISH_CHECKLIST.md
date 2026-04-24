# Android finish checklist (retired)

The Android capture and Android filing checklist is retired.

Current production checklist:

1. Run WhatsApp Web capture through Chrome/Playwright on the Mac mini.
2. Confirm `/whatsapp/status` says the configured chat is ready and `login_required` is false.
3. Resync the Google Sheet with `/admin/resync_sheets`.
4. Run the Playwright NYC311 portal worker for filing jobs.
5. Confirm new SRs land in `Cases311` and the public `Tenant Log`.
6. Run `/admin/sync_311_statuses` after filing so the case row uses the NYC portal/Open Data status.

See:

- [WhatsApp Web capture setup](WHATSAPP_WEB_CAPTURE_SETUP.md)
- [NYC311 portal automation](NYC311_PORTAL_AUTOMATION.md)
- [Verification checklist](VERIFY.md)
