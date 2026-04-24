# Tasker setup (retired)

This Tasker notification-capture flow is retired.

Use WhatsApp Web capture on the Mac mini instead:

- [WhatsApp Web capture setup](WHATSAPP_WEB_CAPTURE_SETUP.md)
- [Verification checklist](VERIFY.md)

Why this is retired:

- Android notification capture was not stable enough for the tenant log.
- Phone-side notification access can silently miss media, history, or edited context.
- The Chrome/Playwright watcher can capture message context, media metadata, and durable batches more consistently.

The `/ingest/tasker` and `/ingest/tasker_batch` endpoints remain in code only as legacy compatibility shims and for tests against older stored data. Do not use them for new production setup.
