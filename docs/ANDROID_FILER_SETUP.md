# Android filing worker setup (retired)

This Android/AutoInput filing path is retired.

Use the browser-based Playwright filer instead:

- [NYC311 portal automation](NYC311_PORTAL_AUTOMATION.md)
- [Verification checklist](VERIFY.md)

Why this is retired:

- Android app UI automation was not stable enough for production use.
- It could miss or misread screens after app updates, accessibility changes, or focus loss.
- The Playwright web-portal worker gives us better logs, repeatability, and status capture.

Do not set up a new Android filing phone. Keep the old `/mobile/*` filing callback API only as a backend compatibility contract for the Playwright worker and tests.
