# iPhone Chat Export Shortcut

Name the Shortcut `Archive 455 Tenant Chat` and enable **Show in Share Sheet** for files.

## Shortcut actions

1. Receive a file from the Share Sheet. If no file was supplied, use **Select File** with iCloud Drive enabled.
2. Use **Get Details of Files** twice to obtain `Name` and `File Size`.
3. Use **Get Contents of URL**:
   - URL: `https://uploads.455tenants.com/v1/uploads`
   - Method: `POST`
   - Headers: `Authorization: Bearer <UPLOAD_AUTH_TOKEN>` and `Content-Type: application/json`
   - Request body: JSON dictionary with `filename` set to the file name and `size_bytes` set to the file size.
4. Use **Get Dictionary Value** to read `upload_url` from that response.
5. Use **Get Contents of URL** again:
   - URL: the returned `upload_url`
   - Method: `PUT`
   - Header: `Content-Type: application/octet-stream`
   - Request body: File, set to the input file.
6. Show a notification only after the `PUT` returns successfully.

The first request creates a short-lived, single-file upload URL. The Shortcut never receives the R2 API credentials.

## Weekly routine

Create a personal weekly Shortcuts automation that opens the Shortcut or shows a notification. iOS and WhatsApp do not provide a supported way to background-trigger or tap **Export Chat**, so the WhatsApp export itself remains a deliberate one-tap action:

1. WhatsApp: open the group, choose **Export Chat**, and include media when needed.
2. Send the resulting ZIP to `Archive 455 Tenant Chat` from the Share Sheet.

Once the ZIP reaches R2, the Mac recovery agent imports and audits it automatically whenever the Mac is available. The cloud archive remains stored even when the Mac is off.
