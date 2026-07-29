# iPhone Chat Export Shortcut

Name the Shortcut `Archive 455 Tenant Chat` and enable **Show in Share Sheet** for files.

## Shortcut actions

1. Receive a file from the Share Sheet. Set **If there's no input** to **Ask For -> Files** so a direct run opens the iCloud Drive file picker instead of sending an empty request.
2. Use **Get Details of Files** twice to obtain `Name` and `File Size`. Set
   both actions' input to the `Shortcut Input` magic variable.
3. Use **Get Contents of URL**:
   - URL: `https://tenant-chat-export-receiver.mponomarenko999.workers.dev/v1/uploads`
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

The first request creates a short-lived, single-file upload URL. Enter the private `UPLOAD_AUTH_TOKEN` only in your personal Shortcut; do not put it in a note, the repository, or a shared Shortcut. The Shortcut never receives the R2 API credentials.

For `filename`, use the Name magic variable. Shortcuts may omit the `.zip`
extension; the receiver restores it. For `size_bytes`, use the File Size magic
variable. The receiver accepts either the raw byte count or the display value
that Shortcuts may provide, such as `196 MB`.

On the first upload from each device, Shortcuts may ask twice before sending the
file: once for the private Worker host and once for the private
`r2.cloudflarestorage.com` upload host. Choose **Always Allow** for both. Also
allow notifications if you want the final stored confirmation. These approvals
are device-specific; a Mac approval does not approve the iPhone.

## Weekly routine

Create a personal weekly Shortcuts automation that opens the Shortcut or shows a notification. iOS and WhatsApp do not provide a supported way to background-trigger or tap **Export Chat**, so the WhatsApp export itself remains a deliberate one-tap action:

1. WhatsApp: open the group, choose **Export Chat**, and include media when needed.
2. Send the resulting ZIP to `Archive 455 Tenant Chat` from the Share Sheet.

Once the ZIP reaches R2, the Mac recovery agent imports and audits it automatically whenever the Mac is available. The cloud archive remains stored even when the Mac is off.
