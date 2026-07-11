# ICON MOBILE Local ERP

ICON MOBILE now runs from one Python/FastAPI server and one local SQLite database. Google Sheets and Apps Script are not used for application data. Every phone, tablet, and computer on the same Wi-Fi opens the Python host's LAN address and works against the same authoritative `erp.db` file.

## Start the system

On the host Windows computer:

```powershell
.\setup.ps1
.\start.ps1
```

Or double-click `START_ICON_MOBILE.bat` after setup. Keep the server window open.

The server prints addresses similar to:

```text
This computer : http://localhost:8000/
Other devices : http://192.168.1.168:8000/
```

Use the `192.168...` address on every other device. All devices must be on the same Wi-Fi/LAN. If Windows asks about firewall access, allow Python on **Private networks**. Do not enter `localhost` on a phone; that points to the phone itself.

For public or reverse-proxy hosting, serve the FastAPI application and these HTML/assets from the same hostname. Invoice pages require `/api/v1/invoices/...` on that host; a static-only host cannot read the SQLite database. Shared invoice links retain the public HTTPS origin. A private LAN address is substituted only when the host computer is opened through `localhost`.

Pages:

- POS: `/`
- Inventory/admin: `/ADMINPRO.html`
- Wholesale/B2B: `/wholesale.html`
- Settings, backups, and connections: `/settings.html`
- Shared invoice: `/invoice.html?id=INVOICE-ID`
- Partner statement: `/B2Binvoice.html?name=PARTNER-NAME`
- Mobile scanner: generated from the POS/inventory scanner QR

## Admin corrections and deletion rules

The admin portal can edit partner contact details, delete partner profiles, delete complete invoices, delete individual invoice lines, delete inventory product groups, and delete individual available inventory units.

- Deleting an invoice reverses its committed stock movement and refreshes product quantities in the same SQLite transaction.
- A sale/issue with linked returns or payments is protected until the linked records are deleted first.
- Deleting one invoice line restores only that unit and recalculates invoice totals. The last line is protected; delete the complete invoice instead.
- Editing a partner updates its linked invoice contact snapshots and any `Partner:` inventory labels.
- Deleting a partner profile preserves historical invoices. A partner holding assigned stock must return or reassign it first.
- Sold and partner-assigned inventory units cannot be deleted. Available and returned units can be removed individually.

Every successful correction increments the central revision and is broadcast to other connected devices.

## Default logins

- POS: `ICONM@2026`
- Admin/settings: `ADMIN@2026`
- Wholesale: `ADMIN@WS`

Change them before production. Copy `.env.example` to `.env`, edit the three password values, then restart the server. Passwords are checked by the Python server and are no longer embedded in the browser pages.

## Backups and pen drives

Open **Settings & Backup** from the POS/wholesale menu or go to `/settings.html`.

- **Download backup** creates a transactionally consistent SQLite `.db` file while users remain online.
- **Save to pen drive** opens the browser file picker when supported. Select the pen drive as the destination. On browsers without that API, the backup downloads normally and can be moved to the pen drive.
- **Share / WhatsApp** uses the phone/computer share sheet with the actual `.db` file when supported. Otherwise it downloads the file and opens WhatsApp with instructions to attach it.
- **Restore / import backup** validates SQLite integrity and the application schema. Before replacement, the server creates a separate pre-restore backup automatically.
- CSV exports are available for inventory, normalized clients, and transactions. CSV is for reporting; use the SQLite `.db` backup for a full restore.

The server also creates a rolling automatic local snapshot (daily by default, retaining 14 automatic files). This does not replace an external pen-drive copy.

## Weekly backup reminder

The default reminder interval is seven days. Its state is stored centrally in SQLite, and logged-in devices display a reminder until a complete backup is downloaded/shared. Change the reminder and automatic-snapshot schedules in Settings.

## Multi-device synchronization

- SQLite WAL mode, foreign keys, busy retry, and `BEGIN IMMEDIATE` serialize writes safely.
- Sale, issue, return, payment, inventory state, audit revision, and idempotency receipt commit in one transaction.
- A unit can be sold by only one device. Concurrent attempts are tested; one succeeds and the other receives a stock conflict.
- WebSocket revision events broadcast immediately after a commit. Other devices refetch the canonical snapshot.
- The Settings page shows live device/IP connections and copyable LAN URLs/QR codes.
- Browser snapshots are read-only when the server is unavailable. Offline checkout is intentionally blocked to prevent two disconnected devices from selling the same unit.

Never put the live `erp.db` file on a shared network drive and never let multiple programs open separate writable copies. Devices share data through the Python API only.

## Import old exported CSV data

The local SQLite files originally present in this folder contained zero records. If historical data still exists in the old system, export the inventory and client/ledger sheets to local CSV files first, then run:

```powershell
.\.venv\Scripts\python.exe scripts\import_legacy_csv.py --inventory inventory.csv --transactions clients.csv
```

The importer never contacts Google. It imports current inventory state as-is and stores historical transactions without replaying old stock changes.

## Architecture

- Python/FastAPI: authenticated runtime API, local static hosting, QR/barcode generation, backup/restore, WebSocket sync.
- SQLite: products, physical units, normalized clients, transactions, transaction items, operation receipts, settings, revision/change log.
- Node.js: setup/build tool only. It vendors React/Babel/fonts and compiles Tailwind CSS so the pages do not need public CDNs. It is not a second backend.
- Mobile scanner transport: a local FastAPI WebSocket channel; no public PeerJS signaling server is used.
- Browser local storage/service worker: read-only last snapshot, drafts, device identity, reminder display state, and offline application shell.

Private files such as `erp.db`, WAL/SHM files, `.env`, Python source, session secret, and `_backups` are not exposed by the web server.

## Verification

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe scripts\smoke_test.py
npm run check:html
npm run build
```

The smoke test uses a temporary database and backup folder; it does not change production data.
