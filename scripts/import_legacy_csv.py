r"""One-time importer for CSV files exported from the old system.

Usage:
    .venv\Scripts\python.exe scripts\import_legacy_csv.py \
        --inventory inventory.csv --transactions clients.csv

The importer never contacts Google. Export/download the two files first and
keep them locally. Current inventory state is imported as-is; historical ledger
rows are stored without replaying their stock changes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend_store import SQLiteStore, StoreError, canonical_json


def joined_json(row: dict) -> str:
    chunks = []
    if row.get("DATA (JSON)"):
        chunks.append(row["DATA (JSON)"])
    numbered = []
    for key, value in row.items():
        if key.upper().startswith("DATA_") and key[5:].isdigit() and value:
            numbered.append((int(key[5:]), value))
    chunks.extend(value for _, value in sorted(numbered))
    return "".join(chunks)


def read_inventory(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            embedded = joined_json(row)
            if embedded:
                row["DATA (JSON)"] = embedded
            if any(str(value or "").strip() for value in row.values()):
                yield row


def read_transactions(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            candidate = joined_json(row)
            if not candidate:
                candidate = next((str(value) for value in row.values() if str(value or "").lstrip().startswith("{")), "")
            if candidate:
                try:
                    value = json.loads(candidate)
                except json.JSONDecodeError as error:
                    raise RuntimeError(f"Invalid transaction JSON near CSV line {reader.line_num}: {error}") from error
                if isinstance(value, dict):
                    yield value


def main() -> int:
    parser = argparse.ArgumentParser(description="Import old local CSV exports into ICON MOBILE SQLite")
    parser.add_argument("--inventory", type=Path, help="Inventory CSV export")
    parser.add_argument("--transactions", type=Path, help="Customer/ledger CSV export")
    parser.add_argument("--db", type=Path, default=ROOT / "erp.db", help="Target SQLite database")
    args = parser.parse_args()
    if not args.inventory and not args.transactions:
        parser.error("provide --inventory and/or --transactions")

    store = SQLiteStore(args.db.resolve(), ROOT / "_backups")
    added = updated = imported = skipped = 0
    try:
        existing = {item["IMEI or Item Code"].casefold() for item in store.snapshot()["inventory"]}
        if args.inventory:
            for row in read_inventory(args.inventory.resolve()):
                code = str(row.get("IMEI or Item Code") or row.get("IMEI") or "").strip()
                if not code:
                    continue
                action = "update_item" if code.casefold() in existing else "add_item"
                digest = hashlib.sha256(canonical_json(row).encode("utf-8")).hexdigest()[:24]
                store.execute_action(
                    {"action": action, "item": row}, actor_role="admin", device_id="migration",
                    operation_id=f"migration-inventory-{digest}",
                )
                if action == "add_item":
                    added += 1
                    existing.add(code.casefold())
                else:
                    updated += 1
        if args.transactions:
            for record in read_transactions(args.transactions.resolve()):
                if store.import_legacy_transaction(record, source_name=args.transactions.name):
                    imported += 1
                else:
                    skipped += 1
    except (StoreError, OSError, RuntimeError) as error:
        print(f"Import failed: {error}", file=sys.stderr)
        return 1

    print(f"Import complete: {added} inventory added, {updated} updated, {imported} transactions imported, {skipped} duplicates skipped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
