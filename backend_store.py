"""SQLite v2 persistence and compatibility logic for ICON MOBILE.

All write operations are serialized with ``BEGIN IMMEDIATE`` and committed
with their operation receipt, revision, audit row, inventory changes, and
transaction record in one SQLite transaction.  Existing legacy tables in
``erp.db`` are intentionally left untouched.
"""

from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import json
import os
import re
import shutil
import sqlite3
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from copy import deepcopy
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple


SCHEMA_VERSION = "icon-mobile.sqlite.v2"
TRANSACTION_TYPES = {"Sale", "Issue", "Return", "B2B_Payment"}
_PARTNER_PREFIX = "Partner:"


class StoreError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def json_object(value: Any, fallback: Optional[dict] = None) -> dict:
    if isinstance(value, dict):
        return deepcopy(value)
    if isinstance(value, str) and value.strip():
        try:
            loaded = json.loads(value)
            return loaded if isinstance(loaded, dict) else (fallback or {})
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
    return deepcopy(fallback or {})


def first_value(mapping: Mapping[str, Any], keys: Sequence[str], default: Any = "") -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None and str(value).strip() != "":
            return value
    return default


def clean_text(value: Any, *, maximum: int = 500) -> str:
    text = str(value if value is not None else "").strip()
    if len(text) > maximum:
        raise StoreError(422, "field_too_long", f"Text field exceeds {maximum} characters")
    return text


def safe_int(value: Any, default: int = 0, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    try:
        result = int(Decimal(str(value).replace(",", "").strip()))
    except (InvalidOperation, ValueError, TypeError):
        result = default
    if minimum is not None and result < minimum:
        result = minimum
    if maximum is not None and result > maximum:
        raise StoreError(422, "number_too_large", f"Number exceeds {maximum}")
    return result


def money_to_cents(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    raw = str(value).replace(",", "").strip()
    raw = re.sub(r"[^0-9.\-+]", "", raw)
    try:
        amount = Decimal(raw)
        if not amount.is_finite():
            raise InvalidOperation
        cents = int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError):
        return default
    if abs(cents) > 100_000_000_000_00:
        raise StoreError(422, "amount_too_large", "Money amount is outside the supported range")
    return cents


def cents_to_legacy(cents: int) -> Any:
    if cents % 100 == 0:
        return cents // 100
    return float(Decimal(cents) / Decimal(100))


def normalized_status(value: Any, default: str = "Available") -> str:
    status = clean_text(value or default, maximum=220)
    lowered = status.lower()
    if lowered == "available":
        return "Available"
    if lowered == "sold":
        return "Sold"
    if lowered == "returned":
        return "Returned"
    if lowered == "deleted":
        return "Deleted"
    if lowered.startswith("partner:"):
        name = status.split(":", 1)[1].strip()
        if not name:
            raise StoreError(422, "invalid_status", "Partner inventory status needs a partner name")
        return f"{_PARTNER_PREFIX}{name}"
    raise StoreError(422, "invalid_status", f"Unsupported inventory status '{status}'")


def request_digest(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


class SQLiteStore:
    def __init__(self, database_path: Path, backup_dir: Path, *, busy_timeout_ms: int = 15_000) -> None:
        self.database_path = Path(database_path)
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.busy_timeout_ms = max(1_000, int(busy_timeout_ms))
        self._maintenance_lock = threading.RLock()
        self._maintenance_condition = threading.Condition(self._maintenance_lock)
        self._active_connections = 0
        self._maintenance = False
        self.initialize()

    def connect(self, *, read_only: bool = False) -> sqlite3.Connection:
        if read_only:
            uri = self.database_path.resolve().as_uri() + "?mode=ro"
            conn = sqlite3.connect(uri, uri=True, timeout=self.busy_timeout_ms / 1000, isolation_level=None)
        else:
            conn = sqlite3.connect(
                str(self.database_path), timeout=self.busy_timeout_ms / 1000,
                isolation_level=None,
            )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys=ON")
        if not read_only:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
        return conn

    @contextmanager
    def connection(self, *, read_only: bool = False) -> Iterator[sqlite3.Connection]:
        with self._maintenance_condition:
            if self._maintenance:
                raise StoreError(503, "maintenance", "Database maintenance is in progress")
            self._active_connections += 1
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = self.connect(read_only=read_only)
            yield conn
        finally:
            if conn is not None:
                conn.close()
            with self._maintenance_condition:
                self._active_connections -= 1
                self._maintenance_condition.notify_all()

    def initialize(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._maintenance_lock:
            conn = self.connect()
            try:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS v2_meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS v2_products (
                        id INTEGER PRIMARY KEY,
                        sku TEXT NOT NULL COLLATE NOCASE UNIQUE,
                        item_type TEXT NOT NULL DEFAULT '',
                        category TEXT NOT NULL DEFAULT '',
                        brand TEXT NOT NULL DEFAULT '',
                        model TEXT NOT NULL DEFAULT '',
                        color TEXT NOT NULL DEFAULT '',
                        specs_json TEXT NOT NULL DEFAULT '{}',
                        price_cents INTEGER NOT NULL DEFAULT 0 CHECK(price_cents >= 0),
                        offer_price_cents INTEGER NOT NULL DEFAULT 0 CHECK(offer_price_cents >= 0),
                        aggregate_quantity INTEGER NOT NULL DEFAULT 0 CHECK(aggregate_quantity >= 0),
                        notes TEXT NOT NULL DEFAULT '',
                        legacy_json TEXT NOT NULL DEFAULT '{}',
                        deleted INTEGER NOT NULL DEFAULT 0 CHECK(deleted IN (0,1)),
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        revision INTEGER NOT NULL DEFAULT 0
                    );

                    CREATE TABLE IF NOT EXISTS v2_units (
                        id INTEGER PRIMARY KEY,
                        product_id INTEGER NOT NULL REFERENCES v2_products(id) ON DELETE RESTRICT,
                        unit_code TEXT NOT NULL COLLATE NOCASE UNIQUE,
                        supplier TEXT NOT NULL DEFAULT '',
                        cost_cents INTEGER NOT NULL DEFAULT 0 CHECK(cost_cents >= 0),
                        status TEXT NOT NULL DEFAULT 'Available'
                            CHECK(status IN ('Available','Sold','Returned','Deleted') OR status LIKE 'Partner:%'),
                        date_added TEXT NOT NULL,
                        deleted INTEGER NOT NULL DEFAULT 0 CHECK(deleted IN (0,1)),
                        updated_at TEXT NOT NULL,
                        revision INTEGER NOT NULL DEFAULT 0
                    );

                    CREATE TABLE IF NOT EXISTS v2_clients (
                        id INTEGER PRIMARY KEY,
                        client_key TEXT NOT NULL COLLATE NOCASE UNIQUE,
                        name TEXT NOT NULL,
                        phone TEXT NOT NULL DEFAULT '',
                        email TEXT NOT NULL DEFAULT '',
                        client_type TEXT NOT NULL DEFAULT 'Retail'
                            CHECK(client_type IN ('Retail','B2B')),
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        revision INTEGER NOT NULL DEFAULT 0
                    );

                    CREATE TABLE IF NOT EXISTS v2_transactions (
                        id INTEGER PRIMARY KEY,
                        invoice_id TEXT NOT NULL COLLATE NOCASE UNIQUE,
                        client_id INTEGER REFERENCES v2_clients(id) ON DELETE RESTRICT,
                        transaction_type TEXT NOT NULL
                            CHECK(transaction_type IN ('Sale','Issue','Return','B2B_Payment')),
                        record_type TEXT NOT NULL DEFAULT '',
                        source_system TEXT NOT NULL DEFAULT '',
                        client_name TEXT NOT NULL DEFAULT '',
                        client_phone TEXT NOT NULL DEFAULT '',
                        client_email TEXT NOT NULL DEFAULT '',
                        payment_method TEXT NOT NULL DEFAULT '',
                        subtotal_cents INTEGER NOT NULL DEFAULT 0,
                        discount_cents INTEGER NOT NULL DEFAULT 0,
                        total_cents INTEGER NOT NULL DEFAULT 0,
                        quantity INTEGER NOT NULL DEFAULT 0,
                        linked_invoice_id TEXT NOT NULL DEFAULT '',
                        request_hash TEXT NOT NULL,
                        raw_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        revision INTEGER NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS v2_transaction_items (
                        id INTEGER PRIMARY KEY,
                        transaction_id INTEGER NOT NULL REFERENCES v2_transactions(id) ON DELETE RESTRICT,
                        product_id INTEGER REFERENCES v2_products(id) ON DELETE RESTRICT,
                        unit_id INTEGER REFERENCES v2_units(id) ON DELETE RESTRICT,
                        unit_code TEXT NOT NULL DEFAULT '',
                        group_code TEXT NOT NULL DEFAULT '',
                        quantity INTEGER NOT NULL DEFAULT 1 CHECK(quantity > 0),
                        price_cents INTEGER NOT NULL DEFAULT 0,
                        discount_cents INTEGER NOT NULL DEFAULT 0,
                        cost_cents INTEGER NOT NULL DEFAULT 0,
                        raw_json TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS v2_operation_receipts (
                        operation_id TEXT PRIMARY KEY,
                        device_id TEXT NOT NULL,
                        actor_role TEXT NOT NULL,
                        action TEXT NOT NULL,
                        request_hash TEXT NOT NULL,
                        invoice_id TEXT NOT NULL DEFAULT '',
                        revision INTEGER NOT NULL,
                        response_json TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS v2_change_log (
                        revision INTEGER PRIMARY KEY,
                        operation_id TEXT NOT NULL,
                        device_id TEXT NOT NULL,
                        actor_role TEXT NOT NULL,
                        action TEXT NOT NULL,
                        entity_type TEXT NOT NULL,
                        entity_id TEXT NOT NULL,
                        summary_json TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS ix_v2_products_updated ON v2_products(deleted, updated_at DESC);
                    CREATE INDEX IF NOT EXISTS ix_v2_units_product ON v2_units(product_id, deleted, status);
                    CREATE INDEX IF NOT EXISTS ix_v2_clients_updated ON v2_clients(updated_at DESC, id DESC);
                    CREATE INDEX IF NOT EXISTS ix_v2_clients_name ON v2_clients(name COLLATE NOCASE);
                    CREATE INDEX IF NOT EXISTS ix_v2_transactions_created ON v2_transactions(created_at DESC, id DESC);
                    CREATE INDEX IF NOT EXISTS ix_v2_transactions_type ON v2_transactions(transaction_type, created_at DESC);
                    CREATE INDEX IF NOT EXISTS ix_v2_transaction_items_unit ON v2_transaction_items(unit_code);
                    CREATE INDEX IF NOT EXISTS ix_v2_receipts_created ON v2_operation_receipts(created_at DESC);
                    """
                )
                conn.execute("INSERT OR IGNORE INTO v2_meta(key,value) VALUES('schema_version',?)", (SCHEMA_VERSION,))
                conn.execute("INSERT OR IGNORE INTO v2_meta(key,value) VALUES('revision','0')")
                conn.execute("INSERT OR IGNORE INTO v2_meta(key,value) VALUES('backup_interval_days','7')")
                conn.execute("INSERT OR IGNORE INTO v2_meta(key,value) VALUES('automatic_backup_interval_hours','24')")
                # v2 databases created by an earlier build did not have the
                # normalized client foreign key. SQLite supports this additive
                # migration without rebuilding historical rows.
                transaction_columns = {
                    row[1] for row in conn.execute("PRAGMA table_info(v2_transactions)")
                }
                if "client_id" not in transaction_columns:
                    conn.execute(
                        "ALTER TABLE v2_transactions ADD COLUMN client_id INTEGER REFERENCES v2_clients(id)"
                    )
                schema = conn.execute("SELECT value FROM v2_meta WHERE key='schema_version'").fetchone()
                if not schema or schema[0] != SCHEMA_VERSION:
                    raise RuntimeError(f"Unsupported SQLite schema: {schema[0] if schema else 'missing'}")
            finally:
                conn.close()

    def current_revision(self, conn: Optional[sqlite3.Connection] = None) -> int:
        if conn is not None:
            row = conn.execute("SELECT value FROM v2_meta WHERE key='revision'").fetchone()
            return int(row[0]) if row else 0
        with self.connection(read_only=True) as opened:
            return self.current_revision(opened)

    def _next_revision(self, conn: sqlite3.Connection) -> int:
        conn.execute("UPDATE v2_meta SET value=CAST(value AS INTEGER)+1 WHERE key='revision'")
        return self.current_revision(conn)

    @staticmethod
    def _meta_get(conn: sqlite3.Connection, key: str, default: str = "") -> str:
        row = conn.execute("SELECT value FROM v2_meta WHERE key=?", (key,)).fetchone()
        return str(row[0]) if row else default

    @staticmethod
    def _meta_set(conn: sqlite3.Connection, key: str, value: Any) -> None:
        conn.execute(
            "INSERT INTO v2_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

    def _begin_immediate(self, conn: sqlite3.Connection) -> None:
        deadline = time.monotonic() + self.busy_timeout_ms / 1000
        delay = 0.02
        while True:
            try:
                conn.execute("BEGIN IMMEDIATE")
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    raise
                if time.monotonic() >= deadline:
                    raise StoreError(503, "database_busy", "Database is busy; retry the operation") from exc
                time.sleep(delay)
                delay = min(delay * 1.8, 0.35)

    @property
    def maintenance(self) -> bool:
        return self._maintenance

    # ------------------------------------------------------------------
    # Canonical inventory conversion
    # ------------------------------------------------------------------

    def _canonical_item(self, raw_item: Mapping[str, Any]) -> Tuple[dict, List[dict]]:
        if not isinstance(raw_item, Mapping):
            raise StoreError(422, "invalid_item", "Inventory item must be an object")
        embedded = json_object(raw_item.get("DATA (JSON)"))
        merged: Dict[str, Any] = dict(raw_item)
        merged.update(embedded)

        sku = clean_text(first_value(
            merged,
            ["IMEI or Item Code", "IMEI", "imei", "Code", "code", "SKU", "sku"],
        ), maximum=180)
        if not sku:
            raise StoreError(422, "missing_sku", "IMEI / Item Code is required")

        item_type = clean_text(first_value(
            merged, ["Select Phone or item", "Type", "type"], "Mobile Phone"
        ), maximum=120)
        category = clean_text(first_value(merged, ["Category", "category"]), maximum=160)
        brand = clean_text(first_value(merged, ["Brand", "brand"]), maximum=160)
        model = clean_text(first_value(merged, ["Model", "model"]), maximum=220)
        color = clean_text(first_value(merged, ["Color", "color"]), maximum=120)
        notes = clean_text(first_value(merged, ["Notes", "notes", "Note", "note"]), maximum=4000)
        price_cents = money_to_cents(first_value(merged, ["Price", "price", "Selling Price"], 0))
        offer_cents = money_to_cents(first_value(merged, ["OfferPrice", "Offer Price", "offerPrice"], 0))
        if price_cents < 0 or offer_cents < 0:
            raise StoreError(422, "invalid_price", "Inventory prices cannot be negative")

        specs_value = first_value(merged, ["Specs", "specs"], {})
        specs_object = json_object(specs_value)
        if not specs_object and clean_text(specs_value, maximum=4000):
            specs_object = {"Other": clean_text(specs_value, maximum=4000)}

        raw_units: Any = merged.get("Units", [])
        if isinstance(raw_units, str):
            try:
                raw_units = json.loads(raw_units)
            except (TypeError, ValueError, json.JSONDecodeError):
                raw_units = []
        if not isinstance(raw_units, list):
            raw_units = []

        fallback_supplier = clean_text(first_value(merged, ["Supplier", "supplier"]), maximum=220)
        fallback_cost = money_to_cents(first_value(
            merged, ["PurchasedPrice", "Purchased Price", "Cost", "cost", "unitCost"], 0
        ))
        if fallback_cost < 0:
            raise StoreError(422, "invalid_cost", "Inventory cost cannot be negative")

        # Older rows may only contain supplier batches. Convert them to real,
        # stable unit rows once so every checkout has a concrete serial.
        if not raw_units:
            suppliers = merged.get("Suppliers", [])
            if isinstance(suppliers, str):
                try:
                    suppliers = json.loads(suppliers)
                except (TypeError, ValueError, json.JSONDecodeError):
                    suppliers = []
            if isinstance(suppliers, list) and suppliers:
                generated: List[dict] = []
                counter = 0
                for supplier in suppliers:
                    if not isinstance(supplier, Mapping):
                        continue
                    quantity = safe_int(first_value(supplier, ["qty", "Quantity", "quantity"], 1), 1, minimum=1, maximum=5000)
                    for _ in range(quantity):
                        counter += 1
                        generated.append({
                            "imei": sku if counter == 1 and quantity == 1 and len(suppliers) == 1 else f"{sku}-{counter:04d}",
                            "supplier": first_value(supplier, ["name", "supplier"], fallback_supplier),
                            "cost": first_value(supplier, ["cost", "Cost"], cents_to_legacy(fallback_cost)),
                            "status": first_value(supplier, ["status", "Status"], merged.get("Status", "Available")),
                            "dateAdded": first_value(supplier, ["dateAdded", "date"], utc_now()),
                        })
                raw_units = generated

        if not raw_units:
            quantity = safe_int(first_value(merged, ["Quantity", "quantity", "Qty", "qty"], 1), 1, minimum=1, maximum=5000)
            raw_units = [{
                "imei": sku if quantity == 1 else f"{sku}-{index + 1:04d}",
                "supplier": fallback_supplier,
                "cost": cents_to_legacy(fallback_cost),
                "status": merged.get("Status", "Available"),
                "dateAdded": utc_now(),
            } for index in range(quantity)]

        units: List[dict] = []
        seen: set[str] = set()
        for index, raw_unit in enumerate(raw_units):
            if not isinstance(raw_unit, Mapping):
                raise StoreError(422, "invalid_unit", f"Unit {index + 1} must be an object")
            code = clean_text(first_value(
                raw_unit, ["imei", "IMEI", "unitImei", "serial", "code", "id"]
            ), maximum=180)
            if not code:
                raise StoreError(422, "missing_unit_code", f"Unit {index + 1} needs an IMEI / serial")
            folded = code.casefold()
            if folded in seen:
                raise StoreError(409, "duplicate_unit", f"Duplicate IMEI / serial '{code}' in this product")
            seen.add(folded)
            cost_cents = money_to_cents(first_value(
                raw_unit, ["cost", "Cost", "buyPrice", "PurchasedPrice"], cents_to_legacy(fallback_cost)
            ))
            if cost_cents < 0:
                raise StoreError(422, "invalid_cost", f"Cost for unit '{code}' cannot be negative")
            units.append({
                "imei": code,
                "supplier": clean_text(first_value(raw_unit, ["supplier", "Supplier", "name"], fallback_supplier), maximum=220),
                "cost": cents_to_legacy(cost_cents),
                "cost_cents": cost_cents,
                "dateAdded": clean_text(first_value(raw_unit, ["dateAdded", "date"], utc_now()), maximum=80),
                "status": normalized_status(first_value(raw_unit, ["status", "Status"], merged.get("Status", "Available"))),
            })

        legacy = dict(embedded)
        for key in (
            "Brand", "Model", "Category", "Color", "Price", "OfferPrice", "Specs",
            "PurchasedPrice", "Suppliers"
        ):
            if key in merged:
                legacy[key] = merged[key]
        legacy.update({
            "Brand": brand,
            "Model": model,
            "Category": category,
            "Color": color,
            "Price": cents_to_legacy(price_cents),
            "OfferPrice": cents_to_legacy(offer_cents) if offer_cents else "",
            "Specs": canonical_json(specs_object) if specs_object else "",
        })
        legacy.pop("Units", None)
        canonical = {
            "Select Phone or item": item_type,
            "Type": item_type,
            "IMEI or Item Code": sku,
            "IMEI": sku,
            "code": sku,
            "Category": category,
            "Brand": brand,
            "Model": model,
            "Color": color,
            "Price": cents_to_legacy(price_cents),
            "OfferPrice": cents_to_legacy(offer_cents) if offer_cents else "",
            "Specs": canonical_json(specs_object) if specs_object else "",
            "Notes": notes,
        }
        return {"canonical": canonical, "legacy": legacy, "specs": specs_object,
                "price_cents": price_cents, "offer_cents": offer_cents}, units

    def _product_to_dict(self, conn: sqlite3.Connection, product: sqlite3.Row) -> dict:
        units_rows = conn.execute(
            "SELECT * FROM v2_units WHERE product_id=? AND deleted=0 ORDER BY id",
            (product["id"],),
        ).fetchall()
        units = [{
            "imei": row["unit_code"],
            "supplier": row["supplier"],
            "cost": cents_to_legacy(row["cost_cents"]),
            "dateAdded": row["date_added"],
            "status": row["status"],
        } for row in units_rows]
        available = sum(1 for unit in units if unit["status"] == "Available")
        base = json_object(product["legacy_json"])
        specs = json_object(product["specs_json"])
        suppliers: Dict[Tuple[str, int], dict] = {}
        for unit_row in units_rows:
            key = (unit_row["supplier"], unit_row["cost_cents"])
            bucket = suppliers.setdefault(key, {
                "name": unit_row["supplier"],
                "cost": cents_to_legacy(unit_row["cost_cents"]),
                "qty": 0,
                "units": [],
            })
            bucket["qty"] += 1
            bucket["units"].append(next(unit for unit in units if unit["imei"] == unit_row["unit_code"]))

        base.update({
            "Select Phone or item": product["item_type"],
            "Type": product["item_type"],
            "IMEI or Item Code": product["sku"],
            "IMEI": product["sku"],
            "code": product["sku"],
            "Category": product["category"],
            "Brand": product["brand"],
            "Model": product["model"],
            "Color": product["color"],
            "Price": cents_to_legacy(product["price_cents"]),
            "OfferPrice": cents_to_legacy(product["offer_price_cents"]) if product["offer_price_cents"] else "",
            "PurchasedPrice": cents_to_legacy(units_rows[0]["cost_cents"]) if units_rows else 0,
            "Specs": canonical_json(specs) if specs else "",
            "Quantity": str(available),
            "Status": "Available" if available else "Sold",
            "Notes": product["notes"],
            "Suppliers": list(suppliers.values()),
            "Units": units,
            "revision": product["revision"],
            "updatedAt": product["updated_at"],
        })
        return base

    def _transaction_to_dict(self, conn: sqlite3.Connection, row: sqlite3.Row) -> dict:
        value = json_object(row["raw_json"])
        value.setdefault("invoiceId", row["invoice_id"])
        value.setdefault("transactionType", row["transaction_type"])
        value.setdefault("date", row["created_at"])
        value.setdefault("revision", row["revision"])
        # Normalized rows are the accounting source of truth.  Enrich legacy-shaped
        # browser payloads with the committed unit cost so every screen calculates
        # profit from the same values, including transactions created by older builds.
        normalized_items = [
            {
                "unitImei": item["unit_code"],
                "groupCode": item["group_code"],
                "quantity": item["quantity"],
                "unitPrice": cents_to_legacy(item["price_cents"]),
                "discount": cents_to_legacy(item["discount_cents"]),
                "unitCost": cents_to_legacy(item["cost_cents"]),
            }
            for item in conn.execute(
                "SELECT unit_code,group_code,quantity,price_cents,discount_cents,cost_cents "
                "FROM v2_transaction_items WHERE transaction_id=? ORDER BY id",
                (row["id"],),
            ).fetchall()
        ]
        value["normalizedItems"] = normalized_items
        if not normalized_items:
            return value
        for key in ("items", "purchasedItems", "returnedItems"):
            lines = value.get(key)
            if not isinstance(lines, list):
                continue
            for line in lines:
                if not isinstance(line, dict):
                    continue
                allocated = {
                    str(code).casefold() for code in line.get("allocatedUnits", [])
                    if str(code).strip()
                }
                unit_code = clean_text(first_value(
                    line, ["unitImei", "displayImei", "IMEI", "IMEI or Item Code"], ""
                ), maximum=180).casefold()
                group_code = clean_text(first_value(
                    line, ["groupCode", "Original IMEI", "sku", "code"], ""
                ), maximum=180).casefold()
                matches = [item for item in normalized_items if (
                    (allocated and item["unitImei"].casefold() in allocated)
                    or (unit_code and item["unitImei"].casefold() == unit_code)
                    or (not allocated and not unit_code and group_code and item["groupCode"].casefold() == group_code)
                )]
                if matches:
                    average_cost = sum(money_to_cents(item["unitCost"]) for item in matches) // len(matches)
                    line["unitCost"] = cents_to_legacy(average_cost)
                    line["cost"] = cents_to_legacy(average_cost)
                    line["allocatedUnits"] = [item["unitImei"] for item in matches]
        return value

    @staticmethod
    def _asset_class(item_type: str, category: str) -> str:
        text = f"{item_type} {category}".casefold()
        if "phone" in text or "smartphone" in text:
            return "phone"
        accessory_markers = (
            "accessory", "earbud", "headphone", "speaker", "charger", "adapter",
            "cable", "case", "cover", "screen protector", "power bank", "battery",
            "display", "motherboard", "camera module", "keyboard", "mouse", "audio",
        )
        return "accessory" if any(marker in text for marker in accessory_markers) else "other"

    def _asset_summary(self, conn: sqlite3.Connection) -> dict:
        """Return an accounting summary from normalized rows inside the snapshot transaction."""
        products = conn.execute(
            "SELECT id,item_type,category,price_cents,offer_price_cents "
            "FROM v2_products WHERE deleted=0"
        ).fetchall()
        product_classes = {
            product["id"]: self._asset_class(product["item_type"], product["category"])
            for product in products
        }
        metrics = {
            "phoneProducts": 0, "accessoryProducts": 0, "otherProducts": 0,
            "phoneUnits": 0, "accessoryUnits": 0, "otherUnits": 0,
            "availableUnits": 0, "soldUnits": 0, "partnerUnits": 0,
            "stockCostCents": 0, "stockSellingCents": 0, "partnerCostCents": 0,
        }
        for kind in product_classes.values():
            metrics[f"{kind}Products"] += 1

        prices = {
            product["id"]: product["offer_price_cents"] or product["price_cents"]
            for product in products
        }
        units = conn.execute(
            "SELECT product_id,cost_cents,status FROM v2_units WHERE deleted=0"
        ).fetchall()
        for unit in units:
            status = unit["status"]
            kind = product_classes.get(unit["product_id"], "other")
            if status in {"Available", "Returned"}:
                metrics[f"{kind}Units"] += 1
                metrics["availableUnits"] += 1
                metrics["stockCostCents"] += unit["cost_cents"]
                metrics["stockSellingCents"] += prices.get(unit["product_id"], 0)
            elif status == "Sold":
                metrics["soldUnits"] += 1
            elif status.startswith(_PARTNER_PREFIX):
                metrics["partnerUnits"] += 1
                metrics["partnerCostCents"] += unit["cost_cents"]

        sales_revenue = sales_cost = issue_value = issue_cost = payments = 0
        sales_units = issued_units = 0
        transaction_rows = conn.execute(
            """
            SELECT t.transaction_type,t.total_cents,t.quantity,
                   COALESCE(SUM(i.cost_cents * i.quantity),0) AS cost_cents,
                   linked.transaction_type AS linked_type
              FROM v2_transactions AS t
              LEFT JOIN v2_transaction_items AS i ON i.transaction_id=t.id
              LEFT JOIN v2_transactions AS linked
                     ON linked.invoice_id=t.linked_invoice_id COLLATE NOCASE
             GROUP BY t.id
            """
        ).fetchall()
        for transaction in transaction_rows:
            kind = transaction["transaction_type"]
            if kind == "Sale":
                sales_revenue += transaction["total_cents"]
                sales_cost += transaction["cost_cents"]
                sales_units += transaction["quantity"]
            elif kind == "Issue":
                issue_value += transaction["total_cents"]
                issue_cost += transaction["cost_cents"]
                issued_units += transaction["quantity"]
            elif kind == "B2B_Payment":
                payments += transaction["total_cents"]
            elif kind == "Return":
                if transaction["linked_type"] == "Issue":
                    issue_value -= transaction["total_cents"]
                    issue_cost -= transaction["cost_cents"]
                    issued_units -= transaction["quantity"]
                else:
                    sales_revenue -= transaction["total_cents"]
                    sales_cost -= transaction["cost_cents"]
                    sales_units -= transaction["quantity"]

        return {
            "phoneProducts": metrics["phoneProducts"],
            "accessoryProducts": metrics["accessoryProducts"],
            "otherProducts": metrics["otherProducts"],
            "phoneUnits": metrics["phoneUnits"],
            "accessoryUnits": metrics["accessoryUnits"],
            "otherUnits": metrics["otherUnits"],
            "availableUnits": metrics["availableUnits"],
            "soldUnits": metrics["soldUnits"],
            "partnerUnits": metrics["partnerUnits"],
            "stockCostValue": cents_to_legacy(metrics["stockCostCents"]),
            "stockSellingValue": cents_to_legacy(metrics["stockSellingCents"]),
            "partnerCostValue": cents_to_legacy(metrics["partnerCostCents"]),
            "netSalesRevenue": cents_to_legacy(sales_revenue),
            "netSalesCost": cents_to_legacy(sales_cost),
            "grossProfit": cents_to_legacy(sales_revenue - sales_cost),
            "netSoldUnits": sales_units,
            "partnerIssuedValue": cents_to_legacy(issue_value),
            "partnerIssuedCost": cents_to_legacy(issue_cost),
            "netIssuedUnits": issued_units,
            "partnerPaymentsReceived": cents_to_legacy(payments),
        }

    @staticmethod
    def _client_identity_key(name: str, phone: str, email: str) -> str:
        digits = re.sub(r"\D", "", phone or "")
        if len(digits) >= 7:
            return f"phone:{digits}"
        normalized_email = (email or "").strip().casefold()
        if "@" in normalized_email:
            return f"email:{normalized_email}"
        normalized_name = re.sub(r"[^a-z0-9]+", "", (name or "").casefold())
        return f"name:{normalized_name or 'unknown'}"

    def _upsert_client(
        self,
        conn: sqlite3.Connection,
        *,
        name: str,
        phone: str,
        email: str,
        client_type: str,
        revision: int,
    ) -> Optional[int]:
        if not (name or phone or email):
            return None
        key = self._client_identity_key(name, phone, email)
        existing = conn.execute("SELECT client_key FROM v2_clients WHERE client_key=?", (key,)).fetchone()
        if not existing and name:
            # A return/payment may omit a phone that was present on the original
            # invoice. Reuse the matching named contact instead of creating a
            # second client row solely because this event has less detail.
            existing = conn.execute(
                "SELECT client_key FROM v2_clients WHERE lower(name)=lower(?) ORDER BY updated_at DESC LIMIT 1",
                (name,),
            ).fetchone()
        if existing:
            key = existing["client_key"]
        now = utc_now()
        conn.execute(
            """
            INSERT INTO v2_clients(client_key,name,phone,email,client_type,created_at,updated_at,revision)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(client_key) DO UPDATE SET
                name=CASE WHEN excluded.name<>'' THEN excluded.name ELSE v2_clients.name END,
                phone=CASE WHEN excluded.phone<>'' THEN excluded.phone ELSE v2_clients.phone END,
                email=CASE WHEN excluded.email<>'' THEN excluded.email ELSE v2_clients.email END,
                client_type=CASE WHEN excluded.client_type='B2B' THEN 'B2B' ELSE v2_clients.client_type END,
                updated_at=excluded.updated_at,
                revision=excluded.revision
            """,
            (key, name or "Unknown", phone, email, client_type, now, now, revision),
        )
        row = conn.execute("SELECT id FROM v2_clients WHERE client_key=?", (key,)).fetchone()
        return int(row[0]) if row else None

    @staticmethod
    def _client_to_dict(row: sqlite3.Row) -> dict:
        return {
            "id": row["id"], "name": row["name"], "phone": row["phone"],
            "email": row["email"], "type": row["client_type"],
            "createdAt": row["created_at"], "updatedAt": row["updated_at"],
            "revision": row["revision"],
        }

    def list_clients(self) -> List[dict]:
        with self.connection(read_only=True) as conn:
            rows = conn.execute("SELECT * FROM v2_clients ORDER BY updated_at DESC, id DESC").fetchall()
            return [self._client_to_dict(row) for row in rows]

    def snapshot(self, *, include_legacy_csv: bool = False) -> dict:
        with self.connection(read_only=True) as conn:
            conn.execute("BEGIN")
            try:
                revision = self.current_revision(conn)
                products = conn.execute(
                    "SELECT * FROM v2_products WHERE deleted=0 ORDER BY updated_at DESC, id DESC"
                ).fetchall()
                transactions = conn.execute(
                    "SELECT * FROM v2_transactions ORDER BY created_at DESC, id DESC"
                ).fetchall()
                clients = conn.execute(
                    "SELECT * FROM v2_clients ORDER BY updated_at DESC, id DESC"
                ).fetchall()
                inventory = [self._product_to_dict(conn, product) for product in products]
                ledger = [self._transaction_to_dict(conn, row) for row in transactions]
                contacts = [self._client_to_dict(row) for row in clients]
                assets = self._asset_summary(conn)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        data = {
            "schemaVersion": SCHEMA_VERSION,
            "revision": revision,
            "serverTime": utc_now(),
            "inventory": inventory,
            "transactions": ledger,
            "clients": contacts,
            "assets": assets,
        }
        if include_legacy_csv:
            data["inventoryCsv"] = self._inventory_csv(inventory)
            data["clientsCsv"] = self._transactions_csv(ledger)
        return data

    def get_invoice(self, invoice_id: str) -> dict:
        invoice_id = clean_text(invoice_id, maximum=180)
        with self.connection(read_only=True) as conn:
            row = conn.execute(
                "SELECT * FROM v2_transactions WHERE invoice_id=? COLLATE NOCASE", (invoice_id,)
            ).fetchone()
            if not row:
                raise StoreError(404, "invoice_not_found", f"Invoice '{invoice_id}' was not found")
            return self._transaction_to_dict(conn, row)

    def get_operation(self, operation_id: str) -> Optional[dict]:
        operation_id = clean_text(operation_id, maximum=180)
        with self.connection(read_only=True) as conn:
            row = conn.execute(
                "SELECT response_json FROM v2_operation_receipts WHERE operation_id=?", (operation_id,)
            ).fetchone()
            return json_object(row[0]) if row else None

    def import_legacy_transaction(self, raw_record: Mapping[str, Any], *, source_name: str = "legacy_csv") -> bool:
        """Import one historical ledger record without replaying its stock move.

        Inventory CSV is the current stock truth during migration. Replaying old
        sales would decrement it a second time, so this path intentionally stores
        the historical event only. It is not exposed through HTTP.
        """
        if not isinstance(raw_record, Mapping):
            raise StoreError(422, "invalid_import", "Legacy transaction must be an object")
        record = deepcopy(dict(raw_record))
        client = record.get("client") if isinstance(record.get("client"), Mapping) else {}
        client = dict(client)
        invoice_id = clean_text(first_value(
            record, ["invoiceId", "id", "ref"], first_value(client, ["invoiceId"])
        ), maximum=180)
        if not invoice_id:
            invoice_id = "MIG-" + hashlib.sha256(canonical_json(record).encode("utf-8")).hexdigest()[:20].upper()
            record["invoiceId"] = invoice_id
        original_type = clean_text(first_value(
            record, ["transactionType"], first_value(client, ["transactionType"], "Sale")
        ), maximum=120) or "Sale"
        folded = original_type.casefold()
        if folded in {"sale", "pos_sale", "retailsale"}:
            database_type = "Sale"
        elif "payment" in folded:
            database_type = "B2B_Payment"
        elif "return" in folded:
            database_type = "Return"
        else:
            # Issue, partner profile, and older wholesale profile rows all remain
            # distinguishable in raw_json while satisfying the normalized CHECK.
            database_type = "Issue"
        digest = request_digest(record)
        now = utc_now()
        created_at = clean_text(first_value(record, ["date", "createdAt", "timestamp"], now), maximum=100)
        client_name = clean_text(first_value(record, ["name", "customerName", "partnerName"], first_value(client, ["name", "partnerName", "shopName"])), maximum=220)
        client_phone = clean_text(first_value(record, ["phone", "whatsapp"], first_value(client, ["phone", "whatsapp"])), maximum=80)
        client_email = clean_text(first_value(record, ["email"], first_value(client, ["email"])), maximum=240)
        operation_id = "migration-" + hashlib.sha256((source_name + invoice_id).encode("utf-8")).hexdigest()[:32]
        with self.connection() as conn:
            self._begin_immediate(conn)
            try:
                existing = conn.execute(
                    "SELECT request_hash FROM v2_transactions WHERE invoice_id=? COLLATE NOCASE", (invoice_id,)
                ).fetchone()
                if existing:
                    if existing["request_hash"] != digest:
                        raise StoreError(409, "migration_conflict", f"Invoice '{invoice_id}' already exists with different data")
                    conn.execute("COMMIT")
                    return False
                revision = self._next_revision(conn)
                record["revision"] = revision
                record.setdefault("date", created_at)
                partner_text = " ".join([
                    original_type, str(record.get("recordType", "")), str(record.get("sourceSystem", "")),
                    str(client.get("partnerType", "")), str(client.get("clientType", "")),
                ]).casefold()
                imported_client_type = "B2B" if (
                    database_type in {"Issue", "B2B_Payment"}
                    or client.get("isPartner") is True
                    or any(value in partner_text for value in ("partner", "b2b", "wholesale"))
                ) else "Retail"
                client_id = self._upsert_client(
                    conn, name=client_name, phone=client_phone, email=client_email,
                    client_type=imported_client_type, revision=revision,
                )
                items = record.get("purchasedItems") or record.get("items") or []
                quantity = safe_int(first_value(record, ["totalQuantity", "totalQty"], len(items) if isinstance(items, list) else 0), 0, minimum=0)
                subtotal = money_to_cents(first_value(record, ["subTotal", "subtotal"], 0))
                discount = money_to_cents(record.get("discount"), 0)
                total = money_to_cents(first_value(record, ["total", "totalPrice", "paymentAmount", "refundValue"], 0))
                linked = clean_text(first_value(record, ["linkedInvoiceId", "invoiceRef"], first_value(client, ["linkedInvoiceId", "invoiceRef"])), maximum=180)
                conn.execute(
                    "INSERT INTO v2_transactions(invoice_id,client_id,transaction_type,record_type,source_system,client_name,client_phone,client_email,payment_method,subtotal_cents,discount_cents,total_cents,quantity,linked_invoice_id,request_hash,raw_json,created_at,revision) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (invoice_id, client_id, database_type, clean_text(record.get("recordType"), maximum=120),
                     clean_text(record.get("sourceSystem") or source_name, maximum=180), client_name,
                     client_phone, client_email, clean_text(first_value(record, ["paymentMethod"], first_value(client, ["paymentMethod"])), maximum=160),
                     subtotal, discount, total, quantity, linked, digest, canonical_json(record), created_at, revision),
                )
                conn.execute(
                    "INSERT INTO v2_change_log(revision,operation_id,device_id,actor_role,action,entity_type,entity_id,summary_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (revision, operation_id, "migration", "admin", "import_transaction", "transaction",
                     invoice_id, canonical_json({"source": source_name}), now),
                )
                conn.execute("COMMIT")
                return True
            except Exception:
                conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------
    # Atomic mutation dispatcher
    # ------------------------------------------------------------------

    def execute_action(
        self,
        payload: Mapping[str, Any],
        *,
        actor_role: str,
        device_id: str,
        operation_id: str,
    ) -> dict:
        if not isinstance(payload, Mapping):
            raise StoreError(422, "invalid_payload", "Request payload must be an object")
        action = clean_text(payload.get("action"), maximum=80)
        aliases = {
            "addInventory": "add_item", "addProduct": "add_item",
            "updateInventory": "update_item", "updateProduct": "update_item",
            "deleteInventory": "delete_item", "deleteProduct": "delete_item",
            "sale": "checkout", "issue": "checkout", "return": "checkout",
        }
        action = aliases.get(action, action)
        if action not in {"add_item", "update_item", "delete_item", "checkout"}:
            raise StoreError(400, "unknown_action", f"Unsupported action '{action}'")
        if actor_role not in {"pos", "admin", "wholesale"}:
            raise StoreError(403, "forbidden", "Invalid application role")
        if action == "delete_item" and actor_role not in {"admin", "wholesale"}:
            raise StoreError(403, "forbidden", "This role cannot delete inventory")

        operation_id = clean_text(operation_id or str(uuid.uuid4()), maximum=180)
        device_id = clean_text(device_id or "unknown-device", maximum=180)
        digest_payload = dict(payload)
        digest_payload.pop("operationId", None)
        digest_payload.pop("deviceId", None)
        digest = request_digest(digest_payload)

        with self.connection() as conn:
            self._begin_immediate(conn)
            try:
                prior = conn.execute(
                    "SELECT * FROM v2_operation_receipts WHERE operation_id=?", (operation_id,)
                ).fetchone()
                if prior:
                    if prior["request_hash"] != digest:
                        raise StoreError(409, "operation_conflict", "Operation ID was already used for different data")
                    response = json_object(prior["response_json"])
                    response["duplicate"] = True
                    conn.execute("COMMIT")
                    return response

                # An invoice ID is a second idempotency boundary. It protects a
                # retry even when an older browser generated a fresh operation ID.
                invoice_id = clean_text(payload.get("invoiceId"), maximum=180) if action == "checkout" else ""
                if invoice_id:
                    existing_tx = conn.execute(
                        "SELECT * FROM v2_transactions WHERE invoice_id=? COLLATE NOCASE", (invoice_id,)
                    ).fetchone()
                    if existing_tx:
                        if existing_tx["request_hash"] != digest:
                            raise StoreError(409, "invoice_conflict", f"Invoice ID '{invoice_id}' already exists")
                        response = {
                            "success": True,
                            "duplicate": True,
                            "message": "This transaction was already saved.",
                            "data": {
                                "revision": existing_tx["revision"],
                                "invoiceId": existing_tx["invoice_id"],
                                "transaction": self._transaction_to_dict(conn, existing_tx),
                            },
                        }
                        conn.execute(
                            "INSERT INTO v2_operation_receipts(operation_id,device_id,actor_role,action,request_hash,invoice_id,revision,response_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                            (operation_id, device_id, actor_role, action, digest, existing_tx["invoice_id"],
                             existing_tx["revision"], canonical_json(response), utc_now()),
                        )
                        conn.execute("COMMIT")
                        return response

                revision = self._next_revision(conn)
                if action in {"add_item", "update_item"}:
                    result, entity_id = self._save_inventory(
                        conn, payload.get("item"), revision=revision, create=(action == "add_item")
                    )
                    message = "Inventory item added." if action == "add_item" else "Inventory item updated."
                    data = {"revision": revision, "item": result}
                    entity_type = "inventory"
                elif action == "delete_item":
                    entity_id = self._delete_inventory(conn, payload, revision=revision)
                    message = "Inventory item deleted."
                    data = {"revision": revision, "deletedCode": entity_id}
                    entity_type = "inventory"
                else:
                    transaction_type = clean_text(payload.get("transactionType") or payload.get("txn_type") or "Sale", maximum=80)
                    if transaction_type not in TRANSACTION_TYPES:
                        raise StoreError(422, "invalid_transaction_type", f"Unsupported transaction type '{transaction_type}'")
                    if transaction_type == "Sale" and actor_role not in {"pos", "admin"}:
                        raise StoreError(403, "forbidden", "Wholesale sessions cannot create retail sales")
                    if transaction_type in {"Issue", "B2B_Payment"} and actor_role not in {"wholesale", "admin"}:
                        raise StoreError(403, "forbidden", "This transaction requires wholesale access")
                    transaction = self._checkout(conn, dict(payload), transaction_type, revision, digest)
                    entity_id = transaction["invoiceId"]
                    entity_type = "transaction"
                    message = {
                        "Sale": "Sale saved and stock committed.",
                        "Issue": "Partner invoice saved and stock assigned.",
                        "Return": "Return saved and stock restored.",
                        "B2B_Payment": "Partner payment saved to the ledger.",
                    }[transaction_type]
                    data = {"revision": revision, "invoiceId": entity_id, "transaction": transaction}

                response = {"success": True, "duplicate": False, "message": message, "data": data}
                conn.execute(
                    "INSERT INTO v2_change_log(revision,operation_id,device_id,actor_role,action,entity_type,entity_id,summary_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (revision, operation_id, device_id, actor_role, action, entity_type, entity_id,
                     canonical_json({"message": message}), utc_now()),
                )
                conn.execute(
                    "INSERT INTO v2_operation_receipts(operation_id,device_id,actor_role,action,request_hash,invoice_id,revision,response_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (operation_id, device_id, actor_role, action, digest,
                     entity_id if entity_type == "transaction" else "", revision,
                     canonical_json(response), utc_now()),
                )
                conn.execute("COMMIT")
                return response
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def _save_inventory(
        self, conn: sqlite3.Connection, raw_item: Any, *, revision: int, create: bool
    ) -> Tuple[dict, str]:
        item, units = self._canonical_item(raw_item if isinstance(raw_item, Mapping) else {})
        canonical = item["canonical"]
        sku = canonical["IMEI or Item Code"]
        existing = conn.execute(
            "SELECT * FROM v2_products WHERE sku=? COLLATE NOCASE", (sku,)
        ).fetchone()
        if create and existing and not existing["deleted"]:
            raise StoreError(409, "duplicate_sku", f"Product code '{sku}' already exists")
        if not create and (not existing or existing["deleted"]):
            raise StoreError(404, "product_not_found", f"Product code '{sku}' was not found")

        now = utc_now()
        if not existing:
            cursor = conn.execute(
                "INSERT INTO v2_products(sku,item_type,category,brand,model,color,specs_json,price_cents,offer_price_cents,aggregate_quantity,notes,legacy_json,deleted,created_at,updated_at,revision) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,?)",
                (sku, canonical["Type"], canonical["Category"], canonical["Brand"], canonical["Model"],
                 canonical["Color"], canonical_json(item["specs"]), item["price_cents"], item["offer_cents"],
                 0, canonical["Notes"], canonical_json(item["legacy"]), now, now, revision),
            )
            product_id = cursor.lastrowid
        else:
            product_id = existing["id"]
            conn.execute(
                "UPDATE v2_products SET item_type=?,category=?,brand=?,model=?,color=?,specs_json=?,price_cents=?,offer_price_cents=?,notes=?,legacy_json=?,deleted=0,updated_at=?,revision=? WHERE id=?",
                (canonical["Type"], canonical["Category"], canonical["Brand"], canonical["Model"],
                 canonical["Color"], canonical_json(item["specs"]), item["price_cents"], item["offer_cents"],
                 canonical["Notes"], canonical_json(item["legacy"]), now, revision, product_id),
            )

        incoming_codes = {unit["imei"].casefold() for unit in units}
        old_units = conn.execute("SELECT * FROM v2_units WHERE product_id=?", (product_id,)).fetchall()
        for old in old_units:
            if old["unit_code"].casefold() not in incoming_codes and not old["deleted"]:
                if old["status"] not in {"Available", "Returned", "Deleted"}:
                    raise StoreError(
                        409, "unit_in_use",
                        f"Unit '{old['unit_code']}' is {old['status']} and cannot be removed from the product",
                    )
                conn.execute(
                    "UPDATE v2_units SET deleted=1,status='Deleted',updated_at=?,revision=? WHERE id=?",
                    (now, revision, old["id"]),
                )

        for unit in units:
            collision = conn.execute(
                "SELECT * FROM v2_units WHERE unit_code=? COLLATE NOCASE", (unit["imei"],)
            ).fetchone()
            if collision and collision["product_id"] != product_id:
                raise StoreError(409, "duplicate_unit", f"IMEI / serial '{unit['imei']}' belongs to another product")
            if collision:
                conn.execute(
                    "UPDATE v2_units SET supplier=?,cost_cents=?,status=?,date_added=?,deleted=0,updated_at=?,revision=? WHERE id=?",
                    (unit["supplier"], unit["cost_cents"], unit["status"], unit["dateAdded"],
                     now, revision, collision["id"]),
                )
            else:
                conn.execute(
                    "INSERT INTO v2_units(product_id,unit_code,supplier,cost_cents,status,date_added,deleted,updated_at,revision) VALUES(?,?,?,?,?,?,0,?,?)",
                    (product_id, unit["imei"], unit["supplier"], unit["cost_cents"], unit["status"],
                     unit["dateAdded"], now, revision),
                )
        self._refresh_product(conn, product_id, revision)
        product = conn.execute("SELECT * FROM v2_products WHERE id=?", (product_id,)).fetchone()
        return self._product_to_dict(conn, product), sku

    def _delete_inventory(self, conn: sqlite3.Connection, payload: Mapping[str, Any], *, revision: int) -> str:
        code = clean_text(first_value(payload, ["imei", "sku", "code", "product_id"]), maximum=180)
        if not code:
            raise StoreError(422, "missing_sku", "Product code is required")
        product = conn.execute(
            "SELECT * FROM v2_products WHERE sku=? COLLATE NOCASE AND deleted=0", (code,)
        ).fetchone()
        if not product:
            unit = conn.execute(
                "SELECT * FROM v2_units WHERE unit_code=? COLLATE NOCASE AND deleted=0", (code,)
            ).fetchone()
            if unit:
                if unit["status"] not in {"Available", "Returned"}:
                    raise StoreError(409, "unit_in_use", f"Unit '{code}' is {unit['status']} and cannot be deleted")
                conn.execute(
                    "UPDATE v2_units SET deleted=1,status='Deleted',updated_at=?,revision=? WHERE id=?",
                    (utc_now(), revision, unit["id"]),
                )
                self._refresh_product(conn, unit["product_id"], revision)
                return code
            raise StoreError(404, "product_not_found", f"Inventory code '{code}' was not found")
        active = conn.execute(
            "SELECT unit_code,status FROM v2_units WHERE product_id=? AND deleted=0 AND status NOT IN ('Available','Returned')",
            (product["id"],),
        ).fetchone()
        if active:
            raise StoreError(409, "product_in_use", f"Unit '{active['unit_code']}' is {active['status']}; product cannot be deleted")
        now = utc_now()
        conn.execute(
            "UPDATE v2_units SET deleted=1,status='Deleted',updated_at=?,revision=? WHERE product_id=? AND deleted=0",
            (now, revision, product["id"]),
        )
        conn.execute(
            "UPDATE v2_products SET deleted=1,aggregate_quantity=0,updated_at=?,revision=? WHERE id=?",
            (now, revision, product["id"]),
        )
        return product["sku"]

    def _refresh_product(self, conn: sqlite3.Connection, product_id: int, revision: int) -> None:
        available = conn.execute(
            "SELECT COUNT(*) FROM v2_units WHERE product_id=? AND deleted=0 AND status='Available'",
            (product_id,),
        ).fetchone()[0]
        conn.execute(
            "UPDATE v2_products SET aggregate_quantity=?,updated_at=?,revision=? WHERE id=?",
            (available, utc_now(), revision, product_id),
        )

    # ------------------------------------------------------------------
    # Checkout / issue / return / payment
    # ------------------------------------------------------------------

    def _checkout(
        self,
        conn: sqlite3.Connection,
        payload: dict,
        transaction_type: str,
        revision: int,
        digest: str,
    ) -> dict:
        invoice_id = clean_text(payload.get("invoiceId"), maximum=180)
        if not invoice_id:
            prefix = {"Sale": "INV", "Issue": "B2B", "Return": "RET", "B2B_Payment": "PMT"}[transaction_type]
            invoice_id = f"{prefix}-{dt.datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:10].upper()}"
        client = payload.get("client") if isinstance(payload.get("client"), Mapping) else {}
        client = dict(client)
        client_name = clean_text(first_value(client, ["name", "partnerName", "shopName"], payload.get("client_name", "")), maximum=220)
        client_phone = clean_text(first_value(client, ["phone", "whatsapp"], payload.get("phone", "")), maximum=80)
        client_email = clean_text(first_value(client, ["email"], payload.get("email", "")), maximum=240)
        if transaction_type in {"Sale", "Issue", "B2B_Payment"} and not client_name:
            raise StoreError(422, "missing_client", "Customer / partner name is required")
        if transaction_type == "Issue" and not client_name:
            raise StoreError(422, "missing_partner", "Partner name is required for an issue")
        partner_markers = " ".join([
            transaction_type,
            str(payload.get("recordType", "")),
            str(payload.get("sourceSystem", "")),
            str(client.get("partnerType", "")),
            str(client.get("clientType", "")),
        ]).casefold()
        client_type = "B2B" if (
            transaction_type in {"Issue", "B2B_Payment"}
            or client.get("isPartner") is True
            or any(marker in partner_markers for marker in ("partner", "b2b", "wholesale"))
        ) else "Retail"
        client_id = self._upsert_client(
            conn, name=client_name, phone=client_phone, email=client_email,
            client_type=client_type, revision=revision,
        )

        raw_items = payload.get("items", [])
        if transaction_type == "Return" and not raw_items:
            raw_items = payload.get("returnedItems", [])
        if not isinstance(raw_items, list):
            raise StoreError(422, "invalid_items", "Transaction items must be an array")
        if transaction_type in {"Sale", "Issue", "Return"} and not raw_items:
            raise StoreError(422, "empty_transaction", "At least one item is required")

        linked_invoice = clean_text(first_value(
            payload, ["linkedInvoiceId", "invoiceRef"], first_value(client, ["linkedInvoiceId", "invoiceRef"])
        ), maximum=180)
        if linked_invoice:
            linked = conn.execute(
                "SELECT id FROM v2_transactions WHERE invoice_id=? COLLATE NOCASE", (linked_invoice,)
            ).fetchone()
            if not linked and transaction_type in {"Return", "B2B_Payment"}:
                raise StoreError(409, "linked_invoice_missing", f"Linked invoice '{linked_invoice}' does not exist")

        stored_items: List[dict] = []
        item_rows: List[dict] = []
        touched_products: set[int] = set()
        calculated_total = 0
        calculated_qty = 0
        line_discount_total = 0

        if transaction_type in {"Sale", "Issue"}:
            destination = "Sold" if transaction_type == "Sale" else f"{_PARTNER_PREFIX}{client_name}"
            for position, raw_line in enumerate(raw_items):
                if not isinstance(raw_line, Mapping):
                    raise StoreError(422, "invalid_item", f"Line {position + 1} must be an object")
                line = dict(raw_line)
                quantity = safe_int(first_value(line, ["cartQty", "Quantity", "qty"], 1), 1, minimum=1, maximum=5000)
                product, selected = self._select_available_units(conn, line, quantity)
                price_cents = money_to_cents(first_value(line, ["finalPrice", "sold_price", "Price", "price", "OfferPrice"], 0))
                discount_cents = money_to_cents(first_value(line, ["itemDiscount", "discount"], 0))
                if price_cents < 0 or discount_cents < 0:
                    raise StoreError(422, "invalid_amount", "Sale price and discount cannot be negative")
                allocated = []
                for unit in selected:
                    changed = conn.execute(
                        "UPDATE v2_units SET status=?,updated_at=?,revision=? WHERE id=? AND deleted=0 AND status='Available'",
                        (destination, utc_now(), revision, unit["id"]),
                    )
                    if changed.rowcount != 1:
                        raise StoreError(409, "stock_conflict", f"Unit '{unit['unit_code']}' was just taken by another device")
                    allocated.append(unit["unit_code"])
                    item_rows.append({
                        "product_id": product["id"], "unit_id": unit["id"],
                        "unit_code": unit["unit_code"], "group_code": product["sku"],
                        "quantity": 1, "price_cents": price_cents,
                        "discount_cents": discount_cents, "cost_cents": unit["cost_cents"],
                        "raw": line,
                    })
                touched_products.add(product["id"])
                canonical_line = dict(line)
                canonical_line.update({
                    "groupCode": product["sku"],
                    "Original IMEI": product["sku"],
                    "allocatedUnits": allocated,
                    "cartQty": quantity,
                })
                if quantity == 1:
                    canonical_line.update({
                        "unitImei": allocated[0], "displayImei": allocated[0],
                        "IMEI": allocated[0], "IMEI or Item Code": allocated[0],
                    })
                stored_items.append(canonical_line)
                calculated_total += price_cents * quantity
                line_discount_total += discount_cents * quantity
                calculated_qty += quantity

        elif transaction_type == "Return":
            for position, raw_line in enumerate(raw_items):
                if not isinstance(raw_line, Mapping):
                    raise StoreError(422, "invalid_item", f"Return line {position + 1} must be an object")
                line = dict(raw_line)
                quantity = safe_int(first_value(line, ["cartQty", "Quantity", "qty"], 1), 1, minimum=1, maximum=5000)
                product, selected = self._select_return_units(conn, line, quantity, linked_invoice)
                price_cents = money_to_cents(first_value(line, ["finalPrice", "Price", "price"], 0))
                allocated = []
                for unit in selected:
                    changed = conn.execute(
                        "UPDATE v2_units SET status='Available',updated_at=?,revision=? WHERE id=? AND deleted=0 AND status NOT IN ('Available','Deleted')",
                        (utc_now(), revision, unit["id"]),
                    )
                    if changed.rowcount != 1:
                        raise StoreError(409, "return_conflict", f"Unit '{unit['unit_code']}' is already available")
                    allocated.append(unit["unit_code"])
                    item_rows.append({
                        "product_id": product["id"], "unit_id": unit["id"],
                        "unit_code": unit["unit_code"], "group_code": product["sku"],
                        "quantity": 1, "price_cents": price_cents,
                        "discount_cents": 0, "cost_cents": unit["cost_cents"], "raw": line,
                    })
                touched_products.add(product["id"])
                canonical_line = dict(line)
                canonical_line.update({"allocatedUnits": allocated, "cartQty": quantity})
                if quantity == 1:
                    canonical_line.update({
                        "unitImei": allocated[0], "displayImei": allocated[0],
                        "IMEI": allocated[0], "IMEI or Item Code": allocated[0],
                    })
                stored_items.append(canonical_line)
                calculated_total += price_cents * quantity
                calculated_qty += quantity

        else:  # B2B_Payment
            amount_cents = money_to_cents(first_value(
                payload, ["paymentAmount", "amount", "paidAmount"],
                first_value(client, ["paymentAmount", "amount", "paidAmount"], 0),
            ))
            if amount_cents <= 0:
                raise StoreError(422, "invalid_payment", "Payment amount must be greater than zero")
            calculated_total = amount_cents

        for product_id in touched_products:
            self._refresh_product(conn, product_id, revision)

        subtotal_cents = money_to_cents(payload.get("subTotal"), calculated_total + line_discount_total)
        discount_cents = money_to_cents(payload.get("discount"), 0)
        global_discount = max(0, discount_cents - line_discount_total)
        server_total = max(0, calculated_total - global_discount)
        if transaction_type == "Return":
            server_total = money_to_cents(
                first_value(payload, ["refundValue", "refundAmount", "total"], cents_to_legacy(calculated_total)),
                calculated_total,
            )
            if server_total < 0 or server_total > calculated_total:
                raise StoreError(422, "invalid_refund", "Refund cannot exceed the returned items' value")
        total_cents = money_to_cents(payload.get("total"), server_total)
        if transaction_type == "B2B_Payment":
            payment_amount = calculated_total
            subtotal_cents = 0
            discount_cents = 0
            total_cents = 0
            payload["paymentAmount"] = cents_to_legacy(payment_amount)
            client["paymentAmount"] = cents_to_legacy(payment_amount)
        if subtotal_cents < 0 or discount_cents < 0 or total_cents < 0:
            raise StoreError(422, "invalid_total", "Transaction totals cannot be negative")
        if transaction_type in {"Sale", "Issue", "Return"} and "total" in payload:
            if abs(total_cents - server_total) > 1:
                raise StoreError(
                    409, "total_mismatch",
                    f"Transaction total does not match the server-calculated item total ({cents_to_legacy(server_total)})",
                )
        total_cents = server_total if transaction_type in {"Sale", "Issue", "Return"} else total_cents

        client_date = clean_text(first_value(payload, ["date", "createdAt", "timestamp"], ""), maximum=100)
        created_at = utc_now()
        if client_date:
            payload["clientDate"] = client_date
        payment_method = clean_text(first_value(
            payload, ["paymentMethod", "payment_method"], first_value(client, ["paymentMethod"], "Cash")
        ), maximum=160)
        payload.update({
            "invoiceId": invoice_id,
            "transactionType": transaction_type,
            "date": created_at,
            "client": client,
            "items": stored_items,
            "purchasedItems": stored_items,
            "paymentMethod": payment_method,
            "subTotal": cents_to_legacy(subtotal_cents),
            "discount": cents_to_legacy(discount_cents),
            "total": cents_to_legacy(total_cents),
            "totalPrice": cents_to_legacy(total_cents),
            "totalQty": calculated_qty,
            "totalQuantity": calculated_qty,
            "revision": revision,
        })
        if transaction_type == "Return":
            payload["returnedItems"] = stored_items

        cursor = conn.execute(
            "INSERT INTO v2_transactions(invoice_id,client_id,transaction_type,record_type,source_system,client_name,client_phone,client_email,payment_method,subtotal_cents,discount_cents,total_cents,quantity,linked_invoice_id,request_hash,raw_json,created_at,revision) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (invoice_id, client_id, transaction_type, clean_text(payload.get("recordType"), maximum=120),
             clean_text(payload.get("sourceSystem"), maximum=180), client_name, client_phone, client_email,
             payment_method, subtotal_cents, discount_cents,
             calculated_total if transaction_type == "B2B_Payment" else total_cents,
             calculated_qty, linked_invoice, digest, canonical_json(payload), created_at, revision),
        )
        transaction_id = cursor.lastrowid
        for row in item_rows:
            conn.execute(
                "INSERT INTO v2_transaction_items(transaction_id,product_id,unit_id,unit_code,group_code,quantity,price_cents,discount_cents,cost_cents,raw_json) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (transaction_id, row["product_id"], row["unit_id"], row["unit_code"], row["group_code"],
                 row["quantity"], row["price_cents"], row["discount_cents"], row["cost_cents"],
                 canonical_json(row["raw"])),
            )
        return payload

    def _resolve_product(self, conn: sqlite3.Connection, line: Mapping[str, Any]) -> sqlite3.Row:
        group_code = clean_text(first_value(
            line,
            ["groupCode", "Original IMEI", "productCode", "sku", "SKU"],
            first_value(line, ["IMEI or Item Code", "IMEI", "unitImei", "displayImei"]),
        ), maximum=180)
        product = conn.execute(
            "SELECT * FROM v2_products WHERE sku=? COLLATE NOCASE AND deleted=0", (group_code,)
        ).fetchone() if group_code else None
        if product:
            return product
        unit_code = clean_text(first_value(
            line, ["unitImei", "displayImei", "IMEI", "IMEI or Item Code"]
        ), maximum=180)
        unit = conn.execute(
            "SELECT product_id FROM v2_units WHERE unit_code=? COLLATE NOCASE AND deleted=0", (unit_code,)
        ).fetchone() if unit_code else None
        if unit:
            return conn.execute("SELECT * FROM v2_products WHERE id=? AND deleted=0", (unit["product_id"],)).fetchone()
        raise StoreError(404, "product_not_found", f"Inventory item '{group_code or unit_code}' was not found")

    def _explicit_unit_code(self, line: Mapping[str, Any], product_sku: str) -> str:
        explicit = clean_text(first_value(line, ["unitImei", "displayImei"], ""), maximum=180)
        if explicit.casefold() == product_sku.casefold():
            # Group-level accessory carts often repeat the SKU in displayImei.
            # Let the server allocate concrete available units in that case.
            explicit = ""
        if not explicit:
            candidate = clean_text(first_value(line, ["IMEI", "IMEI or Item Code"], ""), maximum=180)
            if candidate.casefold() != product_sku.casefold():
                explicit = candidate
        return explicit

    def _select_available_units(
        self, conn: sqlite3.Connection, line: Mapping[str, Any], quantity: int
    ) -> Tuple[sqlite3.Row, List[sqlite3.Row]]:
        product = self._resolve_product(conn, line)
        explicit = self._explicit_unit_code(line, product["sku"])
        selected: List[sqlite3.Row] = []
        if explicit:
            unit = conn.execute(
                "SELECT * FROM v2_units WHERE unit_code=? COLLATE NOCASE AND product_id=? AND deleted=0",
                (explicit, product["id"]),
            ).fetchone()
            if not unit:
                raise StoreError(404, "unit_not_found", f"Unit '{explicit}' was not found in product '{product['sku']}'")
            if unit["status"] != "Available":
                raise StoreError(409, "unit_unavailable", f"Unit '{explicit}' is {unit['status']}")
            selected.append(unit)
        remaining = quantity - len(selected)
        if remaining > 0:
            excluded = [unit["id"] for unit in selected]
            sql = "SELECT * FROM v2_units WHERE product_id=? AND deleted=0 AND status='Available'"
            params: List[Any] = [product["id"]]
            if excluded:
                sql += " AND id NOT IN (" + ",".join("?" for _ in excluded) + ")"
                params.extend(excluded)
            sql += " ORDER BY id LIMIT ?"
            params.append(remaining)
            selected.extend(conn.execute(sql, params).fetchall())
        if len(selected) != quantity:
            raise StoreError(
                409, "insufficient_stock",
                f"Only {len(selected)} available unit(s) remain for '{product['sku']}', requested {quantity}",
            )
        return product, selected

    def _select_return_units(
        self,
        conn: sqlite3.Connection,
        line: Mapping[str, Any],
        quantity: int,
        linked_invoice: str,
    ) -> Tuple[sqlite3.Row, List[sqlite3.Row]]:
        product = self._resolve_product(conn, line)
        explicit = self._explicit_unit_code(line, product["sku"])
        selected: List[sqlite3.Row] = []
        if explicit:
            unit = conn.execute(
                "SELECT * FROM v2_units WHERE unit_code=? COLLATE NOCASE AND product_id=? AND deleted=0",
                (explicit, product["id"]),
            ).fetchone()
            if not unit:
                raise StoreError(404, "unit_not_found", f"Unit '{explicit}' was not found")
            selected.append(unit)
        elif linked_invoice:
            selected = conn.execute(
                "SELECT u.* FROM v2_units u JOIN v2_transaction_items ti ON ti.unit_id=u.id JOIN v2_transactions t ON t.id=ti.transaction_id WHERE t.invoice_id=? COLLATE NOCASE AND u.product_id=? AND u.deleted=0 AND u.status NOT IN ('Available','Deleted') ORDER BY ti.id LIMIT ?",
                (linked_invoice, product["id"], quantity),
            ).fetchall()
        else:
            selected = conn.execute(
                "SELECT * FROM v2_units WHERE product_id=? AND deleted=0 AND status NOT IN ('Available','Deleted') ORDER BY id LIMIT ?",
                (product["id"], quantity),
            ).fetchall()
        if len(selected) != quantity:
            raise StoreError(409, "return_units_missing", f"Could not find {quantity} sold/issued unit(s) to return")
        for unit in selected:
            if unit["status"] in {"Available", "Deleted"}:
                raise StoreError(409, "unit_not_returnable", f"Unit '{unit['unit_code']}' is {unit['status']}")
        return product, selected

    # ------------------------------------------------------------------
    # Export, backup, restore, diagnostics
    # ------------------------------------------------------------------

    @staticmethod
    def _inventory_csv(inventory: Sequence[Mapping[str, Any]]) -> str:
        output = io.StringIO(newline="")
        fields = ["Select Phone or item", "IMEI or Item Code", "Status", "Quantity", "Notes", "DATA (JSON)"]
        writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for item in inventory:
            data = dict(item)
            for key in fields[:-1]:
                data.pop(key, None)
            writer.writerow({
                "Select Phone or item": item.get("Select Phone or item", ""),
                "IMEI or Item Code": item.get("IMEI or Item Code", ""),
                "Status": item.get("Status", ""),
                "Quantity": item.get("Quantity", "0"),
                "Notes": item.get("Notes", ""),
                "DATA (JSON)": canonical_json(data),
            })
        return output.getvalue()

    @staticmethod
    def _transactions_csv(transactions: Sequence[Mapping[str, Any]]) -> str:
        output = io.StringIO(newline="")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(["DATA (JSON)"])
        for transaction in transactions:
            writer.writerow([canonical_json(transaction)])
        return output.getvalue()

    def create_backup(self, label: str = "manual") -> Path:
        safe_label = re.sub(r"[^a-zA-Z0-9_-]", "_", label)[:32] or "backup"
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        destination = self.backup_dir / f"erp_{safe_label}_{stamp}.db"
        with self.connection(read_only=True) as source:
            target = sqlite3.connect(str(destination))
            try:
                source.backup(target)
                check = target.execute("PRAGMA integrity_check").fetchone()
                if not check or check[0] != "ok":
                    raise StoreError(500, "backup_invalid", "Backup integrity validation failed")
            finally:
                target.close()
        return destination

    @staticmethod
    def _parse_timestamp(value: str) -> Optional[dt.datetime]:
        if not value:
            return None
        try:
            parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)
        except (TypeError, ValueError):
            return None

    def backup_status(self) -> dict:
        with self.connection(read_only=True) as conn:
            interval_days = safe_int(self._meta_get(conn, "backup_interval_days", "7"), 7, minimum=1, maximum=30)
            last_export = self._meta_get(conn, "last_external_backup_at", "")
            last_filename = self._meta_get(conn, "last_external_backup_filename", "")
            last_auto = self._meta_get(conn, "last_automatic_backup_at", "")
        now = dt.datetime.now(dt.timezone.utc)
        parsed = self._parse_timestamp(last_export)
        next_due = (parsed + dt.timedelta(days=interval_days)) if parsed else now
        return {
            "intervalDays": interval_days,
            "lastExternalBackupAt": last_export or None,
            "lastExternalBackupFilename": last_filename or None,
            "lastAutomaticBackupAt": last_auto or None,
            "nextReminderAt": next_due.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "due": parsed is None or now >= next_due,
        }

    def record_external_backup(self, filename: str) -> dict:
        with self.connection() as conn:
            self._begin_immediate(conn)
            try:
                self._meta_set(conn, "last_external_backup_at", utc_now())
                self._meta_set(conn, "last_external_backup_filename", clean_text(filename, maximum=255))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return self.backup_status()

    def get_settings(self) -> dict:
        with self.connection(read_only=True) as conn:
            return {
                "backupIntervalDays": safe_int(self._meta_get(conn, "backup_interval_days", "7"), 7),
                "automaticBackupIntervalHours": safe_int(
                    self._meta_get(conn, "automatic_backup_interval_hours", "24"), 24
                ),
            }

    def update_settings(self, values: Mapping[str, Any]) -> dict:
        interval = safe_int(values.get("backupIntervalDays"), 7, minimum=1, maximum=30)
        automatic_hours = safe_int(values.get("automaticBackupIntervalHours"), 24, minimum=1, maximum=168)
        with self.connection() as conn:
            self._begin_immediate(conn)
            try:
                self._meta_set(conn, "backup_interval_days", interval)
                self._meta_set(conn, "automatic_backup_interval_hours", automatic_hours)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return self.get_settings()

    def list_backups(self, limit: int = 30) -> List[dict]:
        rows = []
        for path in sorted(self.backup_dir.glob("erp_*.db"), key=lambda item: item.stat().st_mtime, reverse=True)[:limit]:
            stat = path.stat()
            rows.append({
                "filename": path.name,
                "size": stat.st_size,
                "createdAt": dt.datetime.fromtimestamp(stat.st_mtime, dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "kind": "automatic" if "_automatic_" in path.name else "manual",
            })
        return rows

    def ensure_automatic_backup(self) -> Optional[Path]:
        with self.connection(read_only=True) as conn:
            hours = safe_int(self._meta_get(conn, "automatic_backup_interval_hours", "24"), 24, minimum=1, maximum=168)
            last_value = self._meta_get(conn, "last_automatic_backup_at", "")
        last = self._parse_timestamp(last_value)
        now = dt.datetime.now(dt.timezone.utc)
        if last and now < last + dt.timedelta(hours=hours):
            return None
        backup = self.create_backup("automatic")
        with self.connection() as conn:
            self._begin_immediate(conn)
            try:
                self._meta_set(conn, "last_automatic_backup_at", utc_now())
                self._meta_set(conn, "last_automatic_backup_filename", backup.name)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        automatic = sorted(
            self.backup_dir.glob("erp_automatic_*.db"), key=lambda item: item.stat().st_mtime, reverse=True
        )
        for old in automatic[14:]:
            try:
                old.unlink()
            except OSError:
                pass
        return backup

    def validate_restore_file(self, candidate: Path) -> None:
        if not candidate.exists() or candidate.stat().st_size < 4096:
            raise StoreError(422, "invalid_database", "Uploaded database is empty or invalid")
        conn = sqlite3.connect(f"file:{candidate.resolve().as_posix()}?mode=ro", uri=True)
        try:
            check = conn.execute("PRAGMA integrity_check").fetchone()
            if not check or check[0] != "ok":
                raise StoreError(422, "invalid_database", "SQLite integrity check failed")
            schema = conn.execute(
                "SELECT value FROM v2_meta WHERE key='schema_version'"
            ).fetchone()
            if not schema or schema[0] != SCHEMA_VERSION:
                raise StoreError(422, "invalid_schema", "Backup belongs to an unsupported application schema")
            required = {"v2_products", "v2_units", "v2_transactions", "v2_operation_receipts"}
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not required.issubset(tables):
                raise StoreError(422, "invalid_schema", "Backup is missing required tables")
        except sqlite3.DatabaseError as exc:
            raise StoreError(422, "invalid_database", "Uploaded file is not a valid SQLite database") from exc
        finally:
            conn.close()

    def restore(self, candidate: Path) -> Path:
        candidate = Path(candidate)
        self.validate_restore_file(candidate)
        with self._maintenance_condition:
            self._maintenance = True
            while self._active_connections:
                self._maintenance_condition.wait(timeout=0.5)
            backup = Path()
            swapped = False
            try:
                if self.database_path.exists():
                    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                    backup = self.backup_dir / f"erp_pre_restore_{stamp}.db"
                    source = self.connect(read_only=True)
                    target = sqlite3.connect(str(backup))
                    try:
                        source.backup(target)
                        check = target.execute("PRAGMA integrity_check").fetchone()
                        if not check or check[0] != "ok":
                            raise StoreError(500, "backup_invalid", "Pre-restore backup validation failed")
                    finally:
                        source.close()
                        target.close()
                replacement = self.database_path.with_suffix(".restore.tmp")
                shutil.copy2(candidate, replacement)
                os.replace(replacement, self.database_path)
                swapped = True
                for extension in ("-wal", "-shm"):
                    sidecar = Path(str(self.database_path) + extension)
                    try:
                        sidecar.unlink()
                    except FileNotFoundError:
                        pass
                # Run additive migrations on older valid v2 backups. The RLock
                # remains owned here, so no request can enter during this brief
                # maintenance-flag transition.
                self._maintenance = False
                try:
                    self.initialize()
                finally:
                    self._maintenance = True
                return backup
            except Exception:
                if swapped and backup.is_file():
                    recovery = self.database_path.with_suffix(".recovery.tmp")
                    shutil.copy2(backup, recovery)
                    os.replace(recovery, self.database_path)
                    for extension in ("-wal", "-shm"):
                        try:
                            Path(str(self.database_path) + extension).unlink()
                        except FileNotFoundError:
                            pass
                raise
            finally:
                self._maintenance = False
                self._maintenance_condition.notify_all()

    def integrity_status(self) -> dict:
        with self.connection(read_only=True) as conn:
            integrity = conn.execute("PRAGMA quick_check").fetchone()[0]
            counts = {
                "products": conn.execute("SELECT COUNT(*) FROM v2_products WHERE deleted=0").fetchone()[0],
                "units": conn.execute("SELECT COUNT(*) FROM v2_units WHERE deleted=0").fetchone()[0],
                "clients": conn.execute("SELECT COUNT(*) FROM v2_clients").fetchone()[0],
                "transactions": conn.execute("SELECT COUNT(*) FROM v2_transactions").fetchone()[0],
            }
            return {"integrity": integrity, "revision": self.current_revision(conn), "counts": counts}
