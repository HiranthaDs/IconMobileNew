"""Supabase/Postgres persistence for ICON MOBILE.

This is a faithful port of ``SQLiteStore`` from the original
``backend_store.py``.  Every business rule (idempotency by operation id and
invoice id, atomic stock movement and reversal, client normalization,
accounting rollups, transaction/return validation) is preserved.  The storage
engine is Supabase Postgres instead of a local SQLite file:

  * Writes are serialized by locking the ``revision`` meta row
    (``SELECT ... FOR UPDATE``), the Postgres equivalent of SQLite's
    ``BEGIN IMMEDIATE``.
  * Case-insensitive unique columns use ``citext`` (was ``COLLATE NOCASE``).
  * Backups are JSON table dumps written under the backup directory, since a
    single-file copy does not apply to a hosted database.  Supabase's own
    managed backups remain the disaster-recovery mechanism.
"""

from __future__ import annotations

import datetime as dt
import io
import json
import re
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from app.core import db
from app.store.helpers import (
    SCHEMA_VERSION,
    TRANSACTION_TYPES,
    StoreError,
    _PARTNER_PREFIX,
    canonical_json,
    cents_to_legacy,
    clean_text,
    first_value,
    json_object,
    money_to_cents,
    normalized_status,
    request_digest,
    safe_int,
    utc_now,
)

_DUMP_TABLES = [
    "v2_meta",
    "v2_products",
    "v2_units",
    "v2_clients",
    "v2_transactions",
    "v2_transaction_items",
    "v2_operation_receipts",
    "v2_change_log",
]
_ID_TABLES = [
    "v2_products",
    "v2_units",
    "v2_clients",
    "v2_transactions",
    "v2_transaction_items",
]


class PostgresStore:
    def __init__(self, backup_dir: Path) -> None:
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self._restore_lock = threading.RLock()
        self.initialize()

    def initialize(self) -> None:
        db.init_db(SCHEMA_VERSION)

    # ------------------------------------------------------------------
    # Connection / transaction primitives
    # ------------------------------------------------------------------

    def connection(self, *, read_only: bool = False):
        return db.connection(read_only=read_only)

    def _lock_for_write(self, conn) -> None:
        """Serialize all writers by locking the shared revision row.

        Postgres equivalent of ``BEGIN IMMEDIATE``: every mutating operation
        takes the same row lock, so revisions and stock updates never interleave.
        """
        conn.execute("SELECT value FROM v2_meta WHERE key='revision' FOR UPDATE")

    def current_revision(self, conn=None) -> int:
        if conn is not None:
            row = conn.execute("SELECT value FROM v2_meta WHERE key='revision'").fetchone()
            return int(row[0]) if row else 0
        with self.connection(read_only=True) as opened:
            return self.current_revision(opened)

    def _next_revision(self, conn) -> int:
        conn.execute(
            "UPDATE v2_meta SET value=(CAST(value AS INTEGER)+1)::text WHERE key='revision'"
        )
        return self.current_revision(conn)

    @staticmethod
    def _meta_get(conn, key: str, default: str = "") -> str:
        row = conn.execute("SELECT value FROM v2_meta WHERE key=%s", (key,)).fetchone()
        return str(row[0]) if row else default

    @staticmethod
    def _meta_set(conn, key: str, value: Any) -> None:
        conn.execute(
            "INSERT INTO v2_meta(key,value) VALUES(%s,%s) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

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

    def _product_to_dict(self, conn, product) -> dict:
        units_rows = conn.execute(
            "SELECT * FROM v2_units WHERE product_id=%s AND deleted=0 ORDER BY id",
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

    def _transaction_to_dict(self, conn, row) -> dict:
        value = json_object(row["raw_json"])
        value.setdefault("invoiceId", row["invoice_id"])
        value.setdefault("transactionType", row["transaction_type"])
        value.setdefault("date", row["created_at"])
        value.setdefault("revision", row["revision"])
        value.setdefault("clientId", row["client_id"])
        value.setdefault("name", row["client_name"])
        value.setdefault("phone", row["client_phone"])
        value.setdefault("email", row["client_email"])
        value.setdefault("paymentMethod", row["payment_method"])
        value.setdefault("subTotal", cents_to_legacy(row["subtotal_cents"]))
        value.setdefault("discount", cents_to_legacy(row["discount_cents"]))
        value.setdefault("total", cents_to_legacy(row["total_cents"]))
        value.setdefault("totalPrice", cents_to_legacy(row["total_cents"]))
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
                "FROM v2_transaction_items WHERE transaction_id=%s ORDER BY id",
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

    def _asset_summary(self, conn) -> dict:
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
                     ON linked.invoice_id=t.linked_invoice_id
             GROUP BY t.id, linked.transaction_type
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

    def _upsert_client(self, conn, *, name: str, phone: str, email: str, client_type: str, revision: int) -> Optional[int]:
        if not (name or phone or email):
            return None
        key = self._client_identity_key(name, phone, email)
        existing = conn.execute("SELECT client_key FROM v2_clients WHERE client_key=%s", (key,)).fetchone()
        if not existing and name:
            existing = conn.execute(
                "SELECT client_key FROM v2_clients WHERE lower(name)=lower(%s) ORDER BY updated_at DESC LIMIT 1",
                (name,),
            ).fetchone()
        if existing:
            key = existing["client_key"]
        now = utc_now()
        conn.execute(
            """
            INSERT INTO v2_clients(client_key,name,phone,email,client_type,created_at,updated_at,revision)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
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
        row = conn.execute("SELECT id FROM v2_clients WHERE client_key=%s", (key,)).fetchone()
        return int(row[0]) if row else None

    @staticmethod
    def _client_to_dict(row) -> dict:
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
                "SELECT * FROM v2_transactions WHERE invoice_id=%s", (invoice_id,)
            ).fetchone()
            if not row:
                raise StoreError(404, "invoice_not_found", f"Invoice '{invoice_id}' was not found")
            return self._transaction_to_dict(conn, row)

    def get_operation(self, operation_id: str) -> Optional[dict]:
        operation_id = clean_text(operation_id, maximum=180)
        with self.connection(read_only=True) as conn:
            row = conn.execute(
                "SELECT response_json FROM v2_operation_receipts WHERE operation_id=%s", (operation_id,)
            ).fetchone()
            return json_object(row[0]) if row else None

    # ------------------------------------------------------------------
    # Atomic mutation dispatcher
    # ------------------------------------------------------------------

    def execute_action(self, payload: Mapping[str, Any], *, actor_role: str, device_id: str, operation_id: str) -> dict:
        if not isinstance(payload, Mapping):
            raise StoreError(422, "invalid_payload", "Request payload must be an object")
        action = clean_text(payload.get("action"), maximum=80)
        aliases = {
            "addInventory": "add_item", "addProduct": "add_item",
            "updateInventory": "update_item", "updateProduct": "update_item",
            "deleteInventory": "delete_item", "deleteProduct": "delete_item",
            "deleteInvoice": "delete_transaction", "deleteInvoiceItem": "delete_transaction_item",
            "updatePartner": "update_client", "deletePartner": "delete_client",
            "sale": "checkout", "issue": "checkout", "return": "checkout",
        }
        action = aliases.get(action, action)
        if action not in {
            "add_item", "update_item", "delete_item", "checkout",
            "delete_transaction", "delete_transaction_item", "update_client", "delete_client",
        }:
            raise StoreError(400, "unknown_action", f"Unsupported action '{action}'")
        if actor_role not in {"pos", "admin", "wholesale"}:
            raise StoreError(403, "forbidden", "Invalid application role")
        if action == "delete_item" and actor_role not in {"admin", "wholesale"}:
            raise StoreError(403, "forbidden", "This role cannot delete inventory")
        if action in {"delete_transaction", "delete_transaction_item", "update_client", "delete_client"} and actor_role != "admin":
            raise StoreError(403, "forbidden", "Only an administrator can change accounting or partner master data")

        operation_id = clean_text(operation_id or str(uuid.uuid4()), maximum=180)
        device_id = clean_text(device_id or "unknown-device", maximum=180)
        digest_payload = dict(payload)
        digest_payload.pop("operationId", None)
        digest_payload.pop("deviceId", None)
        digest = request_digest(digest_payload)

        with self.connection() as conn:
            self._lock_for_write(conn)
            try:
                prior = conn.execute(
                    "SELECT * FROM v2_operation_receipts WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if prior:
                    if prior["request_hash"] != digest:
                        raise StoreError(409, "operation_conflict", "Operation ID was already used for different data")
                    response = json_object(prior["response_json"])
                    response["duplicate"] = True
                    conn.execute("COMMIT")
                    return response

                invoice_id = clean_text(payload.get("invoiceId"), maximum=180) if action == "checkout" else ""
                if invoice_id:
                    existing_tx = conn.execute(
                        "SELECT * FROM v2_transactions WHERE invoice_id=%s", (invoice_id,)
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
                            "INSERT INTO v2_operation_receipts(operation_id,device_id,actor_role,action,request_hash,invoice_id,revision,response_json,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
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
                elif action == "delete_transaction":
                    result = self._delete_transaction(conn, payload, revision=revision)
                    entity_id = result["invoiceId"]
                    message = "Invoice deleted and its stock movement was reversed."
                    data = {"revision": revision, **result}
                    entity_type = "transaction"
                elif action == "delete_transaction_item":
                    result = self._delete_transaction_item(conn, payload, revision=revision)
                    entity_id = f"{result['invoiceId']}:{result['unitCode']}"
                    message = "Invoice item deleted and totals were recalculated."
                    data = {"revision": revision, **result}
                    entity_type = "transaction_item"
                elif action == "update_client":
                    result = self._update_client(conn, payload, revision=revision)
                    entity_id = str(result["id"])
                    message = "Partner details updated across linked records."
                    data = {"revision": revision, "client": result}
                    entity_type = "client"
                elif action == "delete_client":
                    result = self._delete_client(conn, payload, revision=revision)
                    entity_id = str(result["clientId"])
                    message = "Partner profile deleted; historical invoices were preserved."
                    data = {"revision": revision, **result}
                    entity_type = "client"
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
                    "INSERT INTO v2_change_log(revision,operation_id,device_id,actor_role,action,entity_type,entity_id,summary_json,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (revision, operation_id, device_id, actor_role, action, entity_type, entity_id,
                     canonical_json({"message": message}), utc_now()),
                )
                conn.execute(
                    "INSERT INTO v2_operation_receipts(operation_id,device_id,actor_role,action,request_hash,invoice_id,revision,response_json,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (operation_id, device_id, actor_role, action, digest,
                     entity_id if entity_type == "transaction" else "", revision,
                     canonical_json(response), utc_now()),
                )
                conn.execute("COMMIT")
                return response
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def _save_inventory(self, conn, raw_item: Any, *, revision: int, create: bool) -> Tuple[dict, str]:
        item, units = self._canonical_item(raw_item if isinstance(raw_item, Mapping) else {})
        canonical = item["canonical"]
        sku = canonical["IMEI or Item Code"]
        existing = conn.execute(
            "SELECT * FROM v2_products WHERE sku=%s", (sku,)
        ).fetchone()
        if create and existing and not existing["deleted"]:
            raise StoreError(409, "duplicate_sku", f"Product code '{sku}' already exists")
        if not create and (not existing or existing["deleted"]):
            raise StoreError(404, "product_not_found", f"Product code '{sku}' was not found")

        now = utc_now()
        if not existing:
            cursor = conn.execute(
                "INSERT INTO v2_products(sku,item_type,category,brand,model,color,specs_json,price_cents,offer_price_cents,aggregate_quantity,notes,legacy_json,deleted,created_at,updated_at,revision) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s,%s) RETURNING id",
                (sku, canonical["Type"], canonical["Category"], canonical["Brand"], canonical["Model"],
                 canonical["Color"], canonical_json(item["specs"]), item["price_cents"], item["offer_cents"],
                 0, canonical["Notes"], canonical_json(item["legacy"]), now, now, revision),
            )
            product_id = cursor.fetchone()[0]
        else:
            product_id = existing["id"]
            conn.execute(
                "UPDATE v2_products SET item_type=%s,category=%s,brand=%s,model=%s,color=%s,specs_json=%s,price_cents=%s,offer_price_cents=%s,notes=%s,legacy_json=%s,deleted=0,updated_at=%s,revision=%s WHERE id=%s",
                (canonical["Type"], canonical["Category"], canonical["Brand"], canonical["Model"],
                 canonical["Color"], canonical_json(item["specs"]), item["price_cents"], item["offer_cents"],
                 canonical["Notes"], canonical_json(item["legacy"]), now, revision, product_id),
            )

        incoming_codes = {unit["imei"].casefold() for unit in units}
        old_units = conn.execute("SELECT * FROM v2_units WHERE product_id=%s", (product_id,)).fetchall()
        for old in old_units:
            if old["unit_code"].casefold() not in incoming_codes and not old["deleted"]:
                if old["status"] not in {"Available", "Returned", "Deleted"}:
                    raise StoreError(
                        409, "unit_in_use",
                        f"Unit '{old['unit_code']}' is {old['status']} and cannot be removed from the product",
                    )
                conn.execute(
                    "UPDATE v2_units SET deleted=1,status='Deleted',updated_at=%s,revision=%s WHERE id=%s",
                    (now, revision, old["id"]),
                )

        for unit in units:
            collision = conn.execute(
                "SELECT * FROM v2_units WHERE unit_code=%s", (unit["imei"],)
            ).fetchone()
            if collision and collision["product_id"] != product_id:
                raise StoreError(409, "duplicate_unit", f"IMEI / serial '{unit['imei']}' belongs to another product")
            if collision:
                conn.execute(
                    "UPDATE v2_units SET supplier=%s,cost_cents=%s,status=%s,date_added=%s,deleted=0,updated_at=%s,revision=%s WHERE id=%s",
                    (unit["supplier"], unit["cost_cents"], unit["status"], unit["dateAdded"],
                     now, revision, collision["id"]),
                )
            else:
                conn.execute(
                    "INSERT INTO v2_units(product_id,unit_code,supplier,cost_cents,status,date_added,deleted,updated_at,revision) VALUES(%s,%s,%s,%s,%s,%s,0,%s,%s)",
                    (product_id, unit["imei"], unit["supplier"], unit["cost_cents"], unit["status"],
                     unit["dateAdded"], now, revision),
                )
        self._refresh_product(conn, product_id, revision)
        product = conn.execute("SELECT * FROM v2_products WHERE id=%s", (product_id,)).fetchone()
        return self._product_to_dict(conn, product), sku

    def _delete_inventory(self, conn, payload: Mapping[str, Any], *, revision: int) -> str:
        code = clean_text(first_value(payload, ["imei", "sku", "code", "product_id"]), maximum=180)
        if not code:
            raise StoreError(422, "missing_sku", "Product code is required")
        product = conn.execute(
            "SELECT * FROM v2_products WHERE sku=%s AND deleted=0", (code,)
        ).fetchone()
        if not product:
            unit = conn.execute(
                "SELECT * FROM v2_units WHERE unit_code=%s AND deleted=0", (code,)
            ).fetchone()
            if unit:
                if unit["status"] not in {"Available", "Returned"}:
                    raise StoreError(409, "unit_in_use", f"Unit '{code}' is {unit['status']} and cannot be deleted")
                conn.execute(
                    "UPDATE v2_units SET deleted=1,status='Deleted',updated_at=%s,revision=%s WHERE id=%s",
                    (utc_now(), revision, unit["id"]),
                )
                self._refresh_product(conn, unit["product_id"], revision)
                return code
            raise StoreError(404, "product_not_found", f"Inventory code '{code}' was not found")
        active = conn.execute(
            "SELECT unit_code,status FROM v2_units WHERE product_id=%s AND deleted=0 AND status NOT IN ('Available','Returned')",
            (product["id"],),
        ).fetchone()
        if active:
            raise StoreError(409, "product_in_use", f"Unit '{active['unit_code']}' is {active['status']}; product cannot be deleted")
        now = utc_now()
        conn.execute(
            "UPDATE v2_units SET deleted=1,status='Deleted',updated_at=%s,revision=%s WHERE product_id=%s AND deleted=0",
            (now, revision, product["id"]),
        )
        conn.execute(
            "UPDATE v2_products SET deleted=1,aggregate_quantity=0,updated_at=%s,revision=%s WHERE id=%s",
            (now, revision, product["id"]),
        )
        return product["sku"]

    @staticmethod
    def _client_id_from_payload(payload: Mapping[str, Any]) -> int:
        client_id = safe_int(first_value(payload, ["clientId", "client_id", "id"], 0), 0, minimum=0)
        if not client_id:
            raise StoreError(422, "missing_client_id", "Partner ID is required")
        return client_id

    def _update_client(self, conn, payload: Mapping[str, Any], *, revision: int) -> dict:
        client_id = self._client_id_from_payload(payload)
        current = conn.execute("SELECT * FROM v2_clients WHERE id=%s", (client_id,)).fetchone()
        if not current:
            raise StoreError(404, "client_not_found", "Partner profile was not found")

        details = payload.get("client") if isinstance(payload.get("client"), Mapping) else payload
        name = clean_text(first_value(details, ["name", "partnerName"], current["name"]), maximum=220)
        phone = clean_text(first_value(details, ["phone", "whatsapp"], current["phone"]), maximum=80)
        email = clean_text(details.get("email", current["email"]), maximum=240)
        client_type = clean_text(first_value(details, ["type", "clientType"], current["client_type"]), maximum=20)
        client_type = "B2B" if client_type.casefold() in {"b2b", "partner", "wholesale"} else "Retail"
        if not name:
            raise StoreError(422, "missing_client", "Partner name is required")

        key = self._client_identity_key(name, phone, email)
        collision = conn.execute(
            "SELECT id FROM v2_clients WHERE client_key=%s AND id<>%s", (key, client_id)
        ).fetchone()
        if collision:
            raise StoreError(409, "duplicate_client", "Another client already uses these contact details")

        now = utc_now()
        old_name = current["name"]
        conn.execute(
            "UPDATE v2_clients SET client_key=%s,name=%s,phone=%s,email=%s,client_type=%s,updated_at=%s,revision=%s WHERE id=%s",
            (key, name, phone, email, client_type, now, revision, client_id),
        )
        if old_name.casefold() != name.casefold():
            conn.execute(
                "UPDATE v2_units SET status=%s,updated_at=%s,revision=%s WHERE deleted=0 AND lower(status)=lower(%s)",
                (f"{_PARTNER_PREFIX}{name}", now, revision, f"{_PARTNER_PREFIX}{old_name}"),
            )

        transactions = conn.execute(
            "SELECT id,raw_json FROM v2_transactions WHERE client_id=%s", (client_id,)
        ).fetchall()
        for transaction in transactions:
            raw = json_object(transaction["raw_json"])
            raw_client = raw.get("client") if isinstance(raw.get("client"), dict) else {}
            raw_client.update({"name": name, "phone": phone, "email": email})
            if client_type == "B2B":
                raw_client["isPartner"] = True
                raw_client["partnerType"] = "B2B_PARTNER"
            raw["client"] = raw_client
            raw.update({"name": name, "phone": phone, "email": email, "revision": revision})
            conn.execute(
                "UPDATE v2_transactions SET client_name=%s,client_phone=%s,client_email=%s,raw_json=%s,revision=%s WHERE id=%s",
                (name, phone, email, canonical_json(raw), revision, transaction["id"]),
            )

        updated = conn.execute("SELECT * FROM v2_clients WHERE id=%s", (client_id,)).fetchone()
        return self._client_to_dict(updated)

    def _delete_client(self, conn, payload: Mapping[str, Any], *, revision: int) -> dict:
        client_id = self._client_id_from_payload(payload)
        client = conn.execute("SELECT * FROM v2_clients WHERE id=%s", (client_id,)).fetchone()
        if not client:
            raise StoreError(404, "client_not_found", "Partner profile was not found")
        assigned = conn.execute(
            "SELECT unit_code FROM v2_units WHERE deleted=0 AND lower(status)=lower(%s) LIMIT 1",
            (f"{_PARTNER_PREFIX}{client['name']}",),
        ).fetchone()
        if assigned:
            raise StoreError(
                409, "partner_has_stock",
                f"Partner still holds unit '{assigned['unit_code']}'. Return or reassign partner stock before deleting the profile.",
            )
        history_count = int(conn.execute(
            "SELECT COUNT(*) FROM v2_transactions WHERE client_id=%s", (client_id,)
        ).fetchone()[0])
        conn.execute("UPDATE v2_transactions SET client_id=NULL WHERE client_id=%s", (client_id,))
        conn.execute("DELETE FROM v2_clients WHERE id=%s", (client_id,))
        return {"clientId": client_id, "name": client["name"], "historyPreserved": history_count}

    @staticmethod
    def _transaction_destination(conn, transaction) -> str:
        if transaction["transaction_type"] == "Sale":
            return "Sold"
        if transaction["transaction_type"] == "Issue":
            return f"{_PARTNER_PREFIX}{transaction['client_name']}"
        if transaction["transaction_type"] == "Return":
            linked_id = transaction["linked_invoice_id"]
            if not linked_id:
                raise StoreError(
                    409, "return_origin_unknown",
                    "This return has no linked invoice, so its stock movement cannot be reversed safely.",
                )
            linked = conn.execute(
                "SELECT * FROM v2_transactions WHERE invoice_id=%s", (linked_id,)
            ).fetchone()
            if not linked or linked["transaction_type"] not in {"Sale", "Issue"}:
                raise StoreError(409, "return_origin_unknown", "The original sale or issue could not be found")
            return "Sold" if linked["transaction_type"] == "Sale" else f"{_PARTNER_PREFIX}{linked['client_name']}"
        return ""

    def _validate_transaction_unit_reversal(self, conn, transaction, item) -> str:
        if not item["unit_id"]:
            return ""
        unit = conn.execute("SELECT * FROM v2_units WHERE id=%s AND deleted=0", (item["unit_id"],)).fetchone()
        if not unit:
            raise StoreError(409, "unit_missing", f"Unit '{item['unit_code']}' is no longer active")
        if transaction["transaction_type"] in {"Sale", "Issue"}:
            expected = self._transaction_destination(conn, transaction)
            if unit["status"].casefold() != expected.casefold():
                raise StoreError(
                    409, "stock_history_conflict",
                    f"Unit '{unit['unit_code']}' is currently {unit['status']}; expected {expected}. Delete linked returns first.",
                )
            return "Available"
        if transaction["transaction_type"] == "Return":
            if unit["status"] != "Available":
                raise StoreError(
                    409, "stock_history_conflict",
                    f"Returned unit '{unit['unit_code']}' is currently {unit['status']} and cannot be reversed.",
                )
            return self._transaction_destination(conn, transaction)
        return ""

    def _dependent_transaction(self, conn, invoice_id: str):
        return conn.execute(
            "SELECT invoice_id,transaction_type FROM v2_transactions "
            "WHERE linked_invoice_id=%s ORDER BY id LIMIT 1",
            (invoice_id,),
        ).fetchone()

    def _delete_transaction(self, conn, payload: Mapping[str, Any], *, revision: int) -> dict:
        invoice_id = clean_text(first_value(payload, ["invoiceId", "invoice_id", "id"], ""), maximum=180)
        if not invoice_id:
            raise StoreError(422, "missing_invoice_id", "Invoice ID is required")
        transaction = conn.execute(
            "SELECT * FROM v2_transactions WHERE invoice_id=%s", (invoice_id,)
        ).fetchone()
        if not transaction:
            raise StoreError(404, "invoice_not_found", f"Invoice '{invoice_id}' was not found")
        dependent = self._dependent_transaction(conn, transaction["invoice_id"])
        if dependent:
            raise StoreError(
                409, "invoice_has_dependents",
                f"Delete linked {dependent['transaction_type']} '{dependent['invoice_id']}' before deleting this invoice.",
            )

        items = conn.execute(
            "SELECT * FROM v2_transaction_items WHERE transaction_id=%s ORDER BY id", (transaction["id"],)
        ).fetchall()
        changes = [(item, self._validate_transaction_unit_reversal(conn, transaction, item)) for item in items]
        touched_products: set[int] = set()
        now = utc_now()
        for item, target in changes:
            if item["unit_id"] and target:
                conn.execute(
                    "UPDATE v2_units SET status=%s,updated_at=%s,revision=%s WHERE id=%s",
                    (target, now, revision, item["unit_id"]),
                )
            if item["product_id"]:
                touched_products.add(int(item["product_id"]))
        conn.execute("DELETE FROM v2_transaction_items WHERE transaction_id=%s", (transaction["id"],))
        conn.execute("DELETE FROM v2_operation_receipts WHERE invoice_id=%s", (transaction["invoice_id"],))
        conn.execute("DELETE FROM v2_transactions WHERE id=%s", (transaction["id"],))
        for product_id in touched_products:
            self._refresh_product(conn, product_id, revision)
        return {"invoiceId": transaction["invoice_id"], "reversedUnits": len(changes)}

    @staticmethod
    def _remove_unit_from_raw_lines(lines: Any, unit_code: str) -> list:
        if not isinstance(lines, list):
            return []
        target = unit_code.casefold()
        updated: list = []
        for raw_line in lines:
            if not isinstance(raw_line, dict):
                continue
            line = dict(raw_line)
            allocated = [str(code) for code in line.get("allocatedUnits", []) if str(code).strip()]
            direct = clean_text(first_value(
                line, ["unitImei", "displayImei", "IMEI", "IMEI or Item Code"], ""
            ), maximum=180)
            matched = any(code.casefold() == target for code in allocated) or direct.casefold() == target
            if not matched:
                updated.append(line)
                continue
            remaining = [code for code in allocated if code.casefold() != target]
            quantity = safe_int(first_value(line, ["cartQty", "Quantity", "qty"], len(allocated) or 1), 1, minimum=1)
            if quantity <= 1 or (allocated and not remaining):
                continue
            line["allocatedUnits"] = remaining
            line["cartQty"] = quantity - 1
            if remaining:
                for key in ("unitImei", "displayImei", "IMEI", "IMEI or Item Code"):
                    if key in line:
                        line[key] = remaining[0]
            updated.append(line)
        return updated

    def _delete_transaction_item(self, conn, payload: Mapping[str, Any], *, revision: int) -> dict:
        invoice_id = clean_text(first_value(payload, ["invoiceId", "invoice_id"], ""), maximum=180)
        unit_code = clean_text(first_value(payload, ["unitCode", "unitImei", "imei", "code"], ""), maximum=180)
        if not invoice_id or not unit_code:
            raise StoreError(422, "missing_item_reference", "Invoice ID and unit IMEI / code are required")
        transaction = conn.execute(
            "SELECT * FROM v2_transactions WHERE invoice_id=%s", (invoice_id,)
        ).fetchone()
        if not transaction:
            raise StoreError(404, "invoice_not_found", f"Invoice '{invoice_id}' was not found")
        item = conn.execute(
            "SELECT * FROM v2_transaction_items WHERE transaction_id=%s AND unit_code=%s ORDER BY id LIMIT 1",
            (transaction["id"], unit_code),
        ).fetchone()
        if not item:
            raise StoreError(404, "invoice_item_not_found", f"Item '{unit_code}' was not found on this invoice")
        remaining_count = int(conn.execute(
            "SELECT COUNT(*) FROM v2_transaction_items WHERE transaction_id=%s", (transaction["id"],)
        ).fetchone()[0])
        if remaining_count <= 1:
            raise StoreError(409, "last_invoice_item", "This is the final invoice item. Delete the complete invoice instead.")
        if transaction["transaction_type"] in {"Sale", "Issue"} and item["unit_id"]:
            dependent = conn.execute(
                "SELECT child.invoice_id FROM v2_transactions child "
                "JOIN v2_transaction_items child_item ON child_item.transaction_id=child.id "
                "WHERE child.linked_invoice_id=%s AND child_item.unit_id=%s LIMIT 1",
                (transaction["invoice_id"], item["unit_id"]),
            ).fetchone()
            if dependent:
                raise StoreError(
                    409, "invoice_item_has_dependent",
                    f"Delete linked transaction '{dependent['invoice_id']}' before deleting this item.",
                )

        target = self._validate_transaction_unit_reversal(conn, transaction, item)
        now = utc_now()
        if item["unit_id"] and target:
            conn.execute(
                "UPDATE v2_units SET status=%s,updated_at=%s,revision=%s WHERE id=%s",
                (target, now, revision, item["unit_id"]),
            )
        conn.execute("DELETE FROM v2_transaction_items WHERE id=%s", (item["id"],))

        rows = conn.execute(
            "SELECT price_cents,discount_cents,quantity FROM v2_transaction_items WHERE transaction_id=%s",
            (transaction["id"],),
        ).fetchall()
        calculated = sum(int(row["price_cents"]) * int(row["quantity"]) for row in rows)
        line_discounts = sum(int(row["discount_cents"]) * int(row["quantity"]) for row in rows)
        old_line_discounts = line_discounts + int(item["discount_cents"]) * int(item["quantity"])
        global_discount = max(0, int(transaction["discount_cents"]) - old_line_discounts)
        total = calculated if transaction["transaction_type"] == "Return" else max(0, calculated - global_discount)
        subtotal = calculated + line_discounts
        discount = line_discounts + global_discount
        quantity = sum(int(row["quantity"]) for row in rows)

        raw = json_object(transaction["raw_json"])
        for key in ("items", "purchasedItems", "returnedItems"):
            if key in raw:
                raw[key] = self._remove_unit_from_raw_lines(raw[key], item["unit_code"])
        raw.update({
            "subTotal": cents_to_legacy(subtotal), "discount": cents_to_legacy(discount),
            "total": cents_to_legacy(total), "totalPrice": cents_to_legacy(total),
            "totalQty": quantity, "totalQuantity": quantity, "revision": revision,
        })
        conn.execute(
            "UPDATE v2_transactions SET subtotal_cents=%s,discount_cents=%s,total_cents=%s,quantity=%s,raw_json=%s,revision=%s WHERE id=%s",
            (subtotal, discount, total, quantity, canonical_json(raw), revision, transaction["id"]),
        )
        conn.execute("DELETE FROM v2_operation_receipts WHERE invoice_id=%s", (transaction["invoice_id"],))
        if item["product_id"]:
            self._refresh_product(conn, int(item["product_id"]), revision)
        updated = conn.execute("SELECT * FROM v2_transactions WHERE id=%s", (transaction["id"],)).fetchone()
        return {
            "invoiceId": transaction["invoice_id"], "unitCode": item["unit_code"],
            "transaction": self._transaction_to_dict(conn, updated),
        }

    def _refresh_product(self, conn, product_id: int, revision: int) -> None:
        available = conn.execute(
            "SELECT COUNT(*) FROM v2_units WHERE product_id=%s AND deleted=0 AND status='Available'",
            (product_id,),
        ).fetchone()[0]
        conn.execute(
            "UPDATE v2_products SET aggregate_quantity=%s,updated_at=%s,revision=%s WHERE id=%s",
            (available, utc_now(), revision, product_id),
        )

    # ------------------------------------------------------------------
    # Checkout / issue / return / payment
    # ------------------------------------------------------------------

    def _checkout(self, conn, payload: dict, transaction_type: str, revision: int, digest: str) -> dict:
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
                "SELECT id FROM v2_transactions WHERE invoice_id=%s", (linked_invoice,)
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
                        "UPDATE v2_units SET status=%s,updated_at=%s,revision=%s WHERE id=%s AND deleted=0 AND status='Available'",
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
                        "UPDATE v2_units SET status='Available',updated_at=%s,revision=%s WHERE id=%s AND deleted=0 AND status NOT IN ('Available','Deleted')",
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
            "INSERT INTO v2_transactions(invoice_id,client_id,transaction_type,record_type,source_system,client_name,client_phone,client_email,payment_method,subtotal_cents,discount_cents,total_cents,quantity,linked_invoice_id,request_hash,raw_json,created_at,revision) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
            (invoice_id, client_id, transaction_type, clean_text(payload.get("recordType"), maximum=120),
             clean_text(payload.get("sourceSystem"), maximum=180), client_name, client_phone, client_email,
             payment_method, subtotal_cents, discount_cents,
             calculated_total if transaction_type == "B2B_Payment" else total_cents,
             calculated_qty, linked_invoice, digest, canonical_json(payload), created_at, revision),
        )
        transaction_id = cursor.fetchone()[0]
        for row in item_rows:
            conn.execute(
                "INSERT INTO v2_transaction_items(transaction_id,product_id,unit_id,unit_code,group_code,quantity,price_cents,discount_cents,cost_cents,raw_json) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (transaction_id, row["product_id"], row["unit_id"], row["unit_code"], row["group_code"],
                 row["quantity"], row["price_cents"], row["discount_cents"], row["cost_cents"],
                 canonical_json(row["raw"])),
            )
        return payload

    def _resolve_product(self, conn, line: Mapping[str, Any]):
        group_code = clean_text(first_value(
            line,
            ["groupCode", "Original IMEI", "productCode", "sku", "SKU"],
            first_value(line, ["IMEI or Item Code", "IMEI", "unitImei", "displayImei"]),
        ), maximum=180)
        product = conn.execute(
            "SELECT * FROM v2_products WHERE sku=%s AND deleted=0", (group_code,)
        ).fetchone() if group_code else None
        if product:
            return product
        unit_code = clean_text(first_value(
            line, ["unitImei", "displayImei", "IMEI", "IMEI or Item Code"]
        ), maximum=180)
        unit = conn.execute(
            "SELECT product_id FROM v2_units WHERE unit_code=%s AND deleted=0", (unit_code,)
        ).fetchone() if unit_code else None
        if unit:
            return conn.execute("SELECT * FROM v2_products WHERE id=%s AND deleted=0", (unit["product_id"],)).fetchone()
        raise StoreError(404, "product_not_found", f"Inventory item '{group_code or unit_code}' was not found")

    def _explicit_unit_code(self, line: Mapping[str, Any], product_sku: str) -> str:
        explicit = clean_text(first_value(line, ["unitImei", "displayImei"], ""), maximum=180)
        if explicit.casefold() == product_sku.casefold():
            explicit = ""
        if not explicit:
            candidate = clean_text(first_value(line, ["IMEI", "IMEI or Item Code"], ""), maximum=180)
            if candidate.casefold() != product_sku.casefold():
                explicit = candidate
        return explicit

    def _select_available_units(self, conn, line: Mapping[str, Any], quantity: int):
        product = self._resolve_product(conn, line)
        explicit = self._explicit_unit_code(line, product["sku"])
        selected: List[Any] = []
        if explicit:
            unit = conn.execute(
                "SELECT * FROM v2_units WHERE unit_code=%s AND product_id=%s AND deleted=0",
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
            sql = "SELECT * FROM v2_units WHERE product_id=%s AND deleted=0 AND status='Available'"
            params: List[Any] = [product["id"]]
            if excluded:
                sql += " AND id NOT IN (" + ",".join("%s" for _ in excluded) + ")"
                params.extend(excluded)
            sql += " ORDER BY id LIMIT %s"
            params.append(remaining)
            selected.extend(conn.execute(sql, params).fetchall())
        if len(selected) != quantity:
            raise StoreError(
                409, "insufficient_stock",
                f"Only {len(selected)} available unit(s) remain for '{product['sku']}', requested {quantity}",
            )
        return product, selected

    def _select_return_units(self, conn, line: Mapping[str, Any], quantity: int, linked_invoice: str):
        product = self._resolve_product(conn, line)
        explicit = self._explicit_unit_code(line, product["sku"])
        selected: List[Any] = []
        if explicit:
            unit = conn.execute(
                "SELECT * FROM v2_units WHERE unit_code=%s AND product_id=%s AND deleted=0",
                (explicit, product["id"]),
            ).fetchone()
            if not unit:
                raise StoreError(404, "unit_not_found", f"Unit '{explicit}' was not found")
            selected.append(unit)
        elif linked_invoice:
            selected = conn.execute(
                "SELECT u.* FROM v2_units u JOIN v2_transaction_items ti ON ti.unit_id=u.id JOIN v2_transactions t ON t.id=ti.transaction_id WHERE t.invoice_id=%s AND u.product_id=%s AND u.deleted=0 AND u.status NOT IN ('Available','Deleted') ORDER BY ti.id LIMIT %s",
                (linked_invoice, product["id"], quantity),
            ).fetchall()
        else:
            selected = conn.execute(
                "SELECT * FROM v2_units WHERE product_id=%s AND deleted=0 AND status NOT IN ('Available','Deleted') ORDER BY id LIMIT %s",
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
        import csv
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
        import csv
        output = io.StringIO(newline="")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(["DATA (JSON)"])
        for transaction in transactions:
            writer.writerow([canonical_json(transaction)])
        return output.getvalue()

    def _raw_dump(self, conn) -> dict:
        dump: Dict[str, list] = {}
        for table in _DUMP_TABLES:
            rows = conn.execute(f"SELECT * FROM {table}").fetchall()
            dump[table] = [dict(row) for row in rows]
        return dump

    def create_backup(self, label: str = "manual") -> Path:
        safe_label = re.sub(r"[^a-zA-Z0-9_-]", "_", label)[:32] or "backup"
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        destination = self.backup_dir / f"erp_{safe_label}_{stamp}.json"
        with self.connection(read_only=True) as conn:
            payload = {
                "schemaVersion": SCHEMA_VERSION,
                "exportedAt": utc_now(),
                "tables": self._raw_dump(conn),
            }
        destination.write_text(canonical_json(payload), encoding="utf-8")
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
            self._lock_for_write(conn)
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
            self._lock_for_write(conn)
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
        for path in sorted(self.backup_dir.glob("erp_*.json"), key=lambda item: item.stat().st_mtime, reverse=True)[:limit]:
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
            self._lock_for_write(conn)
            try:
                self._meta_set(conn, "last_automatic_backup_at", utc_now())
                self._meta_set(conn, "last_automatic_backup_filename", backup.name)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        automatic = sorted(
            self.backup_dir.glob("erp_automatic_*.json"), key=lambda item: item.stat().st_mtime, reverse=True
        )
        for old in automatic[14:]:
            try:
                old.unlink()
            except OSError:
                pass
        return backup

    def validate_restore_file(self, candidate: Path) -> dict:
        candidate = Path(candidate)
        if not candidate.exists() or candidate.stat().st_size < 2:
            raise StoreError(422, "invalid_database", "Uploaded backup is empty or invalid")
        try:
            data = json.loads(candidate.read_text(encoding="utf-8"))
        except (ValueError, OSError) as exc:
            raise StoreError(422, "invalid_database", "Uploaded file is not a valid ICON MOBILE JSON backup") from exc
        if not isinstance(data, dict) or data.get("schemaVersion") != SCHEMA_VERSION:
            raise StoreError(422, "invalid_schema", "Backup belongs to an unsupported application schema")
        tables = data.get("tables")
        if not isinstance(tables, dict):
            raise StoreError(422, "invalid_schema", "Backup is missing its table dump")
        required = {"v2_products", "v2_units", "v2_transactions", "v2_operation_receipts"}
        if not required.issubset(tables.keys()):
            raise StoreError(422, "invalid_schema", "Backup is missing required tables")
        return data

    def restore(self, candidate: Path) -> Path:
        data = self.validate_restore_file(candidate)
        tables = data["tables"]
        with self._restore_lock:
            pre_restore = self.create_backup("pre_restore")
            with self.connection() as conn:
                self._lock_for_write(conn)
                try:
                    for table in reversed(_DUMP_TABLES):
                        conn.execute(f"DELETE FROM {table}")
                    for table in _DUMP_TABLES:
                        for row in tables.get(table, []):
                            if not isinstance(row, dict) or not row:
                                continue
                            columns = list(row.keys())
                            placeholders = ",".join("%s" for _ in columns)
                            column_sql = ",".join(f'"{column}"' for column in columns)
                            conn.execute(
                                f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})",
                                [row[column] for column in columns],
                            )
                    for table in _ID_TABLES:
                        conn.execute(
                            f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                            f"COALESCE((SELECT MAX(id) FROM {table}), 1), "
                            f"(SELECT COUNT(*) FROM {table}) > 0)"
                        )
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
        return pre_restore

    def integrity_status(self) -> dict:
        with self.connection(read_only=True) as conn:
            counts = {
                "products": conn.execute("SELECT COUNT(*) FROM v2_products WHERE deleted=0").fetchone()[0],
                "units": conn.execute("SELECT COUNT(*) FROM v2_units WHERE deleted=0").fetchone()[0],
                "clients": conn.execute("SELECT COUNT(*) FROM v2_clients").fetchone()[0],
                "transactions": conn.execute("SELECT COUNT(*) FROM v2_transactions").fetchone()[0],
            }
            return {"integrity": "ok", "revision": self.current_revision(conn), "counts": counts}
