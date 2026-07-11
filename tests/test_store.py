import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from backend_store import SQLiteStore, StoreError


class SQLiteStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.store = SQLiteStore(root / "test.db", root / "backups")

    def tearDown(self):
        self.temp.cleanup()

    def add_product(self, sku="SKU-1", units=None):
        units = units or [
            {"imei": "UNIT-1", "supplier": "Supplier", "cost": 500, "status": "Available"},
            {"imei": "UNIT-2", "supplier": "Supplier", "cost": 500, "status": "Available"},
        ]
        item = {
            "Select Phone or item": "Mobile Phone",
            "IMEI or Item Code": sku,
            "Status": "Available",
            "DATA (JSON)": json.dumps({
                "Brand": "Apple", "Model": "Test", "Price": 1000, "Units": units,
            }),
        }
        return self.store.execute_action(
            {"action": "add_item", "item": item},
            actor_role="admin", device_id="test", operation_id=f"add-{sku}",
        )

    def test_sale_retry_and_return_are_atomic(self):
        self.add_product()
        sale = {
            "action": "checkout", "transactionType": "Sale", "invoiceId": "INV-1",
            "client": {"name": "Customer", "phone": "077"},
            "items": [{"groupCode": "SKU-1", "unitImei": "UNIT-1", "finalPrice": 1000}],
            "subTotal": 1000, "discount": 0, "total": 1000,
        }
        saved = self.store.execute_action(
            sale, actor_role="pos", device_id="test", operation_id="sale-1"
        )
        duplicate = self.store.execute_action(
            sale, actor_role="pos", device_id="test", operation_id="sale-1"
        )
        self.assertTrue(saved["success"])
        self.assertTrue(duplicate["duplicate"])
        first_snapshot = self.store.snapshot()
        self.assertEqual(first_snapshot["inventory"][0]["Quantity"], "1")
        self.assertEqual(first_snapshot["clients"][0]["name"], "Customer")
        self.assertEqual(first_snapshot["clients"][0]["phone"], "077")

        returned = self.store.execute_action({
            "action": "checkout", "transactionType": "Return", "invoiceId": "RET-1",
            "linkedInvoiceId": "INV-1", "client": {"name": "Customer"},
            "items": [{"groupCode": "SKU-1", "unitImei": "UNIT-1", "finalPrice": 1000}],
            "refundValue": 1000, "total": 1000,
        }, actor_role="pos", device_id="test", operation_id="return-1")
        self.assertTrue(returned["success"])
        self.assertEqual(self.store.snapshot()["inventory"][0]["Quantity"], "2")

    def test_two_devices_cannot_sell_the_same_unit(self):
        self.add_product(units=[
            {"imei": "ONLY-UNIT", "supplier": "Supplier", "cost": 500, "status": "Available"}
        ])

        def sell(number):
            payload = {
                "action": "checkout", "transactionType": "Sale", "invoiceId": f"INV-{number}",
                "client": {"name": f"Customer {number}"},
                "items": [{"groupCode": "SKU-1", "unitImei": "ONLY-UNIT", "finalPrice": 1000}],
                "total": 1000,
            }
            try:
                self.store.execute_action(
                    payload, actor_role="pos", device_id=str(number), operation_id=f"race-{number}"
                )
                return "saved"
            except StoreError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = sorted(executor.map(sell, (1, 2)))
        self.assertEqual(outcomes, ["saved", "unit_unavailable"])
        self.assertEqual(len(self.store.snapshot()["transactions"]), 1)

    def test_tampered_total_rolls_back_stock_and_client(self):
        self.add_product(units=[
            {"imei": "TOTAL-UNIT", "supplier": "Supplier", "cost": 500, "status": "Available"}
        ])
        with self.assertRaises(StoreError) as raised:
            self.store.execute_action({
                "action": "checkout", "transactionType": "Sale", "invoiceId": "BAD-TOTAL",
                "client": {"name": "Tampered Customer", "phone": "0770000000"},
                "items": [{"groupCode": "SKU-1", "unitImei": "TOTAL-UNIT", "finalPrice": 1000}],
                "total": 1,
            }, actor_role="pos", device_id="test", operation_id="bad-total")
        self.assertEqual(raised.exception.code, "total_mismatch")
        snapshot = self.store.snapshot()
        self.assertEqual(snapshot["inventory"][0]["Quantity"], "1")
        self.assertEqual(snapshot["clients"], [])
        self.assertEqual(snapshot["transactions"], [])

    def test_b2b_issue_payment_and_return(self):
        self.add_product(units=[
            {"imei": "PARTNER-UNIT", "supplier": "Supplier", "cost": 500, "status": "Available"}
        ])
        self.store.execute_action({
            "action": "checkout", "transactionType": "Issue", "invoiceId": "B2B-1",
            "recordType": "partner_invoice", "client": {"name": "Partner Shop", "isPartner": True},
            "items": [{"groupCode": "SKU-1", "unitImei": "PARTNER-UNIT", "finalPrice": 1000}],
            "total": 1000,
        }, actor_role="wholesale", device_id="test", operation_id="issue-1")
        self.assertEqual(
            self.store.snapshot()["inventory"][0]["Units"][0]["status"], "Partner:Partner Shop"
        )
        self.store.execute_action({
            "action": "checkout", "transactionType": "B2B_Payment", "invoiceId": "PMT-1",
            "recordType": "partner_payment", "linkedInvoiceId": "B2B-1",
            "client": {"name": "Partner Shop", "paymentAmount": 250}, "paymentAmount": 250,
            "items": [],
        }, actor_role="wholesale", device_id="test", operation_id="payment-1")
        self.store.execute_action({
            "action": "checkout", "transactionType": "Return", "invoiceId": "RET-B2B-1",
            "recordType": "partner_return", "linkedInvoiceId": "B2B-1",
            "client": {"name": "Partner Shop"},
            "items": [{"groupCode": "SKU-1", "unitImei": "PARTNER-UNIT", "finalPrice": 1000}],
            "refundValue": 1000, "total": 1000,
        }, actor_role="wholesale", device_id="test", operation_id="partner-return-1")
        snapshot = self.store.snapshot()
        self.assertEqual(snapshot["inventory"][0]["Quantity"], "1")
        self.assertEqual({row["transactionType"] for row in snapshot["transactions"]},
                         {"Issue", "B2B_Payment", "Return"})

    def test_backup_is_valid(self):
        self.add_product()
        self.assertTrue(self.store.backup_status()["due"])
        backup = self.store.create_backup("unit_test")
        self.assertTrue(backup.is_file())
        self.store.validate_restore_file(backup)
        status = self.store.record_external_backup(backup.name)
        self.assertFalse(status["due"])
        settings = self.store.update_settings({
            "backupIntervalDays": 14, "automaticBackupIntervalHours": 48
        })
        self.assertEqual(settings["backupIntervalDays"], 14)
        self.assertEqual(settings["automaticBackupIntervalHours"], 48)

    def test_snapshot_asset_summary_uses_normalized_costs_and_net_sales(self):
        self.add_product()
        accessory = {
            "Select Phone or item": "Accessory",
            "IMEI or Item Code": "ACC-1",
            "DATA (JSON)": json.dumps({
                "Category": "Chargers & Adapters", "Brand": "Test", "Model": "Charger",
                "Price": 250,
                "Units": [{"imei": "ACC-UNIT", "supplier": "Supplier", "cost": 100, "status": "Available"}],
            }),
        }
        self.store.execute_action(
            {"action": "add_item", "item": accessory},
            actor_role="admin", device_id="test", operation_id="add-accessory",
        )
        before = self.store.snapshot()["assets"]
        self.assertEqual(before["phoneUnits"], 2)
        self.assertEqual(before["accessoryUnits"], 1)
        self.assertEqual(before["availableUnits"], 3)
        self.assertEqual(before["stockCostValue"], 1100)
        self.assertEqual(before["stockSellingValue"], 2250)

        self.store.execute_action({
            "action": "checkout", "transactionType": "Sale", "invoiceId": "ASSET-INV",
            "client": {"name": "Asset Customer"},
            "items": [{"groupCode": "SKU-1", "unitImei": "UNIT-1", "finalPrice": 1000}],
            "total": 1000,
        }, actor_role="pos", device_id="test", operation_id="asset-sale")
        after_sale = self.store.snapshot()
        self.assertEqual(after_sale["assets"]["netSalesRevenue"], 1000)
        self.assertEqual(after_sale["assets"]["netSalesCost"], 500)
        self.assertEqual(after_sale["assets"]["grossProfit"], 500)
        self.assertEqual(after_sale["transactions"][0]["items"][0]["unitCost"], 500)

        self.store.execute_action({
            "action": "checkout", "transactionType": "Return", "invoiceId": "ASSET-RET",
            "linkedInvoiceId": "ASSET-INV", "client": {"name": "Asset Customer"},
            "items": [{"groupCode": "SKU-1", "unitImei": "UNIT-1", "finalPrice": 1000}],
            "refundValue": 1000, "total": 1000,
        }, actor_role="pos", device_id="test", operation_id="asset-return")
        after_return = self.store.snapshot()["assets"]
        self.assertEqual(after_return["netSalesRevenue"], 0)
        self.assertEqual(after_return["netSalesCost"], 0)
        self.assertEqual(after_return["grossProfit"], 0)

    def test_restore_replaces_database_and_keeps_pre_restore_copy(self):
        self.add_product()
        backup = self.store.create_backup("restore_source")
        self.add_product(
            sku="SKU-2",
            units=[{"imei": "UNIT-OTHER", "cost": 100, "status": "Available"}],
        )
        self.assertEqual(len(self.store.snapshot()["inventory"]), 2)
        pre_restore = self.store.restore(backup)
        self.assertTrue(pre_restore.is_file())
        self.assertEqual(len(self.store.snapshot()["inventory"]), 1)

    def test_legacy_partner_profile_import_is_idempotent(self):
        record = {
            "invoiceId": "PROFILE-1", "transactionType": "Partner_Profile",
            "recordType": "partner_profile",
            "client": {"name": "Legacy Partner", "phone": "0779999999", "isPartner": True},
            "date": "2025-01-01T00:00:00Z",
        }
        self.assertTrue(self.store.import_legacy_transaction(record, source_name="clients.csv"))
        self.assertFalse(self.store.import_legacy_transaction(record, source_name="clients.csv"))
        snapshot = self.store.snapshot()
        self.assertEqual(snapshot["transactions"][0]["transactionType"], "Partner_Profile")
        self.assertEqual(snapshot["clients"][0]["type"], "B2B")


if __name__ == "__main__":
    unittest.main()
