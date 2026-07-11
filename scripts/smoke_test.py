"""Start an isolated server and verify the complete HTTP contract."""

from __future__ import annotations

import http.cookiejar
import json
import os
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

from websockets.sync.client import connect as websocket_connect


ROOT = Path(__file__).resolve().parents[1]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"


def main() -> int:
    if not PYTHON.exists():
        raise SystemExit("Run setup.ps1 first")
    with tempfile.TemporaryDirectory() as directory:
        temp = Path(directory)
        port = 8765
        env = os.environ.copy()
        env.update({
            "ICON_DB_FILE": str(temp / "smoke.db"),
            "ICON_BACKUP_DIR": str(temp / "backups"),
            "ICON_PORT": str(port),
            "LOG_LEVEL": "WARNING",
        })
        process = subprocess.Popen(
            [str(PYTHON), "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
            cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        base = f"http://127.0.0.1:{port}"
        jar = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

        def request(path: str, body=None):
            data = json.dumps(body).encode() if body is not None else None
            headers = {"Accept": "application/json"}
            if data is not None:
                headers["Content-Type"] = "application/json"
            call = urllib.request.Request(base + path, data=data, headers=headers)
            with opener.open(call, timeout=10) as response:
                raw = response.read()
                return response, json.loads(raw) if "json" in response.headers.get("Content-Type", "") else raw

        try:
            for _ in range(80):
                try:
                    with urllib.request.urlopen(base + "/api/health", timeout=0.5):
                        break
                except Exception:
                    time.sleep(0.1)
            else:
                raise RuntimeError("Server did not start")

            _, login = request("/api/auth/login", {"role": "admin", "password": "ADMIN@2026"})
            assert login["success"] and list(jar)
            _, settings = request("/api/v1/settings")
            assert settings["data"]["backup"]["due"] is True

            item = {
                "Select Phone or item": "Mobile Phone",
                "IMEI or Item Code": "SMOKE-SKU",
                "DATA (JSON)": json.dumps({
                    "Brand": "Apple", "Model": "Smoke Test", "Price": 1500,
                    "Units": [{"imei": "SMOKE-UNIT", "supplier": "Test", "cost": 800, "status": "Available"}],
                }),
            }
            with websocket_connect(f"ws://127.0.0.1:{port}/api/v1/events?device=smoke-live", open_timeout=5) as websocket:
                initial_event = json.loads(websocket.recv(timeout=5))
                assert initial_event["type"] == "revision"
                _, added = request("/api/v1/actions", {
                    "action": "add_item", "item": item, "operationId": "smoke-add", "deviceId": "smoke-test",
                })
                assert added["success"]
                live_event = json.loads(websocket.recv(timeout=5))
                assert live_event["revision"] == added["data"]["revision"]
                _, live_settings = request("/api/v1/settings")
                assert live_settings["data"]["connections"]["devices"] >= 1
            cookie_header = "; ".join(f"{cookie.name}={cookie.value}" for cookie in jar)
            with websocket_connect(
                f"ws://127.0.0.1:{port}/api/v1/scanner/smoke-channel?role=host",
                additional_headers={"Cookie": cookie_header}, open_timeout=5,
            ) as host_scanner:
                with websocket_connect(
                    f"ws://127.0.0.1:{port}/api/v1/scanner/smoke-channel?role=scanner",
                    open_timeout=5,
                ) as phone_scanner:
                    assert json.loads(host_scanner.recv(timeout=5))["type"] == "scanner-connected"
                    assert json.loads(phone_scanner.recv(timeout=5))["connected"] is True
                    phone_scanner.send(json.dumps({"value": "356789012345678"}))
                    scan_event = json.loads(host_scanner.recv(timeout=5))
                    assert scan_event["type"] == "scan" and scan_event["value"] == "356789012345678"
                    assert json.loads(phone_scanner.recv(timeout=5))["type"] == "ack"
            _, sold = request("/api/v1/actions", {
                "action": "checkout", "transactionType": "Sale", "invoiceId": "SMOKE-INV",
                "client": {"name": "Smoke Customer", "phone": "0771234567"},
                "items": [{"groupCode": "SMOKE-SKU", "unitImei": "SMOKE-UNIT", "finalPrice": 1500}],
                "total": 1500, "operationId": "smoke-sale", "deviceId": "smoke-test",
            })
            assert sold["success"]
            _, snapshot = request("/api/v1/snapshot")
            assert snapshot["data"]["inventory"][0]["Quantity"] == "0"
            assert snapshot["data"]["clients"][0]["name"] == "Smoke Customer"
            _, invoice = request("/api/v1/invoices/SMOKE-INV")
            assert invoice["data"]["transaction"]["client"]["name"] == "Smoke Customer"
            with urllib.request.urlopen(base + "/invoice.html?id=SMOKE-INV", timeout=5) as response:
                invoice_page = response.read()
                assert b"fetchInvoice" in invoice_page and b"invoiceId" in invoice_page

            with opener.open(base + "/api/v1/backup", timeout=15) as response:
                backup = response.read()
                assert len(backup) > 4096 and response.headers.get_content_type() == "application/vnd.sqlite3"
            _, settings = request("/api/v1/settings")
            assert settings["data"]["backup"]["due"] is False
            _, extra = request("/api/v1/actions", {
                "action": "add_item", "operationId": "smoke-extra", "deviceId": "smoke-test",
                "item": {"IMEI or Item Code": "EXTRA-SKU", "DATA (JSON)": json.dumps({
                    "Units": [{"imei": "EXTRA-UNIT", "status": "Available"}]
                })},
            })
            assert extra["success"]
            boundary = "----IconMobileSmokeBoundary"
            multipart = (
                f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"smoke.db\"\r\n"
                "Content-Type: application/vnd.sqlite3\r\n\r\n"
            ).encode() + backup + f"\r\n--{boundary}--\r\n".encode()
            restore_call = urllib.request.Request(
                base + "/api/v1/restore", data=multipart, method="POST",
                headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            )
            with opener.open(restore_call, timeout=20) as response:
                restored = json.loads(response.read())
                assert restored["success"]
            _, restored_snapshot = request("/api/v1/snapshot")
            assert [item["IMEI or Item Code"] for item in restored_snapshot["data"]["inventory"]] == ["SMOKE-SKU"]
            with urllib.request.urlopen(base + "/api/v1/qr?size=80&data=hello", timeout=5) as response:
                assert response.headers.get_content_type() == "image/png" and len(response.read()) > 100
            with urllib.request.urlopen(base + "/settings.html", timeout=5) as response:
                assert b"Settings & Backup" in response.read()
            for private_path in ("/erp.db", "/main.py", "/.env", "/_backups/test.db"):
                try:
                    urllib.request.urlopen(base + private_path, timeout=3)
                    raise AssertionError(f"Private path exposed: {private_path}")
                except urllib.error.HTTPError as error:
                    assert error.code == 404
            print("SMOKE_TEST_OK")
            return 0
        finally:
            process.terminate()
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()


if __name__ == "__main__":
    raise SystemExit(main())
