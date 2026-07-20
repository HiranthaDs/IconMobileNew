"""ICON MOBILE backend (Supabase/Postgres).

FastAPI application that preserves every endpoint and behaviour of the original
local ERP server, but stores all authoritative records in Supabase Postgres via
``PostgresStore`` instead of a local SQLite file.  Static pages and assets are
served from the ``frontend/`` directory.

Run from the ``backend`` directory so the ``app`` package resolves:

    cd backend
    ../.venv/Scripts/python.exe -m uvicorn app.main:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
import socket
import tempfile
import time
import uuid
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, File, HTTPException, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from app.core.security import COOKIE_NAME, PASSWORDS, SessionIdentity, get_signer, password_matches
from app.store.helpers import SCHEMA_VERSION, StoreError, utc_now
from app.store.store import PostgresStore

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKEND_DIR = Path(__file__).resolve().parents[1]
FRONTEND_DIR = PROJECT_ROOT / "frontend"
PAGES_DIR = FRONTEND_DIR / "pages"
ASSETS_DIR = FRONTEND_DIR / "assets"

load_dotenv(PROJECT_ROOT / ".env")

HOST = os.environ.get("ICON_HOST", "0.0.0.0")
PORT = int(os.environ.get("ICON_PORT", "8000"))
MAX_REQUEST_BYTES = max(64 * 1024, int(os.environ.get("MAX_REQUEST_BYTES", str(5 * 1024 * 1024))))
MAX_RESTORE_BYTES = max(MAX_REQUEST_BYTES, int(os.environ.get("MAX_RESTORE_BYTES", str(512 * 1024 * 1024))))
BACKUP_DIR = Path(os.environ.get("ICON_BACKUP_DIR", str(PROJECT_ROOT / "_backups")))
if not BACKUP_DIR.is_absolute():
    BACKUP_DIR = (PROJECT_ROOT / BACKUP_DIR).resolve()

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("icon-mobile")

store = PostgresStore(BACKUP_DIR)
signer = get_signer()

app = FastAPI(
    title="ICON MOBILE Backend API",
    version="4.0.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


class LiveConnectionRegistry:
    def __init__(self) -> None:
        self._connections: Dict[WebSocket, dict] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, device_id: str) -> bool:
        await websocket.accept()
        now = utc_now()
        async with self._lock:
            if len(self._connections) >= 200:
                await websocket.close(code=1013, reason="Too many live connections")
                return False
            self._connections[websocket] = {
                "deviceId": re.sub(r"[^a-zA-Z0-9_.:-]", "", device_id)[:180] or "unknown-device",
                "ip": websocket.client.host if websocket.client else "unknown",
                "connectedAt": now,
                "lastSeenAt": now,
                "sendLock": asyncio.Lock(),
            }
        return True

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._connections.pop(websocket, None)

    async def send(self, websocket: WebSocket, payload: dict) -> bool:
        async with self._lock:
            entry = self._connections.get(websocket)
        if not entry:
            return False
        try:
            async with entry["sendLock"]:
                await asyncio.wait_for(websocket.send_json(payload), timeout=1.0)
            entry["lastSeenAt"] = utc_now()
            return True
        except Exception:
            await self.disconnect(websocket)
            return False

    async def broadcast_revision(self, revision: int) -> None:
        async with self._lock:
            sockets = list(self._connections)
        payload = {"type": "revision", "revision": int(revision), "serverTime": utc_now()}
        if sockets:
            await asyncio.gather(*(self.send(socket, payload) for socket in sockets), return_exceptions=True)

    async def broadcast_reset(self, revision: int) -> None:
        async with self._lock:
            sockets = list(self._connections)
        payload = {"type": "reset", "revision": int(revision), "serverTime": utc_now()}
        if sockets:
            await asyncio.gather(*(self.send(socket, payload) for socket in sockets), return_exceptions=True)

    async def summary(self, include_details: bool = False) -> dict:
        async with self._lock:
            values = list(self._connections.values())
        unique_devices = {value["deviceId"] for value in values}
        result = {"connections": len(values), "devices": len(unique_devices)}
        if include_details:
            result["items"] = [{key: value[key] for key in ("deviceId", "ip", "connectedAt", "lastSeenAt")}
                               for value in values]
        return result


live_connections = LiveConnectionRegistry()
scanner_hosts: Dict[str, dict] = {}
scanner_hosts_lock = asyncio.Lock()
_automatic_backup_task: Optional[asyncio.Task] = None


# ---------------------------------------------------------------------------
# Common response / security handling
# ---------------------------------------------------------------------------


def error_response(status: int, code: str, message: str, details: Any = None) -> JSONResponse:
    payload: Dict[str, Any] = {"success": False, "error": message, "message": message, "code": code}
    if details is not None:
        payload["details"] = details
    return JSONResponse(payload, status_code=status)


@app.exception_handler(StoreError)
async def handle_store_error(_: Request, exc: StoreError) -> JSONResponse:
    return error_response(exc.status, exc.code, exc.message, exc.details)


@app.exception_handler(HTTPException)
async def handle_http_error(_: Request, exc: HTTPException) -> JSONResponse:
    detail = exc.detail
    if isinstance(detail, dict):
        return error_response(exc.status_code, str(detail.get("code", "http_error")), str(detail.get("message", detail)), detail)
    return error_response(exc.status_code, "http_error", str(detail))


@app.exception_handler(RequestValidationError)
async def handle_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    return error_response(422, "invalid_request", "Request validation failed", exc.errors())


@app.exception_handler(Exception)
async def handle_unexpected_error(_: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled server error", exc_info=exc)
    return error_response(500, "internal_error", "The server encountered an unexpected error")


def _same_origin(request: Request) -> bool:
    origin = request.headers.get("origin", "").rstrip("/")
    if not origin:
        return True
    expected = f"{request.url.scheme}://{request.headers.get('host', '')}".rstrip("/")
    return origin == expected


@app.middleware("http")
async def security_middleware(request: Request, call_next):
    if request.url.path.startswith("/api/"):
        length = request.headers.get("content-length")
        if length:
            try:
                request_limit = MAX_RESTORE_BYTES if request.url.path == "/api/v1/restore" else MAX_REQUEST_BYTES
                if int(length) > request_limit:
                    return error_response(413, "request_too_large", "Request is larger than the configured limit")
            except ValueError:
                return error_response(400, "invalid_content_length", "Invalid Content-Length header")
        if request.method not in {"GET", "HEAD", "OPTIONS"} and not _same_origin(request):
            return error_response(403, "origin_rejected", "Cross-origin changes are not allowed")

    response: Response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["Referrer-Policy"] = "same-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=()"
    if request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
    elif request.url.path.startswith("/assets/"):
        response.headers["Cache-Control"] = "public, max-age=3600, must-revalidate"
    return response


# ---------------------------------------------------------------------------
# Authentication and rate limiting
# ---------------------------------------------------------------------------


_login_attempts: Dict[str, deque] = defaultdict(deque)
_login_lock = asyncio.Lock()
LOGIN_WINDOW_SECONDS = 5 * 60
LOGIN_MAX_FAILURES = 8


def _client_key(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _prune_attempts(key: str) -> deque:
    now = time.monotonic()
    attempts = _login_attempts[key]
    while attempts and attempts[0] < now - LOGIN_WINDOW_SECONDS:
        attempts.popleft()
    return attempts


def require_identity(request: Request, roles: Optional[Iterable[str]] = None) -> SessionIdentity:
    identity = signer.verify(request.cookies.get(COOKIE_NAME))
    if not identity:
        raise StoreError(401, "authentication_required", "Please sign in to the server")
    allowed = set(roles or ())
    if allowed and identity.role not in allowed:
        raise StoreError(403, "forbidden", "Your account cannot perform this operation")
    return identity


async def _read_json_object(request: Request) -> dict:
    raw = await request.body()
    if not raw:
        return dict(request.query_params)
    if len(raw) > MAX_REQUEST_BYTES:
        raise StoreError(413, "request_too_large", "Request is larger than the configured limit")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StoreError(400, "invalid_json", "Request body must contain valid JSON") from exc
    if not isinstance(value, dict):
        raise StoreError(422, "invalid_payload", "JSON request body must be an object")
    return value


@app.post("/api/auth/login")
async def login(request: Request) -> JSONResponse:
    body = await _read_json_object(request)
    role = str(body.get("role", "")).strip().lower()
    password = body.get("password", "")
    if role not in PASSWORDS:
        raise StoreError(401, "invalid_credentials", "Incorrect role or password")
    key = _client_key(request)
    async with _login_lock:
        attempts = _prune_attempts(key)
        if len(attempts) >= LOGIN_MAX_FAILURES:
            wait = max(1, int(LOGIN_WINDOW_SECONDS - (time.monotonic() - attempts[0])))
            raise StoreError(429, "login_locked", f"Too many failed logins. Try again in {wait} seconds")
    if not password_matches(password, PASSWORDS[role]):
        async with _login_lock:
            _prune_attempts(key).append(time.monotonic())
        raise StoreError(401, "invalid_credentials", "Incorrect role or password")
    async with _login_lock:
        _login_attempts.pop(key, None)
    token, identity = signer.issue(role)
    response = JSONResponse({
        "success": True,
        "message": "Signed in to the server.",
        "data": {"role": identity.role, "expiresAt": identity.expires_at},
    })
    response.set_cookie(
        COOKIE_NAME,
        token,
        max_age=signer.lifetime_seconds,
        httponly=True,
        secure=request.url.scheme == "https",
        samesite="strict",
        path="/",
    )
    return response


@app.post("/api/auth/logout")
async def logout(_: Request) -> JSONResponse:
    response = JSONResponse({"success": True, "message": "Signed out."})
    response.delete_cookie(COOKIE_NAME, path="/", samesite="strict")
    return response


@app.get("/api/auth/session")
async def session_info(request: Request) -> dict:
    identity = require_identity(request)
    return {"success": True, "data": {"role": identity.role, "expiresAt": identity.expires_at}}


# ---------------------------------------------------------------------------
# LAN discovery helpers
# ---------------------------------------------------------------------------


def lan_ipv4_addresses() -> list:
    addresses: set = set()
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.connect(("192.0.2.1", 80))
        addresses.add(probe.getsockname()[0])
        probe.close()
    except OSError:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            value = info[4][0]
            if value and not value.startswith("127.") and not value.startswith("169.254."):
                addresses.add(value)
    except OSError:
        pass
    return sorted(addresses)


def lan_metadata() -> dict:
    addresses = lan_ipv4_addresses()
    urls = [f"http://{address}:{PORT}" for address in addresses]
    return {"lanUrls": urls, "lanBaseUrl": urls[0] if urls else ""}


async def _automatic_backup_loop() -> None:
    while True:
        try:
            path = await run_in_threadpool(store.ensure_automatic_backup)
            if path:
                logger.info("Created automatic backup: %s", path.name)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Automatic backup check failed")
        await asyncio.sleep(60 * 60)


@app.on_event("startup")
async def start_background_services() -> None:
    global _automatic_backup_task
    _automatic_backup_task = asyncio.create_task(_automatic_backup_loop())


@app.on_event("shutdown")
async def stop_background_services() -> None:
    if _automatic_backup_task:
        _automatic_backup_task.cancel()
        try:
            await _automatic_backup_task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# JSON API and live revision stream
# ---------------------------------------------------------------------------


@app.get("/api/v1/snapshot")
async def get_snapshot(request: Request) -> dict:
    require_identity(request)
    data = await run_in_threadpool(store.snapshot)
    data.update(lan_metadata())
    data["backupReminder"] = await run_in_threadpool(store.backup_status)
    return {"success": True, "data": data}


@app.get("/api/v1/status")
async def get_status() -> dict:
    revision = await run_in_threadpool(store.current_revision)
    connections = await live_connections.summary()
    return {
        "success": True,
        "data": {
            "status": "online", "revision": revision, "serverTime": utc_now(),
            "schemaVersion": SCHEMA_VERSION, **lan_metadata(), **connections,
        },
    }


@app.get("/api/v1/invoices/{invoice_id}")
async def get_invoice(invoice_id: str) -> dict:
    transaction = await run_in_threadpool(store.get_invoice, invoice_id)
    return {"success": True, "data": {"transaction": transaction}}


@app.get("/api/v1/operations/{operation_id}")
async def get_operation(operation_id: str, request: Request) -> dict:
    require_identity(request)
    result = await run_in_threadpool(store.get_operation, operation_id)
    if not result:
        raise StoreError(404, "operation_not_found", "Operation receipt was not found")
    return result


async def _execute_payload(request: Request, body: dict) -> dict:
    identity = require_identity(request)
    operation_id = str(body.get("operationId") or request.headers.get("x-operation-id") or uuid.uuid4())
    device_id = str(body.get("deviceId") or request.headers.get("x-device-id") or "unknown-device")
    result = await run_in_threadpool(
        store.execute_action,
        body,
        actor_role=identity.role,
        device_id=device_id,
        operation_id=operation_id,
    )
    revision = result.get("data", {}).get("revision")
    if revision is not None:
        await live_connections.broadcast_revision(int(revision))
    return result


@app.post("/api/v1/actions")
async def action(request: Request) -> dict:
    return await _execute_payload(request, await _read_json_object(request))


@app.api_route("/exec", methods=["GET", "POST"])
async def compatibility_exec(request: Request) -> dict:
    body = await _read_json_object(request)
    action_name = str(body.get("action", "fetch_data"))
    if action_name in {"fetch_data", "getData", "get", "load"}:
        require_identity(request)
        data = await run_in_threadpool(lambda: store.snapshot(include_legacy_csv=True))
        data.update(lan_metadata())
        return {"success": True, "message": "Snapshot loaded.", "data": data}
    return await _execute_payload(request, body)


@app.websocket("/api/v1/events")
async def revision_events(websocket: WebSocket) -> None:
    device_id = websocket.query_params.get("device", "unknown-device")
    if not await live_connections.connect(websocket, device_id):
        return
    try:
        last_revision = await run_in_threadpool(store.current_revision)
        await live_connections.send(websocket, {"type": "revision", "revision": last_revision, "serverTime": utc_now()})
        while True:
            await asyncio.sleep(5)
            revision = await run_in_threadpool(store.current_revision)
            if revision != last_revision:
                await live_connections.send(websocket, {"type": "revision", "revision": revision, "serverTime": utc_now()})
                last_revision = revision
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        await live_connections.disconnect(websocket)


@app.websocket("/api/v1/scanner/{channel_id}")
async def local_scanner_channel(websocket: WebSocket, channel_id: str) -> None:
    channel_id = re.sub(r"[^a-zA-Z0-9_-]", "", channel_id)[:120]
    role = websocket.query_params.get("role", "scanner")
    if not channel_id:
        await websocket.close(code=1008, reason="Invalid scanner channel")
        return
    if role == "host":
        if not signer.verify(websocket.cookies.get(COOKIE_NAME)):
            await websocket.close(code=4401, reason="Host login required")
            return
        await websocket.accept()
        entry = {"socket": websocket, "sendLock": asyncio.Lock(), "connectedAt": utc_now()}
        async with scanner_hosts_lock:
            previous = scanner_hosts.get(channel_id)
            scanner_hosts[channel_id] = entry
        if previous:
            try:
                await previous["socket"].close(code=1000, reason="Scanner host replaced")
            except Exception:
                pass
        try:
            while True:
                await websocket.receive_text()
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            async with scanner_hosts_lock:
                if scanner_hosts.get(channel_id) is entry:
                    scanner_hosts.pop(channel_id, None)
        return

    await websocket.accept()
    async with scanner_hosts_lock:
        host = scanner_hosts.get(channel_id)
    if host:
        try:
            async with host["sendLock"]:
                await asyncio.wait_for(host["socket"].send_json({"type": "scanner-connected"}), timeout=1)
        except Exception:
            host = None
    await websocket.send_json({"type": "status", "connected": bool(host)})
    try:
        while True:
            message = await websocket.receive_text()
            try:
                parsed = json.loads(message)
                value = str(parsed.get("value", "")) if isinstance(parsed, dict) else str(parsed)
            except json.JSONDecodeError:
                value = message
            value = value.strip()
            if not value or len(value) > 240:
                await websocket.send_json({"type": "error", "message": "Invalid scan value"})
                continue
            async with scanner_hosts_lock:
                host = scanner_hosts.get(channel_id)
            if not host:
                await websocket.send_json({"type": "status", "connected": False})
                continue
            try:
                async with host["sendLock"]:
                    await asyncio.wait_for(
                        host["socket"].send_json({"type": "scan", "value": value, "time": utc_now()}),
                        timeout=1,
                    )
                await websocket.send_json({"type": "ack", "value": value})
            except Exception:
                await websocket.send_json({"type": "status", "connected": False})
    except (WebSocketDisconnect, RuntimeError):
        return


# ---------------------------------------------------------------------------
# Local QR/barcode generation
# ---------------------------------------------------------------------------


@app.get("/api/v1/qr")
async def local_qr(data: str, size: int = 180) -> StreamingResponse:
    if not data or len(data) > 4096:
        raise StoreError(422, "invalid_qr_data", "QR content must be between 1 and 4096 characters")
    size = max(64, min(600, int(size)))
    try:
        import qrcode
        from PIL import Image
    except ImportError as exc:
        raise StoreError(503, "qr_dependency_missing", "Install the QR dependencies from requirements.txt") from exc

    def render() -> bytes:
        code = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_M, border=2)
        code.add_data(data)
        code.make(fit=True)
        image = code.make_image(fill_color="black", back_color="white").convert("RGB")
        image = image.resize((size, size), Image.Resampling.NEAREST)
        buffer = io.BytesIO()
        image.save(buffer, format="PNG", optimize=True)
        return buffer.getvalue()

    return StreamingResponse(io.BytesIO(await run_in_threadpool(render)), media_type="image/png",
                             headers={"Cache-Control": "private, max-age=300"})


@app.get("/api/v1/barcode")
async def local_barcode(text: str) -> StreamingResponse:
    text = str(text or "").strip()
    if not text or len(text) > 180:
        raise StoreError(422, "invalid_barcode", "Barcode text must be between 1 and 180 characters")
    try:
        import barcode
        from barcode.writer import ImageWriter
    except ImportError as exc:
        raise StoreError(503, "barcode_dependency_missing", "Install barcode dependencies from requirements.txt") from exc

    def render() -> bytes:
        buffer = io.BytesIO()
        barcode.get("code128", text, writer=ImageWriter()).write(
            buffer,
            options={"write_text": True, "quiet_zone": 2.0, "module_height": 9.0, "font_size": 8},
        )
        return buffer.getvalue()

    return StreamingResponse(io.BytesIO(await run_in_threadpool(render)), media_type="image/png",
                             headers={"Cache-Control": "private, max-age=300"})


# ---------------------------------------------------------------------------
# Protected settings, backup, restore, export, and health
# ---------------------------------------------------------------------------


@app.get("/api/v1/settings")
async def settings_state(request: Request) -> dict:
    require_identity(request, {"admin"})
    settings, backup, backups, integrity = await asyncio.gather(
        run_in_threadpool(store.get_settings),
        run_in_threadpool(store.backup_status),
        run_in_threadpool(store.list_backups),
        run_in_threadpool(store.integrity_status),
    )
    connections = await live_connections.summary(include_details=True)
    return {"success": True, "data": {
        "settings": settings, "backup": backup, "backups": backups,
        "database": integrity, "connections": connections, **lan_metadata(),
    }}


@app.post("/api/v1/settings")
async def update_settings(request: Request) -> dict:
    require_identity(request, {"admin"})
    values = await _read_json_object(request)
    settings = await run_in_threadpool(store.update_settings, values)
    return {"success": True, "message": "Settings saved.", "data": {"settings": settings}}


@app.get("/api/v1/connections")
async def connection_state(request: Request) -> dict:
    require_identity(request, {"admin"})
    return {"success": True, "data": await live_connections.summary(include_details=True)}


@app.get("/api/v1/backup")
async def download_backup(request: Request) -> FileResponse:
    require_identity(request, {"admin"})
    path = await run_in_threadpool(store.create_backup, "manual")
    await run_in_threadpool(store.record_external_backup, path.name)
    return FileResponse(path, filename=path.name, media_type="application/json")


@app.post("/api/v1/restore")
async def restore_backup(request: Request, file: UploadFile = File(...)) -> dict:
    require_identity(request, {"admin"})
    descriptor, name = tempfile.mkstemp(prefix="icon_restore_", suffix=".upload.tmp", dir=BACKUP_DIR)
    os.close(descriptor)
    candidate = Path(name)
    total = 0
    try:
        with candidate.open("wb") as handle:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_RESTORE_BYTES:
                    raise StoreError(413, "restore_too_large", "Backup upload exceeds the configured limit")
                handle.write(chunk)
            handle.flush()
            os.fsync(handle.fileno())
        pre_restore = await run_in_threadpool(store.restore, candidate)
        await run_in_threadpool(store.record_external_backup, f"restored-{Path(file.filename or 'backup.json').name}")
        status = await run_in_threadpool(store.integrity_status)
        await live_connections.broadcast_reset(int(status["revision"]))
        return {
            "success": True,
            "message": "Database restored and validated.",
            "data": {"preRestoreBackup": pre_restore.name if pre_restore else "", **status},
        }
    finally:
        try:
            candidate.unlink()
        except FileNotFoundError:
            pass


@app.get("/api/v1/export/{kind}.csv")
async def export_csv(kind: str, request: Request) -> Response:
    require_identity(request, {"admin", "wholesale"})
    snapshot = await run_in_threadpool(store.snapshot)
    if kind == "inventory":
        content = store._inventory_csv(snapshot["inventory"])
    elif kind in {"transactions", "ledger"}:
        content = store._transactions_csv(snapshot["transactions"])
    elif kind == "clients":
        rows = snapshot.get("clients", [])
        buffer = io.StringIO(newline="")
        import csv
        writer = csv.DictWriter(
            buffer, fieldnames=["id", "name", "phone", "email", "type", "createdAt", "updatedAt"],
            extrasaction="ignore", lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
        content = buffer.getvalue()
    else:
        raise StoreError(404, "export_not_found", "Export must be inventory, transactions, or clients")
    return Response(content, media_type="text/csv; charset=utf-8", headers={
        "Content-Disposition": f'attachment; filename="{kind}.csv"'
    })


@app.get("/api/health")
async def health() -> dict:
    status = await run_in_threadpool(store.integrity_status)
    return {"success": True, "status": "online", "database": "supabase", **status, **lan_metadata()}


# ---------------------------------------------------------------------------
# Static frontend.  Pages come from frontend/pages, assets from frontend/assets.
# ---------------------------------------------------------------------------


if ASSETS_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")

STATIC_FILES = {
    "/": "index.html",
    "/index.html": "index.html",
    "/ADMINPRO.html": "ADMINPRO.html",
    "/adminpro.html": "ADMINPRO.html",
    "/wholesale.html": "wholesale.html",
    "/invoice.html": "invoice.html",
    "/settings.html": "settings.html",
    "/scanner.html": "scanner.html",
    "/B2Binvoice.html": "B2Binvoice.html",
}


def make_static_handler(path: Path):
    async def serve() -> FileResponse:
        if not path.is_file():
            raise HTTPException(404, "Static file not found")
        return FileResponse(path)
    return serve


for route_path, filename in STATIC_FILES.items():
    app.add_api_route(route_path, make_static_handler(PAGES_DIR / filename), methods=["GET"], include_in_schema=False)


async def _serve_service_worker() -> FileResponse:
    path = FRONTEND_DIR / "service-worker.js"
    if not path.is_file():
        raise HTTPException(404, "Static file not found")
    return FileResponse(path, media_type="application/javascript")


app.add_api_route("/service-worker.js", _serve_service_worker, methods=["GET"], include_in_schema=False)


@app.get("/{unknown_path:path}", include_in_schema=False)
async def static_not_found(unknown_path: str) -> HTMLResponse:
    return HTMLResponse(
        "<h1>404 - Not Found</h1><p>This file is not exposed by the ICON MOBILE server.</p>",
        status_code=404,
    )
