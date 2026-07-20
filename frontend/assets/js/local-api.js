(function () {
  "use strict";

  // Backend origin used when this page is served from a different origin than
  // the API (e.g. a separate static-site frontend). Override by setting
  // window.ICON_API_BASE before this script loads. Left empty ("") means
  // "same origin as this page" — the default when the FastAPI backend serves
  // the frontend itself.
  const BACKEND_ORIGIN = "https://icon-mobile-erp.onrender.com";
  const API_BASE = (function () {
    if (typeof window.ICON_API_BASE === "string") return window.ICON_API_BASE.replace(/\/$/, "");
    if (typeof window !== "undefined" && window.location) {
      const host = window.location.hostname.toLowerCase();
      if (host === "localhost" || host === "127.0.0.1" || window.location.origin.replace(/\/$/, "") === BACKEND_ORIGIN) {
        return "";
      }
    }
    return BACKEND_ORIGIN;
  })();
  const API_ROOT = `${API_BASE}/api/v1`;
  const DEVICE_KEY = "ICON_MOBILE_DEVICE_ID_V1";
  const SNAPSHOT_KEY = "ICON_MOBILE_LOCAL_SNAPSHOT_V1";
  const BACKUP_REMINDER_KEY = "ICON_MOBILE_BACKUP_REMINDER_SHOWN_V1";
  const REQUEST_TIMEOUT_MS = 20000;
  const subscribers = new Set();
  const channel = typeof BroadcastChannel !== "undefined"
    ? new BroadcastChannel("icon-mobile-lan-sync-v1")
    : null;

  function isLoopbackHost(hostname) {
    const host = String(hostname || "").toLowerCase().replace(/^\[|\]$/g, "");
    return host === "localhost" || host === "127.0.0.1" || host === "::1";
  }

  // Links shared outside this device (WhatsApp receipts, invoice QR codes) must
  // always be reachable from the public internet. A private/loopback address
  // only works for other devices on the same LAN as the host computer, so any
  // link generated while viewing the app from one of these must fall back to
  // the known-public backend origin instead.
  function isPrivateHost(hostname) {
    const host = String(hostname || "").toLowerCase().replace(/^\[|\]$/g, "");
    if (isLoopbackHost(host)) return true;
    if (host.endsWith(".local")) return true;
    return /^(10\.|169\.254\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)/.test(host);
  }

  function currentHostedOrigin() {
    return /^https?:$/i.test(window.location.protocol) ? window.location.origin.replace(/\/$/, "") : "";
  }

  let knownRevision = 0;
  let socket = null;
  let reconnectTimer = null;
  let reconnectAttempt = 0;
  let pollTimer = null;
  let manuallyStopped = false;
  let snapshotRequest = null;
  const runtimeOrigin = currentHostedOrigin();

  function storageGet(key, fallback) {
    try {
      const value = window.localStorage.getItem(key);
      return value === null ? fallback : value;
    } catch (_) {
      return fallback;
    }
  }

  function storageSet(key, value) {
    try {
      window.localStorage.setItem(key, value);
      return true;
    } catch (_) {
      return false;
    }
  }

  function randomId(prefix) {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return `${prefix}-${window.crypto.randomUUID()}`;
    }
    const random = Math.random().toString(36).slice(2);
    return `${prefix}-${Date.now().toString(36)}-${random}`;
  }

  function getDeviceId() {
    let id = storageGet(DEVICE_KEY, "");
    if (!id) {
      id = randomId("device");
      storageSet(DEVICE_KEY, id);
    }
    return id;
  }

  function makeError(message, status, payload, code) {
    const error = new Error(message || "The local server request failed.");
    error.status = status || 0;
    error.code = code || (payload && payload.code) || "REQUEST_FAILED";
    error.payload = payload || null;
    return error;
  }

  async function request(path, options) {
    const opts = options || {};
    const url = String(path || "").startsWith("/api") ? `${API_BASE}${path}` : path;
    const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
    const timeout = setTimeout(function () {
      if (controller) controller.abort();
    }, opts.timeoutMs || REQUEST_TIMEOUT_MS);

    const headers = Object.assign(
      { Accept: "application/json", "X-Device-ID": getDeviceId() },
      opts.headers || {}
    );
    if (opts.body !== undefined && !(opts.body instanceof FormData) && !headers["Content-Type"]) {
      headers["Content-Type"] = "application/json";
    }

    try {
      const response = await fetch(url, {
        method: opts.method || "GET",
        headers: headers,
        body: opts.body === undefined
          ? undefined
          : (opts.body instanceof FormData ? opts.body : JSON.stringify(opts.body)),
        credentials: API_BASE ? "include" : "same-origin",
        cache: "no-store",
        signal: controller ? controller.signal : undefined
      });

      const text = await response.text();
      let payload = null;
      if (text) {
        try {
          payload = JSON.parse(text);
        } catch (_) {
          throw makeError("The local server returned invalid JSON.", response.status, null, "INVALID_JSON");
        }
      }

      if (!response.ok || (payload && payload.success === false)) {
        const message = payload && (payload.error || payload.message || payload.detail);
        if (response.status === 401) {
          window.dispatchEvent(new CustomEvent("icon:auth-required"));
        }
        if (payload && payload.code === "token_limited") {
          window.dispatchEvent(new CustomEvent("icon:token-limited", { detail: payload.details }));
        }
        throw makeError(message || `Request failed (${response.status}).`, response.status, payload);
      }
      return payload || { success: true };
    } catch (error) {
      if (error && error.name === "AbortError") {
        throw makeError("The local server request timed out.", 0, null, "TIMEOUT");
      }
      if (error instanceof TypeError) {
        throw makeError("The LAN server is unavailable. Check Wi-Fi and the host computer.", 0, null, "OFFLINE");
      }
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }

  function emit(event) {
    subscribers.forEach(function (callback) {
      try { callback(event); } catch (error) { console.error("ICON sync subscriber failed", error); }
    });
    window.dispatchEvent(new CustomEvent("icon:sync", { detail: event }));
  }

  function acceptRevision(revision, source) {
    const next = Number(revision || 0);
    if (!Number.isFinite(next) || next <= knownRevision) return false;
    knownRevision = next;
    emit({ type: "revision", revision: next, source: source || "server" });
    return true;
  }

  function cacheSnapshot(snapshot) {
    if (!snapshot || typeof snapshot !== "object") return false;
    const envelope = {
      schemaVersion: Number(snapshot.schemaVersion || 1),
      revision: Number(snapshot.revision || 0),
      savedAt: Date.now(),
      data: snapshot
    };
    knownRevision = Math.max(knownRevision, envelope.revision);
    return storageSet(SNAPSHOT_KEY, JSON.stringify(envelope));
  }

  function maybeShowBackupReminder(status) {
    if (!status || status.due !== true) return;
    const today = new Date().toISOString().slice(0, 10);
    if (storageGet(BACKUP_REMINDER_KEY, "") === today) return;
    storageSet(BACKUP_REMINDER_KEY, today);
    window.dispatchEvent(new CustomEvent("icon:backup-due", { detail: status }));
    setTimeout(function () {
      const openSettings = window.confirm(
        "Weekly backup reminder\n\nYour SQLite database backup is due. " +
        "Open Settings & Backup now to save it to a pen drive or share it?"
      );
      if (openSettings) window.location.href = "/settings.html";
    }, 500);
  }

  function getShareUrl(path) {
    const suffix = String(path || "");
    const base = (isPrivateHost(window.location.hostname) ? BACKEND_ORIGIN : runtimeOrigin).replace(/\/$/, "");
    return `${base}${suffix.startsWith("/") ? suffix : `/${suffix}`}`;
  }

  function readCachedSnapshot() {
    try {
      const parsed = JSON.parse(storageGet(SNAPSHOT_KEY, "null"));
      return parsed && parsed.data ? parsed : null;
    } catch (_) {
      return null;
    }
  }

  async function login(role, password) {
    const result = await request(`${API_BASE}/api/auth/login`, {
      method: "POST",
      body: { role: role, password: password }
    });
    startSync();
    return result;
  }

  async function logout() {
    try {
      return await request(`${API_BASE}/api/auth/logout`, { method: "POST", body: {} });
    } finally {
      stopSync();
    }
  }

  async function getSnapshot() {
    // One in-flight snapshot per browser context prevents focus, WebSocket and
    // post-action refreshes from racing and applying responses out of order.
    if (snapshotRequest) return snapshotRequest;
    snapshotRequest = (async function () {
      let result = await request(`${API_ROOT}/snapshot`);
      let serverRevision = Number(result && result.data && result.data.revision || 0);
      // A revision event may arrive while SQLite is preparing this snapshot.
      // Chase it once so callers never paint an already-obsolete committed view.
      if (serverRevision < knownRevision) {
        result = await request(`${API_ROOT}/snapshot`);
        serverRevision = Number(result && result.data && result.data.revision || 0);
      }
      if (result && result.data) {
        cacheSnapshot(result.data);
        maybeShowBackupReminder(result.data.backupReminder);
      }
      return result;
    })();
    try {
      return await snapshotRequest;
    } finally {
      snapshotRequest = null;
    }
  }

  async function getInvoice(invoiceId) {
    return request(`${API_ROOT}/invoices/${encodeURIComponent(String(invoiceId || ""))}`);
  }

  async function getOperation(operationId) {
    return request(`${API_ROOT}/operations/${encodeURIComponent(operationId)}`);
  }

  async function action(payload) {
    const operationId = payload && payload.operationId
      ? String(payload.operationId)
      : randomId("op");
    const body = Object.assign({}, payload || {}, {
      operationId: operationId,
      deviceId: getDeviceId()
    });

    let result;
    try {
      result = await request(`${API_ROOT}/actions`, {
        method: "POST",
        headers: { "X-Operation-ID": operationId },
        body: body,
        timeoutMs: 30000
      });
    } catch (error) {
      if (error && (error.code === "TIMEOUT" || error.code === "OFFLINE")) {
        try {
          result = await getOperation(operationId);
        } catch (_) {
          throw error;
        }
      } else {
        throw error;
      }
    }

    const revision = result && result.data && result.data.revision;
    if (revision) {
      acceptRevision(revision, "action");
      if (channel) channel.postMessage({ type: "revision", revision: revision });
    }
    return result;
  }

  function wsOrigin() {
    if (API_BASE) return API_BASE.replace(/^http/i, "ws");
    const scheme = window.location.protocol === "https:" ? "wss:" : "ws:";
    return `${scheme}//${window.location.host}`;
  }

  function socketUrl() {
    return `${wsOrigin()}/api/v1/events?device=${encodeURIComponent(getDeviceId())}`;
  }

  function scheduleReconnect() {
    if (manuallyStopped || reconnectTimer) return;
    const delay = Math.min(15000, 750 * Math.pow(2, reconnectAttempt++));
    reconnectTimer = setTimeout(function () {
      reconnectTimer = null;
      connectSocket();
    }, delay);
  }

  function startPollingFallback() {
    if (pollTimer || manuallyStopped) return;
    pollTimer = setInterval(async function () {
      try {
        const result = await request(`${API_ROOT}/status`, { timeoutMs: 5000 });
        if (result && result.data) acceptRevision(result.data.revision, "poll");
      } catch (_) {
        // A cached page remains readable while the host is temporarily unavailable.
      }
    }, 7500);
  }

  function stopPollingFallback() {
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = null;
  }

  function connectSocket() {
    if (manuallyStopped || socket || !window.WebSocket || !window.location.host) {
      startPollingFallback();
      return;
    }
    try {
      socket = new WebSocket(socketUrl());
      socket.onopen = function () {
        reconnectAttempt = 0;
        stopPollingFallback();
        emit({ type: "connection", online: true, source: "websocket" });
      };
      socket.onmessage = function (message) {
        try {
          const event = JSON.parse(message.data);
          if (event.type === "revision") {
            const serverRevision = Number(event.revision || 0);
            if (serverRevision < knownRevision) {
              knownRevision = serverRevision;
              emit({ type: "reset", revision: serverRevision, source: "websocket" });
            } else {
              acceptRevision(serverRevision, "websocket");
            }
          }
          if (event.type === "reset") {
            knownRevision = Number(event.revision || 0);
            emit({ type: "reset", revision: knownRevision, source: "websocket" });
          }
        } catch (_) { /* Ignore malformed heartbeat frames. */ }
      };
      socket.onerror = function () {
        try { socket.close(); } catch (_) { /* noop */ }
      };
      socket.onclose = function () {
        socket = null;
        emit({ type: "connection", online: false, source: "websocket" });
        startPollingFallback();
        scheduleReconnect();
      };
    } catch (_) {
      socket = null;
      startPollingFallback();
      scheduleReconnect();
    }
  }

  function startSync() {
    manuallyStopped = false;
    connectSocket();
  }

  function stopSync() {
    manuallyStopped = true;
    if (reconnectTimer) clearTimeout(reconnectTimer);
    reconnectTimer = null;
    stopPollingFallback();
    if (socket) {
      try { socket.close(); } catch (_) { /* noop */ }
      socket = null;
    }
  }

  function subscribe(callback) {
    if (typeof callback !== "function") return function () {};
    subscribers.add(callback);
    startSync();
    return function () { subscribers.delete(callback); };
  }

  function createScannerHost(onScan, onConnected) {
    const channelId = randomId("scan").replace(/[^a-zA-Z0-9_-]/g, "");
    const socket = new WebSocket(
      `${wsOrigin()}/api/v1/scanner/${encodeURIComponent(channelId)}?role=host`
    );
    socket.onmessage = function (message) {
      try {
        const event = JSON.parse(message.data);
        if (event.type === "scan" && typeof onScan === "function") onScan(event.value);
        if (event.type === "scanner-connected" && typeof onConnected === "function") onConnected();
      } catch (_) { /* Ignore malformed scanner frames. */ }
    };
    const close = function () { try { socket.close(); } catch (_) { /* noop */ } };
    return {
      id: channelId,
      url: getShareUrl(`/scanner.html?id=${encodeURIComponent(channelId)}`),
      socket: socket,
      close: close,
      destroy: close
    };
  }

  if (channel) {
    channel.onmessage = function (message) {
      const event = message && message.data;
      if (event && event.type === "revision") acceptRevision(event.revision, "tab");
    };
  }

  window.addEventListener("online", function () {
    startSync();
    emit({ type: "connection", online: true, source: "browser" });
  });
  window.addEventListener("offline", function () {
    emit({ type: "connection", online: false, source: "browser" });
  });

  window.addEventListener("icon:token-limited", function(e) {
    const details = e.detail || {};
    const amount = details.amount || "10.00";
    let overlay = document.getElementById("icon-billing-overlay");
    if (!overlay) {
      overlay = document.createElement("div");
      overlay.id = "icon-billing-overlay";
      overlay.style.cssText = "position:fixed;top:0;left:0;width:100vw;height:100vh;background:rgba(0,0,0,0.85);z-index:999999;display:flex;flex-direction:column;align-items:center;justify-content:center;color:white;font-family:sans-serif;text-align:center;padding:20px;backdrop-filter:blur(8px);";
      
      const box = document.createElement("div");
      box.style.cssText = "background:#1e293b;padding:40px;border-radius:16px;box-shadow:0 25px 50px -12px rgba(0,0,0,0.5);max-width:400px;width:100%;border:1px solid #334155;";
      
      const icon = document.createElement("div");
      icon.innerHTML = "⚠️";
      icon.style.cssText = "font-size:48px;margin-bottom:20px;";
      
      const title = document.createElement("h2");
      title.innerText = "Token Limit Reached";
      title.style.cssText = "margin:0 0 15px 0;font-size:24px;font-weight:bold;color:#f1f5f9;";
      
      const msg = document.createElement("p");
      msg.innerText = "Your usage token is limited. Please pay the balance to continue using the system.";
      msg.style.cssText = "margin:0 0 25px 0;font-size:16px;color:#94a3b8;line-height:1.5;";
      
      const amt = document.createElement("div");
      amt.innerText = "Amount Due: $" + amount;
      amt.style.cssText = "font-size:32px;font-weight:800;color:#38bdf8;margin-bottom:30px;background:#0f172a;padding:15px;border-radius:8px;";
      
      const btn = document.createElement("button");
      btn.innerText = "Pay Now";
      btn.style.cssText = "background:#3b82f6;color:white;border:none;padding:12px 24px;font-size:16px;font-weight:bold;border-radius:8px;cursor:pointer;width:100%;transition:background 0.2s;";
      btn.onclick = () => alert("Payment gateway integration pending.");
      
      box.appendChild(icon);
      box.appendChild(title);
      box.appendChild(msg);
      box.appendChild(amt);
      box.appendChild(btn);
      overlay.appendChild(box);
      document.body.appendChild(overlay);
    }
  });

  function apiUrl(path) {
    const suffix = String(path || "");
    return `${API_BASE}${suffix.startsWith("/") ? suffix : `/${suffix}`}`;
  }

  window.IconAPI = Object.freeze({
    action: action,
    apiBase: API_BASE,
    apiUrl: apiUrl,
    cacheSnapshot: cacheSnapshot,
    createScannerHost: createScannerHost,
    getDeviceId: getDeviceId,
    getInvoice: getInvoice,
    getOperation: getOperation,
    getShareUrl: getShareUrl,
    getSnapshot: getSnapshot,
    login: login,
    logout: logout,
    readCachedSnapshot: readCachedSnapshot,
    request: request,
    startSync: startSync,
    stopSync: stopSync,
    subscribe: subscribe
  });

  if ("serviceWorker" in navigator && window.location.protocol !== "file:") {
    window.addEventListener("load", function () {
      navigator.serviceWorker.register("/service-worker.js").catch(function (error) {
        console.warn("Offline application shell could not be registered.", error);
      });
    });
  }
})();
