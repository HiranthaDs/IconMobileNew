(function () {
  "use strict";

  const API_ROOT = "/api/v1";
  const DEVICE_KEY = "ICON_MOBILE_DEVICE_ID_V1";
  const SNAPSHOT_KEY = "ICON_MOBILE_LOCAL_SNAPSHOT_V1";
  const SHARE_BASE_KEY = "ICON_MOBILE_LAN_SHARE_BASE_V1";
  const BACKUP_REMINDER_KEY = "ICON_MOBILE_BACKUP_REMINDER_SHOWN_V1";
  const REQUEST_TIMEOUT_MS = 20000;
  const subscribers = new Set();
  const channel = typeof BroadcastChannel !== "undefined"
    ? new BroadcastChannel("icon-mobile-lan-sync-v1")
    : null;

  let knownRevision = 0;
  let socket = null;
  let reconnectTimer = null;
  let reconnectAttempt = 0;
  let pollTimer = null;
  let manuallyStopped = false;
  let snapshotRequest = null;
  let shareBaseUrl = storageGet(SHARE_BASE_KEY, "") || window.location.origin;

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
      const response = await fetch(path, {
        method: opts.method || "GET",
        headers: headers,
        body: opts.body === undefined
          ? undefined
          : (opts.body instanceof FormData ? opts.body : JSON.stringify(opts.body)),
        credentials: "same-origin",
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
    if (snapshot.lanBaseUrl) {
      shareBaseUrl = String(snapshot.lanBaseUrl).replace(/\/$/, "");
      storageSet(SHARE_BASE_KEY, shareBaseUrl);
    }
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
    const base = (shareBaseUrl || window.location.origin).replace(/\/$/, "");
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
    const result = await request("/api/auth/login", {
      method: "POST",
      body: { role: role, password: password }
    });
    startSync();
    return result;
  }

  async function logout() {
    try {
      return await request("/api/auth/logout", { method: "POST", body: {} });
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

  function socketUrl() {
    const scheme = window.location.protocol === "https:" ? "wss:" : "ws:";
    return `${scheme}//${window.location.host}${API_ROOT}/events?device=${encodeURIComponent(getDeviceId())}`;
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
    const scheme = window.location.protocol === "https:" ? "wss:" : "ws:";
    const socket = new WebSocket(
      `${scheme}//${window.location.host}${API_ROOT}/scanner/${encodeURIComponent(channelId)}?role=host`
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

  window.IconAPI = Object.freeze({
    action: action,
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
