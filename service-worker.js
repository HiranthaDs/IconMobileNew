"use strict";

const CACHE_NAME = "icon-mobile-shell-v6";
const SHELL_FILES = [
  "/",
  "/index.html",
  "/ADMINPRO.html",
  "/wholesale.html",
  "/invoice.html",
  "/settings.html",
  "/scanner.html",
  "/B2Binvoice.html",
  "/assets/app.css",
  "/assets/local-api.js",
  "/assets/vendor/react.production.min.js",
  "/assets/vendor/react-dom.production.min.js",
  "/assets/vendor/babel.min.js",
  "/logo.png",
  "/logo%20watermark.png",
  "/warranty.png",
  "/Google%20reviews.png",
  "/Google.png",
  "/googlemap.png",
  "/Map%20Qr.png"
];

self.addEventListener("install", function (event) {
  event.waitUntil((async function () {
    const cache = await caches.open(CACHE_NAME);
    await Promise.all(SHELL_FILES.map(async function (url) {
      try { await cache.add(url); } catch (_) { /* Optional asset; keep installing. */ }
    }));
    await self.skipWaiting();
  })());
});

self.addEventListener("activate", function (event) {
  event.waitUntil((async function () {
    const names = await caches.keys();
    await Promise.all(names.filter(function (name) {
      return name.startsWith("icon-mobile-shell-") && name !== CACHE_NAME;
    }).map(function (name) { return caches.delete(name); }));
    await self.clients.claim();
  })());
});

self.addEventListener("fetch", function (event) {
  const request = event.request;
  if (request.method !== "GET") return;

  const url = new URL(request.url);
  if (url.origin !== self.location.origin || url.pathname.startsWith("/api/")) return;

  if (request.mode === "navigate") {
    event.respondWith((async function () {
      try {
        const response = await fetch(request);
        if (response.ok) {
          const cache = await caches.open(CACHE_NAME);
          cache.put(request, response.clone());
        }
        return response;
      } catch (_) {
        return (await caches.match(request)) || (await caches.match("/index.html"));
      }
    })());
    return;
  }

  event.respondWith((async function () {
    const cached = await caches.match(request);
    if (cached) {
      event.waitUntil(fetch(request).then(async function (response) {
        if (response.ok) {
          const cache = await caches.open(CACHE_NAME);
          await cache.put(request, response.clone());
        }
      }).catch(function () {}));
      return cached;
    }
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(CACHE_NAME);
      cache.put(request, response.clone());
    }
    return response;
  })());
});
