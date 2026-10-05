/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * The page you are on lives in the URL hash: #/query, #/server, #/support/ai, #/support/issues ... so a refresh, a bookmark or a
 * link you send keeps the page instead of going back to the first tab.
 *
 * Hash routes, not paths: every call of Studio is a relative URL ("api/v1/..."), which a path like /support would break, and
 * the server would need an index.html fallback; a hash needs neither and works behind a reverse-proxy prefix.
 *
 * - A tab shown by the user pushes its hash (back and forward work); the router's own corrections replace.
 * - The route is applied only once the login has completed (studioRouteReady): a deep link survives the login page.
 * - Opening a tab goes through Bootstrap's Tab.show(), the path a click takes, so the tab's own init code runs.
 * - The Support page has two sub-views, the AI Assistant and the Issues: #/support/ai and #/support/issues.
 */

var STUDIO_ROUTES = {
  query: "tab-query-sel",
  database: "tab-database-sel",
  server: "tab-server-sel",
  profiler: "tab-profiler-sel",
  security: "tab-security-sel",
  cluster: "tab-cluster-sel",
  api: "tab-api-sel",
  info: "tab-resources-sel",
  support: "tab-support-sel",
  settings: "tab-settings-sel",
};
var STUDIO_SUPPORT_VIEWS = { ai: true, issues: true };
var STUDIO_DEFAULT_ROUTE = "query";

var studioRouteLoggedIn = false;
var studioRouteApplying = false;

// ---- pure parts ------------------------------------------------------------------------------------------------------------

/** "#/support/issues" -> {route: "support", tab: "tab-support-sel", sub: "issues"}; null for anything that is not a known route. */
function studioRouteParse(hash) {
  if (typeof hash !== "string") return null;
  var text = hash;
  var q = text.indexOf("?");
  if (q >= 0) text = text.substring(0, q);
  if (text.indexOf("#/") !== 0) return null;
  var parts = text.substring(2).toLowerCase().split("/");
  if (parts.length > 1 && parts[parts.length - 1] === "") parts.pop();
  var route = parts[0];
  if (!Object.prototype.hasOwnProperty.call(STUDIO_ROUTES, route)) return null;
  var sub = "";
  if (route === "support") {
    if (parts.length > 2) return null;
    if (parts.length === 2) {
      if (!Object.prototype.hasOwnProperty.call(STUDIO_SUPPORT_VIEWS, parts[1])) return null;
      sub = parts[1];
    }
  } else if (parts.length > 1) return null;
  return { route: route, tab: STUDIO_ROUTES[route], sub: sub };
}

/** The hash of a tab (by the id of its link), or null for a tab that has no route. `sub` only matters for the Support tab. */
function studioRouteFor(tabSelId, sub) {
  for (var name in STUDIO_ROUTES) {
    if (!Object.prototype.hasOwnProperty.call(STUDIO_ROUTES, name) || STUDIO_ROUTES[name] !== tabSelId) continue;
    if (name === "support") return "#/support/" + (STUDIO_SUPPORT_VIEWS[sub] === true ? sub : "ai");
    return "#/" + name;
  }
  return null;
}

// ---- the glue --------------------------------------------------------------------------------------------------------------

function studioRouteActiveTab() {
  var el = null;
  for (var name in STUDIO_ROUTES) {
    var candidate = document.getElementById(STUDIO_ROUTES[name]);
    if (candidate && candidate.classList.contains("active")) {
      el = candidate;
      break;
    }
  }
  return el ? el.id : STUDIO_ROUTES[STUDIO_DEFAULT_ROUTE];
}

/** Writes the hash. Replaces (no history entry) for the router's own corrections and for a page that only gains its sub-view. */
function studioRouteSet(hash, replace) {
  if (!hash || window.location.hash === hash) return;
  var current = studioRouteParse(window.location.hash);
  var next = studioRouteParse(hash);
  var completes = current && next && current.route === next.route && current.sub === "";
  var url = window.location.pathname + window.location.search + hash;
  if (replace || completes || studioRouteApplying || !current) window.history.replaceState(null, "", url);
  else window.history.pushState(null, "", url);
}

function supportViewNow() {
  return typeof supportCurrentView === "string" ? supportCurrentView : "ai";
}

/** A tab was shown (by a click, or by the router itself): its hash becomes the URL. */
function studioRouteTabShown(tabSelId) {
  var hash = studioRouteFor(tabSelId, supportViewNow());
  if (hash) studioRouteSet(hash, false);
}

/** The Support page switched between its AI Assistant and Issues views. */
function studioRouteSupportView(view) {
  if (!studioRouteLoggedIn || studioRouteActiveTab() !== STUDIO_ROUTES.support) return;
  studioRouteSet(studioRouteFor(STUDIO_ROUTES.support, view), false);
}

/** Opens the page the hash names. An unknown or empty hash keeps the current page and writes its hash. */
function studioRouteApply() {
  if (!studioRouteLoggedIn) return;
  var wanted = studioRouteParse(window.location.hash);
  if (!wanted) {
    var hash = studioRouteFor(studioRouteActiveTab(), supportViewNow());
    if (hash) studioRouteSet(hash, true);
    return;
  }
  var link = document.getElementById(wanted.tab);
  if (!link) return;
  studioRouteApplying = true;
  try {
    if (wanted.route === "support" && wanted.sub) {
      if (link.classList.contains("active") && typeof showSupportView === "function") {
        // Already on the Support page: only its sub-view changes
        showSupportView(wanted.sub);
        return;
      }
      // Chosen BEFORE the tab opens: the tab's own init shows the view that is current
      if (typeof supportCurrentView !== "undefined") supportCurrentView = wanted.sub;
    }
    if (!link.classList.contains("active")) bootstrap.Tab.getOrCreateInstance(link).show();
  } finally {
    // A tab with a fade finishes after this returns; the hash is already the right one then, so nothing is written twice
    studioRouteApplying = false;
  }
}

/** Called when the login completed and Studio is on screen. */
function studioRouteReady() {
  studioRouteLoggedIn = true;
  studioRouteApply();
}

/** Called when Studio goes back to the login page; the hash stays, and is applied again after the next login. */
function studioRouteLoggedOut() {
  studioRouteLoggedIn = false;
}

if (typeof window !== "undefined" && typeof window.addEventListener === "function" && typeof jQuery !== "undefined") {
  // Back, forward, a hash typed by hand, a link pasted into the address bar
  window.addEventListener("hashchange", studioRouteApply);
  window.addEventListener("popstate", studioRouteApply);
  jQuery(function () {
    jQuery('a[data-bs-toggle="tab"]').on("shown.bs.tab", function () {
      if (studioRouteLoggedIn) studioRouteTabShown(this.id);
    });
  });
}
