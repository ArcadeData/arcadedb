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

// Studio keeps the page in the URL hash (#/query, #/support/ai, ...) so a refresh or a shared link does not send you back to
// the first tab. The pure parts of js/studio-router.js are loaded whole into a VM context, as the browser loads them; the
// glue is checked against a small fake of window, history and Bootstrap's Tab.
//
//     node --test studio/test/studio-router.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const STATIC_DIR = path.join(__dirname, "..", "src", "main", "resources", "static");
const src = fs.readFileSync(path.join(STATIC_DIR, "js", "studio-router.js"), "utf8");
const indexSrc = fs.readFileSync(path.join(STATIC_DIR, "index.html"), "utf8");
const plain = (v) => JSON.parse(JSON.stringify(v));

function load() {
  const ctx = vm.createContext({});
  vm.runInContext(src, ctx);
  return ctx;
}

test("every route maps to an existing tab of index.html", () => {
  const ctx = load();
  const routes = plain(ctx.STUDIO_ROUTES);
  assert.deepEqual(Object.keys(routes).sort(), ["api", "cluster", "database", "info", "profiler", "query", "security", "server", "settings", "support"]);
  for (const sel of Object.values(routes)) assert.ok(indexSrc.includes('id="' + sel + '"'), sel + " is not a tab of index.html");
});

test("a hash is parsed into its page and sub-page", () => {
  const ctx = load();
  assert.deepEqual(plain(ctx.studioRouteParse("#/query")), { route: "query", tab: "tab-query-sel", sub: "" });
  assert.deepEqual(plain(ctx.studioRouteParse("#/info")), { route: "info", tab: "tab-resources-sel", sub: "" });
  assert.deepEqual(plain(ctx.studioRouteParse("#/support/issues")), { route: "support", tab: "tab-support-sel", sub: "issues" });
  assert.deepEqual(plain(ctx.studioRouteParse("#/support/ai")), { route: "support", tab: "tab-support-sel", sub: "ai" });
  assert.deepEqual(plain(ctx.studioRouteParse("#/support")), { route: "support", tab: "tab-support-sel", sub: "" });
});

test("trailing slashes and case do not matter, a query string is ignored", () => {
  const ctx = load();
  assert.equal(ctx.studioRouteParse("#/Server/").route, "server");
  assert.equal(ctx.studioRouteParse("#/server?x=1").route, "server");
});

test("empty, unknown, malformed and hostile hashes are no route at all", () => {
  const ctx = load();
  for (const h of ["", "#", "#/", "#nope", "#/nope", "#/support/unknown", "#/query/extra", "#//query", "#/__proto__", "#/constructor", "#/support/ai/extra", "#/%00", "javascript:alert(1)", null, undefined, 7])
    assert.equal(ctx.studioRouteParse(h), null, String(h));
});

test("the route of a tab is its hash, the support tab carries its sub-view", () => {
  const ctx = load();
  assert.equal(ctx.studioRouteFor("tab-server-sel", ""), "#/server");
  assert.equal(ctx.studioRouteFor("tab-resources-sel", ""), "#/info");
  assert.equal(ctx.studioRouteFor("tab-support-sel", "issues"), "#/support/issues");
  assert.equal(ctx.studioRouteFor("tab-support-sel", ""), "#/support/ai");
  assert.equal(ctx.studioRouteFor("tab-support-sel", "bogus"), "#/support/ai");
  assert.equal(ctx.studioRouteFor("tab-nope-sel", ""), null);
});

// ---- the glue ----

function browser(initialHash) {
  const calls = [];
  const shown = [];
  const state = { hash: initialHash || "", active: "tab-query-sel", supportViews: [] };
  const win = {
    location: {
      get hash() { return state.hash; },
      pathname: "/", search: "",
    },
    history: {
      replaceState(_s, _t, url) { calls.push(["replace", url]); state.hash = url.substring(url.indexOf("#")) === url ? "" : url.substring(url.indexOf("#")); },
      pushState(_s, _t, url) { calls.push(["push", url]); state.hash = url.substring(url.indexOf("#")); },
    },
    addEventListener() {},
  };
  const ctx = vm.createContext({ window: win, supportCurrentView: "ai", document: {
    getElementById(id) { return { id, classList: { contains: () => state.active === id } }; },
  }, bootstrap: { Tab: { getOrCreateInstance(el) { return { show() { shown.push(el.id); state.active = el.id; } }; } } },
  showSupportView(view) { state.supportViews.push(view); ctx.supportCurrentView = view; ctx.studioRouteSupportView(view); },
  });
  vm.runInContext(src, ctx);
  return { ctx, calls, shown, state };
}

test("nothing happens before the login completed: a deep link waits", () => {
  const b = browser("#/server");
  b.ctx.studioRouteApply();
  assert.deepEqual(b.shown, []);
  assert.deepEqual(b.calls, []);
});

test("after the login the hash opens its tab through the same path a click uses", () => {
  const b = browser("#/server");
  b.ctx.studioRouteReady();
  assert.deepEqual(b.shown, ["tab-server-sel"]);
});

test("a deep link into the support sub-view survives: the sub-view is chosen before the tab opens", () => {
  const b = browser("#/support/issues");
  b.ctx.studioRouteReady();
  assert.deepEqual(b.shown, ["tab-support-sel"]);
  assert.equal(b.ctx.supportCurrentView, "issues");
});

test("with the support tab already open, a hash change only switches the sub-view", () => {
  const b = browser("#/support/ai");
  b.state.active = "tab-support-sel";
  b.ctx.studioRouteReady();
  b.state.supportViews.length = 0;
  b.state.hash = "#/support/issues";
  b.ctx.studioRouteApply();
  assert.deepEqual(b.shown, []);
  assert.deepEqual(b.state.supportViews, ["issues"]);
});

test("no hash after the login: the page you are on gets its hash, replacing, never a new history entry", () => {
  const b = browser("");
  b.ctx.studioRouteReady();
  assert.deepEqual(b.shown, []);
  assert.deepEqual(b.calls, [["replace", "/#/query"]]);
});

test("an unknown hash leaves the page alone and corrects the hash", () => {
  const b = browser("#/nope");
  b.ctx.studioRouteReady();
  assert.deepEqual(b.shown, []);
  assert.deepEqual(b.calls, [["replace", "/#/query"]]);
});

test("opening a tab by hand pushes its hash, so back and forward work", () => {
  const b = browser("#/query");
  b.ctx.studioRouteReady();
  b.calls.length = 0;
  b.ctx.studioRouteTabShown("tab-cluster-sel");
  assert.deepEqual(b.calls, [["push", "/#/cluster"]]);
});

test("the hash that is already there is not written again", () => {
  const b = browser("#/cluster");
  b.ctx.studioRouteReady();
  b.calls.length = 0;
  b.ctx.studioRouteTabShown("tab-cluster-sel");
  assert.deepEqual(b.calls, []);
});

test("completing a bare #/support into #/support/ai replaces it", () => {
  const b = browser("#/support");
  b.ctx.studioRouteReady();
  b.calls.length = 0;
  b.ctx.studioRouteTabShown("tab-support-sel");
  assert.deepEqual(b.calls, [["replace", "/#/support/ai"]]);
});

test("choosing a support sub-view by hand pushes its hash", () => {
  const b = browser("#/support/ai");
  b.state.active = "tab-support-sel";
  b.ctx.studioRouteReady();
  b.calls.length = 0;
  b.ctx.studioRouteSupportView("issues");
  assert.deepEqual(b.calls, [["push", "/#/support/issues"]]);
});

test("a sub-view change while another tab is open does not move the URL", () => {
  const b = browser("#/server");
  b.state.active = "tab-server-sel";
  b.ctx.studioRouteReady();
  b.calls.length = 0;
  b.ctx.studioRouteSupportView("issues");
  assert.deepEqual(b.calls, []);
});

test("logging out forgets the session: the next login applies the hash again", () => {
  const b = browser("#/security");
  b.ctx.studioRouteReady();
  b.ctx.studioRouteLoggedOut();
  b.shown.length = 0;
  b.state.active = "tab-query-sel";
  b.ctx.studioRouteApply();
  assert.deepEqual(b.shown, []);
  b.ctx.studioRouteReady();
  assert.deepEqual(b.shown, ["tab-security-sel"]);
});

test("index.html loads the router, and the login paths call it", () => {
  assert.match(indexSrc, /<script src="js\/studio-router\.js"><\/script>/);
  const dbSrc = fs.readFileSync(path.join(STATIC_DIR, "js", "studio-database.js"), "utf8");
  assert.match(dbSrc, /studioRouteReady\(\)/);
  assert.match(dbSrc, /studioRouteLoggedOut\(\)/);
  assert.match(indexSrc, /studioRouteReady\(\)/);
});
