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

// Regression test for issue #7124 item 3: showMaterializedViewDetail() starts a 3s poller when the view is
// BUILDING and used to clear it only on re-entry into ITSELF. Navigating to a regular type (showTypeDetail) or to
// a graph analytical view (showGavDetail) rewrote #dbTypeDetail without cancelling the poller, which kept firing
// and rewrote the status badge inside the pane the user was now looking at.
//
// Run with:  node --test studio/test/mv-autorefresh-cancel.test.js
//
// The functions are extracted from studio-database.js and evaluated against stubs, the same way
// display-metrics-zero.test.js does: Studio has no bundler for application JS.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const SRC_PATH = path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-database.js");
const src = fs.readFileSync(SRC_PATH, "utf8");

// Pulls one top-level `function name(...) {...}` out of the source by counting braces. The counter does not
// understand string literals, regexes or comments, so the extracted text is parsed here: a violation fails with a
// message naming the function instead of a bare SyntaxError from the eval below.
function extractFn(name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found in studio-database.js: " + name);
  let i = src.indexOf("{", start);
  let depth = 1;
  i++;
  while (i < src.length && depth > 0) {
    const c = src[i];
    if (c === "{") depth++;
    else if (c === "}") depth--;
    i++;
  }
  if (depth !== 0) throw new Error("unbalanced braces while extracting " + name + ": reached end of file");

  const source = src.substring(start, i);
  try {
    new Function("return (" + source + ")");
  } catch (e) {
    throw new Error("the extracted source of " + name + " does not parse: " + e.message);
  }
  return source;
}

// ---------------------------------------------------------------------------------------------------------------
// Stubs for the globals the extracted functions touch.
// ---------------------------------------------------------------------------------------------------------------

let _mvAutoRefreshInterval = null;

const timers = { started: [], cleared: [] };
let nextTimerId = 1;

function setInterval(fn, delay) {
  const id = nextTimerId++;
  timers.started.push({ id: id, fn: fn, delay: delay });
  return id;
}

function clearInterval(id) {
  timers.cleared.push(id);
}

const rendered = {};

function $(selector) {
  const node = {
    html: function (value) {
      if (value !== undefined) rendered[selector] = value;
      return node;
    },
    each: function () {
      return node;
    },
    find: function () {
      return node;
    },
    text: function () {
      return "";
    },
  };
  for (const noop of ["show", "hide", "val", "attr", "addClass", "removeClass", "css", "empty", "append"])
    node[noop] = function () {
      return node;
    };
  return node;
}

function escapeHtml(s) {
  return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function renderTypeLink(name) {
  return escapeHtml(name);
}

function renderProperties() {
  return "";
}

function renderIndexes() {
  return "";
}

function renderMaterializedViewQuickActions() {
  return "";
}

// The poller's callback re-fetches the view list; the tests below never let it run, so this only has to exist.
function fetchMaterializedViews(callback) {
  if (callback) callback(window._schemaMaterializedViews || []);
}

const window = { _schemaMaterializedViews: null, _schemaTypes: null };

eval(extractFn("mvStatusDotClass"));
eval(extractFn("mvStatusBadgeClass"));
eval(extractFn("mvFormatRelativeTime"));
eval(extractFn("mvFormatInterval"));
eval(extractFn("stopMaterializedViewAutoRefresh"));
eval(extractFn("showMaterializedViewDetail"));

function reset() {
  _mvAutoRefreshInterval = null;
  timers.started.length = 0;
  timers.cleared.length = 0;
  nextTimerId = 1;
  window._schemaMaterializedViews = [{ name: "SalesByDay", status: "BUILDING", query: "SELECT 1", backingType: "SalesByDayMV" }];
  window._schemaTypes = [];
}

test("opening a BUILDING view starts the poller", () => {
  reset();
  showMaterializedViewDetail("SalesByDay");

  assert.strictEqual(timers.started.length, 1, "a BUILDING view must poll for its completion");
  assert.strictEqual(timers.started[0].delay, 3000);
  assert.strictEqual(_mvAutoRefreshInterval, timers.started[0].id);
});

test("a VALID view starts no poller", () => {
  reset();
  window._schemaMaterializedViews[0].status = "VALID";
  showMaterializedViewDetail("SalesByDay");

  assert.strictEqual(timers.started.length, 0);
  assert.strictEqual(_mvAutoRefreshInterval, null);
});

test("stopMaterializedViewAutoRefresh cancels a running poller and forgets the handle", () => {
  reset();
  showMaterializedViewDetail("SalesByDay");
  const id = _mvAutoRefreshInterval;

  stopMaterializedViewAutoRefresh();

  assert.deepStrictEqual(timers.cleared, [id], "the interval the pane started must be the one cancelled");
  assert.strictEqual(_mvAutoRefreshInterval, null, "a stale handle would make a later cancel a no-op");
});

test("stopMaterializedViewAutoRefresh is harmless when no poller is running", () => {
  reset();
  stopMaterializedViewAutoRefresh();
  stopMaterializedViewAutoRefresh();

  assert.deepStrictEqual(timers.cleared, []);
  assert.strictEqual(_mvAutoRefreshInterval, null);
});

test("re-opening the same BUILDING view does not leak the previous poller", () => {
  reset();
  showMaterializedViewDetail("SalesByDay");
  const first = _mvAutoRefreshInterval;
  showMaterializedViewDetail("SalesByDay");

  assert.deepStrictEqual(timers.cleared, [first]);
  assert.strictEqual(timers.started.length, 2);
  assert.strictEqual(_mvAutoRefreshInterval, timers.started[1].id);
});

// Every function that takes the detail pane over from a materialized view must cancel the poller, or it keeps
// writing the old view's status into the pane the user is now looking at. showTypeDetail and showGavDetail are far
// too entangled with the rest of Studio to run headless, so the guard here is structural: the cancel call has to be
// present in each of them. Removing it - which is exactly the regression - fails this test.
for (const fn of ["showTypeDetail", "showGavDetail", "showMaterializedViewDetail", "displaySchema"]) {
  test(fn + " cancels the materialized-view poller before taking over the detail pane", () => {
    assert.ok(
      extractFn(fn).includes("stopMaterializedViewAutoRefresh()"),
      fn + " must call stopMaterializedViewAutoRefresh(): otherwise a BUILDING view keeps overwriting the pane it no longer owns"
    );
  });
}
