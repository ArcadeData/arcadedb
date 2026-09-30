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

// Regression test for issue #7049: clicking a saved query in the Studio sidebar loaded it into the editor AND ran
// it straight away, so a query the user only wanted to tweak first (a DELETE, a heavy scan, a parameter to change)
// was executed before they could touch it. A click now only loads the query; running it is an explicit action,
// either the editor's Run button or the play button on the saved entry itself.
//
// Run with:
//
//     node --test studio/test/saved-query-load-without-running.test.js

const { test, beforeEach } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STATIC_DIR = path.join(__dirname, "..", "src", "main", "resources", "static", "js");
const src = fs.readFileSync(path.join(STATIC_DIR, "studio-database.js"), "utf8");
const utilsSrc = fs.readFileSync(path.join(STATIC_DIR, "studio-utils.js"), "utf8");

function extractFn(source, name) {
  const start = source.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found: " + name);
  let i = source.indexOf("{", start);
  let depth = 1;
  i++;
  while (i < source.length && depth > 0) {
    const c = source[i];
    if (c === "{") depth++;
    else if (c === "}") depth--;
    i++;
  }
  return source.substring(start, i);
}

// --- Browser stubs -------------------------------------------------------------------------------------------------

let storedQueries;
let languageValue;
let panelHtml;
let executed;
let activatedTab;
let editor;

function globalStorageLoad(key) {
  return key === "database.saved.queries" ? JSON.stringify(storedQueries) : null;
}

function globalStorageSave() {}

function getEditorMode() {
  return "mode-for-" + languageValue;
}

function globalActivateTab(tab) {
  activatedTab = tab;
}

function executeCommand(language, query) {
  executed.push({ language: language, query: query });
}

function $(selector) {
  if (selector === "#inputLanguage")
    return {
      val: function (v) {
        if (v === undefined) return languageValue;
        languageValue = v;
        return this;
      },
    };
  if (selector === "#sidebarPanelSaved")
    return {
      html: function (h) {
        panelHtml = h;
      },
    };
  throw new Error("unexpected selector in test: " + selector);
}

eval(extractFn(utilsSrc, "escapeHtml"));
eval(extractFn(src, "getSavedQueries"));
eval(extractFn(src, "populateSavedQueriesPanel"));
eval(extractFn(src, "loadSavedQuery"));
eval(extractFn(src, "executeSavedQuery"));

beforeEach(() => {
  storedQueries = [
    { name: "Wipe logs", l: "sql", c: "DELETE FROM Log", d: "db" },
    { name: "Friends", l: "cypher", c: "MATCH (a)-[:FRIEND]->(b) RETURN b", d: "db" },
  ];
  languageValue = "sql";
  panelHtml = null;
  executed = [];
  activatedTab = null;
  editor = {
    value: "",
    mode: null,
    focused: false,
    setValue: function (v) {
      this.value = v;
    },
    getValue: function () {
      return this.value;
    },
    setOption: function (name, v) {
      if (name === "mode") this.mode = v;
    },
    focus: function () {
      this.focused = true;
    },
  };
});

test("loading a saved query puts it in the editor without running it", () => {
  loadSavedQuery(0);
  assert.equal(editor.value, "DELETE FROM Log");
  assert.equal(executed.length, 0, "a click on a saved query must not execute it");
  assert.equal(activatedTab, "tab-query");
  assert.equal(editor.focused, true, "the editor gets the focus so the user can start editing right away");
});

test("loading a saved query switches the language and the editor's syntax mode to the query's own", () => {
  loadSavedQuery(1);
  assert.equal(languageValue, "cypher");
  assert.equal(editor.mode, "mode-for-cypher", "a cypher query must not stay highlighted as SQL");
  assert.equal(editor.value, "MATCH (a)-[:FRIEND]->(b) RETURN b");
});

test("loading a stale index leaves the editor untouched", () => {
  editor.value = "SELECT 1";
  loadSavedQuery(5);
  assert.equal(editor.value, "SELECT 1");
  assert.equal(languageValue, "sql");
  assert.equal(executed.length, 0);
});

test("a click on a saved entry loads it, and only the explicit run button executes it", () => {
  populateSavedQueriesPanel();
  const entryClick = /class='saved-query-entry'[^>]*onclick='([^']*)'/.exec(panelHtml);
  assert.ok(entryClick, "the saved entry must still be clickable");
  assert.equal(entryClick[1], "loadSavedQuery(0)");
  assert.ok(!/class='saved-query-entry'[^>]*executeSavedQuery/.test(panelHtml), "the entry's own click must not run the query");

  const runClick = /class='saved-query-run'[^>]*onclick='([^']*)'/.exec(panelHtml);
  assert.ok(runClick, "each saved entry offers an explicit run button");
  assert.match(runClick[1], /event\.stopPropagation\(\);\s*executeSavedQuery\(0\)/, "the run button must not also bubble into the load click");
});

test("the explicit run button still executes the saved query with its own language", () => {
  executeSavedQuery(1);
  assert.deepEqual(executed, [{ language: "cypher", query: "MATCH (a)-[:FRIEND]->(b) RETURN b" }]);
});
