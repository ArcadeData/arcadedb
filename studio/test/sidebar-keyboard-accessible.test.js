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

// Regression test for issue #8740: in the Studio Query sidebar the Saved panel's entries, their run/delete controls and
// the History panel's entries were div/span elements with an inline onclick and only a title, so they could not be
// reached or triggered from the keyboard and screen readers did not announce them as buttons. The run and delete icons
// were also revealed on :hover only, so even a focusable control would have stayed invisible while focused.
//
// These tests check the rendered markup and the stylesheet. The keyboard behaviour itself (Tab order, Enter/Space
// dispatching a click that bubbles into the entry's load handler) needs a real browser; the e2e-studio Playwright suite
// runs against a released Docker image, so a spec there could not see this change before it ships. It was checked by
// hand in Chromium, see PR #9376.
//
// Run with:
//
//     node --test studio/test/sidebar-keyboard-accessible.test.js

const { test, beforeEach } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STATIC_DIR = path.join(__dirname, "..", "src", "main", "resources", "static");
const src = fs.readFileSync(path.join(STATIC_DIR, "js", "studio-database.js"), "utf8");
const utilsSrc = fs.readFileSync(path.join(STATIC_DIR, "js", "studio-utils.js"), "utf8");
const css = fs.readFileSync(path.join(STATIC_DIR, "css", "studio.css"), "utf8");

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
let panelHtml;
let focusedLoadButton;

function globalStorageLoad(key) {
  return key === "database.saved.queries" ? JSON.stringify(storedQueries) : null;
}

function globalStorageSave(key, value) {
  if (key === "database.saved.queries") storedQueries = JSON.parse(value);
}

function $(selector) {
  if (selector === "#sidebarPanelSaved")
    return {
      html: function (h) {
        panelHtml = h;
      },
    };
  if (selector === "#sidebarPanelSaved .saved-query-load")
    return {
      eq: function (n) {
        return {
          trigger: function (event) {
            if (event === "focus") focusedLoadButton = n;
          },
        };
      },
    };
  throw new Error("unexpected selector in test: " + selector);
}

eval(extractFn(utilsSrc, "escapeHtml"));
eval(extractFn(src, "getSavedQueries"));
eval(extractFn(src, "storeSavedQueries"));
eval(extractFn(src, "populateSavedQueriesPanel"));
eval(extractFn(src, "deleteSavedQuery"));
eval(extractFn(src, "renderHistoryEntries"));

beforeEach(() => {
  storedQueries = [
    { name: "Wipe logs", l: "sql", c: "DELETE FROM Log", d: "db" },
    { name: "Friends", l: "cypher", c: "MATCH (a)-[:FRIEND]->(b) RETURN b", d: "db" },
    { name: "Count", l: "sql", c: "SELECT count(*) FROM V", d: "db" },
  ];
  panelHtml = null;
  focusedLoadButton = null;
});

// Returns the opening tag of the first element carrying the given class, e.g. "<button type='button' class='x' ...>"
function openingTag(html, cssClass) {
  const m = new RegExp("<(\\w+)[^>]*class='" + cssClass + "'[^>]*>").exec(html);
  assert.ok(m, "no element with class " + cssClass);
  return m;
}

// --- Saved panel ---------------------------------------------------------------------------------------------------

test("the saved entry's load, run and delete controls are real buttons", () => {
  populateSavedQueriesPanel();
  for (const cls of ["saved-query-load", "saved-query-run", "saved-query-delete"]) {
    const tag = openingTag(panelHtml, cls);
    assert.equal(tag[1], "button", cls + " must be a <button> so Tab reaches it and Enter/Space triggers it");
    assert.match(tag[0], /type='button'/, cls + " must not default to a submit button");
  }
  assert.ok(!/<span[^>]*onclick/.test(panelHtml), "no click-only span may remain in the saved panel");
});

test("every saved entry gets its own focusable controls", () => {
  populateSavedQueriesPanel();
  assert.equal(panelHtml.match(/<button type='button' class='saved-query-load'/g).length, 3);
  assert.equal(panelHtml.match(/<button type='button' class='saved-query-run'/g).length, 3);
  assert.equal(panelHtml.match(/<button type='button' class='saved-query-delete'/g).length, 3);
});

test("the controls carry an accessible name naming the saved query", () => {
  populateSavedQueriesPanel();
  assert.match(panelHtml, /class='saved-query-load' aria-label='Load saved query Wipe logs \(sql\) into the editor'/);
  assert.match(panelHtml, /class='saved-query-run'[^>]*aria-label='Run saved query Wipe logs'/);
  assert.match(panelHtml, /class='saved-query-delete'[^>]*aria-label='Delete saved query Wipe logs'/);
  assert.match(panelHtml, /<i class='fa fa-play' aria-hidden='true'>/, "the decorative icon must not be read out");
  assert.match(panelHtml, /<i class='fa fa-times' aria-hidden='true'>/, "the decorative icon must not be read out");
});

test("a name with quotes and markup cannot break out of the aria-label attribute", () => {
  storedQueries = [{ name: "O'Brien <b>x</b>", l: "sql", c: "SELECT 1", d: "db" }];
  populateSavedQueriesPanel();
  assert.match(panelHtml, /aria-label='Load saved query O&#039;Brien &lt;b&gt;x&lt;\/b&gt; \(sql\) into the editor'/);
  assert.ok(!panelHtml.includes("O'Brien"), "the raw single quote would end the attribute early");
});

test("the load button has no handler of its own: its activation bubbles into the entry's load click", () => {
  populateSavedQueriesPanel();
  const load = openingTag(panelHtml, "saved-query-load")[0];
  assert.ok(!/onclick/.test(load), "a second handler on the button would load the query twice");
  // THE BUTTON MUST SIT INSIDE THE ENTRY WHOSE ONCLICK LOADS THE QUERY, NOT INSIDE THE ACTIONS THAT STOP PROPAGATION
  assert.match(panelHtml, /<div class='saved-query-entry'[^>]*onclick='loadSavedQuery\(0\)'><div class='saved-query-name'><button type='button' class='saved-query-load'/);
});

test("run and delete still stop the click from also loading the entry", () => {
  populateSavedQueriesPanel();
  assert.match(openingTag(panelHtml, "saved-query-run")[0], /onclick='event\.stopPropagation\(\); executeSavedQuery\(0\)'/);
  assert.match(openingTag(panelHtml, "saved-query-delete")[0], /onclick='event\.stopPropagation\(\); deleteSavedQuery\(0\)'/);
});

test("deleting a saved query hands the focus to the entry that took its place", () => {
  deleteSavedQuery(1);
  assert.deepEqual(storedQueries.map((q) => q.name), ["Wipe logs", "Count"]);
  assert.equal(focusedLoadButton, 1);
});

test("deleting the last saved query hands the focus to the one before it", () => {
  deleteSavedQuery(2);
  assert.equal(focusedLoadButton, 1);
});

test("deleting the only saved query focuses nothing", () => {
  storedQueries = [{ name: "Only", l: "sql", c: "SELECT 1", d: "db" }];
  deleteSavedQuery(0);
  assert.equal(storedQueries.length, 0);
  assert.equal(focusedLoadButton, null);
});

// --- History panel -------------------------------------------------------------------------------------------------

test("a history entry is a real button that loads it", () => {
  const html = renderHistoryEntries([
    { index: 4, q: { l: "sql", c: "SELECT FROM Person", d: "db", t: Date.now() } },
    { index: 7, q: { l: "gremlin", c: "g.V()", d: "db" } },
  ]);
  const tag = openingTag(html, "history-entry-content");
  assert.equal(tag[1], "button");
  assert.match(tag[0], /type='button'/);
  assert.match(tag[0], /onclick='loadHistoryEntry\(4\)'/);
  assert.match(html, /<button type='button' class='history-entry-content'[^>]*onclick='loadHistoryEntry\(7\)'/);
  assert.ok(!/<div[^>]*onclick/.test(html), "no click-only div may remain in the history entries");
});

test("a history button only holds phrasing content, so the browser does not re-parent it", () => {
  const html = renderHistoryEntries([{ index: 0, q: { l: "sql", c: "SELECT 1", d: "db", t: Date.now() } }]);
  const body = /<button type='button' class='history-entry-content'[^>]*>([\s\S]*?)<\/button>/.exec(html)[1];
  assert.ok(!/<div/.test(body), "a <div> inside a <button> is invalid markup: " + body);
  assert.match(body, /<span class='history-meta'>/);
  assert.match(body, /<span class='history-cmd'>SELECT 1<\/span>/);
});

test("each history selection checkbox has an accessible name telling the rows apart", () => {
  const html = renderHistoryEntries([
    { index: 3, q: { l: "sql", c: "SELECT 1", d: "db" } },
    { index: 4, q: { l: "sql", c: "SELECT 'x' FROM " + "V".repeat(100), d: "db" } },
  ]);
  assert.match(html, /<input type='checkbox' class='history-checkbox history-item-check' data-index='3'[^>]*aria-label='Select history entry: SELECT 1'>/);
  // A LONG COMMAND IS CUT, AND ITS QUOTES ARE ESCAPED SO THEY CANNOT END THE ATTRIBUTE
  const long = /data-index='4'[^>]*aria-label='([^']*)'/.exec(html)[1];
  assert.equal(long, "Select history entry: SELECT &#039;x&#039; FROM " + "V".repeat(44) + "...");
});

// --- Stylesheet ----------------------------------------------------------------------------------------------------

function ruleFor(selectorPart) {
  const rules = css.split("}");
  return rules.filter((r) => r.substring(0, r.indexOf("{")).includes(selectorPart));
}

test("keyboard focus inside a saved entry reveals the hover-only run and delete icons", () => {
  const reveal = ruleFor(".saved-query-entry:focus-within .saved-query-run");
  assert.equal(reveal.length, 1, "the run icon must be revealed on :focus-within as well as :hover");
  assert.match(reveal[0], /\.saved-query-entry:focus-within \.saved-query-delete/);
  assert.match(reveal[0], /opacity:\s*1/);
});

test("the new buttons show a visible focus ring", () => {
  for (const sel of [".saved-query-load:focus-visible", ".saved-query-run:focus-visible", ".saved-query-delete:focus-visible", ".history-entry-content:focus-visible"]) {
    const rules = ruleFor(sel);
    assert.ok(rules.length > 0, "no focus style for " + sel);
    assert.ok(rules.some((r) => /outline:\s*2px solid/.test(r)), "no visible outline for " + sel);
  }
});

test("a focused entry is highlighted like a hovered one", () => {
  assert.ok(ruleFor(".saved-query-entry:focus-within").some((r) => /background-color/.test(r)));
  assert.ok(ruleFor(".history-entry:focus-within").some((r) => /background-color/.test(r)));
});
