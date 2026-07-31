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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

// Regression test for issue #5636 item 3: the profiler-details table rendered a zero-valued stat as
// ABSENT rather than as zero. The value chain was
//
//     else if (entry.count != null && entry.count != 0) ... else continue;
//
// so a counter sitting at 0 was skipped entirely and the operator could not tell "this is zero" from
// "this is not reported". For a health signal whose good state IS zero that is backwards. Run with:
//
//     node --test studio/test/display-metrics-zero.test.js
//
// displayMetrics() is extracted from studio-server.js and run against stubs for the handful of globals
// it touches, so the assertion is on the HTML it produces. Studio has no bundler for application JS.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const SRC_PATH = path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-server.js");
const src = fs.readFileSync(SRC_PATH, "utf8");

// Pulls one top-level `function name(...) {...}` out of the source by counting braces. The counter does
// not understand string literals, regexes or comments, so the extracted text is parsed here: a violation
// fails with a message naming the function instead of a bare SyntaxError from the eval below.
function extractFn(name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found in studio-server.js: " + name);
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

// Captures what displayMetrics() hands to each table, keyed by selector.
const rendered = {};

// Minimal chainable jQuery stub: html() records, everything else displayMetrics() reaches for is a no-op.
// A missing method surfaces as "$(...).foo is not a function", which is a readable failure, so the list is
// deliberately not exhaustive.
function $(selector) {
  const node = {
    html: function (value) {
      rendered[selector] = value;
      return node;
    },
  };
  for (const noop of ["show", "hide", "text", "val", "attr", "addClass", "removeClass", "css", "empty", "append"])
    node[noop] = function () {
      return node;
    };
  return node;
}

function escapeHtml(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function globalFormatDouble(v, decimals) {
  return Number(v).toFixed(decimals);
}

function globalFormatSpace(v) {
  return Number(v) + " b";
}

var serverData = {};

eval(extractFn("displayMetrics"));

// Renders the profiler-details table for the given profiler payload and returns its HTML.
function renderProfilerTable(profiler) {
  serverData = { metrics: { profiler: profiler, meters: {} } };
  displayMetrics();
  return rendered["#srvMetricProfilerTable"] || "";
}

// Splits the rendered table into { name: displayedValue } so assertions do not depend on markup details.
function rowsOf(html) {
  const rows = {};
  const re = /<tr><td>([^<]*)<\/td><td class='text-end'>([^<]*)<\/td><\/tr>/g;
  let m;
  while ((m = re.exec(html)) !== null) rows[m[1]] = m[2];
  return rows;
}

test("a zero-valued count renders as 0 instead of vanishing", () => {
  const html = renderProfilerTable({
    pagesRead: { count: 0 },
    pagesWritten: { count: 1234 },
  });
  const rows = rowsOf(html);

  assert.ok("pagesRead" in rows, "a counter at zero must still get a row: absent and zero are different states");
  assert.strictEqual(rows.pagesRead, "0");
  assert.strictEqual(rows.pagesWritten, "1234");
});

test("a zero-valued space renders as zero instead of vanishing", () => {
  const rows = rowsOf(renderProfilerTable({ pagesWrittenSize: { space: 0 } }));

  assert.ok("pagesWrittenSize" in rows, "a space stat at zero must still get a row");
  assert.strictEqual(rows.pagesWrittenSize, "0 b");
});

test("a stat that reports none of perc/count/space/value is still skipped", () => {
  // This is the ONLY genuinely-absent case, and it must keep being dropped: the row would have nothing
  // to show. `configuration` is the real-world shape - Profiler emits it as {description: "..."}.
  const rows = rowsOf(renderProfilerTable({ walPagesWritten: { count: 7 }, somethingOpaque: { description: "x" } }));

  assert.ok("walPagesWritten" in rows);
  assert.ok(!("somethingOpaque" in rows), "a stat with no numeric member has nothing to render");
});

test("zero and non-zero percentages both render", () => {
  const rows = rowsOf(renderProfilerTable({ diskFreeSpacePerc: { perc: 0 }, ramHeapAvailablePerc: { perc: 42.5 } }));

  assert.strictEqual(rows.diskFreeSpacePerc, "0.00%");
  assert.strictEqual(rows.ramHeapAvailablePerc, "42.50%");
});

test("the rate-tracked metrics stay out of the details table", () => {
  // They have their own table; a zero there was already rendered (no != 0 guard), and this pins that the
  // fix above did not accidentally start duplicating them into the details table.
  const html = renderProfilerTable({ queries: { count: 0 }, writeTx: { count: 0 }, pagesRead: { count: 0 } });
  const rows = rowsOf(html);

  assert.ok(!("queries" in rows));
  assert.ok(!("writeTx" in rows));
  assert.ok("pagesRead" in rows);

  const dbOps = rendered["#srvMetricDbOpsTable"] || "";
  assert.ok(dbOps.includes("Queries"), "the rate-tracked table renders a zero-count row");
  assert.ok(dbOps.includes("Write Tx"));
});
