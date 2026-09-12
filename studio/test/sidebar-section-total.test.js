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

// Regression test, from discussion #7473 (comment https://github.com/ArcadeData/arcadedb/discussions/7473#discussioncomment-18408772)
// after issue #7477 was fixed: the sidebar badge of a LIGHTWEIGHT edge type correctly stopped showing its
// record count (0) as its size, but the section header above it - "Edges (N)" - still summed the raw
// `records` field of every type in the section. A LIGHTWEIGHT type reports 0 records by construction, so a
// graph holding a million lightweight edges still showed "Edges (0)" at the top of the sidebar. The total is
// now marked with a trailing "+" whenever the section holds a type it cannot count, instead of presenting a
// wrong number as if it were exact.
//
// Run with:
//
//     node --test studio/test/sidebar-section-total.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const SRC_PATH = path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-database.js");
const src = fs.readFileSync(SRC_PATH, "utf8");

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
  return src.substring(start, i);
}

eval(extractFn("formatSectionTotal"));

test("a section with only regular types shows the exact sum", () => {
  const total = formatSectionTotal([{ records: 3 }, { records: 4 }]);
  assert.equal(total, "7");
});

test("a section holding a LIGHTWEIGHT edge type marks its total as a lower bound", () => {
  const total = formatSectionTotal([{ records: 0, lightweight: true }]);
  assert.equal(total, "0+", "a graph full of lightweight edges must not read as an empty section");
});

test("a mixed section still adds the regular types' records and marks the total", () => {
  const total = formatSectionTotal([{ records: 5, lightweight: false }, { records: 0, lightweight: true }]);
  assert.equal(total, "5+");
});

test("an empty section shows a plain zero", () => {
  const total = formatSectionTotal([]);
  assert.equal(total, "0");
});

test("a type listing from an older server, with no lightweight flag, is unaffected", () => {
  const total = formatSectionTotal([{ records: 7 }, { records: 3 }]);
  assert.equal(total, "10");
});
