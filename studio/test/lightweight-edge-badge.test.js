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

// Regression test for issue #7477: the sidebar badge of a LIGHTWEIGHT edge type showed "0".
//
// A lightweight edge is a pair of pointers inside the two vertices and allocates no record, so the
// record count of such a type is 0 however many edges the graph holds. Printed as the type's size
// next to a freshly bulk-loaded 75-million-edge type, that 0 reads as "the import loaded nothing" -
// which is exactly the conclusion the report reached. The badge says what the type is instead. Run with:
//
//     node --test studio/test/lightweight-edge-badge.test.js

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

const utilsSrc = fs.readFileSync(path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-utils.js"), "utf8");
const escapeHtmlStart = utilsSrc.indexOf("function escapeHtml(");
eval(utilsSrc.substring(escapeHtmlStart, utilsSrc.indexOf("\n}", escapeHtmlStart) + 2));

// The hint text the badge and the type detail share, declared next to renderTypeSidebarBadge.
const hintStart = src.indexOf("const LIGHTWEIGHT_EDGE_HINT =");
assert.notStrictEqual(hintStart, -1, "LIGHTWEIGHT_EDGE_HINT must be declared in studio-database.js");
// `const` inside eval() is block-scoped to that eval, so it would not be visible to the function evaluated
// below: rebound as `var`, which lands in this module's scope where the function can close over it.
eval("var " + src.substring(hintStart + "const ".length, src.indexOf(";\n", hintStart) + 1));

eval(extractFn("schemaActionAttrs"));
eval(extractFn("renderTypeSidebarBadge"));

test("a LIGHTWEIGHT edge type does not report its record count as its size", () => {
  const html = renderTypeSidebarBadge({ name: "CITE", type: "edge", records: 0, lightweight: true }, "#f97316", "show-type-detail");

  assert.ok(html.includes(">CITE<"), "the type name must still be rendered");
  assert.ok(!html.includes("sidebar-badge-count'>0<"), "the record count of a lightweight type must not be shown as its size");
  assert.ok(html.includes("lightweight"), "the badge must say what the type is instead");
  assert.ok(html.includes("stored inside the vertices"), "the tooltip must explain why there is no count");
});

test("a regular edge type still shows its record count", () => {
  const html = renderTypeSidebarBadge({ name: "WROTE", type: "edge", records: 1234, lightweight: false }, "#f97316", "show-type-detail");

  assert.ok(html.includes("1,234"), "a type that holds records must still show how many");
  assert.ok(!html.includes("lightweight"), "and must not be labelled lightweight");
});

test("a type listing from an older server, with no lightweight flag, is unchanged", () => {
  const html = renderTypeSidebarBadge({ name: "WROTE", type: "edge", records: 7 }, "#f97316", "show-type-detail");

  assert.ok(html.includes("sidebar-badge-count'>7<"), "an absent flag must read as 'not lightweight'");
});

test("the type name is escaped exactly once in both the label and the tooltip", () => {
  const html = renderTypeSidebarBadge({ name: "A<B>", type: "edge", records: 0, lightweight: true }, "#f97316", "show-type-detail");

  assert.ok(!html.includes("<B>"), "an unescaped type name must not reach the markup");
  assert.ok(html.includes("<span class='sidebar-badge-name'>A&lt;B&gt;</span>"), "the label must carry the escaped name");
  // Asserted on the title attribute specifically: the label alone satisfies a bare "contains A&lt;B&gt;" check,
  // so a title built from the already-escaped name and escaped a second time would have gone unnoticed.
  assert.ok(html.includes("title='A&lt;B&gt; - "), "the tooltip must escape the name once, not twice");
  assert.ok(!html.includes("&amp;lt;"), "no part of the badge may be escaped twice");
});

test("a regular type's tooltip is escaped exactly once too", () => {
  const html = renderTypeSidebarBadge({ name: "A&B", type: "edge", records: 12 }, "#f97316", "show-type-detail");

  assert.ok(html.includes("title='A&amp;B (12 records)'"), "the tooltip must read A&B, not A&amp;B");
  assert.ok(!html.includes("&amp;amp;"), "no part of the badge may be escaped twice");
});
