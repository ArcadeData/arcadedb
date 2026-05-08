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

// Regression test for issue #4140: indexes inherited from parent types must show up in the
// Studio "Indexes" section of a child type detail. The schema:types backend uses
// getAllIndexes(false) (own indexes only); Studio walks the parent chain client-side and
// renderIndexes() is responsible for collecting the inherited rows. Run with:
//
//     node --test studio/test/render-indexes.test.js
//
// The test extracts findTypeInResult and renderIndexes from studio-database.js and exercises
// them directly. Studio has no bundler for application JS, so this avoids loading the full
// browser bundle.

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

eval(extractFn("findTypeInResult"));
eval(extractFn("renderIndexes"));

test("inherited index from a single parent type appears under the child detail", () => {
  // Layout reproduced from the issue screenshot: parent has a unique LSM_TREE index, child has none.
  const types = [
    {
      name: "ListadDBUfdr",
      parentTypes: [],
      indexes: [
        { name: "ListadDBUfdr[odbRID]", typeName: "ListadDBUfdr", properties: ["odbRID"], type: "LSM_TREE", unique: true, automatic: true }
      ]
    },
    { name: "AntenaDBUfdr", parentTypes: ["ListadDBUfdr"], indexes: [] }
  ];

  const childOut = renderIndexes(types[1], types);

  assert.notStrictEqual(childOut.length, 0, "child type should render the inherited index row");
  assert.ok(childOut.includes("ListadDBUfdr[odbRID]"), "inherited index name must be present");
  // The "Defined In" column must still resolve to the owning ancestor, not the child.
  assert.ok(childOut.includes("<td>ListadDBUfdr</td>"), "'Defined In' cell must point at the parent type");
});

test("a parent type that has its own index still renders correctly (no regression)", () => {
  const types = [
    {
      name: "ListadDBUfdr",
      parentTypes: [],
      indexes: [
        { name: "ListadDBUfdr[odbRID]", typeName: "ListadDBUfdr", properties: ["odbRID"], type: "LSM_TREE", unique: true, automatic: true }
      ]
    }
  ];
  const out = renderIndexes(types[0], types);
  assert.ok(out.includes("ListadDBUfdr[odbRID]"));
  assert.strictEqual((out.match(/<tr>/g) || []).length, 1);
});

test("diamond inheritance shows each ancestor index only once", () => {
  // D extends B and C; B and C both extend A. A's index must surface on D exactly once.
  const types = [
    { name: "A", parentTypes: [], indexes: [{ name: "A[k]", typeName: "A", properties: ["k"], type: "LSM_TREE", unique: false, automatic: true }] },
    { name: "B", parentTypes: ["A"], indexes: [] },
    { name: "C", parentTypes: ["A"], indexes: [] },
    { name: "D", parentTypes: ["B", "C"], indexes: [] }
  ];
  const out = renderIndexes(types[3], types);
  assert.strictEqual((out.match(/<tr>/g) || []).length, 1, "diamond inheritance must dedupe by index name");
});

test("multi-level chain surfaces indexes from every ancestor", () => {
  const types = [
    { name: "G", parentTypes: [], indexes: [{ name: "G[g]", typeName: "G", properties: ["g"], type: "LSM_TREE", unique: false, automatic: true }] },
    { name: "P", parentTypes: ["G"], indexes: [{ name: "P[p]", typeName: "P", properties: ["p"], type: "LSM_TREE", unique: false, automatic: true }] },
    { name: "C", parentTypes: ["P"], indexes: [] }
  ];
  const out = renderIndexes(types[2], types);
  assert.ok(out.includes("G[g]"), "grandparent index must be visible on the leaf");
  assert.ok(out.includes("P[p]"), "parent index must be visible on the leaf");
});

test("type with no own and no inherited indexes renders an empty string", () => {
  const types = [
    { name: "Lonely", parentTypes: [], indexes: [] }
  ];
  const out = renderIndexes(types[0], types);
  assert.strictEqual(out, "", "no indexes anywhere in the chain -> empty HTML so the caller shows the placeholder");
});
