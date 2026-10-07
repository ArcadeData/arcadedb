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

// Issue #9437: the "Create Graph Analytical View" dialog writes the CCH clause for the contraction hierarchies to keep.
// The engine side of the same clause is pinned by
// engine/src/test/java/com/arcadedb/graph/olap/GraphAnalyticalViewCCHTest.java. Run with:
//
//     node --test studio/test/gav-cch-clause.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const JS_DIR = path.join(__dirname, "..", "src", "main", "resources", "static", "js");
const dbSrc = fs.readFileSync(path.join(JS_DIR, "studio-database.js"), "utf8");
const utilsSrc = fs.readFileSync(path.join(JS_DIR, "studio-utils.js"), "utf8");

// Pulls one top-level `function name(...) {...}` out of a Studio source file (no brace may appear inside a string or regex).
function extractFn(src, name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found: " + name);
  let i = src.indexOf("{", start);
  let depth = 1;
  i++;
  while (i < src.length && depth > 0) {
    const c = src[i];
    if (c === "{") depth++;
    else if (c === "}") depth--;
    i++;
  }
  if (depth !== 0) throw new Error("unbalanced braces while extracting " + name);
  return src.substring(start, i);
}

eval(extractFn(utilsSrc, "quoteSqlName"));
eval(extractFn(dbSrc, "parseGavCchWeights"));
eval(extractFn(dbSrc, "buildGavCchClause"));
eval(extractFn(dbSrc, "gavCchEstimateBytes"));

test("no weight, no clause", () => {
  assert.equal(buildGavCchClause(undefined), "");
  assert.equal(buildGavCchClause(""), "");
  assert.equal(buildGavCchClause(" , ,"), "");
});

test("each weight is trimmed and quoted", () => {
  assert.equal(buildGavCchClause("distance"), " CCH (`distance`)");
  assert.equal(buildGavCchClause(" distance , travelTime ,"), " CCH (`distance`, `travelTime`)");
});

test("a weight name cannot break out of its quotes", () => {
  assert.equal(buildGavCchClause("a`b"), " CCH (`a\\`b`)");
});

test("the RAM estimate counts each hierarchy over the routed edges", () => {
  assert.equal(gavCchEstimateBytes(1000, 0), 0);
  assert.equal(gavCchEstimateBytes(1000, 1), 1000 * 4 * 52);
  assert.equal(gavCchEstimateBytes(1000, 2), 2 * 1000 * 4 * 52);
  assert.deepEqual(parseGavCchWeights(" distance , ,time"), ["distance", "time"]);
});
