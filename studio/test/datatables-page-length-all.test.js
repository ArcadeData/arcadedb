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
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */

// Regression guard for issue #6985: with "All" entries per page the query result table showed every row on
// one page, but the paging control still drew one button per row ("1, 2, 3, 4, 5, ..., 500"). The defect
// was in DataTables 3.0.0 itself - "When the page length was set to -1, the paging control wasn't correctly
// updated", fixed upstream in 3.0.1 - and studio-table.js passes the -1 of its "All" option through as is.
//
// The behavior is covered end to end by e2e-studio/tests/datatables-page-length-all.spec.ts, which needs a
// running server. This test is the cheap half that runs in every Maven build: it fails if the dependency
// range or the lockfile ever allows the broken release back in, and it pins that the "All" option is the one
// page length that reaches DataTables' -1 branch. Run with:
//
//     node --test studio/test/datatables-page-length-all.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STUDIO_DIR = path.join(__dirname, "..");
const FIRST_FIXED = [3, 0, 1];

function parseVersion(v) {
  const m = /(\d+)\.(\d+)\.(\d+)/.exec(v);
  if (!m) throw new Error("not a semantic version: " + v);
  return [parseInt(m[1], 10), parseInt(m[2], 10), parseInt(m[3], 10)];
}

function compare(a, b) {
  for (let i = 0; i < 3; i++) if (a[i] !== b[i]) return a[i] - b[i];
  return 0;
}

test("the locked DataTables release draws one page for page length -1", () => {
  const lock = JSON.parse(fs.readFileSync(path.join(STUDIO_DIR, "package-lock.json"), "utf8"));
  const entry = lock.packages["node_modules/datatables.net"];
  assert.ok(entry, "datatables.net is missing from studio/package-lock.json");
  assert.ok(
    compare(parseVersion(entry.version), FIRST_FIXED) >= 0,
    "datatables.net " + entry.version + " predates 3.0.1, whose paging control draws a button per row for 'All' (#6985)"
  );
});

test("the declared DataTables range cannot resolve to the broken 3.0.0", () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(STUDIO_DIR, "package.json"), "utf8"));
  const range = pkg.dependencies["datatables.net"];
  assert.ok(range, "datatables.net is missing from studio/package.json");
  // The floor of a caret/tilde/plain range is its own version: ^3.0.4 cannot resolve below 3.0.4.
  assert.match(range, /^[\^~]?\d+\.\d+\.\d+$/, "unexpected range shape, re-check the floor by hand: " + range);
  assert.ok(
    compare(parseVersion(range), FIRST_FIXED) >= 0,
    "the range " + range + " still admits datatables.net 3.0.0, whose paging control is wrong for 'All' (#6985)"
  );
});

test("the result table's 'All' option is page length -1", () => {
  const src = fs.readFileSync(path.join(STUDIO_DIR, "src", "main", "resources", "static", "js", "studio-table.js"), "utf8");
  const m = /lengthMenu:\s*\[\s*\[([^\]]*)\]\s*,\s*\[([^\]]*)\]/.exec(src);
  assert.ok(m, "lengthMenu not found in studio-table.js");
  const values = m[1].split(",").map((s) => s.trim());
  const labels = m[2].split(",").map((s) => s.trim().replace(/^["']|["']$/g, "").toLowerCase());
  const all = labels.indexOf("all");
  assert.notEqual(all, -1, "the result table no longer offers an 'All' page length");
  assert.equal(values[all], "-1", "'All' must map to DataTables' page length -1, the value its paging control treats as one page");
});
