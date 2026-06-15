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

// Regression test for issue #4638: groups cannot be assigned to users via the Studio GUI.
// The user-edit form used to hardcode the group <select> to "admin" and "*", so custom,
// per-database groups never appeared in the dropdown, and editing a user that already had a
// custom group assigned showed an empty field (.val(customGroup) matched no <option>).
//
// getGroupNamesForDatabase()/buildGroupOptionsHtml() now derive the options from the live
// group definitions (default "*" groups + database-specific groups). This test extracts those
// pure functions from studio-security.js and exercises them directly. Run with:
//
//     node --test studio/test/security-group-select.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const SRC_PATH = path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-security.js");
const src = fs.readFileSync(SRC_PATH, "utf8");

function extractFn(name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found in studio-security.js: " + name);
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

// Minimal escapeHtml stub mirroring studio-utils.js (only used for the characters we test).
global.escapeHtml = function (value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
};

eval(extractFn("getGroupNamesForDatabase"));
eval(extractFn("buildGroupOptionsHtml"));

// Reproduces the server-groups.json shape: default "*" database holds admin + "*" groups,
// a "blog" database additionally defines a custom "readers" group.
function sampleGroups() {
  return {
    databases: {
      "*": { groups: { admin: {}, "*": {} } },
      blog: { groups: { readers: {} } }
    }
  };
}

test("custom per-database group is offered for that database", () => {
  global.groupsData = sampleGroups();
  const names = getGroupNamesForDatabase("blog");
  assert.deepStrictEqual(names, ["admin", "*", "readers"], "default groups plus the blog-specific 'readers' group");

  const html = buildGroupOptionsHtml("blog", "readers");
  assert.ok(html.includes('<option value="readers" selected>readers</option>'), "custom group must be selectable and pre-selected");
});

test("custom group does NOT leak into an unrelated database", () => {
  global.groupsData = sampleGroups();
  const names = getGroupNamesForDatabase("other");
  assert.deepStrictEqual(names, ["admin", "*"], "only the default groups apply to a database without custom groups");
});

test("wildcard (*) database only offers the default groups", () => {
  global.groupsData = sampleGroups();
  const names = getGroupNamesForDatabase("*");
  assert.deepStrictEqual(names, ["admin", "*"]);
});

test("an assigned group that no longer exists is still shown (no empty field)", () => {
  // The user has 'legacy' assigned for 'blog' but the group was since deleted.
  global.groupsData = sampleGroups();
  const html = buildGroupOptionsHtml("blog", "legacy");
  assert.ok(html.includes('<option value="legacy" selected>legacy</option>'), "stale assignment must remain visible and selected");
});

test("'* (default)' label is rendered for the '*' group while keeping value '*'", () => {
  global.groupsData = sampleGroups();
  const html = buildGroupOptionsHtml("blog", "*");
  assert.ok(html.includes('<option value="*" selected>* (default)</option>'), "the '*' group keeps its raw value but a friendly label");
});

test("falls back to admin/* when the group list could not be loaded", () => {
  global.groupsData = null;
  assert.deepStrictEqual(getGroupNamesForDatabase("blog"), ["admin", "*"], "missing group data must not break the form");
});
