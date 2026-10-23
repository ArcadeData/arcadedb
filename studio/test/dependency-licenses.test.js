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

// Issue #10579: ApexCharts changed its licence at 5.1.0 from MIT to one that refuses embedding in a product other people
// use, and Studio followed it from ^3.45.2 to ^8.3.0 through a hand upgrade and a run of Dependabot bumps without anyone
// noticing: the npm licence report in studio-security-audit.yml only warns. This test reads the licence npm recorded
// for every package Studio ships (package-lock.json, everything not marked dev) and fails on any licence outside the
// allow-list of the root CLAUDE.md, so a bump that crosses a licence change is red in the build that proposes it.
//
// The lock file is read rather than node_modules, so the check needs no install and sees exactly what a bump changes.
//
//     node --test studio/test/dependency-licenses.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const LOCK = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "package-lock.json"), "utf8"));

// The npm subset of the ALLOWED row in the root CLAUDE.md
const ALLOWED = new Set(["Apache-2.0", "MIT", "BSD-2-Clause", "BSD-3-Clause", "ISC", "0BSD", "CC0-1.0", "Unlicense"]);

// Shipped packages whose recorded licence the expression check above cannot pass, each pinned to the EXACT text npm
// recorded: a new version that changes it fails again and has to be looked at. These predate #10579; the ones outside
// the CLAUDE.md allow-list wait for a maintainer's sign-off (issue #10626).
const REVIEWED = {
  // icons CC-BY-4.0, fonts OFL-1.1, code MIT: Font Awesome Free's own split
  "@fortawesome/fontawesome-free": "(CC-BY-4.0 AND OFL-1.1 AND MIT)",
  // pako 1.x carries zlib's permissive licence for its port of the zlib sources
  "pako": "(MIT AND Zlib)",
  // png-js (a pdfmake dependency) declares no licence field; its LICENSE file is MIT
  "png-js": undefined,
  // sax (a pdfmake dependency) moved to the permissive Blue Oak Model License
  "sax": "BlueOak-1.0.0"
};

/** True when an SPDX expression is satisfied by the allow-list: every AND term allowed, any one OR alternative allowed. */
function allowed(expression) {
  if (typeof expression !== "string") return false;
  const text = expression.trim().replace(/^\((.*)\)$/, "$1");
  if (/\(/.test(text)) return false; // nested expressions are reviewed by hand
  if (/ OR /.test(text)) return text.split(/ OR /).some((alt) => allowed(alt));
  if (/ AND /.test(text)) return text.split(/ AND /).every((term) => allowed(term));
  return ALLOWED.has(text);
}

/** Every package Studio ships: `{name, version, license}`, from the lock file entries not marked dev. */
function shippedPackages(lock) {
  const out = [];
  for (const [key, entry] of Object.entries(lock.packages || {})) {
    if (key === "" || entry.dev) continue;
    out.push({ name: key.substring(key.lastIndexOf("node_modules/") + "node_modules/".length), version: entry.version, license: entry.license });
  }
  return out;
}

function offenders(lock) {
  return shippedPackages(lock)
    .filter((p) => !allowed(p.license))
    .filter((p) => !(Object.prototype.hasOwnProperty.call(REVIEWED, p.name) && REVIEWED[p.name] === p.license))
    .map((p) => p.name + "@" + p.version + ": " + JSON.stringify(p.license));
}

test("every package Studio ships is under a licence the project allows", () => {
  const found = offenders(LOCK);
  assert.deepEqual(found, [], "licences outside the allow-list (see the root CLAUDE.md):\n" + found.join("\n"));
});

test("the lock file is one this test can read: version 2 or 3, with the shipped packages listed", () => {
  assert.ok(LOCK.lockfileVersion >= 2, "lockfileVersion " + LOCK.lockfileVersion + " has no `packages` map");
  const names = shippedPackages(LOCK).map((p) => p.name);
  for (const name of ["jquery", "bootstrap", "echarts", "cytoscape"]) assert.ok(names.includes(name), name + " is shipped");
});

test("ApexCharts' licence from 5.1.0 on is refused, as is any licence that only points at a file", () => {
  const lock = {
    packages: {
      "": {},
      "node_modules/apexcharts": { version: "8.3.0", license: "SEE LICENSE IN LICENSE.md" },
      "node_modules/apexcharts-dev": { version: "1.0.0", license: "SEE LICENSE IN LICENSE.md", dev: true }
    }
  };
  assert.deepEqual(offenders(lock), ['apexcharts@8.3.0: "SEE LICENSE IN LICENSE.md"']);
});

test("an expression passes on an allowed alternative or on all-allowed terms, never on a forbidden licence", () => {
  assert.equal(allowed("MIT"), true);
  assert.equal(allowed("(MIT OR GPL-3.0-or-later)"), true);
  assert.equal(allowed("(MIT AND BSD-3-Clause)"), true);
  assert.equal(allowed("GPL-3.0-only"), false);
  assert.equal(allowed("(MIT AND GPL-3.0-only)"), false);
  assert.equal(allowed("SSPL-1.0"), false);
  assert.equal(allowed(undefined), false);
});

test("a reviewed exception holds for the exact licence it was reviewed with, not for a changed one", () => {
  const lock = { packages: { "node_modules/sax": { version: "9.0.0", license: "BUSL-1.1" } } };
  assert.equal(offenders(lock).length, 1);
});
