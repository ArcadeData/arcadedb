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

// Regression test for issue #10101 (found while replacing the probe-then-skip graph-export e2e tests with ones that call
// the Export menu): Export > GraphML threw "e.isArray is not a function" and downloaded nothing. The cytoscape-graphml
// plugin calls $.isArray, which jQuery 4 removed, and no test ever reached it. studio-notify.js restores it as the alias
// of Array.isArray it always was.
//
// The shim is exercised against a jQuery 4 stand-in that lacks the function. The end-to-end half is the GraphML case of
// e2e-studio/tests/graph-export.spec.ts, which downloads the file through the real menu.
//
// @author Luca Garulli (l.garulli@arcadedata.com)
//
// Run with:
//
//     node --test studio/test/jquery-compat-shim.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const NOTIFY = path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-notify.js");

function loadNotify(jquery) {
  const ready = [];
  const $ = Object.assign(() => ({ ready: (fn) => ready.push(fn) }), jquery);
  const context = vm.createContext({ $, document: { addEventListener() {} }, Array });
  vm.runInContext(fs.readFileSync(NOTIFY, "utf8"), context);
  return $;
}

test("a jQuery without $.isArray (4.x) gets the alias of Array.isArray back", () => {
  const $ = loadNotify({});
  assert.equal(typeof $.isArray, "function");
  assert.equal($.isArray([]), true);
  assert.equal($.isArray({}), false);
  assert.equal($.isArray("x"), false);
});

test("a jQuery that still has $.isArray (3.x) is left alone", () => {
  const own = () => "own";
  assert.equal(loadNotify({ isArray: own }).isArray, own);
});
