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

// A distribution built without the MCP module answers 404 on /api/v1/mcp/config. That is a build-time
// choice, not an error, so the MCP tab must explain itself instead of raising the generic error toast
// with an empty body. Run with:
//
//     node --test studio/test/mcp-module-absent.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const SRC_PATH = path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-server.js");
const src = fs.readFileSync(SRC_PATH, "utf8");

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
  return src.substring(start, i);
}

eval(extractFn("isMCPModuleAbsent"));

test("a 404 means the module was excluded from this build", () => {
  assert.equal(isMCPModuleAbsent({ status: 404 }), true);
});

test("a 403 is a real error and must still be reported", () => {
  assert.equal(isMCPModuleAbsent({ status: 403 }), false);
});

test("a 500 is a real error and must still be reported", () => {
  assert.equal(isMCPModuleAbsent({ status: 500 }), false);
});

test("a missing or malformed jqXHR is not treated as an absent module", () => {
  assert.equal(isMCPModuleAbsent(null), false);
  assert.equal(isMCPModuleAbsent(undefined), false);
  assert.equal(isMCPModuleAbsent({}), false);
});
