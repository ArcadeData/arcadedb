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

// The export buttons of the result table. The PDF button was dropped with pdfmake: its font engine (fontkit) pulls
// unmaintained packages (fast-deep-equal, tiny-inflate) no upgrade removes, and pdfmake plus its fonts were about 2 MB
// of the Studio jar for what the Print button already does through the browser's "Save as PDF".
//
//     node --test studio/test/table-export-buttons.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STUDIO = path.join(__dirname, "..");
const read = (...p) => fs.readFileSync(path.join(STUDIO, ...p), "utf8");

test("the result table offers copy, Excel, CSV and print, and no PDF", () => {
  const table = read("src", "main", "resources", "static", "js", "studio-table.js");
  const start = table.indexOf("buttons: [");
  assert.ok(start > 0, "studio-table.js declares its export buttons");
  const block = table.substring(start, table.indexOf("]", start));
  const kinds = [...block.matchAll(/extend:\s*"(\w+)"/g)].map((m) => m[1]);
  assert.deepEqual(kinds, ["copy", "excel", "csv", "print"]);
});

test("pdfmake is neither loaded, copied nor depended on", () => {
  const html = read("src", "main", "resources", "static", "index.html");
  assert.ok(!/src="[^"]*(pdfmake|vfs_fonts)/i.test(html), "index.html still loads pdfmake");
  assert.ok(!/pdfmake|vfs_fonts/i.test(read("webpack.config.js")), "webpack.config.js still copies pdfmake");

  const pkg = JSON.parse(read("package.json"));
  assert.equal(pkg.dependencies.pdfmake, undefined, "package.json still depends on pdfmake");

  const shipped = Object.entries(JSON.parse(read("package-lock.json")).packages)
    .filter(([key, entry]) => key !== "" && !entry.dev)
    .map(([key]) => key.substring(key.lastIndexOf("node_modules/") + "node_modules/".length));
  for (const name of ["pdfmake", "pdfkit", "fontkit", "fast-deep-equal", "tiny-inflate"])
    assert.ok(!shipped.includes(name), name + " is still shipped");
});
