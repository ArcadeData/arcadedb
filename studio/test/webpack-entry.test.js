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

// Regression test for issue #10703: src/main/js, the directory webpack takes its entry from, held a second file,
// studio-main.js, a sketch of a bundled Studio from #2260 that nothing built. It imported ApexCharts' stylesheet after
// ApexCharts left package.json (#10579) and eight Studio modules that never existed under src/main/js, so the day anything
// made it an entry, `npm run build` would have failed on an import no test had looked at. Studio's application scripts are
// loaded by index.html from static/js and are not bundled (studio/CLAUDE.md), so the sketch is gone, and this pins the two
// things that keep such a file from coming back unseen:
//  - every file under src/main/js is a webpack entry: a source there that no entry names is code nothing builds;
//  - every import of an entry, and every file the copy plugin copies, resolves against package.json and node_modules.
//
// @author Luca Garulli (l.garulli@arcadedata.com)
//
// Run with:
//
//     node --test studio/test/webpack-entry.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STUDIO = path.join(__dirname, "..");
const ENTRY_DIR = path.join(STUDIO, "src", "main", "js");
const NODE_MODULES = path.join(STUDIO, "node_modules");
const PKG = JSON.parse(fs.readFileSync(path.join(STUDIO, "package.json"), "utf8"));
const DECLARED = new Set(Object.keys(Object.assign({}, PKG.dependencies, PKG.devDependencies)));
const config = require(path.join(STUDIO, "webpack.config.js"));

/** The module specifiers a source names through `import ... from`, a bare `import`, a dynamic `import()` or `require()`. */
function specifiers(code) {
  const stripped = code.replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*\/\/.*$/gm, "");
  const found = [];
  const pattern = /\bimport\s+(?:[\w*{}\s,$]+\s+from\s+)?["']([^"']+)["']|\b(?:import|require)\s*\(\s*["']([^"']+)["']\s*\)/g;
  for (let m; (m = pattern.exec(stripped)) !== null; ) found.push(m[1] || m[2]);
  return found;
}

/** Why a specifier named by `fromFile` does not resolve, or null when it does. */
function unresolved(spec, fromFile) {
  if (spec.startsWith(".")) {
    const target = path.resolve(path.dirname(fromFile), spec);
    return [target, target + ".js"].some((f) => fs.existsSync(f) && fs.statSync(f).isFile()) ? null : "no file " + path.relative(STUDIO, target);
  }
  const parts = spec.split("/");
  const pkg = spec.startsWith("@") ? parts.slice(0, 2).join("/") : parts[0];
  if (!DECLARED.has(pkg)) return "package " + pkg + " is not in package.json";
  const target = path.join(NODE_MODULES, spec);
  return fs.existsSync(target) || fs.existsSync(target + ".js") ? null : "node_modules has no " + spec;
}

const entries = Object.values(config.entry).map((e) => path.resolve(STUDIO, e));

test("every source under src/main/js is a webpack entry", () => {
  assert.ok(entries.length > 0, "webpack declares an entry");
  for (const entry of entries) assert.ok(fs.existsSync(entry), path.relative(STUDIO, entry) + " exists");
  for (const file of fs.readdirSync(ENTRY_DIR).filter((f) => f.endsWith(".js")))
    assert.ok(entries.includes(path.join(ENTRY_DIR, file)), "src/main/js/" + file + " is no webpack entry, so nothing builds it");
});

test("every import of a webpack entry resolves against package.json and node_modules", () => {
  for (const entry of entries)
    for (const spec of specifiers(fs.readFileSync(entry, "utf8"))) {
      const why = unresolved(spec, entry);
      assert.equal(why, null, path.relative(STUDIO, entry) + " imports '" + spec + "': " + why);
    }
});

test("the import check refuses what the removed sketch imported", () => {
  const sketch = path.join(ENTRY_DIR, "sketch.js");
  const code = "// import 'commented/out';\nimport 'apexcharts/dist/apexcharts.css';\nimport $ from 'jquery';\nimport './studio-utils';\n" +
    "const x = require('echarts');\n";
  assert.deepEqual(specifiers(code), ["apexcharts/dist/apexcharts.css", "jquery", "./studio-utils", "echarts"]);
  assert.match(unresolved("apexcharts/dist/apexcharts.css", sketch), /apexcharts is not in package\.json/);
  assert.match(unresolved("./studio-utils", sketch), /no file src\/main\/js\/studio-utils/);
  assert.equal(unresolved("jquery", sketch), null);
  assert.equal(unresolved("echarts", sketch), null);
});

test("every file the copy plugin copies exists in node_modules", () => {
  const copy = config.plugins.find((p) => Array.isArray(p.patterns));
  assert.ok(copy && copy.patterns.length > 0, "webpack copies the vendor files");
  for (const pattern of copy.patterns) {
    const from = typeof pattern === "string" ? pattern : pattern.from;
    assert.ok(fs.existsSync(path.join(STUDIO, from)), "webpack copies " + from + ", which npm ci does not install");
  }
});
