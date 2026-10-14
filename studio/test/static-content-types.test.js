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

// Regression test for issue #9993: the server picks the Content-Type of a Studio file, and whether it runs it through
// its HTML template engine, from the CONTENT_TYPES table of GetDynamicContentHandler. An extension missing from it is
// served as text/plain (the five shipped PNGs were, until #9993), and an entry marked templated other than a page has
// any "${" sequence eaten and loses its caching (stylesheets were, until #9993). This walks everything Studio ships,
// the vendor bundles webpack copies into dist/ included, since the build runs this after webpack: a new kind of file
// added later (.webp, .map, .avif) fails here instead of being served as text.
//
// @author Luca Garulli (l.garulli@arcadedata.com)
//
// Run with:
//
//     node --test studio/test/static-content-types.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STATIC_DIR = path.join(__dirname, "..", "src", "main", "resources", "static");
const HANDLER = path.join(__dirname, "..", "..", "server", "src", "main", "java", "com", "arcadedb", "server", "http",
  "handler", "GetDynamicContentHandler.java");

// entry("css", new ContentTypeSpec("text/css", false))
function readContentTypeTable() {
  const source = fs.readFileSync(HANDLER, "utf8");
  const table = new Map();
  const re = /entry\("([a-z0-9]+)",\s*new ContentTypeSpec\("([^"]+)",\s*(true|false)\)\)/g;
  let m;
  while ((m = re.exec(source)) !== null) table.set(m[1], { contentType: m[2], templated: m[3] === "true" });
  return table;
}

function shippedExtensions(dir, found = new Map()) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) shippedExtensions(full, found);
    else {
      const dot = entry.name.lastIndexOf(".");
      if (dot > 0) {
        const ext = entry.name.substring(dot + 1).toLowerCase();
        if (!found.has(ext)) found.set(ext, path.relative(STATIC_DIR, full));
      }
    }
  }
  return found;
}

test("the server's content-type table is read", () => {
  const table = readContentTypeTable();
  for (const ext of ["html", "js", "css", "png", "svg", "woff2"]) assert.ok(table.has(ext), `table has .${ext}`);
});

// The server keeps the bytes and digest of at most STATIC_CACHE_MAX_ENTRIES assets (issue #9961): every page render looks
// up each asset it references, so an asset past the cap would be read and hashed again on every render
test("Studio ships fewer static assets than the server caches", () => {
  const source = fs.readFileSync(HANDLER, "utf8");
  const cap = Number(/STATIC_CACHE_MAX_ENTRIES\s*=\s*(\d+)/.exec(source)[1]);
  let assets = 0;
  const count = (dir) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      if (entry.isDirectory()) count(path.join(dir, entry.name));
      else if (!/\.html?$/i.test(entry.name)) assets++;
    }
  };
  count(STATIC_DIR);
  assert.ok(assets > 0 && assets < cap, `${assets} static assets against a cache of ${cap}: raise STATIC_CACHE_MAX_ENTRIES`);
});

test("every extension Studio ships has a Content-Type, and only pages are templated", () => {
  const table = readContentTypeTable();
  const shipped = shippedExtensions(STATIC_DIR);
  assert.ok(shipped.size > 0, "Studio's static tree is read");
  for (const [ext, example] of shipped) {
    const spec = table.get(ext);
    assert.ok(spec, `.${ext} (${example}) has no entry in GetDynamicContentHandler.CONTENT_TYPES: it would be served as text/plain`);
    assert.equal(spec.templated, ext === "html" || ext === "htm", `only a page is templated: .${ext}`);
  }
});
