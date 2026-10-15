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
// Issue #10103: the same list decides a second thing, whether Maven copies a binary asset byte for byte or through a
// text Reader that turns it into garbage (fa-solid-900.woff2 arrived as 215,949 bytes instead of 119,488). The parent
// POM's resource carve-out is that second list, and it was widened by hand and separately: webp, avif and wasm were
// served with the right type and still filtered, bmp, eot and swf were protected from the filter and still served as
// text/plain. The last tests of this file read the POM and assert the two lists are the same set.
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
const ROOT_POM = path.join(__dirname, "..", "..", "pom.xml");
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

// --- Issue #10103: the server's table and the POM's binary carve-out are one list ----------------------------------

// What is NOT a binary: text and JSON, which the build may legitimately filter. Everything else the server serves is a
// binary, so a type added to the table later is covered by default instead of being exempted by omission.
function isTextContentType(contentType) {
  return contentType.startsWith("text/") || contentType === "application/json";
}

function binaryExtensionsOfTable() {
  const binaries = new Set();
  for (const [ext, spec] of readContentTypeTable()) if (!isTextContentType(spec.contentType)) binaries.add(ext);
  return binaries;
}

// <exclude>**/*.png</exclude> -> png
function extensionsOf(xml, tag) {
  const set = new Set();
  const re = new RegExp("<" + tag + ">\\*\\*/\\*\\.([a-z0-9]+)</" + tag + ">", "g");
  let m;
  while ((m = re.exec(xml)) !== null) set.add(m[1]);
  return set;
}

function between(text, open, close) {
  const start = text.indexOf(open);
  const end = text.indexOf(close, start);
  assert.ok(start >= 0 && end > start, `${open} ... ${close} is in the root pom.xml`);
  return text.substring(start, end);
}

function readPomLists() {
  const pom = fs.readFileSync(ROOT_POM, "utf8");
  const build = pom.substring(pom.indexOf("<build>"));
  const resources = between(build, "<resources>", "</resources>");
  const plugin = /<nonFilteredFileExtensions>([^<]*)<\/nonFilteredFileExtensions>/.exec(build);
  assert.ok(plugin, "the root POM declares nonFilteredFileExtensions");
  return {
    excludes: extensionsOf(between(resources, "<excludes>", "</excludes>"), "exclude"),
    includes: extensionsOf(between(resources, "<includes>", "</includes>"), "include"),
    nonFiltered: new Set(plugin[1].split(",").map((e) => e.trim()).filter((e) => e.length > 0)),
  };
}

const sorted = (set) => [...set].sort();

test("the POM's three binary lists are read", () => {
  const pom = readPomLists();
  for (const list of [pom.excludes, pom.includes, pom.nonFiltered])
    assert.ok(list.has("png") && list.has("woff2"), "the list holds the shipped binaries: " + sorted(list));
});

test("the POM's three binary lists agree with each other", () => {
  const pom = readPomLists();
  assert.deepEqual(sorted(pom.includes), sorted(pom.excludes), "<includes> of the unfiltered resource vs <excludes> of the filtered one");
  assert.deepEqual(sorted(pom.nonFiltered), sorted(pom.excludes), "nonFilteredFileExtensions vs the resource carve-out");
});

test("every binary the server serves is carved out of Maven's resource filtering", () => {
  const pom = readPomLists();
  const missing = sorted(binaryExtensionsOfTable()).filter((ext) => !pom.nonFiltered.has(ext));
  assert.deepEqual(missing, [], `served with a binary Content-Type but not in the root pom.xml carve-out, so Maven would copy `
    + `it through a text Reader and corrupt it: add ${missing.join(", ")} to the <excludes>, the <includes> and nonFilteredFileExtensions`);
});

test("every extension carved out of Maven's resource filtering has a binary Content-Type on the server", () => {
  const pom = readPomLists();
  const binaries = binaryExtensionsOfTable();
  const missing = sorted(pom.nonFiltered).filter((ext) => !binaries.has(ext));
  assert.deepEqual(missing, [], `carved out of filtering as binary but not served with a binary Content-Type, so the server `
    + `would send it as text/plain: add ${missing.join(", ")} to GetDynamicContentHandler.CONTENT_TYPES`);
});
