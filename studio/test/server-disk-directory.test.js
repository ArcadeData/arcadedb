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

// Issue #7223: the disk figures on the server summary describe the filesystem the DATABASES live on, which on a
// container is a mounted volume and not the one the process was started in. The card therefore has to name it -
// a figure that does not say which filesystem it is answering about cannot be acted on. Run with:
//
//     node --test studio/test/server-disk-directory.test.js
//
// displayServerSummary() is extracted from studio-server.js and run against stubs for the globals it touches, so
// the assertion is on what it hands to each element. Studio has no bundler for application JS.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const SRC_PATH = path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-server.js");
const src = fs.readFileSync(SRC_PATH, "utf8");

// Pulls one top-level `function name(...) {...}` out of the source by counting braces, as the sibling tests do.
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
  if (depth !== 0) throw new Error("unbalanced braces while extracting " + name + ": reached end of file");

  const source = src.substring(start, i);
  try {
    new Function("return (" + source + ")");
  } catch (e) {
    throw new Error("the extracted source of " + name + " does not parse: " + e.message);
  }
  return source;
}

// What displayServerSummary() hands to each element, keyed by selector.
const text = {};
const attrs = {};

function $(selector) {
  const node = {
    text: function (value) {
      text[selector] = value;
      return node;
    },
    attr: function (name, value) {
      (attrs[selector] || (attrs[selector] = {}))[name] = value;
      return node;
    },
  };
  for (const noop of ["html", "show", "hide", "val", "addClass", "removeClass", "css", "empty", "append"])
    node[noop] = function () {
      return node;
    };
  return node;
}

function globalFormatDouble(v, decimals) {
  return Number(v).toFixed(decimals);
}

function globalFormatSpace(v) {
  return Number(v) + " b";
}

// The tail of the function builds the ops chart; none of it is what this test is about, so it gets the smallest
// stubs that let it run to completion.
const document = { documentElement: {}, querySelector: () => ({}) };
const getComputedStyle = () => ({ getPropertyValue: () => "" });
function ApexCharts() {
  this.render = function () {};
  this.destroy = function () {};
}

var opsPerSecHistory = {};
var serverChartCommands = null;
var serverData = {};

eval(extractFn("displayServerSummary"));

function render(profiler) {
  serverData = { metrics: { profiler: profiler, events: {} } };
  opsPerSecHistory = {};
  displayServerSummary();
}

test("the disk card names the filesystem the figures describe", () => {
  render({
    diskFreeSpace: { space: 30 },
    diskTotalSpace: { space: 100 },
    diskDirectory: { value: "/mnt/data/databases" },
  });

  assert.strictEqual(text["#summDiskUsed"], "70 b");
  assert.strictEqual(text["#summDiskTotal"], "100 b");
  assert.strictEqual(text["#summDiskDir"], "/mnt/data/databases");
  // Also as a tooltip, because the element truncates: a long container path is exactly the case that needs naming.
  assert.strictEqual(attrs["#summDiskDir"].title, "/mnt/data/databases");
});

test("a server that does not report the directory leaves the card readable", () => {
  // An older server, or a reading taken before the field existed. The figures still render and the line is blank
  // rather than showing "undefined".
  render({ diskFreeSpace: { space: 30 }, diskTotalSpace: { space: 100 } });

  assert.strictEqual(text["#summDiskUsed"], "70 b");
  assert.strictEqual(text["#summDiskDir"].trim(), "");
  assert.strictEqual(attrs["#summDiskDir"].title, "");
});
