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

// Issue #7266, finding 2: the "Databases Disk" card derived used as `total - free`, and after #7223 those two
// readings no longer agree on what the blocks a filesystem reserves for root are - getTotalSpace() counts them in
// the total, getUsableSpace() (which is what diskFreeSpace became) counts them as neither free nor available. So
// the reservation, 5% of an ext4 by default, landed in the "used by databases" number: on a mostly-empty volume it
// is the dominant term, and an operator watching the card for growth reads a constant offset as data. The server
// now reports the allocated figure itself. Run with:
//
//     node --test studio/test/server-disk-used.test.js
//
// The numbers here are synthetic on purpose - the arithmetic is the whole defect, and a real filesystem that
// reserves nothing cannot tell the two formulas apart.

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
const css = {};

function $(selector) {
  const node = {
    text: function (value) {
      text[selector] = value;
      return node;
    },
    css: function (name, value) {
      (css[selector] || (css[selector] = {}))[name] = value;
      return node;
    },
  };
  for (const noop of ["html", "show", "hide", "val", "addClass", "removeClass", "attr", "empty", "append"])
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

eval(extractFn("diskDirectoryOf"));
eval(extractFn("displayServerSummary"));

function render(profiler) {
  serverData = { metrics: { profiler: profiler, events: {} } };
  opsPerSecHistory = {};
  displayServerSummary();
}

// A 100 GB volume holding 1 GB of databases, with the 5% ext4 reservation: 5 GB is reserved, so usable is 94 GB
// while 99 GB is genuinely unallocated. `total - usable` answers 6 GB, five sixths of which is the reservation.
const GB = 1024 * 1024 * 1024;
const MOSTLY_EMPTY_EXT4 = {
  diskTotalSpace: { space: 100 * GB },
  diskFreeSpace: { space: 94 * GB },
  diskUsedSpace: { space: 1 * GB },
};

test("the card renders the used figure the server reports, not a difference across two definitions of free", () => {
  render(MOSTLY_EMPTY_EXT4);

  assert.strictEqual(text["#summDiskUsed"], 1 * GB + " b");
  assert.strictEqual(text["#summDiskTotal"], 100 * GB + " b");
  // and not the 6 GB the old arithmetic produced
  assert.notStrictEqual(text["#summDiskUsed"], 6 * GB + " b");
});

test("the bar is drawn from the same figure the label shows", () => {
  render(MOSTLY_EMPTY_EXT4);

  // 1 of 100 GB. The old arithmetic drew 6%, so a bar that looked like real occupancy on an empty volume.
  assert.strictEqual(css["#summDiskBar"].width, "1%");
});

test("a used figure of zero renders as zero rather than falling back", () => {
  // #5636's rule applied here: a value sitting at zero is DATA. A truthiness test would have discarded it and
  // silently shown the old difference instead, which is the one case where the two disagree most.
  render({ diskTotalSpace: { space: 100 * GB }, diskFreeSpace: { space: 94 * GB }, diskUsedSpace: { space: 0 } });

  assert.strictEqual(text["#summDiskUsed"], "0 b");
});

test("a server that does not report the used figure keeps the old difference", () => {
  // An older build. There is no way to derive the right number from a payload that does not carry it, and the
  // fallback is what keeps the card rendering at all.
  render({ diskTotalSpace: { space: 100 * GB }, diskFreeSpace: { space: 94 * GB } });

  assert.strictEqual(text["#summDiskUsed"], 6 * GB + " b");
});

test("the used figure is not repeated in the profiler details table", () => {
  // The card already shows it, which is what the skip list is for - every other figure the card renders is in it.
  const skipList = src.substring(src.indexOf("var skipProfiler = {"), src.indexOf("var profilerHtml"));
  assert.ok(/\bdiskUsedSpace:\s*1\b/.test(skipList), "diskUsedSpace must be in skipProfiler alongside its siblings");
});
