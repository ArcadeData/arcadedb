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

// Issue #9689: the Server > Running Queries panel lists what the "list queries" server command answers and terminates a
// row with "terminate query <id>". The logic worth testing lives in pure functions; the jQuery handlers only collect and
// render. Run with:
//
//     node --test studio/test/server-running-queries.test.js

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
  return src.substring(start, i);
}

const formatRunningQueryElapsed = eval("(" + extractFn("formatRunningQueryElapsed") + ")");
const buildRunningQueryRows = eval("(" + extractFn("buildRunningQueryRows") + ")");
const buildTerminateQueryCommand = eval("(" + extractFn("buildTerminateQueryCommand") + ")");
const describeTerminateOutcome = eval("(" + extractFn("describeTerminateOutcome") + ")");

test("the elapsed time reads like a person would say it", () => {
  assert.equal(formatRunningQueryElapsed(350), "350 ms");
  assert.equal(formatRunningQueryElapsed(12_345), "12.3 s");
  assert.equal(formatRunningQueryElapsed(245_000), "4m 05s");
  assert.equal(formatRunningQueryElapsed(7_380_000), "2h 03m");
  assert.equal(formatRunningQueryElapsed(null), "");
  assert.equal(formatRunningQueryElapsed(-1), "");
});

test("the rows carry every column, the longest-running statement first", () => {
  const rows = buildRunningQueryRows([
    { id: "q1-ab", database: "db", user: "root", protocol: "http", language: "sql", text: "SELECT 1", elapsedMs: 10, terminating: false },
    { id: "q2-ab", database: "db", user: "alice", protocol: "bolt", language: "opencypher", text: "MATCH (n) RETURN n", tag: "bench",
      elapsedMs: 9000, terminating: true },
    { id: "q3-ab", database: "db", user: "root", protocol: "http", language: "sql", text: "INSERT ...", elapsedMs: 500,
      forwardedTo: "leader-1" },
  ]);
  assert.equal(rows.length, 3);
  assert.deepEqual(rows[0], ["q2-ab", "db", "alice", "bolt", "opencypher", "MATCH (n) RETURN n", "bench", 9000, "terminating"]);
  assert.equal(rows[1][0], "q3-ab");
  assert.equal(rows[1][8], "running (on leader-1)");
  assert.deepEqual(rows[2], ["q1-ab", "db", "root", "http", "sql", "SELECT 1", "", 10, "running"]);
});

test("a malformed answer produces no rows rather than an error", () => {
  assert.deepEqual(buildRunningQueryRows(undefined), []);
  assert.deepEqual(buildRunningQueryRows({ error: "x" }), []);
  assert.deepEqual(buildRunningQueryRows([null]), []);
});

test("a terminate names exactly one statement", () => {
  assert.equal(buildTerminateQueryCommand("q7-1a2b"), "terminate query q7-1a2b");
  assert.equal(buildTerminateQueryCommand("  q7-1a2b "), "terminate query q7-1a2b");
  // An id is one token: anything that could smuggle a second argument into the server command is refused
  assert.equal(buildTerminateQueryCommand("q7 tag x"), null);
  assert.equal(buildTerminateQueryCommand(""), null);
  assert.equal(buildTerminateQueryCommand(null), null);
});

test("every terminate outcome the server answers is told apart", () => {
  assert.equal(describeTerminateOutcome("terminated").type, "success");
  assert.equal(describeTerminateOutcome("completed").type, "warning");
  assert.match(describeTerminateOutcome("completed").text, /stands/);
  assert.equal(describeTerminateOutcome("terminating").type, "warning");
  assert.equal(describeTerminateOutcome("not found").type, "info");
});
