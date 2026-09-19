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

// Regression test for issue #7847: the graph view's radial menu offered only "expand everything", in three
// directions, so a node with many connections produced a hairball that shows nothing.
//
// The picker asks the server for a per-type edge count first - an aggregate, so a supernode's edges are not
// pulled down merely to be counted - and then expands only the chosen (direction, type) pairs, with a ceiling.
//
// What these builders can assert is the TEXT of the command. What that text means is an engine question, and it
// is pinned next door by engine/src/test/java/com/arcadedb/query/sql/Issue7847EdgeExpansionQueriesTest.java:
// the per-type aggregate, `outE(:t0, :t1)` taking the type names as named parameters (including one containing
// a quote), and LIMIT bounding the expansion.
//
// Run with:
//
//     node --test studio/test/graph-expand-relationship-picker.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const SRC_PATH = path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-graph-widget.js");
const src = fs.readFileSync(SRC_PATH, "utf8");

function extractFn(name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found in studio-graph-widget.js: " + name);
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

eval(extractFn("sqlRid"));
eval(extractFn("edgeTypeCountsCommand"));
eval(extractFn("neighborExpansionCommand"));
eval(extractFn("parseEdgeTypeCounts"));
eval(extractFn("groupSelectedEdgeTypes"));
eval(extractFn("directionLabel"));

test("the unfiltered expansion is byte-for-byte the command that shipped before", () => {
  // The three existing radial commands must keep working exactly as they did: the picker is an addition, not a
  // change of what "expand everything" means.
  for (const direction of ["out", "in", "both"]) {
    const expansion = neighborExpansionCommand(direction, "#12:0", [], 0);
    assert.equal(expansion.command, "select expand( " + direction + "E() ) from #12:0");
    assert.deepEqual(expansion.params, {});
  }
});

test("chosen edge types travel as named parameters, never as quoted literals", () => {
  const expansion = neighborExpansionCommand("out", "#12:0", ["Knows", "WorksAt"], 0);

  assert.equal(expansion.command, "select expand( outE(:t0, :t1) ) from #12:0");
  assert.deepEqual(expansion.params, { t0: "Knows", t1: "WorksAt" });
});

test("a type name carrying a quote needs no escaping convention at all", () => {
  // The escaping for a SQL string literal here is backslash, not doubling - a detail a builder gets wrong once
  // and then carries. A parameter has no convention to get wrong, and this is why the builder uses one.
  const expansion = neighborExpansionCommand("in", "#12:0", ["it's complicated"], 0);

  assert.ok(!expansion.command.includes("it's"), "the name must not be in the command text");
  assert.equal(expansion.params.t0, "it's complicated");
});

test("the ceiling is applied only when it is a positive integer", () => {
  assert.match(neighborExpansionCommand("out", "#12:0", ["Knows"], 50).command, / limit 50$/);
  assert.match(neighborExpansionCommand("out", "#12:0", ["Knows"], "50").command, / limit 50$/);

  for (const bad of [0, -1, null, undefined, "", "all", NaN, {}])
    assert.ok(
      !neighborExpansionCommand("out", "#12:0", ["Knows"], bad).command.includes("limit"),
      "limit " + JSON.stringify(bad) + " must be dropped, not concatenated"
    );
});

test("a limit that is not a whole number cannot smuggle SQL into the command", () => {
  const expansion = neighborExpansionCommand("out", "#12:0", [], "1; DROP TYPE Person");
  assert.equal(expansion.command, "select expand( outE() ) from #12:0 limit 1", "parseInt stops at the digits");
});

test("a value that is not a record id produces no command at all", () => {
  for (const bad of ["#12", "12:0", "#a:0", "#12:0 OR 1=1", "", null, undefined, 12, {}]) {
    assert.equal(sqlRid(bad), null, JSON.stringify(bad));
    assert.equal(neighborExpansionCommand("out", bad, [], 0), null, JSON.stringify(bad));
    assert.equal(edgeTypeCountsCommand("out", bad), null, JSON.stringify(bad));
  }
});

test("the counts are asked of the server as an aggregate, which is the whole point", () => {
  const command = edgeTypeCountsCommand("out", "#12:0");

  assert.match(command, /count\(\*\)/, "counting on the client would mean fetching the supernode's edges");
  assert.match(command, /group by @type/);
  assert.match(command, /expand\( outE\(\) \) from #12:0/);
});

test("the picker rows are ordered biggest first, which is the row that matters", () => {
  const rows = parseEdgeTypeCounts({
    result: [
      { type: "Knows", total: 3 },
      { type: "Bought", total: 4210 },
      { type: "Likes", total: 3 },
    ],
  });

  assert.deepEqual(
    rows.map((r) => r.type),
    ["Bought", "Knows", "Likes"],
    "the type that would flood the canvas must not be the one you scroll to; ties break by name"
  );
  assert.equal(rows[0].total, 4210);
});

test("a node with no edges, and a malformed answer, produce no rows rather than a broken picker", () => {
  for (const data of [null, undefined, {}, { result: [] }, { result: [{ total: 7 }] }, { result: { records: [] } }])
    assert.deepEqual(parseEdgeTypeCounts(data), [], JSON.stringify(data));
});

// The shape the 'studio' serializer answers with. It is an OBJECT - {vertices, edges, records} - and puts a
// non-element row in `records`, so a reader that knew only the flat-array shape found nothing and the picker
// reported "no connections" for every node. The count query now asks for 'record', which IS the flat array,
// and the parser reads both so the call site can change serializer without silently emptying the picker.
test("the counts are found whichever serializer answered", () => {
  const expected = [{ type: "Knows", total: 2 }];

  assert.deepEqual(parseEdgeTypeCounts({ result: [{ type: "Knows", total: 2 }] }), expected, "'record'");
  assert.deepEqual(
    parseEdgeTypeCounts({ result: { vertices: [], edges: [], records: [{ type: "Knows", total: 2 }] } }),
    expected,
    "'studio'"
  );
});

// The picker opens only once BOTH directions have answered, so a request that never resolves would leave the
// spinner up and nothing said. jQuery sets no timeout of its own.
test("neither count request can hang the picker open forever", () => {
  const prompt = extractFn("expandNodePrompt");

  assert.match(prompt, /timeout: COUNT_TIMEOUT_MS/, "the count request needs a ceiling of its own");
  assert.match(prompt, /const COUNT_TIMEOUT_MS = \d+;/);
  assert.match(prompt, /textStatus === "timeout"/, "a timeout has no responseText to render");
  // Built from the same label the table rows use: concatenating direction + "going" said "ingoing".
  assert.match(prompt, /directionLabel\(direction\)/);
  assert.equal(directionLabel("in"), "incoming");
  assert.equal(directionLabel("out"), "outgoing");
  assert.match(prompt, /counts\[direction\] = \[\];/, "a failed direction still has to let the picker open");
});

test("the count query asks for the serializer whose shape is the counts", () => {
  const prompt = extractFn("expandNodePrompt");
  assert.match(prompt, /serializer: "record"/, "an aggregate has no element to expand into a graph document");
  assert.ok(!/serializer: "studio"/.test(prompt), "the studio serializer would bury the rows in result.records");
});

test("a non-numeric count is shown as zero rather than as NaN", () => {
  assert.deepEqual(parseEdgeTypeCounts({ result: [{ type: "Knows", total: null }] }), [{ type: "Knows", total: 0 }]);
});

test("the selection collapses into one expansion per direction", () => {
  const grouped = groupSelectedEdgeTypes([
    { direction: "out", type: "Knows" },
    { direction: "in", type: "Knows" },
    { direction: "out", type: "Bought" },
    { direction: "out", type: "Knows" },
  ]);

  assert.deepEqual(grouped.out, ["Knows", "Bought"], "one traversal per direction, duplicates collapsed");
  assert.deepEqual(grouped.in, ["Knows"]);
  assert.deepEqual(groupSelectedEdgeTypes([]), { out: [], in: [] });
});

// jQuery's .data() coerces a data-* attribute that looks like a literal, so an edge type genuinely named
// "null", "true" or "42" would arrive as that value instead of as its name.
test("the picker reads the edge type as an attribute, not through jQuery's type coercion", () => {
  const modal = extractFn("showExpandNodeModal");
  assert.match(modal, /attr\("data-type"\)/);
  assert.ok(!/data\("type"\)/.test(modal), ".data() would turn the type named 'null' into null");
});

// Cytoscape throws on a second element with the same id, and the throw would escape before endBatch(). With
// the picker, expanding one relationship type and then another from the same node is the ordinary way to use
// it, so the two expansions share their endpoints by construction.
test("a re-expansion cannot add an element that is already on the canvas", () => {
  const load = extractFn("loadNodeNeighbors");
  assert.match(load, /if \(globalRenderedVerticesRID\[vertex\.r\]\) continue;/, "a vertex already drawn");
  assert.match(load, /globalCy\.getElementById\(edge\.r\)\.nonempty\(\)/, "an edge already drawn, self-loops too");

  // And the skip must come first, or a node that is merely being re-expanded eats the ceiling.
  assert.ok(
    load.indexOf("if (globalRenderedVerticesRID[vertex.r]) continue;") <
      load.indexOf("reachedMax = true"),
    "the duplicate check has to precede the max-elements check"
  );
});

test("the radial menu actually offers the picker, and loadNodeNeighbors uses the builder", () => {
  // A helper nothing calls is the same bug as no helper at all.
  assert.match(src, /expandNodePrompt\(ele\.data\("id"\)\)/, "the node radial menu must carry the new command");
  assert.match(
    extractFn("loadNodeNeighbors"),
    /neighborExpansionCommand\(direction, rid, edgeTypes, limit\)/,
    "the expansion must go through the builder, or the picker's choices never reach the server"
  );
  assert.match(extractFn("showExpandNodeModal"), /loadNodeNeighbors\("out", rid, grouped\.out, limit\)/);
  assert.match(extractFn("showExpandNodeModal"), /loadNodeNeighbors\("in", rid, grouped\.in, limit\)/);
});
