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

// Support requests in Studio (studio-support-requests.js): the statement rules (the same vector file as the customer portal and
// the platform), how a query result becomes a table, how masks are applied in the browser before anything is sent, and that
// nothing a client's colleague wrote is ever rendered as HTML. Run with:
//
//     node --test studio/test/support-requests.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS = path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-support-requests.js");
const vectors = JSON.parse(fs.readFileSync(path.join(__dirname, "support-requests-vectors.json"), "utf8"));

// The file is a classic script: globals, jQuery handlers registered at load. Load it into a context with just enough around it.
function load() {
  const store = {};
  const noop = () => ({ on() {}, off() {}, text() {}, html() {}, prop() { return this; }, val() {}, empty() { return this; }, append() { return this; } });
  const context = {
    $: () => noop(),
    jQuery: {},
    document: {},
    globalThis: undefined,
    escapeHtml: (v) => String(v).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;"),
    supportEsc: (v) => String(v == null ? "" : v).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;"),
    supportSpinner: (t) => t,
    supportTimeline: (issue) => issue.timeline || [],
    globalStorageLoad: (k) => store[k],
    globalStorageSave: (k, v) => void (store[k] = v),
    TextEncoder,
    Promise,
    Uint8Array,
    JSON,
    Object,
    Array,
    Number,
    String,
    Math,
    isFinite,
    console,
  };
  context.globalThis = { crypto: globalThis.crypto };
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(JS, "utf8"), context, { filename: "studio-support-requests.js" });
  return { run: (code) => vm.runInContext(code, context), context, store };
}

test("the shared vectors: every accepted statement is accepted as normalised, every refused one with its code", () => {
  const { run, context } = load();
  context.vectors = vectors;
  const bad = run(`(function () {
    var problems = [];
    vectors.accept.forEach(function (v) {
      var r = supportReqValidate(v.language, v.statement);
      var expected = v.normalized !== undefined ? v.normalized : v.statement.trim();
      if (!r.ok || r.statement !== expected) problems.push("accept " + JSON.stringify(v.statement) + " -> " + JSON.stringify(r));
    });
    vectors.reject.forEach(function (v) {
      var r = supportReqValidate(v.language, v.statement);
      if (r.ok || r.code !== v.code) problems.push("reject " + JSON.stringify(v.statement) + " -> " + JSON.stringify(r) + " expected " + v.code);
    });
    return problems;
  })()`);
  assert.deepEqual(Array.from(bad), []);
  assert.ok(vectors.accept.length > 10 && vectors.reject.length > 30);
});

test("records become a table: no @ properties, union of columns, types, nested values as text, the cap", () => {
  const { run, context } = load();
  context.records = [
    { "@rid": "#1:0", "@type": "Doc", name: "a", n: 1, ok: true, tags: ["x", "y"] },
    { "@rid": "#1:1", name: "b", n: 2.5, extra: null },
  ];
  const table = JSON.parse(run(`JSON.stringify(supportReqTable(records, "", 1000))`));
  assert.deepEqual(table.columns.map((c) => c.name), ["name", "n", "ok", "tags", "extra"]);
  assert.deepEqual(table.columns.map((c) => c.type), ["STRING", "LONG", "BOOLEAN", "STRING", "STRING"]);
  assert.deepEqual(table.rows[0], ["a", 1, true, '["x","y"]', null]);
  assert.deepEqual(table.rows[1], ["b", 2.5, null, null, null]);
  assert.equal(table.truncated, false);

  context.many = Array.from({ length: 1500 }, (_, i) => ({ i }));
  const cut = JSON.parse(run(`JSON.stringify(supportReqTable(many, "", 1000))`));
  assert.equal(cut.rows.length, 1000);
  assert.equal(cut.truncated, true);

  const node = JSON.parse(run(`JSON.stringify(supportReqTable([{ c: 1 }], "arcadedb-0", 1000))`));
  assert.deepEqual(node.columns.map((c) => c.name), ["node", "c"]);
  assert.deepEqual(node.rows[0], ["arcadedb-0", 1]);
  assert.equal(JSON.parse(run(`JSON.stringify(supportReqTable(null, "", 10))`)).rows.length, 0);
});

test("sensitive-looking columns start masked, the user's earlier choices win, and cells, rows and columns toggle", () => {
  const { run, context } = load();
  context.table = { columns: [{ name: "id", type: "LONG" }, { name: "email", type: "STRING" }, { name: "api_key", type: "STRING" }, { name: "note", type: "STRING" }], rows: [[1, "a@x", "k1", "n1"], [2, "b@x", "k2", "n2"]], truncated: false };
  const initial = JSON.parse(run(`JSON.stringify(supportReqInitialMask(table, {}))`));
  assert.deepEqual(Object.keys(initial.columns).sort(), ["api_key", "email"]);
  const remembered = JSON.parse(run(`JSON.stringify(supportReqInitialMask(table, { email: false, note: true }))`));
  assert.deepEqual(Object.keys(remembered.columns).sort(), ["api_key", "note"]);

  run(`mask = supportReqInitialMask(table, {})`);
  run(`supportReqToggleCell(mask, 0, 0)`);
  assert.equal(run(`supportReqIsMasked(mask, table, 0, 0)`), true);
  assert.equal(run(`supportReqIsMasked(mask, table, 1, 0)`), false);
  run(`supportReqToggleCell(mask, 0, 0)`);
  assert.equal(run(`supportReqIsMasked(mask, table, 0, 0)`), false);
  run(`supportReqToggleColumn(mask, table, 3)`);
  assert.equal(run(`supportReqIsMasked(mask, table, 1, 3)`), true);
  run(`supportReqToggleColumn(mask, table, 3)`);
  assert.equal(run(`supportReqIsMasked(mask, table, 1, 3)`), false);
  run(`supportReqToggleRow(mask, table, 1)`);
  assert.equal(run(`supportReqIsMasked(mask, table, 1, 0) && supportReqIsMasked(mask, table, 1, 3)`), true);
  run(`supportReqToggleRow(mask, table, 1)`);
  assert.equal(run(`supportReqIsMasked(mask, table, 1, 0)`), false);
  // email and api_key are masked columns: 2 rows x 2 columns
  assert.equal(run(`supportReqMaskedCount(mask, table)`), 4);
});

test("the answer replaces masked values in the browser: dots, or a code that only matches inside the answer; the original is never in it", async () => {
  const { run, context } = load();
  context.table = { columns: [{ name: "id", type: "LONG" }, { name: "email", type: "STRING" }, { name: "note", type: "STRING" }], rows: [[1, "secret@x.com", "same"], [2, "secret@x.com", "same"], [3, "other@x.com", "n3"]], truncated: true };
  run(`mask = { columns: { email: true }, cells: { "2:2": true }, mode: "redact" }`);
  const redacted = await run(`supportReqBuildAnswer(table, mask, 412.6)`);
  assert.equal(redacted.outcome, "answered");
  assert.equal(redacted.durationMs, 413);
  assert.equal(redacted.result.truncated, true);
  assert.deepEqual(redacted.result.rows, [[1, "••••", "same"], [2, "••••", "same"], [3, "••••", "••••"]]);
  assert.deepEqual(JSON.parse(JSON.stringify(redacted.result.masked)), { cells: [[2, 2]], columns: ["email"], mode: "redact" });
  assert.ok(!JSON.stringify(redacted).includes("secret@x.com") && !JSON.stringify(redacted).includes("other@x.com"));

  run(`mask.mode = "hash"`);
  const hashed = await run(`supportReqBuildAnswer(table, mask, 0)`);
  const rows = hashed.result.rows;
  assert.match(rows[0][1], /^#[0-9a-f]{12}$/);
  assert.equal(rows[0][1], rows[1][1], "equal values stay equal inside one answer");
  assert.notEqual(rows[0][1], rows[2][1]);
  assert.ok(!JSON.stringify(hashed).includes("secret@x.com"));
  assert.equal(hashed.durationMs, undefined);
  const again = await run(`supportReqBuildAnswer(table, mask, 0)`);
  assert.notEqual(again.result.rows[0][1], rows[0][1], "a new answer has a new salt: nothing matches across answers");
});

test("nothing from the portal is rendered as HTML: labels, statements, errors and results are text", () => {
  const { run, context } = load();
  context.entry = { requests: [{ id: "rq_0123abcd", label: "<img src=x onerror=alert(1)>", language: "sql", statement: "SELECT '<script>alert(1)</script>' FROM X", database: "", nodes: "current", status: "open" }] };
  const html = run(`supportRequestsHtml(entry)`);
  assert.ok(!html.includes("<img src=x") && !html.includes("<script>alert"), html);
  assert.ok(html.includes("&lt;img src=x onerror=alert(1)&gt;"));

  // a statement the rules refuse is never offered for running
  context.bad = { requests: [{ id: "rq_89abcdef", label: "x", language: "sql", statement: "DELETE FROM X", database: "", nodes: "current", status: "open" }] };
  const refused = run(`supportRequestsHtml(bad)`);
  assert.ok(refused.includes("will not run this statement") && !refused.includes("sp-rq-run"), refused);

  // a result with markup in a cell
  context.request = { id: "rq_aaaaaaaa", label: "t", language: "sql", statement: "SELECT 1", database: "", nodes: "current", status: "open" };
  run(`s = supportReqStateFor(request); s.table = { columns: [{ name: "<b>c</b>", type: "STRING" }], rows: [["<script>x</script>"]], truncated: false }; s.mask = supportReqInitialMask(s.table, {}); s.status = "done";`);
  const table = run(`supportReqTableHtml(request, s)`);
  assert.ok(!table.includes("<script>x") && !table.includes("<b>c</b>"), table);
  assert.ok(table.includes("&lt;script&gt;x&lt;/script&gt;"));

  assert.equal(run(`supportRequestsHtml({ requests: [] }) + supportRequestsHtml({})`), "");
});

test("only open requests can be run, and the Run all bar appears from two open ones", () => {
  const { run, context } = load();
  const open = (id) => ({ id, label: id, language: "sql", statement: "SELECT 1", database: "", nodes: "current", status: "open" });
  context.one = { timeline: [{ requests: [open("rq_00000001"), { ...open("rq_00000002"), status: "answered" }] }] };
  context.two = { timeline: [{ requests: [open("rq_00000001")] }, { requests: [open("rq_00000003")] }] };
  assert.equal(run(`supportRequestsBarHtml(one)`), "");
  assert.ok(run(`supportRequestsBarHtml(two)`).includes("spRqRunAll"));
  assert.equal(run(`supportReqAllOpen(one).length`), 1);
  assert.ok(!run(`supportRequestsHtml(one.timeline[0])`).match(/rq_00000002[\s\S]*sp-rq-run/));
});

test("a cluster answer is ONE table: a leading node column, the union of the columns, a failed node is its own row", () => {
  const { run } = load();
  const merged = JSON.parse(run(`JSON.stringify(supportReqMergeTables([
    { node: "a", table: supportReqTable([{ c: 112914 }], "", 1000) },
    { node: "b", table: supportReqTable([{ c: 112623, extra: "x" }], "", 1000) },
    { node: "c", error: "timed out" } ], 1000))`));
  assert.deepEqual(merged.columns.map((c) => c.name), ["node", "c", "extra", "error"]);
  assert.deepEqual(merged.rows, [["a", 112914, null, null], ["b", 112623, "x", null], ["c", null, null, "timed out"]]);
  assert.equal(merged.columns[1].type, "LONG");
  assert.equal(merged.truncated, false);
});

test("without a failure there is no error column, and a result column named node or error does not clash with ours", () => {
  const { run } = load();
  const merged = JSON.parse(run(`JSON.stringify(supportReqMergeTables([
    { node: "a", table: supportReqTable([{ node: "x", error: "y" }], "", 1000) },
    { node: "b", table: supportReqTable([{ node: "z", error: "w" }], "", 1000) } ], 1000))`));
  assert.deepEqual(merged.columns.map((c) => c.name), ["node", "node_value", "error_value"]);
  assert.deepEqual(merged.rows, [["a", "x", "y"], ["b", "z", "w"]]);
});

test("the rows are cut evenly per node so a wide cluster cannot make an answer too large", () => {
  const { run } = load();
  const merged = JSON.parse(run(`(function () {
    var rows = []; for (var i = 0; i < 50; i++) rows.push({ n: i });
    var parts = [];
    for (var k = 0; k < 4; k++) parts.push({ node: "n" + k, table: supportReqTable(rows, "", 1000) });
    return JSON.stringify(supportReqMergeTables(parts, 100));
  })()`));
  assert.equal(merged.rows.length, 100);
  assert.equal(merged.rows.filter((r) => r[0] === "n3").length, 25);
  assert.equal(merged.truncated, true);
});

test("where a request runs: what support asked for unless the user chose another place", () => {
  const { run } = load();
  assert.equal(run(`supportReqTarget({ nodes: "all" }, {})`), "all");
  assert.equal(run(`supportReqTarget({ nodes: "current" }, {})`), "current");
  assert.equal(run(`supportReqTarget({}, {})`), "current");
  assert.equal(run(`supportReqTarget({ nodes: "arcadedb-1" }, {})`), "arcadedb-1");
  assert.equal(run(`supportReqTarget({ nodes: "all" }, { nodes: "current" })`), "current");
  assert.equal(run(`supportReqTarget({ nodes: "current" }, { nodes: "arcadedb-2" })`), "arcadedb-2");
});

test("the node selector exists only in a cluster, offers this node, all nodes and each peer, and node names are text", () => {
  const { run } = load();
  assert.equal(run(`supportReqNodeSelectHtml({ id: "rq_1", nodes: "all" }, {})`), "");
  const html = run(`(function () {
    supportReqNode = "arcadedb-0";
    supportReqPeers = { loaded: true, ha: true, peers: ["arcadedb-1", "<img src=x onerror=alert(1)>"] };
    return supportReqNodeSelectHtml({ id: "rq_1", nodes: "all" }, {});
  })()`);
  assert.match(html, /<option value="current">This node \(arcadedb-0\)<\/option>/);
  assert.match(html, /<option value="all" selected>All nodes<\/option>/);
  assert.match(html, /arcadedb-1/);
  assert.ok(!html.includes("<img"), html);
});

test("a request for every node on a server that is not in a cluster says it runs here only", () => {
  const { run } = load();
  const html = run(`(function () {
    supportReqNode = "solo";
    supportReqPeers = { loaded: true, ha: false, peers: [] };
    var request = { id: "rq_1", kind: "query", label: "l", language: "sql", statement: "SELECT 1", nodes: "all", status: "open" };
    return supportRequestBodyHtml(request);
  })()`);
  assert.match(html, /not part of a cluster/);
  assert.match(html, /this server only/);
});
