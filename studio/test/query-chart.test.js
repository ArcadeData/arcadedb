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

// The Query panel's Chart tab: it charts the rows of ANY result (SQL, Cypher, PromQL...), so the column roles are
// inferred from the values. The inference, the series assembly and the PromQL conversion are pure functions in
// studio-chart.js; the file is loaded whole into a VM context, so the tests see exactly what the browser loads.
//
//     node --test studio/test/query-chart.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const src = fs.readFileSync(path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-chart.js"), "utf8");
const jqueryStub = function () {
  return { on: function () {} };
};
const ctx = vm.createContext({ $: jqueryStub, jQuery: jqueryStub, document: {} });
vm.runInContext(src, ctx);

const plain = (v) => JSON.parse(JSON.stringify(v));
const T0 = Date.UTC(2026, 0, 1, 0, 0, 0);

test("columns are classified by their values: epoch ms and ISO strings are time, metadata and nested values are skipped", () => {
  const records = [
    { "@rid": "#1:0", "@type": "M", ts: T0, host: "a", cpu: 0.5, when: "2026-01-01T00:00:00Z", tags: { x: 1 }, n: 3 },
    { "@rid": "#1:1", "@type": "M", ts: T0 + 1000, host: "b", cpu: 0.7, when: "2026-01-01 00:00:01", tags: { x: 2 }, n: 4 }
  ];
  const kinds = {};
  for (const c of plain(ctx.chartColumns(records))) kinds[c.name] = c.kind;
  assert.deepEqual(kinds, { ts: "time", host: "text", cpu: "number", when: "time", tags: "other", n: "number" });
});

test("a small integer is a number, not a time, and a mixed column falls back to text", () => {
  const kinds = {};
  for (const c of plain(ctx.chartColumns([{ id: 1, mixed: 5 }, { id: 2, mixed: "x" }]))) kinds[c.name] = c.kind;
  assert.deepEqual(kinds, { id: "number", mixed: "text" });
});

test("nulls do not decide a column's kind and an all-null column is skipped", () => {
  const cols = plain(ctx.chartColumns([{ a: null, b: null }, { a: 5, b: null }]));
  assert.deepEqual(cols, [{ name: "a", kind: "number" }]);
});

test("the default spec puts the first time column on x, else the first text column, and every other number on y", () => {
  const timed = ctx.chartDefaultSpec(plain(ctx.chartColumns([{ host: "a", ts: T0, cpu: 1, mem: 2 }])));
  assert.deepEqual(plain(timed), { x: "ts", xKind: "time", split: "", ys: ["cpu", "mem"] });
  const grouped = ctx.chartDefaultSpec(plain(ctx.chartColumns([{ style: "IPA", total: 5, avgAbv: 6.5 }])));
  assert.deepEqual(plain(grouped), { x: "style", xKind: "text", split: "", ys: ["total", "avgAbv"] });
  const flat = ctx.chartDefaultSpec(plain(ctx.chartColumns([{ v: 1 }, { v: 2 }])));
  assert.deepEqual(plain(flat), { x: "", xKind: "", split: "", ys: ["v"] });
});

test("a time chart splits by the first text column that has a few distinct values, but not by an id-like one", () => {
  const few = [];
  for (let i = 0; i < 6; i++) few.push({ ts: T0 + i * 1000, host: i % 2 ? "a" : "b", v: i });
  const spec = plain(ctx.chartDefaultSpec(plain(ctx.chartColumns(few)), few));
  assert.equal(spec.split, "host");
  const many = [];
  for (let i = 0; i < 40; i++) many.push({ ts: T0 + i * 1000, name: "n" + i, v: i });
  assert.equal(plain(ctx.chartDefaultSpec(plain(ctx.chartColumns(many)), many)).split, "");
  // a category chart (text on x) never splits
  const cat = [{ style: "IPA", host: "a", v: 1 }, { style: "Stout", host: "b", v: 2 }];
  assert.equal(plain(ctx.chartDefaultSpec(plain(ctx.chartColumns(cat)), cat)).split, "");
});

test("a time chart is sorted by time, converts ISO strings, and keeps gaps as null", () => {
  const records = [
    { when: "2026-01-01T00:00:02Z", v: 3 },
    { when: "2026-01-01T00:00:00Z", v: 1 },
    { when: "2026-01-01T00:00:01Z", v: null }
  ];
  const model = plain(ctx.chartBuildSeries(records, { x: "when", xKind: "time", split: "", ys: ["v"] }));
  assert.equal(model.xType, "datetime");
  assert.deepEqual(model.series, [{ name: "v", data: [{ x: T0, y: 1 }, { x: T0 + 1000, y: null }, { x: T0 + 2000, y: 3 }] }]);
});

test("split-by makes one series per value, named after the value alone when a single measure is charted", () => {
  const records = [
    { ts: T0, host: "a", cpu: 1 },
    { ts: T0, host: "b", cpu: 2 },
    { ts: T0 + 1000, host: "a", cpu: 3 }
  ];
  const one = plain(ctx.chartBuildSeries(records, { x: "ts", xKind: "time", split: "host", ys: ["cpu"] }));
  assert.deepEqual(one.series.map((s) => s.name), ["a", "b"]);
  assert.deepEqual(one.series[0].data, [{ x: T0, y: 1 }, { x: T0 + 1000, y: 3 }]);
  const two = plain(ctx.chartBuildSeries(records.map((r) => ({ ...r, mem: 9 })), { x: "ts", xKind: "time", split: "host", ys: ["cpu", "mem"] }));
  assert.deepEqual(two.series.map((s) => s.name), ["a - cpu", "a - mem", "b - cpu", "b - mem"]);
});

test("a text x column gives a category chart in row order, and no x column numbers the rows", () => {
  const cat = plain(ctx.chartBuildSeries([{ style: "IPA", n: 5 }, { style: "Stout", n: 2 }], { x: "style", xKind: "text", split: "", ys: ["n"] }));
  assert.equal(cat.xType, "category");
  assert.deepEqual(cat.series[0].data, [{ x: "IPA", y: 5 }, { x: "Stout", y: 2 }]);
  const flat = plain(ctx.chartBuildSeries([{ v: 7 }, { v: 8 }], { x: "", xKind: "", split: "", ys: ["v"] }));
  assert.equal(flat.xType, "category");
  assert.deepEqual(flat.series[0].data, [{ x: "1", y: 7 }, { x: "2", y: 8 }]);
});

test("no measure means no series, and a non-numeric value in a measure becomes a gap", () => {
  assert.deepEqual(plain(ctx.chartBuildSeries([{ a: 1 }], { x: "", xKind: "", split: "", ys: [] }).series), []);
  const m = plain(ctx.chartBuildSeries([{ v: "x" }, { v: 2 }], { x: "", xKind: "", split: "", ys: ["v"] }));
  assert.deepEqual(m.series[0].data.map((p) => p.y), [null, 2]);
});

test("a PromQL matrix becomes one row per point, labelled with the series, in series then time order", () => {
  const response = {
    status: "success",
    data: {
      resultType: "matrix",
      result: [
        { metric: { __name__: "cpu", host: "a" }, values: [[1767225600, "0.5"], [1767225660, "0.6"]] },
        { metric: { __name__: "cpu", host: "b" }, values: [[1767225600, "1.5"]] }
      ]
    }
  };
  const rows = plain(ctx.chartRecordsFromPromQL(response));
  assert.deepEqual(rows, [
    { timestamp: T0, metric: "cpu{host=a}", value: 0.5 },
    { timestamp: T0 + 60000, metric: "cpu{host=a}", value: 0.6 },
    { timestamp: T0, metric: "cpu{host=b}", value: 1.5 }
  ]);
});

test("a PromQL vector and scalar become rows too, an error or an unknown type becomes null", () => {
  const vector = plain(ctx.chartRecordsFromPromQL({ status: "success", data: { resultType: "vector", result: [{ metric: { __name__: "up" }, value: [1767225600, "1"] }] } }));
  assert.deepEqual(vector, [{ timestamp: T0, metric: "up", value: 1 }]);
  const scalar = plain(ctx.chartRecordsFromPromQL({ status: "success", data: { resultType: "scalar", result: [1767225600, "42"] } }));
  assert.deepEqual(scalar, [{ timestamp: T0, metric: "value", value: 42 }]);
  assert.equal(ctx.chartRecordsFromPromQL({ status: "error", error: "bad" }), null);
  assert.equal(ctx.chartRecordsFromPromQL({ status: "success", data: { resultType: "string", result: [] } }), null);
  assert.deepEqual(plain(ctx.chartRecordsFromPromQL({ status: "success", data: { resultType: "matrix", result: [] } })), []);
});

test("only promql is a PromQL language of the Query panel", () => {
  assert.equal(ctx.chartIsPromQL("promql"), true);
  assert.equal(ctx.chartIsPromQL("sql"), false);
});
