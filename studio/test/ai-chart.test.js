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

// The AI Assistant's charts (the model asks, Studio runs the query and draws it) and the result tables under the Execute
// buttons. The pure functions of studio-ai-chart.js are loaded whole into a VM context, as the browser loads them.
//
//     node --test studio/test/ai-chart.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const src = fs.readFileSync(path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-ai-chart.js"), "utf8");
const ctx = vm.createContext({});
vm.runInContext(src, ctx);

const plain = (v) => JSON.parse(JSON.stringify(v));
const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
const valid = () => ({ type: "bar", title: "Top styles", language: "sql", query: "SELECT s, count(*) AS n FROM B GROUP BY s", x: "s", y: ["n"] });

test("a well-formed chart is kept with only the members Studio uses", () => {
  const out = plain(ctx.aiChartsClean([Object.assign(valid(), { onclick: "alert(1)" })]));
  assert.equal(out.length, 1);
  assert.deepEqual(Object.keys(out[0]).sort(), ["language", "query", "title", "type", "x", "y"]);
});

test("not a list, nothing at all, and not an object are all empty or dropped", () => {
  assert.deepEqual(plain(ctx.aiChartsClean(null)), []);
  assert.deepEqual(plain(ctx.aiChartsClean("x")), []);
  assert.deepEqual(plain(ctx.aiChartsClean([null, "x", [1], 7])), []);
});

test("an unknown type or language, an empty or huge query are dropped", () => {
  assert.equal(ctx.aiChartsClean([Object.assign(valid(), { type: "radar" })]).length, 0);
  assert.equal(ctx.aiChartsClean([Object.assign(valid(), { language: "python" })]).length, 0);
  assert.equal(ctx.aiChartsClean([Object.assign(valid(), { query: "   " })]).length, 0);
  assert.equal(ctx.aiChartsClean([Object.assign(valid(), { query: "x".repeat(4001) })]).length, 0);
});

test("columns are names, never expressions; y has one to five of them", () => {
  assert.equal(ctx.aiChartsClean([Object.assign(valid(), { x: "s; DROP TYPE B" })]).length, 0);
  assert.equal(ctx.aiChartsClean([Object.assign(valid(), { x: "" })]).length, 0);
  assert.equal(ctx.aiChartsClean([Object.assign(valid(), { y: [] })]).length, 0);
  assert.equal(ctx.aiChartsClean([Object.assign(valid(), { y: ["a", "b", "c", "d", "e", "f"] })]).length, 0);
  assert.equal(ctx.aiChartsClean([Object.assign(valid(), { y: [7] })]).length, 0);
  assert.equal(ctx.aiChartsClean([Object.assign(valid(), { y: "n" })]).length, 0);
});

test("at most three charts, the valid ones, in order", () => {
  const list = [valid(), Object.assign(valid(), { type: "nope" }), valid(), valid(), valid()];
  assert.equal(ctx.aiChartsClean(list).length, 3);
});

test("numbers come from numbers and numeric strings only", () => {
  assert.equal(ctx.aiChartNumber(4), 4);
  assert.equal(ctx.aiChartNumber("4.5"), 4.5);
  assert.ok(isNaN(ctx.aiChartNumber("abc")));
  assert.ok(isNaN(ctx.aiChartNumber("")));
  assert.ok(isNaN(ctx.aiChartNumber(null)));
  assert.ok(isNaN(ctx.aiChartNumber(Infinity)));
  assert.ok(isNaN(ctx.aiChartNumber({})));
});

test("the model of a bar chart: categories from x, numbers from y, rows without a number skipped", () => {
  const rows = [{ s: "Ale", n: 5 }, { s: "Lager", n: "7" }, { s: "Stout", n: null }, { s: "IPA", n: "many" }, { s: "Sour", n: 2 }];
  const m = plain(ctx.aiChartModel(rows, valid()));
  assert.deepEqual(m.categories, ["Ale", "Lager", "Sour"]);
  assert.deepEqual(m.series, [{ name: "n", data: [5, 7, 2] }]);
  assert.equal(m.dropped, 2);
});

test("with several measures a missing one is a gap, not a zero, and a row with none is skipped", () => {
  const spec = Object.assign(valid(), { y: ["a", "b"] });
  const m = plain(ctx.aiChartModel([{ s: "x", a: 1, b: 2 }, { s: "y", a: 3, b: "no" }, { s: "z", a: "no", b: "no" }], spec));
  assert.deepEqual(m.categories, ["x", "y"]);
  assert.deepEqual(m.series, [{ name: "a", data: [1, 3] }, { name: "b", data: [2, null] }]);
  assert.equal(m.dropped, 1);
});

test("at most 100 points, the rest counted as left out", () => {
  const rows = [];
  for (let i = 0; i < 130; i++) rows.push({ s: "c" + i, n: i });
  const m = ctx.aiChartModel(rows, valid());
  assert.equal(m.categories.length, 100);
  assert.equal(m.dropped, 30);
});

test("a pie takes the first measure only and skips negative values", () => {
  const spec = Object.assign(valid(), { type: "pie", y: ["n", "other"] });
  const m = plain(ctx.aiChartModel([{ s: "a", n: 3, other: 9 }, { s: "b", n: -1, other: 9 }, { s: "c", n: 1, other: 9 }], spec));
  assert.deepEqual(m.categories, ["a", "c"]);
  assert.equal(m.series.length, 1);
  assert.deepEqual(m.series[0].data, [3, 1]);
});

test("labels are text: nested values become short JSON, nulls a marker, long text is cut", () => {
  const m = plain(ctx.aiChartModel([{ s: { a: 1 }, n: 1 }, { s: null, n: 2 }, { s: "L".repeat(200), n: 3 }, { s: "<img src=x onerror=alert(1)>", n: 4 }], valid()));
  assert.equal(m.categories[0], "{\u201da\u201d:1}"); // the quotes are look-alikes (see aiChartLabel)
  assert.equal(m.categories[1], "(none)");
  assert.ok(m.categories[2].length <= 60);
  // ApexCharts writes some texts (a pie's legend, the tooltip) as HTML: no label may contain a character that forms a tag or leaves an attribute
  assert.ok(m.categories.every((c) => !/[<>"']/.test(c)));
  assert.equal(m.categories[3], "\u2039img src=x onerror=alert(1)\u203a");
});

test("a label that tries to break out of HTML or an attribute cannot", () => {
  const m = ctx.aiChartModel([{ s: '"><script>alert(1)</script>', n: 1 }, { s: "' onmouseover='x", n: 2 }, { s: "R&B", n: 3 }], valid());
  assert.ok(m.categories.every((c) => !/[<>"']/.test(c)));
  assert.equal(m.categories[2], "R&B");
});

test("rows that are not objects, or no rows, give an empty model", () => {
  assert.equal(ctx.aiChartModel(null, valid()).categories.length, 0);
  assert.equal(ctx.aiChartModel([1, "x", null], valid()).categories.length, 0);
});

test("the options: a bar, a horizontal bar, a line and a pie, in the light and the dark theme", () => {
  const model = ctx.aiChartModel([{ s: "a", n: 1 }, { s: "b", n: 2 }], valid());
  const bar = ctx.aiChartOptions(valid(), model, false);
  assert.equal(bar.chart.type, "bar");
  assert.equal(bar.theme.mode, "light");
  assert.deepEqual(plain(bar.xaxis.categories), ["a", "b"]);
  assert.equal(bar.plotOptions, undefined);
  const horizontal = ctx.aiChartOptions(Object.assign(valid(), { type: "horizontalBar" }), model, true);
  assert.equal(horizontal.chart.type, "bar");
  assert.equal(horizontal.plotOptions.bar.horizontal, true);
  assert.equal(horizontal.theme.mode, "dark");
  assert.equal(ctx.aiChartOptions(Object.assign(valid(), { type: "line" }), model, false).chart.type, "line");
  assert.equal(ctx.aiChartOptions(Object.assign(valid(), { type: "area" }), model, false).stroke.width, 2);
  const pie = ctx.aiChartOptions(Object.assign(valid(), { type: "donut" }), ctx.aiChartModel([{ s: "a", n: 1 }], Object.assign(valid(), { type: "donut" })), false);
  assert.equal(pie.chart.type, "donut");
  assert.deepEqual(plain(pie.series), [1]);
  assert.deepEqual(plain(pie.labels), ["a"]);
});

// ===== result tables =====

test("a result of records becomes a table: columns in order, @rid kept, other @ members left out", () => {
  const t = plain(ctx.aiResultTable([{ "@rid": "#1:0", "@type": "Beer", "@cat": "v", name: "A", abv: 5.5 }, { "@rid": "#1:1", "@type": "Beer", name: "B", abv: 4 }]));
  assert.deepEqual(t.columns, ["@rid", "name", "abv"]);
  assert.deepEqual(t.rows, [["#1:0", "A", "5.5"], ["#1:1", "B", "4"]]);
  assert.equal(t.total, 2);
  assert.equal(t.truncated, false);
});

test("nothing tabular gives null: no rows, a write's count, plain values", () => {
  assert.equal(ctx.aiResultTable([]), null);
  assert.equal(ctx.aiResultTable(null), null);
  assert.equal(ctx.aiResultTable([1, 2, 3]), null);
  assert.equal(ctx.aiResultTable([{ "@type": "x" }]), null);
});

test("at most 50 rows and 12 columns, and it says it was cut", () => {
  const rows = [];
  for (let i = 0; i < 80; i++) {
    const r = {};
    for (let c = 0; c < 20; c++) r["c" + c] = i;
    rows.push(r);
  }
  const t = ctx.aiResultTable(rows);
  assert.equal(t.rows.length, 50);
  assert.equal(t.columns.length, 12);
  assert.equal(t.rows[0].length, 12);
  assert.equal(t.truncated, true);
  assert.equal(t.total, 80);
});

test("cells are text cut at 200, nested values short JSON, nulls empty", () => {
  const t = ctx.aiResultTable([{ a: "x".repeat(500), b: { k: [1, 2] }, c: null, d: true }]);
  assert.ok(t.rows[0][0].length <= 200);
  assert.equal(t.rows[0][1], '{"k":[1,2]}');
  assert.equal(t.rows[0][2], "");
  assert.equal(t.rows[0][3], "true");
});

test("the HTML escapes every column name and every value", () => {
  const t = ctx.aiResultTable([{ "<b>col</b>": "<script>alert(1)</script>", n: "a&b" }]);
  const html = ctx.aiResultTableHtml(t, esc);
  assert.ok(!html.includes("<script>"));
  assert.ok(!html.includes("<b>col"));
  assert.ok(html.includes("&lt;script&gt;alert(1)&lt;/script&gt;"));
  assert.ok(html.includes("a&amp;b"));
});

// ---- the model names columns a little differently from the query's real ones ----

test("a column the model spelled with other case is the same column", () => {
  const m = plain(ctx.aiChartModel([{ Style: "Ale", Beers: 5 }, { Style: "Lager", Beers: 7 }], Object.assign(valid(), { x: "style", y: ["BEERS"] })));
  assert.deepEqual(m.categories, ["Ale", "Lager"]);
  assert.deepEqual(m.series, [{ name: "Beers", data: [5, 7] }]);
  assert.deepEqual(m.notes, []);
});

test("x \"style\" resolves to the one column style_name (suffix and prefix match), silently", () => {
  const rows = [{ style_name: "Ale", beers: 5 }, { style_name: "Lager", beers: 7 }];
  const m = plain(ctx.aiChartModel(rows, Object.assign(valid(), { x: "style", y: ["beers"] })));
  assert.deepEqual(m.categories, ["Ale", "Lager"]);
  assert.equal(m.x, "style_name");
  assert.deepEqual(m.notes, []);
  const byEnd = plain(ctx.aiChartModel([{ beer_style: "Ale", n: 1 }], Object.assign(valid(), { x: "style" })));
  assert.equal(byEnd.x, "beer_style");
  assert.deepEqual(byEnd.notes, []);
});

test("an ambiguous name is no match: two columns fit, so none is guessed", () => {
  const rows = [{ style_name: "Ale", style_code: "A1", n: 3 }];
  assert.equal(ctx.aiChartResolve("style", ["style_name", "style_code", "n"]), null);
  const m = plain(ctx.aiChartModel(rows, Object.assign(valid(), { x: "style" })));
  assert.equal(m.x, "style_name"); // the fallback: the first non-numeric column
  assert.equal(m.notes.length, 1);
  assert.match(m.notes[0], /style_name/);
});

test("a missing x falls back to the first non-numeric column and says so; the result has no NaN anywhere", () => {
  const rows = [{ "@rid": "#1:1", cnt: 4, label: "A" }, { "@rid": "#1:2", cnt: 6, label: "B" }];
  const m = plain(ctx.aiChartModel(rows, Object.assign(valid(), { x: "nothing", y: ["cnt"] })));
  assert.deepEqual(m.categories, ["A", "B"]); // "@rid" is text too, but internal columns are not picked as the categories
  assert.equal(m.x, "label");
  assert.equal(m.series[0].data.join(), "4,6");
  assert.ok(m.notes.length === 1);
  assert.ok(m.categories.every((c) => c !== "NaN" && c !== "undefined"));
});

test("a missing y falls back to the first numeric column not used as x, with a note", () => {
  const rows = [{ name: "A", total: 4, other: 1 }, { name: "B", total: 6, other: 2 }];
  const m = plain(ctx.aiChartModel(rows, Object.assign(valid(), { x: "name", y: ["missing"] })));
  assert.deepEqual(m.y, ["total"]);
  assert.deepEqual(m.series, [{ name: "total", data: [4, 6] }]);
  assert.match(m.notes[0], /total/);
});

test("a null, undefined, empty or NaN category is \"(none)\", never NaN or undefined", () => {
  const m = plain(ctx.aiChartModel([{ s: null, n: 1 }, { s: undefined, n: 2 }, { s: "", n: 3 }, { s: NaN, n: 4 }, { n: 5 }, { s: "ok", n: 6 }], valid()));
  assert.deepEqual(m.categories, ["(none)", "(none)", "(none)", "(none)", "(none)", "ok"]);
  assert.equal(ctx.aiChartLabel(NaN), "(none)");
  assert.equal(ctx.aiChartLabel(Infinity), "(none)");
  assert.equal(ctx.aiChartLabel("  "), "(none)");
  assert.equal(ctx.aiChartLabel(0), "0");
});

test("when every value is not a number there is nothing to chart (no NaN points)", () => {
  const m = plain(ctx.aiChartModel([{ s: "a", n: "x" }, { s: "b", n: NaN }, { s: "c", n: null }], valid()));
  assert.equal(m.categories.length, 0);
  assert.equal(m.dropped, 3);
});

test("no columns at all (an empty result) is an empty model, not an error", () => {
  const m = plain(ctx.aiChartModel([], valid()));
  assert.equal(m.categories.length, 0);
  assert.deepEqual(m.series, []); // no columns, so nothing to name a series after
  assert.deepEqual(m.notes, []);
});

test("a horizontal chart gets an explicit category axis, so labels are never read as numbers", () => {
  const model = ctx.aiChartModel([{ s: "a", n: 1 }], valid());
  const options = ctx.aiChartOptions(Object.assign(valid(), { type: "horizontalBar" }), model, false);
  assert.equal(options.xaxis.type, "category");
});

test("a number formatter is on the axis that shows numbers: a horizontal chart's category names are never turned into NaN", () => {
  const model = ctx.aiChartModel([{ s: "American-Style Pale Ale", n: 1.234567 }], valid());
  const horizontal = ctx.aiChartOptions(Object.assign(valid(), { type: "horizontalBar" }), model, false);
  const yFormat = horizontal.yaxis && horizontal.yaxis.labels && horizontal.yaxis.labels.formatter;
  assert.ok(!yFormat || yFormat("American-Style Pale Ale") === "American-Style Pale Ale"); // the y axis carries the names
  assert.equal(horizontal.xaxis.labels.formatter(1.234567), 1.2346);
  assert.equal(horizontal.xaxis.labels.formatter("text"), "text");
  const vertical = ctx.aiChartOptions(Object.assign(valid(), { type: "bar" }), model, false);
  assert.equal(vertical.yaxis.labels.formatter(2.5), 2.5);
  assert.equal(vertical.yaxis.labels.formatter(null), "");
});
