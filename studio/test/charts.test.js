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

// Issue #10579: Studio drew its charts with ApexCharts, which left MIT after 5.0.0 for a licence that refuses embedding in a
// product other people use - and the Studio jar ships inside every ArcadeDB distribution. The charts are now drawn with
// Apache ECharts (Apache-2.0). This file checks three things:
//
//  - ApexCharts is gone from what Studio ships, and every chart is drawn through studio-charts.js;
//  - studio-charts.js draws, reuses, re-themes and disposes charts as the panels rely on;
//  - each panel's option builder produces what it promises, and the ECharts build Studio ships (the "common" build)
//    actually renders it: the builders are fed to the real library in its server-side SVG mode, so an option that needs
//    a component the common build leaves out fails here instead of in a browser.
//
//     node --test studio/test/charts.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const STUDIO = path.join(__dirname, "..");
const STATIC_DIR = path.join(STUDIO, "src", "main", "resources", "static");
const JS_DIR = path.join(STATIC_DIR, "js");
const read = (file) => fs.readFileSync(path.join(JS_DIR, file), "utf8");
const plain = (v) => JSON.parse(JSON.stringify(v));

// The build webpack.config.js copies to dist/js/echarts.min.js
const echarts = require(path.join(STUDIO, "node_modules", "echarts", "dist", "echarts.common.min.js"));

/** Pulls one top-level `function name(...) {...}` out of a source by counting braces, as the sibling tests do. */
function extractFn(src, name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found: " + name);
  let i = src.indexOf("{", start) + 1;
  let depth = 1;
  while (i < src.length && depth > 0) {
    if (src[i] === "{") depth++;
    else if (src[i] === "}") depth--;
    i++;
  }
  if (depth !== 0) throw new Error("unbalanced braces while extracting " + name);
  return src.substring(start, i);
}

/** A context with studio-charts.js loaded, as index.html loads it before the panels, plus the named panel functions. */
function contextWith(globals, functions) {
  const ctx = vm.createContext(Object.assign({}, globals));
  vm.runInContext(read("studio-charts.js"), ctx);
  for (const [file, names] of Object.entries(functions || {})) {
    const src = read(file);
    for (const name of names) vm.runInContext(extractFn(src, name), ctx);
  }
  return ctx;
}

/** Renders an option with the shipped ECharts build, as the browser would, and returns the SVG. */
function renderSvg(option) {
  const chart = echarts.init(null, null, { renderer: "svg", ssr: true, width: 640, height: 400 });
  try {
    chart.setOption(option);
    return chart.renderToSVGString();
  } finally {
    chart.dispose();
  }
}

// ===== ApexCharts is gone =====

test("ApexCharts is no dependency of Studio, and ECharts is", () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(STUDIO, "package.json"), "utf8"));
  assert.equal(pkg.dependencies.apexcharts, undefined);
  assert.ok(pkg.dependencies.echarts, "echarts is a dependency");
  const webpack = fs.readFileSync(path.join(STUDIO, "webpack.config.js"), "utf8");
  assert.ok(!/apexcharts/i.test(webpack), "webpack.config.js copies no ApexCharts file");
  assert.ok(webpack.includes("node_modules/echarts/dist/echarts.common.min.js"));
});

test("no Studio page loads ApexCharts, and index.html loads ECharts before the chart helper", () => {
  for (const file of fs.readdirSync(STATIC_DIR).filter((f) => f.endsWith(".html"))) {
    const html = fs.readFileSync(path.join(STATIC_DIR, file), "utf8");
    assert.ok(!/apexcharts/i.test(html), file + " references ApexCharts");
  }
  const index = fs.readFileSync(path.join(STATIC_DIR, "index.html"), "utf8");
  const library = index.indexOf('src="dist/js/echarts.min.js"');
  const helper = index.indexOf('src="js/studio-charts.js"');
  assert.ok(library > 0 && helper > library, "echarts.min.js, then js/studio-charts.js");
  assert.ok(helper < index.indexOf('src="js/studio-database.js"'), "the helper loads before the panels that draw charts");
});

// Issue #10703: this census used to list the directories it read (static/js, static/css, the pages) and missed
// src/main/js, the one webpack reads its entry from, where an import of ApexCharts' stylesheet survived the migration.
// It now walks every script, stylesheet and page under studio/src, so a directory added later is in it by construction.
// Only dist/ is left out: webpack writes it from node_modules, and the dependency test above already pins what it copies.
const SRC_DIR = path.join(STUDIO, "src");
const DIST_DIR = path.join(STATIC_DIR, "dist");

function studioSources(dir = SRC_DIR, found = []) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (full !== DIST_DIR) studioSources(full, found);
    } else if (/\.(js|css|html)$/.test(entry.name)) found.push(full);
  }
  return found;
}

test("no Studio source uses ApexCharts, and only studio-charts.js creates an ECharts chart", () => {
  const sources = studioSources();
  assert.ok(sources.includes(path.join(SRC_DIR, "main", "js", "vendor-libs.js")), "the census reaches the webpack entry");
  assert.ok(sources.includes(path.join(JS_DIR, "studio-charts.js")), "the census reaches the page scripts");
  for (const file of sources) {
    const name = path.relative(STUDIO, file);
    // A page is read whole: a comment naming ApexCharts in a page is as much a leftover as a tag loading it
    const text = fs.readFileSync(file, "utf8");
    const code = file.endsWith(".html") ? text : text.replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*\/\/.*$/gm, "");
    assert.ok(!/apexcharts/i.test(code), name + " still uses ApexCharts");
    if (path.basename(file) !== "studio-charts.js") assert.ok(!/echarts\.init\s*\(/.test(code), name + " creates a chart without studio-charts.js");
  }
});

// ===== studio-charts.js =====

/** A stand-in for the `echarts` global that records what studio-charts.js asks of it. */
function fakeEcharts() {
  const byDom = new Map();
  const lib = {
    created: [],
    init(dom, theme, opts) {
      const chart = {
        dom,
        theme,
        opts,
        options: [],
        disposed: false,
        themes: [],
        resized: 0,
        setOption(option, notMerge) {
          this.options.push({ option, notMerge });
        },
        resize() {
          this.resized++;
        },
        getDom() {
          return this.dom;
        },
        isDisposed() {
          return this.disposed;
        },
        dispose() {
          this.disposed = true;
          byDom.delete(this.dom);
        },
        setTheme(t) {
          this.themes.push(t);
        }
      };
      byDom.set(dom, chart);
      lib.created.push(chart);
      return chart;
    },
    getInstanceByDom(dom) {
      return byDom.get(dom);
    }
  };
  return lib;
}

function chartsContext(theme) {
  const lib = fakeEcharts();
  const attached = new Set();
  const document = {
    documentElement: { getAttribute: (name) => (name === "data-theme" ? theme.value : null) },
    body: { contains: (el) => attached.has(el) }
  };
  const ctx = contextWith({ echarts: lib, document });
  return { ctx, lib, attached };
}

test("a chart is drawn as SVG, at the asked height, on a transparent background, in the light or the dark theme", () => {
  const theme = { value: "light" };
  const { ctx, lib } = chartsContext(theme);
  const el = { style: {} };
  const chart = ctx.studioChartDraw(el, { series: [] }, 300);
  assert.equal(el.style.height, "300px");
  assert.equal(chart.theme, null);
  assert.equal(chart.opts.renderer, "svg");
  assert.equal(chart.options[0].option.backgroundColor, "transparent");
  theme.value = "dark";
  assert.equal(ctx.studioChartDraw({ style: {} }, { series: [] }, 200).theme, "dark");
  assert.equal(lib.created.length, 2);
});

test("a refresh updates the chart already on the element instead of building another", () => {
  const { ctx, lib } = chartsContext({ value: "light" });
  const el = { style: {} };
  const first = ctx.studioChartDraw(el, { series: [1] }, 300);
  const again = ctx.studioChartDraw(el, { series: [2] }, 300, first);
  assert.equal(again, first);
  assert.equal(lib.created.length, 1);
  assert.equal(first.options[1].notMerge, true); // the new option replaces the old one, no series left over
});

test("a chart left on an element is disposed before another is drawn there; a missing element or library draws nothing", () => {
  const { ctx, lib } = chartsContext({ value: "light" });
  const el = { style: {} };
  const orphan = ctx.studioChartDraw(el, { series: [] }, 300);
  const fresh = ctx.studioChartDraw(el, { series: [] }, 300); // no `previous`: the panel lost its handle
  assert.equal(orphan.disposed, true);
  assert.notEqual(fresh, orphan);
  assert.equal(ctx.studioChartDraw(null, { series: [] }, 300), null);
  assert.equal(contextWith({}).studioChartDraw({ style: {} }, {}, 300), null);
  ctx.studioChartDispose(null);
  ctx.studioChartDispose(fresh);
  ctx.studioChartDispose(fresh);
  assert.equal(fresh.disposed, true);
  assert.equal(lib.created.length, 2);
});

test("a theme switch repaints the charts on the page and drops the ones whose element is gone", () => {
  const theme = { value: "light" };
  const { ctx, attached } = chartsContext(theme);
  const shown = { style: {} };
  attached.add(shown);
  const onPage = ctx.studioChartDraw(shown, { series: [] }, 300);
  const removed = ctx.studioChartDraw({ style: {} }, { series: [] }, 300);
  theme.value = "dark";
  ctx.studioChartsRetheme();
  assert.deepEqual(onPage.themes, ["dark"]);
  assert.equal(removed.disposed, true);
  theme.value = "light";
  ctx.studioChartsRetheme();
  assert.deepEqual(plain(onPage.themes), ["dark", {}]);
  assert.equal(ctx.STUDIO_CHARTS.length, 1);
});

test("numbers are shown with at most the asked decimals; text stays text; no value is blank", () => {
  const ctx = contextWith({});
  assert.equal(ctx.studioChartNumber(1.234567, 4), "1.2346");
  assert.equal(ctx.studioChartNumber(2, 4), "2");
  assert.equal(ctx.studioChartNumber("3.50", 1), "3.5");
  assert.equal(ctx.studioChartNumber("IPA", 4), "IPA");
  assert.equal(ctx.studioChartNumber(null, 4), "");
  assert.equal(ctx.studioChartNumber(undefined, 4), "");
});

// ===== the panels' options, rendered by the shipped ECharts build =====

test("Query > Chart: a time axis is zoomable, a category axis lists every x once, area fills, bar has no line", () => {
  const ctx = contextWith({}, { "studio-chart.js": ["chartOption"] });
  const timed = { xType: "datetime", series: [{ name: "cpu", data: [{ x: 1000, y: 1 }, { x: 2000, y: null }] }] };
  const line = ctx.chartOption(timed, "line");
  assert.equal(line.xAxis.type, "time");
  assert.equal(line.dataZoom.length, 2);
  assert.deepEqual(plain(line.series[0].data), [[1000, 1], [2000, null]]);
  assert.equal(line.series[0].type, "line");
  assert.equal(line.yAxis.axisLabel.formatter(0.123456), "0.1235");
  assert.ok(ctx.chartOption(timed, "area").series[0].areaStyle);

  const categories = {
    xType: "category",
    series: [
      { name: "a", data: [{ x: "IPA", y: 5 }, { x: "Stout", y: 2 }] },
      { name: "b", data: [{ x: "Stout", y: 1 }, { x: "Lager", y: 4 }] }
    ]
  };
  const bar = ctx.chartOption(categories, "bar");
  assert.deepEqual(plain(bar.xAxis.data), ["IPA", "Stout", "Lager"]);
  assert.equal(bar.series[0].type, "bar");
  assert.equal(bar.series[0].smooth, undefined);
  assert.equal(bar.dataZoom, undefined);

  for (const option of [line, ctx.chartOption(timed, "area"), bar]) assert.ok(renderSvg(option).includes("<svg"));
  assert.ok(renderSvg(bar).includes("Stout"));
});

test("AI Assistant: every chart type renders, and a label made of markup is drawn as text", () => {
  const ctx = contextWith({});
  vm.runInContext(read("studio-ai-chart.js"), ctx);
  const rows = [{ s: "<img src=x onerror=alert(1)>", n: 3 }, { s: "Stout", n: 2 }];
  for (const type of ["bar", "horizontalBar", "line", "area", "pie", "donut"]) {
    const spec = { type, title: "t", language: "sql", query: "q", x: "s", y: ["n"] };
    const svg = renderSvg(ctx.aiChartOptions(spec, ctx.aiChartModel(rows, spec)));
    assert.ok(svg.includes("Stout"), type + " draws its categories");
    assert.ok(!/<img/i.test(svg), type + " put a label into the page as markup");
  }
});

test("Server: one line per operation over the refresh times, with the theme's grid colours", () => {
  const ctx = contextWith({}, { "studio-server.js": ["serverCommandsChartOption"] });
  const history = [{ x: "10:00:02", y: 7 }, { x: "10:00:01", y: 5 }];
  const option = ctx.serverCommandsChartOption([{ name: "Queries", data: history }, { name: "Write Tx", data: [{ x: "10:00:02", y: 1 }, { x: "10:00:01", y: 0 }] }], "#ddd", "#eee");
  assert.deepEqual(plain(option.xAxis.data), ["10:00:02", "10:00:01"]);
  assert.deepEqual(plain(option.series.map((s) => s.name)), ["Queries", "Write Tx"]);
  assert.deepEqual(plain(option.series[0].data), [7, 5]);
  assert.equal(option.yAxis.splitLine.lineStyle.color, "#ddd");
  assert.equal(option.yAxis.splitArea.areaStyle.color[0], "#eee");
  assert.equal(option.yAxis.axisLabel.formatter(2.6), 3);
  assert.deepEqual(plain(ctx.serverCommandsChartOption([], "#ddd", "#eee").xAxis.data), []);
  assert.ok(renderSvg(option).includes("Ops/Min"));
});

test("Database: the CRUD donut divides the operations, and says so when there are none", () => {
  const ctx = contextWith({}, { "studio-database.js": ["dbCrudChartOption"] });
  const option = ctx.dbCrudChartOption([1, 3, 0, 0]);
  assert.deepEqual(plain(option.series[0].data.map((d) => d.value)), [1, 3, 0, 0]);
  assert.deepEqual(plain(option.series[0].data.map((d) => d.name)), ["Create", "Read", "Update", "Delete"]);
  assert.equal(option.series[0].label.formatter({ percent: 74.6 }), "75%");
  assert.equal(option.series[0].label.formatter({ percent: 0 }), "");
  assert.ok(renderSvg(option).includes("Read"));
  const empty = ctx.dbCrudChartOption([0, 0, 0, 0]);
  assert.deepEqual(plain(empty.series), []);
  assert.ok(renderSvg(empty).includes("No CRUD operations yet"));
});

test("Profiler: one row per timed step, in order, with its average and maximum", () => {
  const ctx = contextWith({}, { "studio-profiler.js": ["profilerStepChartOption"] });
  const option = ctx.profilerStepChartOption([
    { name: "FetchFromTypeStep", avgCostMs: 1.5, maxCostMs: 4 },
    { name: "FilterStep", avgCostMs: 0.2, maxCostMs: 0.9 }
  ]);
  assert.deepEqual(plain(option.yAxis.data), ["FetchFromTypeStep", "FilterStep"]);
  assert.equal(option.yAxis.inverse, true);
  assert.deepEqual(plain(option.series.map((s) => s.data)), [[1.5, 0.2], [4, 0.9]]);
  assert.equal(option.tooltip.valueFormatter(3), "3 ms");
  assert.ok(renderSvg(option).includes("FilterStep"));
});

test("Time Series: points on a zoomable time axis, as lines or areas", () => {
  const ctx = contextWith({}, { "studio-timeseries.js": ["tsChartOption"] });
  const series = [{ name: "temperature", data: [{ x: 1767225600000, y: 21.5 }, { x: 1767225660000, y: 22 }] }];
  const line = ctx.tsChartOption(series, "line");
  assert.equal(line.xAxis.type, "time");
  assert.deepEqual(plain(line.series[0].data), [[1767225600000, 21.5], [1767225660000, 22]]);
  assert.equal(line.series[0].areaStyle, undefined);
  assert.equal(line.yAxis.axisLabel.formatter(1.234), "1.23");
  assert.ok(ctx.tsChartOption(series, "area").series[0].areaStyle);
  assert.ok(renderSvg(line).includes("temperature"));
});
