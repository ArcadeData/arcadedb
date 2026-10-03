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

// The pure parts of two things the AI Assistant shows from query results:
//
//  - CHARTS: the model asks for a chart as {type, title, language, query, x, y[]}; Studio runs the query itself (read-only,
//    through api/v1/query, which refuses anything that writes) and draws the rows with ApexCharts. The model never sees the
//    rows. The spec comes from a model and the rows from a database: both are untrusted, so the spec is checked again here and
//    every label is escaped (or handed to ApexCharts as text), never put into HTML.
//  - RESULT TABLES: what an Execute button shows under the command: a compact table of the first rows.
//
// No DOM here, so studio/test/ai-chart.test.js drives these functions as the browser loads them.

var AI_CHART_TYPES = ["bar", "horizontalBar", "line", "area", "pie", "donut"];
var AI_CHART_LANGUAGES = ["sql", "cypher", "gremlin", "graphql"];
var AI_CHART_COLUMN = /^[A-Za-z_@][A-Za-z0-9_.@$-]{0,63}$/;
var AI_CHART_MAX_CHARTS = 3;
var AI_CHART_MAX_QUERY = 4000;
var AI_CHART_MAX_POINTS = 100;
var AI_CHART_MAX_SERIES = 5;
var AI_CHART_LABEL_LENGTH = 60;

var AI_TABLE_MAX_ROWS = 50;
var AI_TABLE_MAX_COLUMNS = 12;
var AI_TABLE_CELL_LENGTH = 200;

/** The valid charts of a list (at most three), each reduced to the members Studio uses. Anything else is dropped. */
function aiChartsClean(list) {
  var out = [];
  if (!Array.isArray(list)) return out;
  for (var i = 0; i < list.length && out.length < AI_CHART_MAX_CHARTS; i++) {
    var c = aiChartCleanOne(list[i]);
    if (c !== null) out.push(c);
  }
  return out;
}

function aiChartCleanOne(spec) {
  if (!spec || typeof spec !== "object" || Array.isArray(spec)) return null;
  if (AI_CHART_TYPES.indexOf(spec.type) < 0) return null;
  var language = typeof spec.language === "string" ? spec.language.toLowerCase() : "sql";
  if (AI_CHART_LANGUAGES.indexOf(language) < 0) return null;
  if (typeof spec.query !== "string" || spec.query.trim() === "" || spec.query.length > AI_CHART_MAX_QUERY) return null;
  if (typeof spec.x !== "string" || !AI_CHART_COLUMN.test(spec.x)) return null;
  if (!Array.isArray(spec.y) || spec.y.length === 0 || spec.y.length > AI_CHART_MAX_SERIES) return null;
  for (var i = 0; i < spec.y.length; i++) if (typeof spec.y[i] !== "string" || !AI_CHART_COLUMN.test(spec.y[i])) return null;
  var title = typeof spec.title === "string" ? spec.title : "";
  return { type: spec.type, title: title.substring(0, 120), language: language, query: spec.query.trim(), x: spec.x, y: spec.y.slice() };
}

/** A number from a number or a numeric string (ids and sums often come back as text); NaN for anything else. */
function aiChartNumber(value) {
  if (typeof value === "number") return isFinite(value) ? value : NaN;
  if (typeof value === "string" && value.trim() !== "" && isFinite(Number(value))) return Number(value);
  return NaN;
}

/**
 * A category label: any value as short text. Nested values become short JSON.
 *
 * ⚠️ ApexCharts puts some of its texts (the legend of a pie, the tooltip) into the page as HTML, and a label is whatever a
 * database holds. Observed: a category called `<img src=x onerror=alert(1)>` ran its script in a donut's legend. So the
 * characters that make markup are swapped for look-alikes (and a quote, which could leave an attribute, too): the label stays
 * readable, no tag or attribute can be formed from it.
 */
function aiChartLabel(value) {
  var text;
  if (value === null || value === undefined) text = "(none)";
  else if (typeof value === "object") text = JSON.stringify(value);
  else text = String(value);
  text = text.replace(/</g, "\u2039").replace(/>/g, "\u203a").replace(/"/g, "\u201d").replace(/'/g, "\u2019");
  return text.length > AI_CHART_LABEL_LENGTH ? text.substring(0, AI_CHART_LABEL_LENGTH - 1) + "…" : text;
}

/**
 * The points of a chart from the rows of its query: `{categories: [text], series: [{name, data: [number|null]}], dropped}`.
 * A row whose measures are all non-numeric is skipped; with several measures a single non-numeric one is a gap (null), not a
 * zero. At most 100 rows are used. Pie and donut charts take the first measure only and skip negative values.
 */
function aiChartModel(records, spec) {
  var pie = spec.type === "pie" || spec.type === "donut";
  var ys = pie ? [spec.y[0]] : spec.y;
  var categories = [];
  var data = ys.map(function () {
    return [];
  });
  var used = 0;
  var dropped = 0;
  var rows = Array.isArray(records) ? records : [];
  for (var r = 0; r < rows.length; r++) {
    var row = rows[r];
    if (!row || typeof row !== "object") {
      dropped++;
      continue;
    }
    var values = ys.map(function (name) {
      return aiChartNumber(row[name]);
    });
    var any = values.some(function (v) {
      return !isNaN(v) && (!pie || v >= 0);
    });
    if (!any) {
      dropped++;
      continue;
    }
    if (used >= AI_CHART_MAX_POINTS) {
      dropped++;
      continue;
    }
    categories.push(aiChartLabel(row[spec.x]));
    for (var k = 0; k < values.length; k++) data[k].push(isNaN(values[k]) || (pie && values[k] < 0) ? (pie ? 0 : null) : values[k]);
    used++;
  }
  return {
    categories: categories,
    series: ys.map(function (name, index) {
      return { name: name, data: data[index] };
    }),
    dropped: dropped
  };
}

/** The ApexCharts options of a chart. Titles, categories and series names reach ApexCharts as text. */
function aiChartOptions(spec, model, dark) {
  var pie = spec.type === "pie" || spec.type === "donut";
  var theme = { mode: dark ? "dark" : "light" };
  if (pie)
    return {
      chart: { type: spec.type, height: 320, background: "transparent", toolbar: { show: false } },
      series: model.series[0].data,
      labels: model.categories,
      legend: { position: "bottom" },
      dataLabels: { enabled: true },
      theme: theme
    };
  var horizontal = spec.type === "horizontalBar";
  var apexType = horizontal ? "bar" : spec.type;
  var bars = apexType === "bar";
  var options = {
    chart: { type: apexType, height: horizontal ? Math.max(240, 60 + model.categories.length * 30) : 320, background: "transparent", toolbar: { show: true } },
    series: model.series,
    xaxis: { categories: model.categories, labels: { rotate: horizontal ? 0 : -45, trim: true } },
    yaxis: { labels: { formatter: function (val) { return val != null ? Number(Number(val).toFixed(4)) : ""; } } },
    stroke: { curve: "smooth", width: bars ? 0 : 2 },
    dataLabels: { enabled: false },
    legend: { show: model.series.length > 1, position: "bottom" },
    theme: theme
  };
  if (horizontal) options.plotOptions = { bar: { horizontal: true } };
  return options;
}

// ===== Result tables (the Execute button) =====

function aiCellText(value) {
  if (value === null || value === undefined) return "";
  var text = typeof value === "object" ? JSON.stringify(value) : String(value);
  return text.length > AI_TABLE_CELL_LENGTH ? text.substring(0, AI_TABLE_CELL_LENGTH - 1) + "…" : text;
}

/**
 * What to show of a command's records: `{columns, rows: [[text]], total, truncated}`, or null when there is nothing tabular
 * (no rows, or rows that are not objects: counts of a write, plain values). The columns are the keys of the first rows in order
 * of appearance, at most 12; internal `@` members are left out except `@rid`. At most 50 rows; every cell is text cut at 200.
 */
function aiResultTable(records) {
  if (!Array.isArray(records) || records.length === 0) return null;
  var objects = records.filter(function (r) {
    return r && typeof r === "object" && !Array.isArray(r);
  });
  if (objects.length === 0) return null;
  var columns = [];
  var seen = {};
  var sample = Math.min(objects.length, 20);
  for (var i = 0; i < sample && columns.length < AI_TABLE_MAX_COLUMNS; i++) {
    for (var key in objects[i]) {
      if (!Object.prototype.hasOwnProperty.call(objects[i], key)) continue;
      if (key.charAt(0) === "@" && key !== "@rid") continue;
      if (Object.prototype.hasOwnProperty.call(seen, key)) continue;
      seen[key] = true;
      columns.push(key);
      if (columns.length >= AI_TABLE_MAX_COLUMNS) break;
    }
  }
  if (columns.length === 0) return null;
  var shown = objects.slice(0, AI_TABLE_MAX_ROWS);
  var rows = shown.map(function (row) {
    return columns.map(function (c) {
      return aiCellText(row[c]);
    });
  });
  return { columns: columns, rows: rows, total: objects.length, truncated: objects.length > shown.length };
}

/** The HTML of a result table; `esc` escapes text (Studio's escapeHtml). Every value and column name goes through it. */
function aiResultTableHtml(table, esc) {
  var html = '<div style="overflow-x: auto; max-height: 340px; overflow-y: auto; margin-top: 6px;"><table class="table table-sm table-bordered mb-0" style="font-size: 0.78rem;">';
  html += "<thead><tr>";
  for (var c = 0; c < table.columns.length; c++) html += '<th style="white-space: nowrap;">' + esc(table.columns[c]) + "</th>";
  html += "</tr></thead><tbody>";
  for (var r = 0; r < table.rows.length; r++) {
    html += "<tr>";
    for (var k = 0; k < table.rows[r].length; k++) html += '<td style="max-width: 320px; word-break: break-word;">' + esc(table.rows[r][k]) + "</td>";
    html += "</tr>";
  }
  html += "</tbody></table></div>";
  return html;
}
