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
//    through api/v1/query, which refuses anything that writes) and draws the rows with Apache ECharts. The model never sees
//    the rows. The spec comes from a model and the rows from a database: both are untrusted, so the spec is checked again here
//    and every label is defused (or handed to the chart as text), never put into HTML.
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
 * A category label: any value as short text. Nested values become short JSON. Null, undefined, an empty text and a number that is
 * not a number (NaN, Infinity) are all "(none)": a label never reads "NaN" or "undefined".
 *
 * ⚠️ A chart library can put some of its texts (a legend, a tooltip) into the page as HTML, and a label is whatever a
 * database holds. Observed with ApexCharts, Studio's chart library before #10579: a category called
 * `<img src=x onerror=alert(1)>` ran its script in a donut's legend. ECharts escapes its tooltip, but the label does not rely
 * on that: the characters that make markup are swapped for look-alikes (and a quote, which could leave an attribute, too),
 * so the label stays readable and no tag or attribute can be formed from it whatever draws it.
 */
function aiChartLabel(value) {
  var text;
  if (value === null || value === undefined || (typeof value === "number" && !isFinite(value))) text = "(none)";
  else if (typeof value === "object") text = JSON.stringify(value);
  else text = String(value);
  if (text.trim() === "") text = "(none)";
  text = text.replace(/</g, "\u2039").replace(/>/g, "\u203a").replace(/"/g, "\u201d").replace(/'/g, "\u2019");
  return text.length > AI_CHART_LABEL_LENGTH ? text.substring(0, AI_CHART_LABEL_LENGTH - 1) + "…" : text;
}

/** The column names of a result, in the order they first appear in the first rows. */
function aiChartColumns(rows) {
  var columns = [];
  var seen = {};
  for (var r = 0; r < rows.length && r < 20; r++) {
    var row = rows[r];
    if (!row || typeof row !== "object") continue;
    var keys = Object.keys(row);
    for (var k = 0; k < keys.length; k++)
      if (!seen[keys[k]]) {
        seen[keys[k]] = true;
        columns.push(keys[k]);
      }
  }
  return columns;
}

/**
 * The real column a name from the model stands for, or null. The model often names a column a little differently from the query
 * ("style" for `style_name`). In order, and only when exactly ONE column fits at that step: the name itself; the same ignoring
 * case; a column that starts or ends with the name after an underscore ("style_name", "beer_style"); a column that starts or ends
 * with the name ignoring case. Two columns fitting is no match: guessing between them would draw the wrong chart.
 */
function aiChartResolve(name, columns) {
  if (typeof name !== "string" || name === "") return null;
  if (columns.indexOf(name) >= 0) return name;
  var lower = name.toLowerCase();
  var steps = [
    function (c) {
      return c.toLowerCase() === lower;
    },
    function (c) {
      var l = c.toLowerCase();
      return l.indexOf(lower + "_") === 0 || (l.length > lower.length + 1 && l.lastIndexOf("_" + lower) === l.length - lower.length - 1);
    },
    function (c) {
      var l = c.toLowerCase();
      return l.length > lower.length && (l.indexOf(lower) === 0 || l.lastIndexOf(lower) === l.length - lower.length);
    }
  ];
  for (var i = 0; i < steps.length; i++) {
    var hits = columns.filter(steps[i]);
    if (hits.length === 1) return hits[0];
    if (hits.length > 1) return null;
  }
  return null;
}

/** A column is numeric when it has at least one value and every non-empty value is a number (or numeric text). */
function aiChartIsNumeric(rows, column) {
  var seen = 0;
  for (var r = 0; r < rows.length && r < 50; r++) {
    var v = rows[r] && typeof rows[r] === "object" ? rows[r][column] : undefined;
    if (v === null || v === undefined || v === "") continue;
    if (isNaN(aiChartNumber(v))) return false;
    seen++;
  }
  return seen > 0;
}

/**
 * The points of a chart from the rows of its query:
 * `{categories: [text], series: [{name, data: [number|null]}], dropped, x, y: [columns], notes: [text]}`.
 * The columns the model named are matched to the real ones (aiChartResolve); an x that matches nothing falls back to the first
 * non-numeric column, a y that matches nothing to the first numeric column not used as x, and each fallback is said in `notes`.
 * A row whose measures are all non-numeric is skipped; with several measures a single non-numeric one is a gap (null), not a
 * zero. At most 100 rows are used. Pie and donut charts take the first measure only and skip negative values.
 */
function aiChartModel(records, spec) {
  var pie = spec.type === "pie" || spec.type === "donut";
  var rows = Array.isArray(records) ? records : [];
  var columns = aiChartColumns(rows);
  var notes = [];

  var x = aiChartResolve(spec.x, columns);
  if (x === null && columns.length > 0) {
    var firstText = columns.filter(function (c) {
      return c.charAt(0) !== "@" && !aiChartIsNumeric(rows, c);
    })[0];
    x = firstText !== undefined ? firstText : null;
    if (x !== null) notes.push("showing " + x + " as the categories (the chart asked for \"" + spec.x + "\")");
  }

  var ys = [];
  spec.y.forEach(function (name) {
    var col = aiChartResolve(name, columns);
    if (col !== null && col !== x && ys.indexOf(col) < 0) ys.push(col);
  });
  if (ys.length === 0) {
    var firstNumber = columns.filter(function (c) {
      return c !== x && aiChartIsNumeric(rows, c);
    })[0];
    if (firstNumber !== undefined) {
      ys.push(firstNumber);
      notes.push("showing " + firstNumber + " as the values (the chart asked for \"" + spec.y.join(", ") + "\")");
    }
  }
  if (pie) ys = ys.slice(0, 1);

  var categories = [];
  var data = ys.map(function () {
    return [];
  });
  var used = 0;
  var dropped = 0;
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
    categories.push(aiChartLabel(x === null ? undefined : row[x]));
    for (var k = 0; k < values.length; k++) data[k].push(isNaN(values[k]) || (pie && values[k] < 0) ? (pie ? 0 : null) : values[k]);
    used++;
  }
  return {
    categories: categories,
    series: ys.map(function (name, index) {
      return { name: name, data: data[index] };
    }),
    dropped: dropped,
    x: x,
    y: ys,
    notes: notes
  };
}

/** The height in pixels of a chart: a horizontal bar chart grows with its categories so every name keeps its row. */
function aiChartHeight(spec, model) {
  return spec.type === "horizontalBar" ? Math.max(240, 60 + model.categories.length * 30) : 320;
}

/** The ECharts option of a chart. Titles, categories and series names reach the chart as text. */
function aiChartOptions(spec, model) {
  var number = function (val) {
    return studioChartNumber(val, 4);
  };
  if (spec.type === "pie" || spec.type === "donut")
    return {
      animation: false,
      tooltip: { trigger: "item" },
      legend: { type: "scroll", bottom: 0 },
      series: [
        {
          name: model.series[0].name,
          type: "pie",
          radius: spec.type === "donut" ? ["40%", "65%"] : "65%",
          center: ["50%", "45%"],
          label: { formatter: "{d}%" },
          data: model.categories.map(function (category, index) {
            return { name: category, value: model.series[0].data[index] };
          })
        }
      ]
    };
  var horizontal = spec.type === "horizontalBar";
  var bars = horizontal || spec.type === "bar";
  // ⚠️ The NUMBER formatter goes on the value axis only: on a horizontal bar chart that is the x axis, and a number formatter
  // on the category axis turns every name into "NaN" (observed with ApexCharts, which flips the axes the same way).
  var categoryAxis = { type: "category", data: model.categories, axisLabel: { rotate: horizontal ? 0 : 45, hideOverlap: true } };
  var valueAxis = { type: "value", axisLabel: { formatter: number } };
  if (horizontal) categoryAxis.inverse = true; // the first category on top, as the rows came
  return {
    animation: false,
    tooltip: { trigger: "axis" },
    legend: { show: model.series.length > 1, type: "scroll", bottom: 0 },
    grid: { left: 16, right: 24, top: 40, bottom: model.series.length > 1 ? 40 : 16 },
    toolbox: { feature: { saveAsImage: {} } },
    xAxis: horizontal ? valueAxis : categoryAxis,
    yAxis: horizontal ? categoryAxis : valueAxis,
    series: model.series.map(function (serie) {
      var out = { name: serie.name, type: bars ? "bar" : "line", data: serie.data.slice() };
      if (!bars) {
        out.smooth = true;
        out.lineStyle = { width: 2 };
      }
      if (spec.type === "area") out.areaStyle = { opacity: 0.3 };
      return out;
    })
  };
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
