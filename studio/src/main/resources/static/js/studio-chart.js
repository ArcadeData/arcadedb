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

// The Query panel's Chart tab, and the PromQL language that feeds it.
//
// The Chart tab charts the rows of ANY result, whatever language produced them (SQL, Cypher, Gremlin, PromQL...), so the
// role of each column is inferred from its values: a time column (epoch milliseconds or an ISO date string) goes on the
// x axis, number columns are the measures, a text column can split a measure into one series per value. Every choice can
// be overridden in the tab.
//
// The inference, the series assembly and the PromQL conversion are pure functions of plain values, so
// studio/test/query-chart.test.js can drive them without a DOM. The jQuery handlers only collect the inputs.

var chartInstance = null;
var chartType = "line";
var chartSpec = null;
var chartSignature = "";

// An epoch in milliseconds between 1973 and 2286: a plain integer id or count is far below it, a seconds epoch far below.
var CHART_MIN_EPOCH_MS = 1e11;
var CHART_MAX_EPOCH_MS = 1e13;
var CHART_ISO_DATE = /^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?)?$/;

// ===== Pure functions =====

/** True for the language dropdown value that runs a PromQL expression. */
function chartIsPromQL(language) {
  return language === "promql";
}

/** The milliseconds since the epoch a time cell stands for, or NaN. */
function chartToMillis(value) {
  if (typeof value === "number") return value;
  if (typeof value === "string") return Date.parse(value.indexOf("T") < 0 ? value.replace(" ", "T") : value);
  return NaN;
}

function chartCellKind(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return value >= CHART_MIN_EPOCH_MS && value <= CHART_MAX_EPOCH_MS ? "time" : "number";
  if (typeof value === "string") return CHART_ISO_DATE.test(value) && !isNaN(chartToMillis(value)) ? "time" : "text";
  if (typeof value === "boolean") return "text";
  return "other";
}

/**
 * The charted columns of a result and what each holds, in first-appearance order: "time", "number", "text" or "other"
 * (nested values). Record metadata (`@rid`, `@type`...) and columns with no value at all are left out. A column whose
 * values disagree is "text", except that a mix of time and number cells is a number column.
 */
function chartColumns(records) {
  var order = [];
  var kinds = {};
  var sample = Math.min((records || []).length, 200);
  for (var i = 0; i < sample; i++) {
    var row = records[i];
    for (var name in row) {
      if (!Object.prototype.hasOwnProperty.call(row, name) || name.charAt(0) === "@") continue;
      var kind = chartCellKind(row[name]);
      if (kind === null) continue;
      if (!Object.prototype.hasOwnProperty.call(kinds, name)) {
        order.push(name);
        kinds[name] = kind;
      } else if (kinds[name] !== kind) {
        var pair = [kinds[name], kind].sort().join("+");
        kinds[name] = pair === "number+time" ? "number" : "text";
      }
    }
  }
  return order.map(function (name) {
    return { name: name, kind: kinds[name] };
  });
}

// A time chart splits by a text column only when it has few distinct values: a host or a metric label, not an id or a name.
var CHART_MAX_SPLIT_VALUES = 12;

/**
 * The spec a result is charted with until the user changes it: first time (else text) column on x, other numbers as
 * measures, and, on a time chart, the first text column with a handful of distinct values as the series split.
 */
function chartDefaultSpec(columns, records) {
  var x = null;
  for (var i = 0; i < columns.length && x === null; i++) if (columns[i].kind === "time") x = columns[i];
  for (var j = 0; j < columns.length && x === null; j++) if (columns[j].kind === "text") x = columns[j];
  var ys = [];
  for (var k = 0; k < columns.length; k++)
    if (columns[k].kind === "number" && (x === null || columns[k].name !== x.name)) ys.push(columns[k].name);
  var split = "";
  if (x !== null && x.kind === "time" && records)
    for (var c = 0; c < columns.length && split === ""; c++)
      if (columns[c].kind === "text" && chartDistinctAtMost(records, columns[c].name, CHART_MAX_SPLIT_VALUES)) split = columns[c].name;
  return { x: x === null ? "" : x.name, xKind: x === null ? "" : x.kind, split: split, ys: ys };
}

/** True when the column holds at least two and at most `max` distinct values. */
function chartDistinctAtMost(records, name, max) {
  var seen = {};
  var count = 0;
  for (var i = 0; i < records.length; i++) {
    var key = String(records[i][name]);
    if (Object.prototype.hasOwnProperty.call(seen, key)) continue;
    seen[key] = true;
    if (++count > max) return false;
  }
  return count >= 2;
}

/**
 * The series a spec describes: `{ xType: "datetime" | "category", series: [{ name, data: [{ x, y }] }] }`. A time x axis is
 * converted to milliseconds and sorted; a category axis keeps the row order; with no x column the rows are numbered. A
 * measure cell that is not a number is a gap (null), not a zero.
 */
function chartBuildSeries(records, spec) {
  var timed = spec.x !== "" && spec.xKind === "time";
  var groups = [];
  var groupIndex = {};
  var rows = records || [];
  for (var r = 0; r < rows.length; r++) {
    var row = rows[r];
    var key = spec.split === "" ? "" : String(row[spec.split]);
    if (!Object.prototype.hasOwnProperty.call(groupIndex, key)) {
      groupIndex[key] = groups.length;
      groups.push({ key: key, rows: [] });
    }
    groups[groupIndex[key]].rows.push({ row: row, index: r });
  }

  var series = [];
  for (var g = 0; g < groups.length; g++) {
    for (var y = 0; y < spec.ys.length; y++) {
      var measure = spec.ys[y];
      var name = spec.split === "" ? measure : spec.ys.length === 1 ? groups[g].key : groups[g].key + " - " + measure;
      var data = [];
      for (var p = 0; p < groups[g].rows.length; p++) {
        var item = groups[g].rows[p];
        var value = item.row[measure];
        var x = spec.x === "" ? String(item.index + 1) : timed ? chartToMillis(item.row[spec.x]) : String(item.row[spec.x]);
        if (timed && isNaN(x)) continue;
        data.push({ x: x, y: typeof value === "number" ? value : null });
      }
      if (timed)
        data.sort(function (a, b) {
          return a.x - b.x;
        });
      series.push({ name: name, data: data });
    }
  }
  return { xType: timed ? "datetime" : "category", series: series };
}

/** The label of a PromQL series: `name{label=value, ...}`, or `value` when it has neither. */
function chartPromLabel(metric) {
  if (!metric) return "value";
  var parts = [];
  for (var key in metric) if (key !== "__name__") parts.push(key + "=" + metric[key]);
  var name = metric["__name__"] || "";
  if (parts.length === 0) return name || "value";
  return name + "{" + parts.join(", ") + "}";
}

/**
 * A Prometheus-style response as rows `{ timestamp (ms), metric, value }`, one per point, so it can be charted (and shown
 * in the table) like any other result. Returns null for an error or a result type with no time axis.
 */
function chartRecordsFromPromQL(response) {
  if (!response || response.status !== "success" || !response.data) return null;
  var type = response.data.resultType;
  var result = response.data.result;
  var rows = [];
  if (type === "matrix") {
    for (var s = 0; s < (result || []).length; s++) {
      var label = chartPromLabel(result[s].metric);
      for (var v = 0; v < result[s].values.length; v++)
        rows.push({ timestamp: Math.round(result[s].values[v][0] * 1000), metric: label, value: parseFloat(result[s].values[v][1]) });
    }
    return rows;
  }
  if (type === "vector") {
    for (var i = 0; i < (result || []).length; i++)
      rows.push({ timestamp: Math.round(result[i].value[0] * 1000), metric: chartPromLabel(result[i].metric), value: parseFloat(result[i].value[1]) });
    return rows;
  }
  if (type === "scalar") return [{ timestamp: Math.round(result[0] * 1000), metric: "value", value: parseFloat(result[1]) }];
  return null;
}

// ===== DOM handlers =====

/** Shows the PromQL range and step controls instead of Auto Limit while the PromQL language is selected. */
function chartLanguageChanged() {
  var promql = chartIsPromQL($("#inputLanguage").val());
  $("#promqlControls").toggle(promql);
  if (promql) $("#inputLimit").closest("label").hide();
  else if (typeof vecCurrentMode === "function" && vecCurrentMode() == null) $("#inputLimit").closest("label").show();
}

/** Runs the editor's PromQL expression over the selected range and shows the points as rows (Table, Json) and as a chart. */
function executePromQLCommand() {
  var database = getCurrentDatabase();
  var expr = editor.getValue().trim();
  if (!database || expr === "") return;

  var now = Date.now();
  var rangeMs = parseInt($("#promqlRange").val());
  var stepSec = parseInt($("#promqlStep").val()) / 1000;
  var url =
    "api/v1/ts/" + encodeDatabaseName(database) + "/prom/api/v1/query_range" +
    "?query=" + encodeURIComponent(expr) +
    "&start=" + (now - rangeMs) / 1000 +
    "&end=" + now / 1000 +
    "&step=" + stepSec;

  var beginTime = new Date();
  $("#executeSpinner").show();
  jQuery
    .ajax({
      type: "GET",
      url: url,
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      }
    })
    .done(function (data) {
      if (data.status === "error") {
        globalNotify("PromQL Error", escapeHtml(data.error || "Query failed"), "danger");
        return;
      }
      var records = chartRecordsFromPromQL(data);
      if (records === null) {
        globalNotify("Warning", "The result has no time axis to show", "warning");
        return;
      }
      $("#result-elapsed").html(new Date() - beginTime);
      renderResultCount({}, records.length);
      $("#resultJson").val(JSON.stringify(data, null, 2));
      $("#resultExplain").val("PromQL range query\nstart: " + new Date(now - rangeMs).toISOString() + "\nend: " + new Date(now).toISOString() + "\nstep: " + stepSec + " s");
      globalExplainPlan = null;
      renderFlameGraph(null, null);
      globalResultset = { records: records, vertices: [], edges: [] };
      globalCy = null;
      renderTable();
      globalActivateTab("tab-chart");
      renderQueryChart();
    })
    .fail(function (jqXHR) {
      var message = "Query failed";
      try {
        var json = JSON.parse(jqXHR.responseText);
        if (json.error) message = json.error;
      } catch (e) {
        // not JSON: keep the generic message
      }
      globalNotify("Error", escapeHtml(message), "danger");
    })
    .always(function () {
      $("#executeSpinner").hide();
    });
}

/** Called after a result has been stored in globalResultset: refreshes the chart when its tab is the one showing. */
function queryChartResultChanged() {
  if ($("#tabs-command .active").attr("id") == "tab-chart-sel") renderQueryChart();
}

function chartSpecFromControls(columns) {
  var kinds = {};
  for (var i = 0; i < columns.length; i++) kinds[columns[i].name] = columns[i].kind;
  var x = $("#chartX").val() || "";
  var ys = [];
  $("#chartYs input:checked").each(function () {
    ys.push($(this).val());
  });
  return { x: x, xKind: x === "" ? "" : kinds[x], split: $("#chartSplit").val() || "", ys: ys };
}

function chartFillControls(columns, spec) {
  var option = function (value, label, selected) {
    return "<option value='" + escapeHtml(value) + "'" + (selected ? " selected" : "") + ">" + escapeHtml(label) + "</option>";
  };
  var xHtml = option("", "(row number)", spec.x === "");
  var splitHtml = option("", "(none)", spec.split === "");
  var ysHtml = "";
  for (var i = 0; i < columns.length; i++) {
    var c = columns[i];
    if (c.kind !== "other") xHtml += option(c.name, c.name, spec.x === c.name);
    if (c.kind === "text" || c.kind === "time") splitHtml += option(c.name, c.name, spec.split === c.name);
    if (c.kind === "number")
      ysHtml +=
        "<div class='form-check form-check-inline'><input class='form-check-input' type='checkbox' id='chartY" + i + "' value='" + escapeHtml(c.name) + "'" +
        (spec.ys.indexOf(c.name) >= 0 ? " checked" : "") + "><label class='form-check-label' for='chartY" + i + "'>" + escapeHtml(c.name) + "</label></div>";
  }
  $("#chartX").html(xHtml);
  $("#chartSplit").html(splitHtml);
  $("#chartYs").html(ysHtml || "<span class='text-muted' style='font-size: 0.85rem;'>no numeric column</span>");
}

/** Charts globalResultset. The column roles are re-inferred only when the result's columns change, so a re-run keeps the user's picks. */
function renderQueryChart() {
  var records = globalResultset == null ? [] : globalResultset.records || [];
  var columns = chartColumns(records);
  var signature = columns
    .map(function (c) {
      return c.name + ":" + c.kind;
    })
    .join("|");
  if (chartSpec === null || signature !== chartSignature) {
    chartSpec = chartDefaultSpec(columns, records);
    chartSignature = signature;
  }
  chartFillControls(columns, chartSpec);

  if (chartInstance) {
    chartInstance.destroy();
    chartInstance = null;
  }
  var model = chartBuildSeries(records, chartSpec);
  var hasPoints = model.series.some(function (s) {
    return s.data.length > 0;
  });
  $("#queryChartEmpty").toggle(!hasPoints);
  $("#queryChart").toggle(hasPoints);
  if (!hasPoints) {
    $("#queryChartEmpty").text(
      globalResultset == null ? "Run a query to chart its result." : records.length === 0 ? "The result has no rows to chart." : "Nothing to chart: the result needs at least one numeric column."
    );
    return;
  }

  var options = {
    chart: { type: chartType, height: 380, zoom: { enabled: model.xType === "datetime" }, toolbar: { show: true } },
    series: model.series,
    xaxis: model.xType === "datetime" ? { type: "datetime", labels: { datetimeUTC: false } } : { type: "category", labels: { rotate: -45, trim: true } },
    yaxis: { labels: { formatter: function (val) { return val != null ? Number(val.toFixed(4)) : ""; } } },
    stroke: { curve: "smooth", width: chartType === "bar" ? 0 : 2 },
    dataLabels: { enabled: false },
    tooltip: { x: { format: "yyyy-MM-dd HH:mm:ss" } },
    theme: { mode: document.documentElement.getAttribute("data-theme") === "dark" ? "dark" : "light" }
  };
  chartInstance = new ApexCharts(document.querySelector("#queryChart"), options);
  chartInstance.render();
}

function chartSetType(type) {
  chartType = type;
  $("#chartTypeBtns button").removeClass("active");
  $("#chartTypeBtns button[data-chart-type='" + type + "']").addClass("active");
  renderQueryChart();
}

$(document).on("change", "#chartX, #chartSplit, #chartYs input", function () {
  chartSpec = chartSpecFromControls(chartColumns(globalResultset == null ? [] : globalResultset.records || []));
  renderQueryChart();
});
