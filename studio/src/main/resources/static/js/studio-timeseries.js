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

var tsChartInstance = null;
var tsChartType = "line";
var tsAutoRefreshTimer = null;
var tsCurrentFields = [];
var tsCurrentTags = [];
var tsLastResult = null;
var tsQueryMode = "builder";

function initTimeSeries() {
  tsRestorePanelToggles();
  tsLoadTypes();
}

function tsToggleQueryMode(mode) {
  tsQueryMode = mode;

  if (mode === "builder") {
    $("#tsModeBuilderBtn").css({ background: "var(--bs-primary,#0d6efd)", color: "#fff" });
    $("#tsModePromQLBtn").css({ background: "transparent", color: "#6c757d" });
  } else {
    $("#tsModePromQLBtn").css({ background: "var(--bs-primary,#0d6efd)", color: "#fff" });
    $("#tsModeBuilderBtn").css({ background: "transparent", color: "#6c757d" });
  }

  $(".ts-builder-only").toggle(mode === "builder");
  $("#tsPromQLInputGroup").toggle(mode === "promql");
  $("#tsLatestBtn").toggle(mode === "builder");

  // Clear results when switching modes
  tsLastResult = null;
  $("#tsQueryStats").html("");
  if (tsChartInstance) {
    tsChartInstance.destroy();
    tsChartInstance = null;
  }
  $("#tsChart").empty();
  if ($.fn.DataTable.isDataTable("#tsDataTable")) {
    $("#tsDataTable").DataTable().destroy();
    $("#tsDataTable").empty();
  }
}

function tsLoadTypes() {
  var db = getCurrentDatabase();
  if (!db)
    return;

  var typeSelect = $("#tsType");
  typeSelect.empty().append('<option value="">-- select type --</option>');
  $("#tsTypeCount").html("");

  jQuery.ajax({
    type: "POST",
    url: "api/v1/command/" + db,
    data: JSON.stringify({ language: "sql", command: "SELECT FROM schema:types" }),
    contentType: "application/json",
    beforeSend: function (xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function (data) {
    var records = data.result.records || data.result;
    for (var i = 0; i < records.length; i++) {
      var rec = records[i];
      if (rec.type === "t")
        typeSelect.append('<option value="' + escapeHtml(rec.name) + '">' + escapeHtml(rec.name) + '</option>');
    }
  })
  .fail(function (jqXHR) {
    globalNotify("Error", "Could not load types", "danger");
  });
}

function tsTypeChanged() {
  var db = getCurrentDatabase();
  var typeName = $("#tsType").val();
  var container = $("#tsFieldCheckboxes");
  var tagContainer = $("#tsTagFilters");
  container.empty();
  tagContainer.empty();
  tsCurrentFields = [];
  tsCurrentTags = [];
  $("#tsTypeCount").html("");
  $("#tsTagFiltersGroup").hide();

  if (!typeName) {
    container.html('<span class="text-muted" style="font-size: 0.8rem;">Select a type first</span>');
    return;
  }

  jQuery.ajax({
    type: "POST",
    url: "api/v1/command/" + db,
    data: JSON.stringify({ language: "sql", command: "SELECT FROM schema:types WHERE name = '" + typeName + "'" }),
    contentType: "application/json",
    beforeSend: function (xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function (data) {
    var records = data.result.records || data.result;
    if (records.length === 0)
      return;

    var typeInfo = records[0];
    var count = typeInfo.records || 0;
    $("#tsTypeCount").html("(" + count.toLocaleString() + " samples)");

    var tsColumns = typeInfo.tsColumns;
    if (!tsColumns)
      return;

    for (var i = 0; i < tsColumns.length; i++) {
      var col = tsColumns[i];
      if (col.role === "TAG") {
        tsCurrentTags.push(col.name);
        tagContainer.append(
          '<div class="input-group input-group-sm" style="width: auto; min-width: 130px;">' +
          '<span class="input-group-text" style="font-size:0.75rem; padding: 2px 6px;">' + escapeHtml(col.name) + '</span>' +
          '<input type="text" class="form-control form-control-sm ts-tag-input" data-tag="' + escapeHtml(col.name) + '" ' +
          'placeholder="filter..." style="width: 80px; font-size:0.8rem;">' +
          '</div>'
        );
      } else if (col.role === "FIELD") {
        tsCurrentFields.push(col.name);
        container.append(
          '<div class="form-check form-check-inline">' +
          '<input class="form-check-input ts-field-check" type="checkbox" value="' + escapeHtml(col.name) + '" id="tsField_' + i + '" checked>' +
          '<label class="form-check-label" for="tsField_' + i + '" style="font-size:0.8rem;">' + escapeHtml(col.name) + '</label>' +
          '</div>'
        );
      }
    }

    if (tsCurrentTags.length > 0)
      $("#tsTagFiltersGroup").show();

    if (tsCurrentFields.length === 0)
      container.html('<span class="text-muted" style="font-size: 0.8rem;">No fields found</span>');
  })
  .fail(function (jqXHR) {
    globalNotify("Error", "Could not load type info", "danger");
  });
}

function tsTimeRangeChanged() {
  var val = $("#tsTimeRange").val();
  if (val === "custom") {
    $("#tsCustomFromGroup").show();
    $("#tsCustomToGroup").show();
  } else {
    $("#tsCustomFromGroup").hide();
    $("#tsCustomToGroup").hide();
  }
}

function tsGetTimeRange() {
  var range = $("#tsTimeRange").val();
  var now = Date.now();

  if (range === "all")
    return { from: null, to: null };
  if (range === "custom") {
    var fromVal = $("#tsCustomFrom").val();
    var toVal = $("#tsCustomTo").val();
    return {
      from: fromVal ? new Date(fromVal).getTime() : null,
      to: toVal ? new Date(toVal).getTime() : null
    };
  }

  var ms = 0;
  if (range === "5m") ms = 5 * 60 * 1000;
  else if (range === "1h") ms = 60 * 60 * 1000;
  else if (range === "24h") ms = 24 * 60 * 60 * 1000;
  else if (range === "7d") ms = 7 * 24 * 60 * 60 * 1000;

  return { from: now - ms, to: now };
}

function tsExecuteQuery() {
  if (tsQueryMode === "promql") {
    tsExecutePromQLQuery();
    return;
  }

  var db = getCurrentDatabase();
  var typeName = $("#tsType").val();

  if (!db || !typeName) {
    globalNotify("Warning", "Please select a database and type", "warning");
    return;
  }

  var selectedFields = [];
  $(".ts-field-check:checked").each(function () {
    selectedFields.push($(this).val());
  });

  var timeRange = tsGetTimeRange();
  var aggType = $("#tsAggType").val();

  var request = { type: typeName };

  if (timeRange.from != null)
    request.from = timeRange.from;
  if (timeRange.to != null)
    request.to = timeRange.to;

  if (selectedFields.length > 0 && selectedFields.length < tsCurrentFields.length)
    request.fields = selectedFields;

  // Collect tag filters
  var tags = tsCollectTagFilters();
  if (tags)
    request.tags = tags;

  var bucketInterval = parseInt($("#tsBucketInterval").val());
  var effectiveAgg = aggType || "AVG";

  var fields = selectedFields.length > 0 ? selectedFields : tsCurrentFields;
  var requests = [];
  for (var i = 0; i < fields.length; i++) {
    requests.push({
      field: fields[i],
      type: effectiveAgg,
      alias: aggType ? fields[i] + "_" + effectiveAgg.toLowerCase() : fields[i]
    });
  }
  request.aggregation = {
    bucketInterval: bucketInterval,
    requests: requests
  };

  request.limit = 20000;

  var beginTime = new Date();

  jQuery.ajax({
    type: "POST",
    url: "api/v1/ts/" + db + "/query",
    data: JSON.stringify(request),
    contentType: "application/json",
    beforeSend: function (xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function (data) {
    var elapsed = new Date() - beginTime;
    var count = data.count || 0;
    $("#tsQueryStats").html("Returned <b>" + count + "</b> records in <b>" + elapsed + "</b> ms");

    tsLastResult = data;
    tsRenderResult(data);
  })
  .fail(function (jqXHR) {
    $("#tsQueryStats").html("");
    globalNotify("Error", "Query failed: " + (jqXHR.responseText || "unknown error"), "danger");
  });
}

function tsExecutePromQLQuery() {
  var db = getCurrentDatabase();
  var expr = $("#tsPromQLExpr").val().trim();

  if (!db) {
    globalNotify("Warning", "Please select a database", "warning");
    return;
  }
  if (!expr) {
    globalNotify("Warning", "Please enter a PromQL expression", "warning");
    return;
  }

  var timeRange = tsGetTimeRange();
  var now = Date.now();
  // "all" has no meaning in a range query — default to last 1 hour
  var startSec = timeRange.from != null ? timeRange.from / 1000 : (now - 3600000) / 1000;
  var endSec   = timeRange.to   != null ? timeRange.to   / 1000 : now / 1000;
  var stepSec  = parseInt($("#tsBucketInterval").val()) / 1000;

  var url = "api/v1/ts/" + db + "/prom/api/v1/query_range" +
    "?query=" + encodeURIComponent(expr) +
    "&start=" + startSec +
    "&end="   + endSec +
    "&step="  + stepSec;

  var beginTime = new Date();

  jQuery.ajax({
    type: "GET",
    url: url,
    beforeSend: function (xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function (data) {
    var elapsed = new Date() - beginTime;

    if (data.status === "error") {
      globalNotify("PromQL Error", escapeHtml(data.error || "Query failed"), "danger");
      return;
    }

    var converted = tsConvertPromQLResult(data);
    if (!converted) {
      globalNotify("Warning", "No data returned", "warning");
      return;
    }

    var count = converted.count || 0;
    $("#tsQueryStats").html("Returned <b>" + count + "</b> records in <b>" + elapsed + "</b> ms");

    tsLastResult = converted;
    tsRenderResult(converted);
  })
  .fail(function (jqXHR) {
    $("#tsQueryStats").html("");
    var errMsg = "Query failed";
    try {
      var errData = JSON.parse(jqXHR.responseText);
      if (errData.error) errMsg = errData.error;
    } catch (e) {}
    globalNotify("Error", escapeHtml(errMsg), "danger");
  });
}

function tsConvertPromQLResult(data) {
  if (!data || data.status !== "success" || !data.data)
    return null;

  var resultType = data.data.resultType;
  var result = data.data.result;

  if (resultType === "matrix") {
    if (!result || result.length === 0)
      return { aggregations: [], buckets: [], count: 0 };

    var aggregations = result.map(function (s) { return tsPromQLSeriesLabel(s.metric); });

    // Collect all unique timestamps across all series
    var tsSet = {};
    for (var s = 0; s < result.length; s++)
      for (var v = 0; v < result[s].values.length; v++)
        tsSet[Math.round(result[s].values[v][0] * 1000)] = true;

    var timestamps = Object.keys(tsSet).map(Number).sort(function (a, b) { return a - b; });

    // Build per-series lookup: timestampMs → value
    var seriesIndex = result.map(function (s) {
      var idx = {};
      for (var v = 0; v < s.values.length; v++)
        idx[Math.round(s.values[v][0] * 1000)] = parseFloat(s.values[v][1]);
      return idx;
    });

    var buckets = timestamps.map(function (ts) {
      return {
        timestamp: ts,
        values: seriesIndex.map(function (idx) { return idx.hasOwnProperty(ts) ? idx[ts] : null; })
      };
    });

    return { aggregations: aggregations, buckets: buckets, count: buckets.length * result.length };
  }

  if (resultType === "vector") {
    if (!result || result.length === 0)
      return { columns: ["Timestamp", "Metric", "Value"], rows: [], count: 0 };

    var rows = result.map(function (s) {
      return [Math.round(s.value[0] * 1000), tsPromQLSeriesLabel(s.metric), parseFloat(s.value[1])];
    });
    return { columns: ["Timestamp", "Metric", "Value"], rows: rows, count: rows.length };
  }

  if (resultType === "scalar") {
    var ts = Math.round(data.data.result[0] * 1000);
    var val = parseFloat(data.data.result[1]);
    return { columns: ["Timestamp", "Value"], rows: [[ts, val]], count: 1 };
  }

  return null;
}

function tsPromQLSeriesLabel(metric) {
  if (!metric) return "value";
  var name = metric["__name__"] || "";
  var parts = [];
  for (var k in metric) {
    if (k !== "__name__")
      parts.push(k + "=" + metric[k]);
  }
  if (parts.length === 0) return name || "value";
  return name + "{" + parts.join(", ") + "}";
}

function tsRenderResult(data) {
  if (tsIsPanelEnabled("chart")) {
    if (data.buckets)
      tsRenderAggregatedChart(data);
    else if (data.rows)
      tsRenderRawChart(data);
  }

  if (tsIsPanelEnabled("table"))
    tsRenderTable(data);
}

function tsRenderAggregatedChart(data) {
  var aggNames = data.aggregations;
  var buckets = data.buckets;
  var series = [];

  for (var a = 0; a < aggNames.length; a++) {
    var seriesData = [];
    for (var b = 0; b < buckets.length; b++)
      seriesData.push({ x: buckets[b].timestamp, y: buckets[b].values[a] });

    series.push({ name: aggNames[a], data: seriesData });
  }

  tsDrawChart(series);
}

function tsRenderRawChart(data) {
  var columns = data.columns;
  var rows = data.rows;
  var tsColIdx = 0;

  // Find numeric columns (skip timestamp and string columns)
  var numericCols = [];
  for (var c = 0; c < columns.length; c++) {
    if (c === tsColIdx) continue;
    // Check if first row has a number in this column
    if (rows.length > 0 && typeof rows[0][c] === "number")
      numericCols.push(c);
  }

  var series = [];
  for (var n = 0; n < numericCols.length; n++) {
    var colIdx = numericCols[n];
    var seriesData = [];
    for (var r = 0; r < rows.length; r++)
      seriesData.push({ x: rows[r][tsColIdx], y: rows[r][colIdx] });

    series.push({ name: columns[colIdx], data: seriesData });
  }

  tsDrawChart(series);
}

function tsDrawChart(series) {
  if (tsChartInstance) {
    tsChartInstance.destroy();
    tsChartInstance = null;
  }

  if (series.length === 0) {
    $("#tsChart").html('<div class="text-center text-muted py-5">No numeric data to chart</div>');
    return;
  }

  var options = {
    chart: {
      type: tsChartType,
      height: 350,
      zoom: { enabled: true },
      toolbar: { show: true }
    },
    series: series,
    xaxis: {
      type: "datetime",
      labels: { datetimeUTC: false }
    },
    yaxis: {
      labels: { formatter: function (val) { return val != null ? val.toFixed(2) : ""; } }
    },
    stroke: { curve: "smooth", width: 2 },
    tooltip: { x: { format: "yyyy-MM-dd HH:mm:ss" } },
    theme: {
      mode: document.documentElement.getAttribute("data-theme") === "dark" ? "dark" : "light"
    }
  };

  tsChartInstance = new ApexCharts(document.querySelector("#tsChart"), options);
  tsChartInstance.render();
}

function tsToggleChartType(type) {
  tsChartType = type;
  $("#tsChartLineBtn").toggleClass("active", type === "line");
  $("#tsChartAreaBtn").toggleClass("active", type === "area");

  if (tsLastResult)
    tsRenderResult(tsLastResult);
}

function tsGetLatest() {
  var db = getCurrentDatabase();
  var typeName = $("#tsType").val();

  if (!db || !typeName) {
    globalNotify("Warning", "Please select a database and type", "warning");
    return;
  }

  var url = "api/v1/ts/" + db + "/latest?type=" + encodeURIComponent(typeName);

  // Add first non-empty tag filter as query param (API supports single tag via query string)
  $(".ts-tag-input").each(function () {
    var tagVal = $(this).val().trim();
    if (tagVal && !url.includes("&tag=")) {
      url += "&tag=" + encodeURIComponent($(this).data("tag") + ":" + tagVal);
    }
  });

  jQuery.ajax({
    type: "GET",
    url: url,
    beforeSend: function (xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function (data) {
    if (data.latest == null) {
      globalNotify("Latest", "No data available", "warning");
      return;
    }

    var columns = data.columns;
    var latest = data.latest;
    var msg = "";
    for (var i = 0; i < columns.length; i++) {
      if (i > 0) msg += ", ";
      msg += columns[i] + ": " + latest[i];
    }
    globalNotify("Latest Value", msg, "success");
  })
  .fail(function (jqXHR) {
    globalNotify("Error", "Failed to get latest value", "danger");
  });
}

function tsRenderTable(data) {
  // Destroy existing DataTable
  if ($.fn.DataTable.isDataTable("#tsDataTable"))
    $("#tsDataTable").DataTable().destroy();
  $("#tsDataTable").empty();

  if (data.buckets) {
    // Aggregated data
    var aggNames = data.aggregations;
    var colDefs = [{ title: "Timestamp" }];
    for (var a = 0; a < aggNames.length; a++)
      colDefs.push({ title: aggNames[a] });

    var tableData = [];
    var buckets = data.buckets;
    for (var b = 0; b < buckets.length; b++) {
      var row = [new Date(buckets[b].timestamp).toISOString()];
      for (var v = 0; v < buckets[b].values.length; v++)
        row.push(buckets[b].values[v] != null ? buckets[b].values[v].toFixed(4) : "");
      tableData.push(row);
    }

    $("#tsDataTable").DataTable({
      columns: colDefs,
      data: tableData,
      pageLength: 25,
      order: [[0, "desc"]]
    });
  } else if (data.rows) {
    // Raw data
    var columns = data.columns;
    var colDefs = [];
    for (var c = 0; c < columns.length; c++)
      colDefs.push({ title: columns[c] });

    var tableData = [];
    for (var r = 0; r < data.rows.length; r++) {
      var row = [];
      for (var c = 0; c < data.rows[r].length; c++)
        row.push(data.rows[r][c] != null ? data.rows[r][c] : "");
      tableData.push(row);
    }

    $("#tsDataTable").DataTable({
      columns: colDefs,
      data: tableData,
      pageLength: 25,
      order: [[0, "desc"]]
    });
  }
}

function tsCollectTagFilters() {
  var tags = {};
  var hasTag = false;
  $(".ts-tag-input").each(function () {
    var tagVal = $(this).val().trim();
    if (tagVal) {
      tags[$(this).data("tag")] = tagVal;
      hasTag = true;
    }
  });
  return hasTag ? tags : null;
}

function tsStartAutoRefresh(intervalMs) {
  if (tsAutoRefreshTimer) {
    clearInterval(tsAutoRefreshTimer);
    tsAutoRefreshTimer = null;
  }

  if (intervalMs > 0) {
    tsAutoRefreshTimer = setInterval(function () {
      if (studioCurrentTab === "timeseries")
        tsExecuteQuery();
    }, intervalMs);
    globalNotify("Auto-refresh", "Enabled (" + (intervalMs / 1000) + "s)", "success");
  } else {
    globalNotify("Auto-refresh", "Disabled", "success");
  }
}

function tsDropType() {
  var typeName = $("#tsType").val();
  if (!typeName) {
    globalNotify("Warning", "Please select a TimeSeries type to drop", "warning");
    return;
  }

  var db = getCurrentDatabase();
  if (!db)
    return;

  globalConfirm("Drop TimeSeries", "Are you sure you want to drop TimeSeries type <b>" + escapeHtml(typeName) + "</b>? This will delete all data.", "warning",
    function () {
      jQuery.ajax({
        type: "POST",
        url: "api/v1/command/" + db,
        data: JSON.stringify({ language: "sql", command: "DROP TYPE `" + typeName + "` IF EXISTS" }),
        contentType: "application/json",
        beforeSend: function (xhr) {
          xhr.setRequestHeader("Authorization", globalCredentials);
        }
      })
      .done(function () {
        globalNotify("Success", "TimeSeries type '" + escapeHtml(typeName) + "' dropped", "success");
        tsLoadTypes();
        $("#tsFieldCheckboxes").empty().html('<span class="text-muted" style="font-size: 0.8rem;">Select a type first</span>');
        $("#tsTagFilters").empty();
        $("#tsTagFiltersGroup").hide();
        tsCurrentFields = [];
        tsCurrentTags = [];
      })
      .fail(function (jqXHR) {
        globalNotify("Error", "Failed to drop type: " + (jqXHR.responseText || "unknown error"), "danger");
      });
    }
  );
}

function tsAddDownsamplingPolicy(typeName) {
  var html = "<p style='font-size:0.85rem;'>Add downsampling tiers to <b>" + escapeHtml(typeName) + "</b>. Each tier defines when data older than a threshold gets downsampled to a coarser granularity.</p>";
  html += "<div id='tsDownsamplingTiers'></div>";
  html += "<button type='button' class='btn btn-sm btn-outline-secondary mt-2' onclick='tsAddDownsamplingTierRow()'><i class='fa fa-plus'></i> Add Tier</button>";

  globalPrompt("Add Downsampling Policy", html, "Apply", function () {
    var tiers = [];
    $("#tsDownsamplingTiers .ts-ds-tier-row").each(function () {
      var afterVal = parseInt($(this).find(".ts-ds-after").val());
      var afterUnit = $(this).find(".ts-ds-after-unit").val();
      var granVal = parseInt($(this).find(".ts-ds-gran").val());
      var granUnit = $(this).find(".ts-ds-gran-unit").val();
      if (afterVal > 0 && granVal > 0)
        tiers.push({ after: afterVal, afterUnit: afterUnit, gran: granVal, granUnit: granUnit });
    });

    if (tiers.length === 0) {
      globalNotify("Error", "At least one tier is required", "danger");
      return;
    }

    var command = "ALTER TIMESERIES TYPE `" + typeName + "` ADD DOWNSAMPLING POLICY";
    for (var i = 0; i < tiers.length; i++)
      command += " AFTER " + tiers[i].after + " " + tiers[i].afterUnit + " GRANULARITY " + tiers[i].gran + " " + tiers[i].granUnit;

    var db = getCurrentDatabase();
    jQuery.ajax({
      type: "POST",
      url: "api/v1/command/" + db,
      data: JSON.stringify({ language: "sql", command: command }),
      contentType: "application/json",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      }
    })
    .done(function () {
      globalNotify("Success", "Downsampling policy added", "success");
      tsLoadSchema();
    })
    .fail(function (jqXHR) {
      globalNotify("Error", "Failed: " + (jqXHR.responseText || "unknown error"), "danger");
    });
  });

  setTimeout(function () {
    tsAddDownsamplingTierRow();
  }, 200);
}

function tsAddDownsamplingTierRow() {
  var unitOptions = "<option value='HOURS' selected>Hours</option><option value='DAYS'>Days</option><option value='MINUTES'>Minutes</option>";
  var html = "<div class='d-flex gap-2 mb-1 ts-ds-tier-row align-items-center'>";
  html += "<span style='font-size:0.82rem; white-space:nowrap;'>After</span>";
  html += "<input class='form-control form-control-sm ts-ds-after' type='number' min='1' placeholder='1' style='width:60px;'>";
  html += "<select class='form-select form-select-sm ts-ds-after-unit' style='width:auto;'>" + unitOptions + "</select>";
  html += "<span style='font-size:0.82rem; white-space:nowrap;'>Granularity</span>";
  html += "<input class='form-control form-control-sm ts-ds-gran' type='number' min='1' placeholder='5' style='width:60px;'>";
  html += "<select class='form-select form-select-sm ts-ds-gran-unit' style='width:auto;'>" + unitOptions + "</select>";
  html += "<button type='button' class='btn btn-sm btn-outline-danger' onclick='$(this).closest(\".ts-ds-tier-row\").remove()'><i class='fa fa-times'></i></button>";
  html += "</div>";
  $("#tsDownsamplingTiers").append(html);
}

function tsDropDownsamplingPolicy(typeName) {
  globalConfirm("Drop Downsampling Policy", "Remove all downsampling tiers from <b>" + escapeHtml(typeName) + "</b>?", "warning",
    function () {
      var db = getCurrentDatabase();
      jQuery.ajax({
        type: "POST",
        url: "api/v1/command/" + db,
        data: JSON.stringify({ language: "sql", command: "ALTER TIMESERIES TYPE `" + typeName + "` DROP DOWNSAMPLING POLICY" }),
        contentType: "application/json",
        beforeSend: function (xhr) {
          xhr.setRequestHeader("Authorization", globalCredentials);
        }
      })
      .done(function () {
        globalNotify("Success", "Downsampling policy removed", "success");
        tsLoadSchema();
      })
      .fail(function (jqXHR) {
        globalNotify("Error", "Failed: " + (jqXHR.responseText || "unknown error"), "danger");
      });
    }
  );
}

function tsPanelStorageKey(panel) {
  var db = getCurrentDatabase() || "_default";
  return "arcadedb-ts-" + panel + "-enabled-" + db;
}

function tsIsPanelEnabled(panel) {
  var id = panel === "chart" ? "#tsChartEnabled" : "#tsTableEnabled";
  return $(id).is(":checked");
}

function tsTogglePanel(panel) {
  var enabled = tsIsPanelEnabled(panel);
  globalStorageSave(tsPanelStorageKey(panel), enabled ? "true" : "false");

  if (!enabled) {
    if (panel === "chart") {
      if (tsChartInstance) {
        tsChartInstance.destroy();
        tsChartInstance = null;
      }
      $("#tsChart").empty();
    } else if (panel === "table") {
      if ($.fn.DataTable.isDataTable("#tsDataTable"))
        $("#tsDataTable").DataTable().destroy();
      $("#tsDataTable").empty();
    }
  } else if (tsLastResult) {
    tsRenderResult(tsLastResult);
  }
}

function tsRestorePanelToggles() {
  var db = getCurrentDatabase() || "_default";

  var chartVal = globalStorageLoad("arcadedb-ts-chart-enabled-" + db, "true");
  $("#tsChartEnabled").prop("checked", chartVal !== "false");

  var tableVal = globalStorageLoad("arcadedb-ts-table-enabled-" + db, "true");
  $("#tsTableEnabled").prop("checked", tableVal !== "false");
}

function tsLoadSchema() {
  var db = getCurrentDatabase();
  if (!db) {
    $("#tsSchemaContent").html('<p class="text-muted">Select a database first.</p>');
    return;
  }

  $("#tsSchemaContent").html('<div class="text-center py-3"><div class="spinner-border text-primary" role="status" style="width:1.5rem;height:1.5rem;"></div> <span class="text-muted small ms-2">Loading schema...</span></div>');

  jQuery.ajax({
    type: "POST",
    url: "api/v1/command/" + db,
    data: JSON.stringify({ language: "sql", command: "SELECT FROM schema:types" }),
    contentType: "application/json",
    beforeSend: function (xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function (data) {
    var records = data.result.records || data.result;
    var tsTypes = [];
    for (var i = 0; i < records.length; i++)
      if (records[i].type === "t")
        tsTypes.push(records[i]);

    if (tsTypes.length === 0) {
      $("#tsSchemaContent").html('<p class="text-muted">No TimeSeries types defined in this database.</p>');
      return;
    }

    var html = "";
    for (var t = 0; t < tsTypes.length; t++) {
      if (t > 0)
        html += "<hr style='margin: 24px 0;'>";
      html += tsRenderTypeSchema(tsTypes[t]);
    }
    $("#tsSchemaContent").html(html);
  })
  .fail(function () {
    $("#tsSchemaContent").html('<p class="text-danger">Failed to load schema.</p>');
  });
}

function tsRenderTypeSchema(row) {
  var html = "";

  // Header
  html += "<div class='d-flex align-items-center gap-3 mb-3'>";
  html += "<h4 style='margin:0;'>" + escapeHtml(row.name) + "</h4>";
  html += "<span class='db-type-category-badge' style='background-color:#ec4899;'>TimeSeries</span>";
  html += "<span style='color:#888; font-size:0.9rem;'>" + (row.records || 0).toLocaleString() + " records</span>";
  html += "</div>";

  // TimeSeries Columns
  if (row.tsColumns && row.tsColumns.length > 0) {
    html += "<div class='db-detail-section'>";
    html += "<h6><i class='fa fa-chart-line'></i> TimeSeries Columns</h6>";
    html += "<div class='table-responsive'>";
    html += "<table class='table table-sm db-detail-table'>";
    html += "<thead><tr><th>Name</th><th>Data Type</th><th>Role</th></tr></thead><tbody>";
    for (var c = 0; c < row.tsColumns.length; c++) {
      var col = row.tsColumns[c];
      var roleBadge = col.role === "TIMESTAMP" ? "<span class='badge bg-primary'>TIMESTAMP</span>" :
                      (col.role === "TAG" ? "<span class='badge bg-warning text-dark'>TAG</span>" :
                       "<span class='badge bg-success'>FIELD</span>");
      html += "<tr><td>" + escapeHtml(col.name) + "</td><td>" + escapeHtml(col.dataType || "") + "</td><td>" + roleBadge + "</td></tr>";
    }
    html += "</tbody></table></div>";

    // Configuration summary
    html += "<div class='mt-2' style='font-size:0.85rem; color:#888;'>";
    if (row.shardCount != null)
      html += "<span class='me-3'>Shards: <strong>" + row.shardCount + "</strong></span>";
    if (row.retentionMs != null && row.retentionMs > 0)
      html += "<span class='me-3'>Retention: <strong>" + formatTsDuration(row.retentionMs) + "</strong></span>";
    if (row.compactionBucketIntervalMs != null && row.compactionBucketIntervalMs > 0)
      html += "<span class='me-3'>Compaction Interval: <strong>" + formatTsDuration(row.compactionBucketIntervalMs) + "</strong></span>";
    html += "</div>";
    html += "</div>";
  }

  // Diagnostics
  html += "<div class='db-detail-section'>";
  html += "<h6><i class='fa fa-stethoscope'></i> TimeSeries Diagnostics</h6>";

  html += "<div class='row mb-3'>";
  html += "<div class='col-md-3'><div class='card'><div class='card-body py-2 px-3'><small class='text-muted'>Total Samples</small><div style='font-size:1.2rem;font-weight:600;'>" + (row.records || 0).toLocaleString() + "</div></div></div></div>";
  html += "<div class='col-md-3'><div class='card'><div class='card-body py-2 px-3'><small class='text-muted'>Shards</small><div style='font-size:1.2rem;font-weight:600;'>" + (row.shardCount || 1) + "</div></div></div></div>";
  html += "<div class='col-md-3'><div class='card'><div class='card-body py-2 px-3'><small class='text-muted'>Time Range (min)</small><div style='font-size:1.0rem;font-weight:600;'>" + (row.globalMinTimestamp != null ? formatTsTimestamp(row.globalMinTimestamp) : "\u2014") + "</div></div></div></div>";
  html += "<div class='col-md-3'><div class='card'><div class='card-body py-2 px-3'><small class='text-muted'>Time Range (max)</small><div style='font-size:1.0rem;font-weight:600;'>" + (row.globalMaxTimestamp != null ? formatTsTimestamp(row.globalMaxTimestamp) : "\u2014") + "</div></div></div></div>";
  html += "</div>";

  // Configuration table
  html += "<h6 style='font-size:0.82rem;margin-top:12px;'>Configuration</h6>";
  html += "<div class='table-responsive'>";
  html += "<table class='table table-sm db-detail-table'><tbody>";
  html += "<tr><td style='width:200px;'>Timestamp Column</td><td><code>" + escapeHtml(row.timestampColumn || "ts") + "</code></td></tr>";
  html += "<tr><td>Retention</td><td>" + (row.retentionMs > 0 ? formatTsDuration(row.retentionMs) : "<span class='text-muted'>unlimited</span>") + "</td></tr>";
  html += "<tr><td>Compaction Interval</td><td>" + (row.compactionBucketIntervalMs > 0 ? formatTsDuration(row.compactionBucketIntervalMs) : "<span class='text-muted'>none</span>") + "</td></tr>";
  var hasAutoMaintenance = (row.retentionMs > 0) || (row.downsamplingTiers && row.downsamplingTiers.length > 0);
  html += "<tr><td>Auto Maintenance</td><td>" + (hasAutoMaintenance ? "<span class='badge bg-success'>Active</span> <small class='text-muted'>scheduler runs every 60s</small>" : "<span class='text-muted'>inactive (no retention or downsampling configured)</span>") + "</td></tr>";
  html += "</tbody></table></div>";

  // Downsampling tiers
  html += "<h6 style='font-size:0.82rem;margin-top:12px;'>Downsampling Tiers</h6>";
  if (row.downsamplingTiers && row.downsamplingTiers.length > 0) {
    html += "<div class='table-responsive'>";
    html += "<table class='table table-sm db-detail-table'>";
    html += "<thead><tr><th>After</th><th>Granularity</th></tr></thead><tbody>";
    for (var d = 0; d < row.downsamplingTiers.length; d++) {
      var tier = row.downsamplingTiers[d];
      html += "<tr><td>" + formatTsDuration(tier.afterMs) + "</td><td>" + formatTsDuration(tier.granularityMs) + "</td></tr>";
    }
    html += "</tbody></table></div>";
    html += "<button class='btn btn-sm btn-outline-danger mt-1' onclick='tsDropDownsamplingPolicy(\"" + escapeHtml(row.name) + "\")'><i class='fa fa-trash'></i> Drop Policy</button> ";
  } else {
    html += "<p class='text-muted' style='font-size:0.82rem;'>No downsampling policy configured.</p>";
  }
  html += "<button class='btn btn-sm btn-outline-primary mt-1' onclick='tsAddDownsamplingPolicy(\"" + escapeHtml(row.name) + "\")'><i class='fa fa-plus'></i> Add Downsampling Policy</button>";

  // Per-shard stats
  if (row.shards && row.shards.length > 0) {
    html += "<h6 style='font-size:0.82rem;margin-top:12px;'>Shard Details</h6>";
    html += "<div class='table-responsive'>";
    html += "<table class='table table-sm db-detail-table'>";
    html += "<thead><tr><th>Shard</th><th>Sealed Blocks</th><th>Sealed Samples</th><th>Mutable Samples</th><th>Total Samples</th><th>Min Timestamp</th><th>Max Timestamp</th></tr></thead><tbody>";
    for (var s = 0; s < row.shards.length; s++) {
      var sh = row.shards[s];
      if (sh.error) {
        html += "<tr><td>" + sh.shard + "</td><td colspan='6' class='text-danger'>" + escapeHtml(sh.error) + "</td></tr>";
      } else {
        html += "<tr>";
        html += "<td>" + sh.shard + "</td>";
        html += "<td>" + (sh.sealedBlocks || 0).toLocaleString() + "</td>";
        html += "<td>" + (sh.sealedSamples || 0).toLocaleString() + "</td>";
        html += "<td>" + (sh.mutableSamples || 0).toLocaleString() + "</td>";
        html += "<td><strong>" + (sh.totalSamples || 0).toLocaleString() + "</strong></td>";
        html += "<td>" + (sh.minTimestamp != null ? formatTsTimestamp(sh.minTimestamp) : (sh.mutableMinTimestamp != null ? formatTsTimestamp(sh.mutableMinTimestamp) : "\u2014")) + "</td>";
        html += "<td>" + (sh.maxTimestamp != null ? formatTsTimestamp(sh.maxTimestamp) : (sh.mutableMaxTimestamp != null ? formatTsTimestamp(sh.mutableMaxTimestamp) : "\u2014")) + "</td>";
        html += "</tr>";
      }
    }
    html += "</tbody></table></div>";
  }

  html += "</div>";
  return html;
}
