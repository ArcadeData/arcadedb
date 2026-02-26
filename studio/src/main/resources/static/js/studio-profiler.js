var profilerData = null;
var profilerTimer = null;
var profilerStartTime = null;
var profilerTimeoutMs = 60000;
var profilerQueryDT = null;

function initProfiler() {
  profilerRefresh();
  profilerLoadSavedRuns();
}

function profilerStart() {
  var timeout = jQuery("#profilerTimeout").val() || "60";
  jQuery.ajax({
    type: "POST",
    url: "api/v1/server",
    data: JSON.stringify({ command: "profiler start " + timeout }),
    headers: { Authorization: globalCredentials },
    contentType: "application/json",
    success: function() {
      profilerStartTime = Date.now();
      profilerTimeoutMs = parseInt(timeout) * 1000;
      profilerSetRecordingUI(true);
      globalNotify("Profiler", "Recording started (auto-stop in " + timeout + "s)", "success");
    },
    error: function(jqXHR) { globalNotifyError(jqXHR.responseText); }
  });
}

function profilerStop() {
  jQuery.ajax({
    type: "POST",
    url: "api/v1/server",
    data: JSON.stringify({ command: "profiler stop" }),
    headers: { Authorization: globalCredentials },
    contentType: "application/json",
    success: function(data) {
      profilerSetRecordingUI(false);
      profilerData = typeof data === "string" ? JSON.parse(data) : data;
      profilerRenderResults();
      profilerLoadSavedRuns();
      globalNotify("Profiler", "Recording stopped — " + (profilerData.totalQueries || 0) + " queries captured", "success");
    },
    error: function(jqXHR) { globalNotifyError(jqXHR.responseText); }
  });
}

function profilerReset() {
  jQuery.ajax({
    type: "POST",
    url: "api/v1/server",
    data: JSON.stringify({ command: "profiler reset" }),
    headers: { Authorization: globalCredentials },
    contentType: "application/json",
    success: function() {
      profilerData = null;
      profilerSetRecordingUI(false);
      profilerClearUI();
      globalNotify("Profiler", "Profiler reset", "success");
    },
    error: function(jqXHR) { globalNotifyError(jqXHR.responseText); }
  });
}

function profilerRefresh() {
  jQuery.ajax({
    type: "POST",
    url: "api/v1/server",
    data: JSON.stringify({ command: "profiler results" }),
    headers: { Authorization: globalCredentials },
    contentType: "application/json",
    success: function(data) {
      profilerData = typeof data === "string" ? JSON.parse(data) : data;
      if (profilerData && profilerData.recording) {
        profilerStartTime = profilerData.startTime;
        profilerTimeoutMs = (profilerData.timeoutSeconds || 60) * 1000;
        profilerSetRecordingUI(true);
      } else if (profilerData && !profilerData.recording && profilerData.totalQueries > 0) {
        profilerSetRecordingUI(false);
      }
      if (profilerData && profilerData.totalQueries > 0)
        profilerRenderResults();
    },
    error: function() { /* silent on refresh */ }
  });
}

function profilerLoadSavedRuns() {
  jQuery.ajax({
    type: "POST",
    url: "api/v1/server",
    data: JSON.stringify({ command: "profiler list" }),
    headers: { Authorization: globalCredentials },
    contentType: "application/json",
    success: function(data) {
      var parsed = typeof data === "string" ? JSON.parse(data) : data;
      var files = parsed.result || [];
      var menu = jQuery("#profilerSavedRunsMenu");
      menu.empty();
      if (files.length === 0) {
        menu.append('<li><span class="dropdown-item text-muted">No saved runs</span></li>');
        return;
      }
      for (var i = 0; i < files.length; i++) {
        var f = files[i];
        var sizeKb = Math.round(f.size / 1024);
        var dateStr = new Date(f.lastModified).toLocaleString();
        menu.append('<li><a class="dropdown-item" style="cursor:pointer;" onclick="profilerLoadRun(\'' +
          f.fileName.replace(/'/g, "\\'") + '\')">' + escapeHtml(f.fileName) +
          ' <small class="text-muted">(' + sizeKb + ' KB, ' + dateStr + ')</small></a></li>');
      }
    },
    error: function() { /* silent */ }
  });
}

function profilerLoadRun(fileName) {
  jQuery.ajax({
    type: "POST",
    url: "api/v1/server",
    data: JSON.stringify({ command: "profiler load " + fileName }),
    headers: { Authorization: globalCredentials },
    contentType: "application/json",
    success: function(data) {
      profilerData = typeof data === "string" ? JSON.parse(data) : data;
      profilerRenderResults();
      globalNotify("Profiler", "Loaded: " + fileName, "success");
    },
    error: function(jqXHR) { globalNotifyError(jqXHR.responseText); }
  });
}

function profilerSetRecordingUI(recording) {
  if (recording) {
    jQuery("#profilerStartGroup").find("button").hide();
    jQuery("#profilerTimeout").prop("disabled", true);
    jQuery("#profilerStopBtn").show();
    jQuery("#profilerRecordingBadge").show();
    if (!profilerTimer) {
      profilerTimer = setInterval(function() {
        var elapsedMs = Date.now() - (profilerStartTime || Date.now());
        var remainMs = profilerTimeoutMs - elapsedMs;
        if (remainMs <= 0) {
          // Server has auto-stopped, refresh results
          profilerSetRecordingUI(false);
          profilerRefresh();
          globalNotify("Profiler", "Recording auto-stopped (timeout reached)", "info");
          return;
        }
        var remainSec = Math.ceil(remainMs / 1000);
        var min = Math.floor(remainSec / 60);
        var sec = remainSec % 60;
        jQuery("#profilerElapsed").text((min > 0 ? min + "m " : "") + sec + "s left");
      }, 1000);
    }
  } else {
    jQuery("#profilerStartGroup").find("button").show();
    jQuery("#profilerTimeout").prop("disabled", false);
    jQuery("#profilerStopBtn").hide();
    jQuery("#profilerRecordingBadge").hide();
    if (profilerTimer) {
      clearInterval(profilerTimer);
      profilerTimer = null;
    }
  }
}

function profilerClearUI() {
  jQuery("#profilerSummary").hide();
  jQuery("#profilerQueryListContainer").hide();
  jQuery("#profilerQueryDetail").hide();
  jQuery("#profilerEmpty").show();
  if (profilerQueryDT) {
    profilerQueryDT.destroy();
    profilerQueryDT = null;
  }
}

function profilerRenderResults() {
  if (!profilerData || profilerData.totalQueries === 0) {
    profilerClearUI();
    return;
  }

  jQuery("#profilerEmpty").hide();
  jQuery("#profilerQueryDetail").hide();

  // Summary
  jQuery("#profilerDuration").text("Duration: " + (profilerData.durationMs / 1000).toFixed(1) + "s");
  jQuery("#profilerTotalQueries").text(profilerData.totalQueries + " queries");
  profilerRenderDeltaCards();
  profilerRenderDbBreakdown();
  jQuery("#profilerSummary").show();

  // Query table
  profilerRenderQueryTable();
  jQuery("#profilerQueryListContainer").show();
}

function profilerRenderDeltaCards() {
  var container = jQuery("#profilerDeltaCards");
  container.empty();

  var summary = profilerData.summary;
  if (!summary || !summary.snapshotStart || !summary.snapshotStop)
    return;

  var startP = summary.snapshotStart.profiler || {};
  var stopP = summary.snapshotStop.profiler || {};

  var cards = [
    { label: "Heap Available", key: "ramHeapAvailablePerc", field: "perc", unit: "%", invert: true },
    { label: "CPU Load", key: "cpuLoad", field: "perc", unit: "%", invert: false },
    { label: "Queries", key: "queries", field: "count", unit: "", invert: false },
    { label: "Write Tx", key: "writeTx", field: "count", unit: "", invert: false },
    { label: "Records Created", key: "createRecord", field: "count", unit: "", invert: false },
    { label: "Records Updated", key: "updateRecord", field: "count", unit: "", invert: false },
    { label: "Pages Read", key: "pagesRead", field: "count", unit: "", invert: false },
    { label: "Cache Hits", key: "pageCacheHits", field: "count", unit: "", invert: false },
    { label: "GC Time", key: "gcTime", field: "count", unit: " ms", invert: false }
  ];

  for (var i = 0; i < cards.length; i++) {
    var c = cards[i];
    var startVal = (startP[c.key] && startP[c.key][c.field] !== undefined) ? startP[c.key][c.field] : null;
    var stopVal = (stopP[c.key] && stopP[c.key][c.field] !== undefined) ? stopP[c.key][c.field] : null;
    if (startVal === null || stopVal === null)
      continue;

    var delta = stopVal - startVal;
    var deltaStr = (delta >= 0 ? "+" : "") + profilerFormatNum(delta) + c.unit;
    var deltaClass = "";
    if (Math.abs(delta) > 0.01) {
      if (c.invert)
        deltaClass = delta < 0 ? "text-danger" : "text-success";
      else
        deltaClass = delta > 0 ? "text-warning" : "text-success";
    }

    container.append(
      '<div class="col-auto">' +
      '  <div class="card" style="background: var(--bg-card); border: 1px solid var(--border-ddd); min-width: 120px;">' +
      '    <div class="card-body p-2 text-center">' +
      '      <div style="font-size: 0.72rem; color: var(--text-lightest); text-transform: uppercase;">' + c.label + '</div>' +
      '      <div style="font-size: 1rem; font-weight: 600; color: var(--text-primary);">' + profilerFormatNum(stopVal) + c.unit + '</div>' +
      '      <div class="' + deltaClass + '" style="font-size: 0.78rem;">' + deltaStr + '</div>' +
      '    </div>' +
      '  </div>' +
      '</div>'
    );
  }
}

function profilerRenderDbBreakdown() {
  var summary = profilerData.summary;
  if (!summary || !summary.snapshotStart || !summary.snapshotStop)
    return;

  var startDbs = summary.snapshotStart.databases || {};
  var stopDbs = summary.snapshotStop.databases || {};
  var dbNames = Object.keys(stopDbs);
  if (dbNames.length === 0) {
    jQuery("#profilerDbBreakdown").hide();
    return;
  }

  var html = '<table class="table table-sm" style="font-size: 0.82rem;"><thead><tr><th>Database</th><th>Queries</th><th>Write Tx</th><th>Read Tx</th><th>Created</th><th>Updated</th><th>Deleted</th></tr></thead><tbody>';
  var fields = ["queries", "writeTx", "readTx", "createRecord", "updateRecord", "deleteRecord"];
  for (var i = 0; i < dbNames.length; i++) {
    var name = dbNames[i];
    html += '<tr><td><strong>' + escapeHtml(name) + '</strong></td>';
    for (var j = 0; j < fields.length; j++) {
      var startVal = (startDbs[name] && startDbs[name][fields[j]]) || 0;
      var stopVal = (stopDbs[name] && stopDbs[name][fields[j]]) || 0;
      var delta = stopVal - startVal;
      html += '<td>' + stopVal + ' <small class="text-muted">(+' + delta + ')</small></td>';
    }
    html += '</tr>';
  }
  html += '</tbody></table>';

  jQuery("#profilerDbTable").html(html);
  jQuery("#profilerDbBreakdown").show();
}

function profilerRenderQueryTable() {
  if (profilerQueryDT) {
    profilerQueryDT.destroy();
    profilerQueryDT = null;
  }
  jQuery("#profilerQueryTable tbody").empty();

  var queries = profilerData.queries || [];
  for (var i = 0; i < queries.length; i++) {
    var q = queries[i];
    var truncated = q.queryText.length > 80 ? q.queryText.substring(0, 80) + "..." : q.queryText;
    jQuery("#profilerQueryTable tbody").append(
      '<tr style="cursor: pointer;" onclick="profilerShowDetail(' + i + ')">' +
      '<td title="' + escapeHtml(q.queryText) + '">' + escapeHtml(truncated) + '</td>' +
      '<td>' + escapeHtml(q.language) + '</td>' +
      '<td>' + escapeHtml(q.database) + '</td>' +
      '<td>' + q.executionCount + '</td>' +
      '<td>' + q.totalTimeMs + '</td>' +
      '<td>' + q.avgTimeMs + '</td>' +
      '<td>' + q.maxTimeMs + '</td>' +
      '<td>' + q.p99TimeMs + '</td>' +
      '</tr>'
    );
  }

  profilerQueryDT = jQuery("#profilerQueryTable").DataTable({
    paging: true,
    pageLength: 25,
    searching: true,
    ordering: true,
    order: [[4, "desc"]],
    columnDefs: [
      { targets: [3, 4, 5, 6, 7], type: "num" }
    ],
    responsive: true,
    dom: '<"d-flex justify-content-between"lf>rt<"d-flex justify-content-between"ip>'
  });
}

function profilerShowDetail(index) {
  var q = profilerData.queries[index];
  if (!q) return;

  jQuery("#profilerQueryListContainer").hide();
  jQuery("#profilerSummary").hide();

  jQuery("#profilerDetailLang").text(q.language.toUpperCase());
  jQuery("#profilerDetailDb").text(q.database);
  jQuery("#profilerDetailQuery").text(q.queryText);

  // Stats cards
  var statsHtml = "";
  var stats = [
    { label: "Executions", value: q.executionCount },
    { label: "Total", value: q.totalTimeMs + " ms" },
    { label: "Avg", value: q.avgTimeMs + " ms" },
    { label: "Min", value: q.minTimeMs + " ms" },
    { label: "Max", value: q.maxTimeMs + " ms" },
    { label: "P99", value: q.p99TimeMs + " ms" }
  ];
  for (var i = 0; i < stats.length; i++) {
    statsHtml += '<div class="col"><div class="text-center"><div style="font-size:0.72rem;color:var(--text-lightest);text-transform:uppercase;">' +
      stats[i].label + '</div><div style="font-size:1.1rem;font-weight:600;color:var(--text-primary);">' +
      stats[i].value + '</div></div></div>';
  }
  jQuery("#profilerDetailStats").html(statsHtml);

  // Steps
  var steps = q.steps || [];
  jQuery("#profilerStepTable tbody").empty();
  for (var j = 0; j < steps.length; j++) {
    var s = steps[j];
    jQuery("#profilerStepTable tbody").append(
      '<tr><td>' + escapeHtml(s.name) + '</td>' +
      '<td>' + s.executionCount + '</td>' +
      '<td>' + s.totalCostMs + '</td>' +
      '<td>' + s.minCostMs + '</td>' +
      '<td>' + s.avgCostMs + '</td>' +
      '<td>' + s.maxCostMs + '</td>' +
      '<td>' + s.p99CostMs + '</td></tr>'
    );
  }

  // Step chart (horizontal bar)
  jQuery("#profilerStepChart").empty();
  if (steps.length > 0 && typeof ApexCharts !== "undefined") {
    var categories = [];
    var avgData = [];
    var maxData = [];
    for (var k = 0; k < steps.length; k++) {
      categories.push(steps[k].name);
      avgData.push(steps[k].avgCostMs);
      maxData.push(steps[k].maxCostMs);
    }
    var chart = new ApexCharts(document.querySelector("#profilerStepChart"), {
      chart: { type: "bar", height: Math.max(150, steps.length * 40) },
      plotOptions: { bar: { horizontal: true, barHeight: "60%" } },
      series: [
        { name: "Avg (ms)", data: avgData },
        { name: "Max (ms)", data: maxData }
      ],
      xaxis: { categories: categories, title: { text: "Time (ms)" } },
      colors: ["#3b82f6", "#ef4444"],
      legend: { position: "top" },
      tooltip: { y: { formatter: function(val) { return val + " ms"; } } }
    });
    chart.render();
  }

  jQuery("#profilerQueryDetail").show();
}

function profilerBackToList() {
  jQuery("#profilerQueryDetail").hide();
  jQuery("#profilerSummary").show();
  jQuery("#profilerQueryListContainer").show();
}

function profilerFormatNum(val) {
  if (val === null || val === undefined) return "-";
  if (typeof val === "number") {
    if (val === Math.floor(val)) return val.toLocaleString();
    return val.toFixed(1);
  }
  return String(val);
}
