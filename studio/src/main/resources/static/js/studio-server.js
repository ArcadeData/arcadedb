var GB_SIZE = 1024 * 1024 * 1024;

var lastUpdate = null;
var serverData = null;
var eventsData = {};
var serverChartOSCPU = null;
var serverChartOSRAM = null;
var serverChartOSDisk = null;
var serverChartServerRAM = null;
var serverChartCache = null;
var serverChartCommands = null;
var reqPerSecLastMinute = {};
var serverRefreshTimer = null;

function updateServer(callback) {
  let currentDate = new Date();
  let currentSecond = currentDate.getHours() + ":" + currentDate.getMinutes() + ":" + currentDate.getSeconds();
  if (currentSecond == lastUpdate)
    // SKIP SAME SECOND
    return;

  lastUpdate = currentSecond;

  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/server",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      let version = data.version;
      let buildInfo = '';
      let pos = data.version.indexOf("(build");
      if (pos > -1) {
        buildInfo = version.substring(pos);
        version = version.substring(0, pos).trim();
      }

      // Compact header label
      let compactLabel = "Connected to <b>" + escapeHtml(data.user) + "@" + escapeHtml(data.serverName) + "</b> - v." + escapeHtml(version);
      $("#serverConnectionLabel").html(compactLabel);

      // Popover details
      let popoverHtml = "<div style='margin-bottom:8px;font-weight:600;color:var(--text-primary);'>Server Details</div>";
      popoverHtml += "<div style='margin-bottom:6px;'><b>Server:</b> " + escapeHtml(data.user) + "@" + escapeHtml(data.serverName) + "</div>";
      popoverHtml += "<div style='margin-bottom:6px;'><b>Version:</b> " + escapeHtml(version) + "</div>";
      if (buildInfo)
        popoverHtml += "<div style='margin-bottom:6px;font-size:0.78rem;color:var(--text-muted);word-break:break-all;'><b>Build:</b> " + escapeHtml(buildInfo) + "</div>";
      if (data.metrics && data.metrics.profiler && data.metrics.profiler.configuration && data.metrics.profiler.configuration.description)
        popoverHtml += "<div style='font-size:0.78rem;color:var(--text-muted);'><b>Platform:</b> " + escapeHtml(data.metrics.profiler.configuration.description) + "</div>";
      $("#serverInfoPopoverBody").html(popoverHtml);

      serverData = data;

      displayServerSummary();
      displayMetrics();
      displayServerSettings();

      startServerRefreshTimer();

      if (callback) callback();
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      globalNotifyError(jqXHR.responseText);
    });
}

function renderDatabases(databases) {
  let result = '<table cellpadding="1" cellspacing="0" border="0" style="padding-left:50px;">';

  for (let i in databases) {
    let db = databases[i];
    result += "<tr><td>" + db.name + "</td><td><button enabled='false'></td>";
  }
  result += "</table>";

  return result;
}

function displayServerSettings() {
  if ($.fn.dataTable.isDataTable("#serverSettings"))
    try {
      $("#serverSettings").DataTable().destroy();
      $("#serverSettings").empty();
    } catch (e) {}

  var tableRecords = [];

  for (let i in serverData.settings) {
    let row = serverData.settings[i];

    let record = [];
    record.push(row.key);
    record.push(row.value);
    record.push(row.description);
    record.push(row.default);
    record.push(row.overridden);
    tableRecords.push(record);
  }

  $("#serverSettings").DataTable({
    paging: false,
    ordering: false,
    autoWidth: false,
    columns: [
      { title: "Key", width: "25%" },
      {
        title: "Value",
        width: "20%",
        render: function (data, type, row) {
          return "<a href='#' onclick='updateServerSetting(\"" + row[0] + '", "' + row[1] + "\")' style='color: green;'><b>" + data + "</b></a>";
        },
      },
      { title: "Description", width: "33%" },
      { title: "Default", width: "15%" },
      { title: "Overridden", width: "7%" },
    ],
    data: tableRecords,
  });
}

function displayServerSummary() {
  // COMMANDS
  let currentDate = new Date();
  let x = currentDate.getHours() + ":" + currentDate.getMinutes() + ":" + currentDate.getSeconds();
  if (reqPerSecLastMinute.length > 0 && x == reqPerSecLastMinute[0].x)
    // SKIP SAME SECOND
    return;

  if (serverData.metrics.meters) {
    let series = [];
    for (commandsMetricName in serverData.metrics.meters) {
      let metric = serverData.metrics.meters[commandsMetricName];
      let array = reqPerSecLastMinute[commandsMetricName];
      if (!array) {
        array = [];
        reqPerSecLastMinute[commandsMetricName] = array;
      }
      array.unshift({ x: x, y: metric.reqPerSecSinceLastTime });

      if (array.length > 50)
        // KEEP ONLY THE LATEST 50 VALUES
        array.pop();

      series.push({ name: commandsMetricName, data: array });
    }

    var serverCommandsOptions = {
      series: series,
      labels: ["Used", "Available"],
      chart: { type: "line", height: 300, animations: { enabled: false } },
      legend: { show: false },
      tooltip: { enabled: true },
      fill: { opacity: [0.24, 1, 1] },
      dataLabels: { enabled: true },
      stroke: { curve: "smooth" },
      grid: {
        borderColor: getComputedStyle(document.documentElement).getPropertyValue('--border-ddd').trim() || "#e7e7e7",
        row: {
          colors: [getComputedStyle(document.documentElement).getPropertyValue('--bg-sidebar').trim() || "#f3f3f3", "transparent"],
          opacity: 0.5,
        },
      },
      markers: { size: 1 },
      yaxis: { title: { text: "Req/Sec" } },
    };

    if (serverChartCommands != null) serverChartCommands.destroy();

    serverChartCommands = new ApexCharts(document.querySelector("#serverChartCommands"), serverCommandsOptions);
    serverChartCommands.render();
  }

  // CPU CHART
  let cpuLoad = serverData.metrics.profiler.cpuLoad.perc;

  var cpuOptions = {
    series: [cpuLoad, 100 - cpuLoad],
    labels: ["Used", "Available"],
    fill: { colors: ["#FFA502", "#48C392"] },
    chart: { type: "donut", selection: { enable: false }, height: 300, toolbar: { show: false }, animations: { enabled: false } },
    legend: { show: false },
    tooltip: { enabled: false },
    dataLabels: {
      enabled: false,
      formatter: function (val) {
        return globalFormatDouble(val, 0) + "%";
      },
    },
    plotOptions: {
      pie: {
        expandOnClick: false,
        donut: {
          labels: {
            show: true,
            name: { show: true },
            value: { formatter: () => globalFormatDouble(cpuLoad, 0) + "%" },
            total: { show: true, label: "OS CPU", formatter: () => globalFormatDouble(cpuLoad, 0) + "%" },
          },
        },
      },
    },
  };

  if (serverChartOSCPU != null) serverChartOSCPU.destroy();

  serverChartOSCPU = new ApexCharts(document.querySelector("#serverChartOSCPU"), cpuOptions);
  serverChartOSCPU.render();

  // OS RAM
  let ramOsUsed = serverData.metrics.profiler.ramOsUsed.space;
  let ramOsTotal = serverData.metrics.profiler.ramOsTotal.space;

  var serverRamOSOptions = {
    series: [ramOsUsed, ramOsTotal - ramOsUsed],
    labels: ["Used", "Available"],
    fill: { colors: ["#FFA502", "#48C392"] },
    chart: { type: "donut", selection: { enable: false }, height: 300, toolbar: { show: false }, animations: { enabled: false } },
    legend: { show: false },
    tooltip: { enabled: false },
    dataLabels: {
      enabled: false,
      formatter: function (val) {
        return globalFormatDouble(val, 0) + "%";
      },
    },
    plotOptions: {
      pie: {
        expandOnClick: false,
        donut: {
          labels: {
            show: true,
            name: { show: true },
            value: { formatter: (val) => globalFormatDouble(val / GB_SIZE, 2) + "GB" },
            total: { show: true, label: "OS RAM", formatter: () => globalFormatDouble(ramOsUsed / GB_SIZE, 2) + "GB" },
          },
        },
      },
    },
  };

  if (serverChartOSRAM != null) serverChartOSRAM.destroy();

  serverChartOSRAM = new ApexCharts(document.querySelector("#serverChartOSRAM"), serverRamOSOptions);
  serverChartOSRAM.render();

  // OS DISK
  let diskFreeSpace = serverData.metrics.profiler.diskFreeSpace.space;
  let diskTotalSpace = serverData.metrics.profiler.diskTotalSpace.space;

  var serverDiskOSOptions = {
    series: [diskTotalSpace - diskFreeSpace, diskFreeSpace],
    labels: ["Used", "Available"],
    fill: { colors: ["#FFA502", "#48C392"] },
    chart: { type: "donut", selection: { enable: false }, height: 300, toolbar: { show: false }, animations: { enabled: false } },
    legend: { show: false },
    tooltip: { enabled: false },
    dataLabels: {
      enabled: false,
      formatter: function (val) {
        return globalFormatDouble(val, 0) + "%";
      },
    },
    plotOptions: {
      pie: {
        expandOnClick: false,
        donut: {
          labels: {
            show: true,
            name: { show: true },
            value: { formatter: (val) => globalFormatDouble(val / GB_SIZE, 2) + "GB" },
            total: { show: true, label: "OS DISK", formatter: () => globalFormatDouble((diskTotalSpace - diskFreeSpace) / GB_SIZE, 2) + "GB" },
          },
        },
      },
    },
  };

  if (serverChartOSDisk != null) serverChartOSDisk.destroy();

  serverChartOSDisk = new ApexCharts(document.querySelector("#serverChartOSDisk"), serverDiskOSOptions);
  serverChartOSDisk.render();

  // SERVER RAM
  let ramHeapUsed = serverData.metrics.profiler.ramHeapUsed.space;
  let ramHeapMax = serverData.metrics.profiler.ramHeapMax.space;

  var serverRamOptions = {
    series: [ramHeapUsed, ramHeapMax - ramHeapUsed],
    labels: ["Used", "Available"],
    fill: { colors: ["#FFA502", "#48C392"] },
    chart: { type: "donut", selection: { enable: false }, height: 300, toolbar: { show: false }, animations: { enabled: false } },
    legend: { show: false },
    tooltip: { enabled: false },
    dataLabels: {
      enabled: false,
      formatter: function (val) {
        return globalFormatDouble(val, 0) + "%";
      },
    },
    plotOptions: {
      pie: {
        expandOnClick: false,
        donut: {
          labels: {
            show: true,
            name: { show: true },
            value: { formatter: (val) => globalFormatDouble(val / GB_SIZE, 2) + "GB" },
            total: { show: true, label: "Server RAM", formatter: () => globalFormatDouble(ramHeapUsed / GB_SIZE, 2) + "GB" },
          },
        },
      },
    },
  };

  if (serverChartServerRAM != null) serverChartServerRAM.destroy();

  serverChartServerRAM = new ApexCharts(document.querySelector("#serverChartServerRAM"), serverRamOptions);
  serverChartServerRAM.render();

  // CACHE
  let readCacheUsed = serverData.metrics.profiler.readCacheUsed.space;
  let cacheMax = serverData.metrics.profiler.cacheMax.space;

  var serverCacheOptions = {
    series: [readCacheUsed, cacheMax - readCacheUsed],
    labels: ["Used", "Available"],
    fill: { colors: ["#FFA502", "#48C392"] },
    chart: { type: "donut", selection: { enable: false }, height: 300, toolbar: { show: false }, animations: { enabled: false } },
    legend: { show: false },
    tooltip: { enabled: false },
    dataLabels: {
      enabled: false,
      formatter: function (val) {
        return globalFormatDouble(val, 0) + "%";
      },
    },
    plotOptions: {
      pie: {
        expandOnClick: false,
        donut: {
          labels: {
            show: true,
            name: { show: true },
            value: { formatter: (val) => globalFormatDouble(val / GB_SIZE, 2) + "GB" },
            total: { show: true, label: "Server Cache", formatter: () => globalFormatDouble(readCacheUsed / GB_SIZE, 2) + "GB" },
          },
        },
      },
    },
  };

  if (serverChartCache != null) serverChartCache.destroy();

  serverChartCache = new ApexCharts(document.querySelector("#serverChartCache"), serverCacheOptions);
  serverChartCache.render();

  if (serverData.metrics.events) {
    $("#serverEventsSummaryErrors").html(serverData.metrics.events.errors);
    $("#serverEventsSummaryWarnings").html(serverData.metrics.events.warnings);
    $("#serverEventsSummaryInfo").html(serverData.metrics.events.info);
    $("#serverEventsSummaryHints").html(serverData.metrics.events.hints);
  }
}

function displayMetrics() {
  if ($.fn.dataTable.isDataTable("#serverMetrics"))
    try {
      $("#serverMetrics").DataTable().destroy();
      $("#serverMetrics").empty();
    } catch (e) {}

  var tableRecords = [];

  for (let name in serverData.metrics.meters) {
    let meter = serverData.metrics.meters[name];

    let record = [];
    record.push(escapeHtml(name));
    record.push(meter.count);
    record.push(globalFormatDouble(meter.reqPerSecLastMinute));
    tableRecords.push(record);
  }

  for (let name in serverData.metrics.profiler) {
    let entry = serverData.metrics.profiler[name];

    let record = [];
    record.push(escapeHtml(name));

    if (entry.perc != null) record.push(globalFormatDouble(entry.perc, 2) + "%");
    else if (entry.count != null && entry.count != 0) record.push(globalFormatDouble(entry.count, 0));
    else if (entry.space != null && entry.space != 0) record.push(globalFormatSpace(entry.space));
    else if (entry.value != null) record.push(entry.value);
    else continue;

    record.push("");

    tableRecords.push(record);
  }

  // Sort alphabetically by metric name
  tableRecords.sort(function(a, b) { return a[0].localeCompare(b[0]); });

  $("#serverMetrics").DataTable({
    paging: false,
    ordering: true,
    order: [[0, "asc"]],
    columns: [{ title: "Metric Name" }, { title: "Value" }, { title: "Req/Sec" }],
    data: tableRecords,
  });
}

function updateServerSetting(key, value) {
  let html = "<b>" + escapeHtml(key) + "</b> = <input class='form-control mt-2' id='updateSettingInput' value='" + escapeHtml(value) + "' " +
    "onkeydown='if (event.which === 13) document.getElementById(\"globalModalConfirmBtn\").click()'>";
  html += "<br><p><i>The update will not be persistent and will be reset at the next restart of the server.</i></p>";

  globalPrompt("Update Server Setting", html, "Update", function() {
    jQuery
      .ajax({
        type: "POST",
        url: "api/v1/server",
        data: JSON.stringify({
          language: "sql",
          command: "set server setting " + key + " " + $("#updateSettingInput").val(),
        }),
        beforeSend: function (xhr) {
          xhr.setRequestHeader("Authorization", globalCredentials);
        },
      })
      .done(function (data) {
        if (data.error) {
          $("#authorizationCodeMessage").html(data.error);
          return false;
        }
        displayServerSettings();
        return true;
      });
  });
}

function toggleServerInfoPopover() {
  var el = document.getElementById('serverInfoPopover');
  if (el.style.display === 'none')
    el.style.display = 'block';
  else
    el.style.display = 'none';
}

// Close popover when clicking outside
document.addEventListener('click', function(e) {
  var popover = document.getElementById('serverInfoPopover');
  var trigger = document.getElementById('serverConnectionCompact');
  if (popover && trigger && !trigger.contains(e.target) && !popover.contains(e.target))
    popover.style.display = 'none';
});

function loadServerSessions() {
  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/sessions",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      if ($.fn.dataTable.isDataTable("#serverSessions")) {
        try {
          $("#serverSessions").DataTable().destroy();
          $("#serverSessions").empty();
        } catch (e) {}
      }

      var tableRecords = [];

      for (let i in data.result) {
        let session = data.result[i];

        let record = [];
        record.push(session.user);
        record.push(formatDateTime(session.createdAt));
        record.push(formatDateTime(session.lastUpdate));
        record.push(session.sourceIp || "");
        record.push(truncateUserAgent(session.userAgent));
        record.push(formatLocation(session.country, session.city));
        record.push(session.token);
        tableRecords.push(record);
      }

      $("#serverSessions").DataTable({
        paging: true,
        ordering: true,
        order: [[1, "desc"]],
        pageLength: 25,
        columns: [
          { title: "User", width: "10%" },
          { title: "Created", width: "15%" },
          { title: "Last Activity", width: "15%" },
          { title: "IP Address", width: "12%" },
          {
            title: "User Agent",
            width: "25%",
            render: function (data, type, row) {
              if (type === "display" && data) {
                return '<span title="' + escapeHtml(row[4]) + '">' + escapeHtml(data) + "</span>";
              }
              return data;
            },
          },
          { title: "Location", width: "10%" },
          {
            title: "Token",
            width: "13%",
            render: function (data, type, row) {
              if (type === "display" && data) {
                // Show truncated token with copy button
                let shortToken = data.substring(0, 12) + "...";
                return (
                  '<span class="text-muted" title="' +
                  escapeHtml(data) +
                  '">' +
                  escapeHtml(shortToken) +
                  "</span>"
                );
              }
              return data;
            },
          },
        ],
        data: tableRecords,
      });
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      globalNotifyError(jqXHR.responseText);
    });
}

function formatDateTime(timestamp) {
  if (!timestamp) return "";
  let date = new Date(timestamp);
  return (
    date.toLocaleDateString() +
    " " +
    date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })
  );
}

function truncateUserAgent(userAgent) {
  if (!userAgent) return "";
  // Extract browser name and version from user agent
  let match = userAgent.match(/(Chrome|Firefox|Safari|Edge|Opera|MSIE|Trident)[\/\s]?(\d+(\.\d+)?)?/i);
  if (match) {
    return match[1] + (match[2] ? " " + match[2] : "");
  }
  // Fallback: truncate if too long
  if (userAgent.length > 30) {
    return userAgent.substring(0, 30) + "...";
  }
  return userAgent;
}

function formatLocation(country, city) {
  if (!country && !city) return "";
  if (city && country) return city + ", " + country;
  return country || city || "";
}

function getServerEvents(file) {
  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/server",
      data: "{ command: 'get server events" + (file != null ? " " + file : "") + "' }",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      eventsData = data;

      let html = "";
      for (let i in data.result.files) {
        let file = data.result.files[i];
        html += "<option value='" + file + "'" + (i == 0 ? " selected" : "") + ">" + file + "</option>";
      }
      $("#serverEventsFile").html(html);

      // BUILD SELECT FOR FILTERING BASED ON REAL VALUES
      let components = { ALL: true };
      let databases = { ALL: true };
      for (let i in eventsData.result.events) {
        let event = eventsData.result.events[i];

        if (event.component != null) {
          if (components[event.component] == null) components[event.component] = true;
        }
        if (event.db != null) {
          if (databases[event.db] == null) databases[event.db] = true;
        }
      }

      html = "";
      for (comp in components) html += "<option value='" + comp + "'>" + comp + "</option>";
      $("#serverEventsComponent").html(html);

      html = "";
      for (db in databases) html += "<option value='" + db + "'>" + db + "</option>";
      $("#serverEventsDb").html(html);

      filterServerEvents();
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      globalNotifyError(jqXHR.responseText);
    });
}

function filterServerEvents() {
  if ($.fn.dataTable.isDataTable("#serverEvents"))
    try {
      $("#serverEvents").DataTable().destroy();
      $("#serverEvents").empty();
    } catch (e) {}

  let serverEventsType = $("#serverEventsType").val();
  let serverEventsComponent = $("#serverEventsComponent").val();
  let serverEventsDb = $("#serverEventsDb").val();

  let rows = [];

  let serverEventsSummaryErrors = 0;
  let serverEventsSummaryWarnings = 0;
  let serverEventsSummaryInfo = 0;
  let serverEventsSummaryHints = 0;

  for (let i in eventsData.result.events) {
    let event = eventsData.result.events[i];

    if (serverEventsType != null && serverEventsType.length > 0 && serverEventsType != "ALL" && serverEventsType != event.type)
      // FILTER IT OUT
      continue;

    if (serverEventsComponent != null && serverEventsComponent.length > 0 && serverEventsComponent != "ALL" && serverEventsComponent != event.component)
      // FILTER IT OUT
      continue;

    if (serverEventsDb != null && serverEventsDb.length > 0 && serverEventsDb != "ALL" && serverEventsDb != event.db)
      // FILTER IT OUT
      continue;

    let row = [];
    row.push(event.time != null ? event.time : "");
    row.push(event.type != null ? event.type : "");
    row.push(event.component != null ? event.component : "");
    row.push(event.db != null ? event.db : "");
    row.push(event.message != null ? event.message : "");
    rows.push(row);
  }

  $("#serverEvents").DataTable({
    paging: true,
    ordering: false,
    columns: [
      { title: "Time", width: "10%" },
      { title: "Type", width: "10%" },
      { title: "Component", width: "10%" },
      { title: "Database", width: "10%" },
      { title: "Message", width: "60%" },
    ],
    data: rows,
  });
}

function startServerRefreshTimer(userChange) {
  if (serverRefreshTimer != null) clearTimeout(serverRefreshTimer);

  const serverRefreshTimeoutInSecs = $("#serverRefreshTimeout").val();
  if (serverRefreshTimeoutInSecs > 0) {
    serverRefreshTimer = setTimeout(function () {
      if (studioCurrentTab == "server") updateServer();
    }, serverRefreshTimeoutInSecs * 1000);
  }

  if (userChange) globalSetCookie("serverRefreshTimeoutInSecs", serverRefreshTimeoutInSecs, 365);
}

// Backup configuration data
var backupConfigData = null;
var backupConfigLoaded = false;

function loadBackupConfig() {
  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/server",
      data: JSON.stringify({ command: "get backup config" }),
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      backupConfigData = data;
      backupConfigLoaded = true;

      if (data.config == null || data.config === "null") {
        $("#backupStatusMessage").html(
          'Auto-backup is not configured. <a href="#" onclick="enableBackupConfig()">Click here</a> to create a default configuration.'
        );
        $("#backupConfigForm").hide();
        $("#backupConfigStatus").show();
      } else {
        // Config exists - show it
        if (!data.enabled && data.message) {
          // Config saved but plugin not active
          $("#backupStatusMessage").html(
            '<i class="fa fa-exclamation-triangle text-warning"></i> ' + escapeHtml(data.message)
          );
          $("#backupConfigStatus").show();
        } else if (data.enabled) {
          $("#backupConfigStatus").hide();
        } else {
          $("#backupStatusMessage").html(
            '<i class="fa fa-info-circle text-info"></i> Configuration saved. Restart server to enable auto-backup.'
          );
          $("#backupConfigStatus").show();
        }
        $("#backupConfigForm").show();
        populateBackupConfigForm(data.config);
      }
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      $("#backupStatusMessage").html("Error loading backup configuration: " + escapeHtml(jqXHR.responseText));
      backupConfigLoaded = false;
    });
}

function enableBackupConfig() {
  // Create default config
  backupConfigData = {
    enabled: true,
    config: {
      version: 1,
      enabled: true,
      backupDirectory: "./backups",
      defaults: {
        enabled: true,
        runOnServer: "$leader",
        schedule: {
          type: "frequency",
          frequencyMinutes: 60,
        },
        retention: {
          maxFiles: 10,
        },
      },
    },
  };

  $("#backupConfigStatus").hide();
  $("#backupConfigForm").show();
  populateBackupConfigForm(backupConfigData.config);
}

function populateBackupConfigForm(config) {
  $("#backupEnabled").val(config.enabled ? "true" : "false");
  $("#backupDirectory").val(config.backupDirectory || "./backups");

  if (config.defaults) {
    var defaults = config.defaults;
    $("#backupRunOnServer").val(defaults.runOnServer || "$leader");

    if (defaults.schedule) {
      var schedType = defaults.schedule.type || "frequency";
      $("#backupScheduleType").val(schedType);
      toggleBackupScheduleFields();

      if (schedType === "frequency") {
        $("#backupFrequency").val(defaults.schedule.frequencyMinutes || 60);
      } else if (schedType === "cron") {
        $("#backupCron").val(defaults.schedule.expression || "");
      }

      if (defaults.schedule.timeWindow) {
        $("#backupWindowStart").val(defaults.schedule.timeWindow.start || "");
        $("#backupWindowEnd").val(defaults.schedule.timeWindow.end || "");
      }
    }

    if (defaults.retention) {
      $("#backupMaxFiles").val(defaults.retention.maxFiles || 10);

      if (defaults.retention.tiered) {
        $("#backupUseTiered").prop("checked", true);
        toggleTieredRetention();
        $("#backupHourly").val(defaults.retention.tiered.hourly || 24);
        $("#backupDaily").val(defaults.retention.tiered.daily || 7);
        $("#backupWeekly").val(defaults.retention.tiered.weekly || 4);
        $("#backupMonthly").val(defaults.retention.tiered.monthly || 12);
        $("#backupYearly").val(defaults.retention.tiered.yearly || 3);
      }
    }
  }
}

function toggleBackupScheduleFields() {
  var schedType = $("#backupScheduleType").val();
  if (schedType === "frequency") {
    $("#backupFrequencyGroup").show();
    $("#backupCronGroup").hide();
  } else {
    $("#backupFrequencyGroup").hide();
    $("#backupCronGroup").show();
  }
}

function toggleTieredRetention() {
  if ($("#backupUseTiered").is(":checked")) {
    $("#tieredRetentionGroup").show();
  } else {
    $("#tieredRetentionGroup").hide();
  }
}

function saveBackupConfig() {
  var config = {
    version: 1,
    enabled: $("#backupEnabled").val() === "true",
    backupDirectory: $("#backupDirectory").val(),
    defaults: {
      enabled: true,
      runOnServer: $("#backupRunOnServer").val(),
      schedule: {
        type: $("#backupScheduleType").val(),
      },
      retention: {
        maxFiles: parseInt($("#backupMaxFiles").val()),
      },
    },
  };

  // Add schedule-specific fields
  if (config.defaults.schedule.type === "frequency") {
    config.defaults.schedule.frequencyMinutes = parseInt($("#backupFrequency").val());
  } else if (config.defaults.schedule.type === "cron") {
    config.defaults.schedule.expression = $("#backupCron").val();
  }

  // Add time window if specified
  var windowStart = $("#backupWindowStart").val();
  var windowEnd = $("#backupWindowEnd").val();
  if (windowStart && windowEnd) {
    config.defaults.schedule.timeWindow = {
      start: windowStart,
      end: windowEnd,
    };
  }

  // Add tiered retention if enabled
  if ($("#backupUseTiered").is(":checked")) {
    config.defaults.retention.tiered = {
      hourly: parseInt($("#backupHourly").val()),
      daily: parseInt($("#backupDaily").val()),
      weekly: parseInt($("#backupWeekly").val()),
      monthly: parseInt($("#backupMonthly").val()),
      yearly: parseInt($("#backupYearly").val()),
    };
  }

  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/server",
      data: JSON.stringify({
        command: "set backup config",
        config: config,
      }),
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      globalNotify("Backup Configuration", "Configuration saved successfully", "success");
      loadBackupConfig();
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      globalNotifyError(jqXHR.responseText);
    });
}

// MCP configuration
var mcpConfigData = null;
var mcpConfigLoaded = false;

function loadMCPConfig() {
  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/mcp/config",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      mcpConfigData = data;
      mcpConfigLoaded = true;
      populateMCPConfigForm(data);
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      globalNotifyError(jqXHR.responseText);
      mcpConfigLoaded = false;
    });
}

function populateMCPConfigForm(config) {
  $("#mcpEnabled").val(config.enabled ? "true" : "false");
  $("#mcpAllowReads").prop("checked", config.allowReads !== false);
  $("#mcpAllowInsert").prop("checked", config.allowInsert === true);
  $("#mcpAllowUpdate").prop("checked", config.allowUpdate === true);
  $("#mcpAllowDelete").prop("checked", config.allowDelete === true);
  $("#mcpAllowSchemaChange").prop("checked", config.allowSchemaChange === true);

  renderMCPUserList(config.allowedUsers || ["root"]);
  updateMCPConnectionInfo();
}

function renderMCPUserList(users) {
  var html = "";
  for (var i = 0; i < users.length; i++) {
    html +=
      '<span class="badge bg-secondary me-1 mb-1">' +
      escapeHtml(users[i]) +
      ' <a href="#" class="mcp-remove-user text-white ms-1" data-username="' +
      escapeHtml(users[i]) +
      '"><i class="fa fa-times"></i></a></span>';
  }
  $("#mcpUserList").html(html);
}

$(document).on("click", ".mcp-remove-user", function (e) {
  e.preventDefault();
  removeMCPUser($(this).data("username"));
});

function addMCPUser() {
  var username = $("#mcpNewUser").val().trim();
  if (!username) return;

  var users = getMCPUsers();
  if (users.indexOf(username) === -1) {
    users.push(username);
    renderMCPUserList(users);
  }
  $("#mcpNewUser").val("");
}

function removeMCPUser(username) {
  var users = getMCPUsers();
  users = users.filter(function (u) {
    return u !== username;
  });
  if (users.length === 0) users = ["root"];
  renderMCPUserList(users);
}

function getMCPUsers() {
  var users = [];
  $("#mcpUserList .badge").each(function () {
    var text = $(this).clone().children().remove().end().text().trim();
    if (text) users.push(text);
  });
  return users;
}

function saveMCPConfig() {
  var config = {
    enabled: $("#mcpEnabled").val() === "true",
    allowReads: true,
    allowInsert: $("#mcpAllowInsert").is(":checked"),
    allowUpdate: $("#mcpAllowUpdate").is(":checked"),
    allowDelete: $("#mcpAllowDelete").is(":checked"),
    allowSchemaChange: $("#mcpAllowSchemaChange").is(":checked"),
    allowedUsers: getMCPUsers(),
  };

  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/mcp/config",
      data: JSON.stringify(config),
      contentType: "application/json",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      mcpConfigData = data;
      globalNotify("MCP Configuration", "Configuration saved successfully", "success");
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      globalNotifyError(jqXHR.responseText);
    });
}

function onMcpAuthMethodChange() {
  var method = $("#mcpAuthMethod").val();
  if (method === "apitoken") {
    $("#mcpTokenSelect").show();
    loadApiTokensForMCP();
  } else {
    $("#mcpTokenSelect").hide();
  }
  updateMCPConnectionInfo();
}

function loadApiTokensForMCP() {
  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/server/api-tokens",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      var select = $("#mcpTokenSelect");
      select.find("option:not(:first)").remove();
      var tokens = data.result || [];
      for (var i = 0; i < tokens.length; i++) {
        select.append(
          '<option value="' + escapeHtml(tokens[i].token) + '">' +
          escapeHtml(tokens[i].name) + " (" + escapeHtml(tokens[i].token) + ")" +
          "</option>"
        );
      }
    });
}

function updateMCPConnectionInfo() {
  var host = window.location.hostname;
  var port = window.location.port || "2480";
  var url = window.location.protocol + "//" + host + ":" + port + "/api/v1/mcp";

  var authHeader;
  if ($("#mcpAuthMethod").val() === "apitoken") {
    var selectedToken = $("#mcpTokenSelect").val();
    if (selectedToken)
      authHeader = "Bearer <paste-your-full-token-here>";
    else
      authHeader = "Bearer <paste-your-api-token-here>";
  } else {
    authHeader = globalBasicAuth || "Basic <base64(username:password)>";
  }

  // Claude Desktop format (uses npx mcp-remote bridge)
  var desktopConfig = {
    mcpServers: {
      arcadedb: {
        command: "npx",
        args: [
          "mcp-remote",
          url,
          "--header",
          "Authorization: " + authHeader
        ]
      }
    }
  };

  // Claude Code / Cursor format (direct Streamable HTTP)
  var codeConfig = {
    mcpServers: {
      arcadedb: {
        url: url,
        headers: {
          Authorization: authHeader
        }
      }
    }
  };

  $("#mcpConfigDesktop").text(JSON.stringify(desktopConfig, null, 2));
  $("#mcpConfigCode").text(JSON.stringify(codeConfig, null, 2));
}

function copyMCPConfig(elementId) {
  var text = $("#" + elementId).text();
  navigator.clipboard.writeText(text).then(function () {
    globalNotify("Copied", "MCP configuration copied to clipboard", "success");
  });
}

document.addEventListener("DOMContentLoaded", function (event) {
  $('a[data-toggle="tab"]').on("shown.bs.tab", function (e) {
    var activeTab = this.id;
    if (activeTab == "tab-server-sessions-sel") {
      loadServerSessions();
    } else if (activeTab == "tab-server-events-sel") {
      getServerEvents();
    } else if (activeTab == "tab-server-backup-sel") {
      if (!backupConfigLoaded) {
        loadBackupConfig();
      }
    } else if (activeTab == "tab-server-mcp-sel") {
      if (!mcpConfigLoaded) {
        loadMCPConfig();
      }
    }
  });

  $("#serverEventsFile").change(function () {
    getServerEvents($("#serverEventsFile").val());
  });

  $("#serverEventsType").change(function () {
    filterServerEvents();
  });
  $("#serverEventsComponent").change(function () {
    filterServerEvents();
  });
  $("#serverEventsDb").change(function () {
    filterServerEvents();
  });

  let serverRefreshTimeoutInSecs = globalGetCookie("serverRefreshTimeoutInSecs");
  if (serverRefreshTimeoutInSecs == null) serverRefreshTimeoutInSecs = 0;
  $("#serverRefreshTimeout").val(serverRefreshTimeoutInSecs);
});
