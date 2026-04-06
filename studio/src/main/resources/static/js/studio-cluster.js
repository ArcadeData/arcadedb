var clusterRefreshTimer = null;
var clusterLagChart = null;
var clusterCommitChart = null;
var clusterLagHistory = {};
var clusterCommitHistory = [];
var clusterMaxHistoryPoints = 60;
var clusterLagWarningThreshold = 0;

function updateCluster(callback) {
  jQuery.ajax({
    type: "GET",
    url: "api/v1/server?mode=cluster",
    beforeSend: function(xhr) { xhr.setRequestHeader("Authorization", globalCredentials); }
  })
  .done(function(data) {
    if (data.ha != null)
      renderClusterData(data.ha);
    if (callback) callback();
    startClusterRefreshTimer();
  })
  .fail(function(jqXHR) {
    globalNotifyError(jqXHR.responseText);
  });
}

function renderClusterData(ha) {
  // Header
  $("#clusterNameLabel").text(ha.clusterName || "");

  var role = ha.isLeader ? "LEADER" : ha.electionStatus;
  $("#clusterRoleBadge")
    .text(role)
    .removeClass("bg-success bg-warning bg-secondary bg-primary")
    .addClass(ha.isLeader ? "bg-success" : role === "FOLLOWER" ? "bg-primary" : "bg-warning");

  var healthy = ha.electionStatus !== "ELECTING" && ha.leader != null;
  $("#clusterHealthBadge")
    .text(healthy ? "HEALTHY" : "ELECTING")
    .removeClass("bg-success bg-warning")
    .addClass(healthy ? "bg-success" : "bg-warning");

  // Info bar
  $("#clusterLeaderName").text(formatPeerId(ha.leader) || "none");
  $("#clusterTerm").text(ha.currentTerm != null ? ha.currentTerm : "-");
  $("#clusterCommitIndex").text(ha.commitIndex != null ? ha.commitIndex : "-");
  $("#clusterAppliedIndex").text(ha.lastAppliedIndex != null ? ha.lastAppliedIndex : "-");
  $("#clusterQuorum").text(ha.quorum || "-");
  $("#clusterServerCount").text(ha.configuredServers || "-");

  // Node cards
  renderNodeCards(ha);

  // Databases table
  renderDatabasesTable(ha.databases || []);

  // Transfer leader dropdown
  renderTransferDropdown(ha);

  // Peer management list
  renderPeerManagement(ha);

  // Verify database buttons
  renderVerifyButtons(ha.databases || []);

  // Update metrics charts and summary cards
  updateMetricsCharts(ha);
  updateMetricsSummary(ha);
}

function renderNodeCards(ha) {
  var container = $("#clusterNodeCards");
  container.empty();

  var peers = ha.peers || [];
  var colClass = peers.length <= 3 ? "col-md-4" : "col-md-3";

  for (var i = 0; i < peers.length; i++) {
    var peer = peers[i];
    var isLeader = peer.role === "LEADER";
    var isLocal = peer.isLocal;

    var borderColor = isLeader ? "var(--color-brand)" : "var(--border-light)";
    var roleBadge = isLeader
      ? '<span class="badge bg-success">LEADER</span>'
      : '<span class="badge bg-secondary">FOLLOWER</span>';
    var localBadge = isLocal ? ' <span class="badge bg-info" style="font-size:0.6rem;">LOCAL</span>' : '';

    var isLagging = peer.lagging === true;
    var dotColor = isLagging ? "red" : "limegreen";
    var statusDot = '<span style="display:inline-block; width:8px; height:8px; border-radius:50%; background:' + dotColor + '; margin-right:6px;"></span>';

    var replicationInfo = "";
    if (!isLeader && peer.matchIndex != null && ha.commitIndex != null) {
      var lag = ha.commitIndex - peer.matchIndex;
      var lagColor = lag === 0 ? "limegreen" : lag < 10 ? "orange" : "red";
      var lagWarning = isLagging ? ' <i class="fas fa-exclamation-triangle" style="color:#ef4444; font-size:0.7rem;" title="Exceeds lag warning threshold"></i>' : '';
      replicationInfo = '<div class="mt-1" style="font-size:0.75rem;"><span class="text-muted">Lag:</span> <b style="color:' + lagColor + ';">' + lag + '</b> entries' + lagWarning
        + ' <span class="text-muted ms-2">Match:</span> ' + peer.matchIndex
        + ' <span class="text-muted ms-2">Next:</span> ' + (peer.nextIndex || "-") + '</div>';
    }
    if (isLeader && ha.commitIndex != null) {
      replicationInfo = '<div class="mt-1" style="font-size:0.75rem;"><span class="text-muted">Commit:</span> ' + ha.commitIndex
        + ' <span class="text-muted ms-2">Applied:</span> ' + (ha.lastAppliedIndex || "-") + '</div>';
    }

    var card = '<div class="' + colClass + '">'
      + '<div class="card h-100" style="border: 2px solid ' + borderColor + '; border-radius: 10px;">'
      + '<div class="card-body py-2 px-3">'
      + '<div class="d-flex align-items-center justify-content-between">'
      + '<div>' + statusDot + '<b style="font-size:0.85rem;">' + escapeHtml(formatPeerId(peer.id)) + '</b>' + localBadge + '</div>'
      + '<div>' + roleBadge + '</div>'
      + '</div>'
      + '<div style="font-size:0.78rem; color:var(--text-secondary); margin-top:4px;">'
      + '<i class="fas fa-network-wired" style="margin-right:4px;"></i>' + escapeHtml(peer.address || "")
      + (peer.httpAddress ? ' <span class="text-muted">| HTTP:</span> ' + escapeHtml(peer.httpAddress) : '')
      + '</div>'
      + replicationInfo
      + '</div></div></div>';

    container.append(card);
  }
}

function renderDatabasesTable(databases) {
  if ($.fn.dataTable.isDataTable("#clusterDatabasesTable"))
    try { $("#clusterDatabasesTable").DataTable().destroy(); $("#clusterDatabasesTable").empty(); } catch(e) {}

  if (databases.length === 0) return;

  var rows = [];
  for (var i = 0; i < databases.length; i++) {
    var db = databases[i];
    rows.push([escapeHtml(db.name), escapeHtml(db.quorum)]);
  }

  $("#clusterDatabasesTable").DataTable({
    searching: false, paging: false, ordering: false, info: false,
    columns: [{ title: "Database" }, { title: "Quorum" }],
    data: rows
  });
}

function renderTransferDropdown(ha) {
  var select = $("#transferLeaderSelect");
  select.empty();
  var peers = ha.peers || [];
  for (var i = 0; i < peers.length; i++) {
    var label = escapeHtml(formatPeerId(peers[i].id)) + (peers[i].isLocal ? " (this server)" : "");
    select.append('<option value="' + escapeHtml(peers[i].id) + '">' + label + '</option>');
  }
}

function renderPeerManagement(ha) {
  var container = $("#peerManagementList");
  container.empty();
  var peers = ha.peers || [];
  for (var i = 0; i < peers.length; i++) {
    var peer = peers[i];
    var isLeader = peer.role === "LEADER";
    var roleBadge = isLeader
      ? '<span class="badge bg-success" style="font-size:0.65rem;">LEADER</span>'
      : '<span class="badge bg-secondary" style="font-size:0.65rem;">FOLLOWER</span>';
    var localTag = peer.isLocal ? ' <span class="badge bg-info" style="font-size:0.6rem;">LOCAL</span>' : '';
    var removeBtn = peer.isLocal ? '' :
      '<button class="btn btn-sm btn-outline-danger py-0 px-1" onclick="haRemovePeer(\'' + escapeHtml(peer.id) + '\')" title="Remove from cluster">'
      + '<i class="fas fa-times" style="font-size:0.7rem;"></i></button>';

    container.append(
      '<div class="d-flex align-items-center justify-content-between py-1 px-2 mb-1" '
      + 'style="font-size:0.82rem; background:var(--bg-main); border-radius:6px; border:1px solid var(--border-light);">'
      + '<div><i class="fa fa-server" style="color:var(--color-brand); margin-right:6px;"></i>'
      + escapeHtml(formatPeerId(peer.id)) + localTag + ' ' + roleBadge + '</div>'
      + '<div>' + removeBtn + '</div></div>'
    );
  }
}

/** Converts internal peer ID (host_port) to display format (host:port). */
function formatPeerId(id) {
  if (!id) return id;
  var lastUnderscore = id.lastIndexOf("_");
  if (lastUnderscore > 0) {
    var afterUnderscore = id.substring(lastUnderscore + 1);
    if (/^\d+$/.test(afterUnderscore))
      return id.substring(0, lastUnderscore) + ":" + afterUnderscore;
  }
  return id;
}

function renderVerifyButtons(databases) {
  var container = $("#verifyDatabaseButtons");
  container.empty();
  for (var i = 0; i < databases.length; i++) {
    var dbName = databases[i].name;
    container.append(
      '<button class="btn btn-sm btn-outline-info me-2 mb-1" onclick="haVerifyDatabase(\'' + escapeHtml(dbName) + '\')">'
      + '<i class="fas fa-check-circle"></i> Verify ' + escapeHtml(dbName) + '</button>'
    );
  }
}

// ==================== METRICS CHARTS ====================

function updateMetricsCharts(ha) {
  var now = new Date().toLocaleTimeString();
  var peers = ha.peers || [];

  // Replication lag chart
  for (var i = 0; i < peers.length; i++) {
    var peer = peers[i];
    if (peer.role === "LEADER" || peer.matchIndex == null) continue;
    var lag = (ha.commitIndex || 0) - peer.matchIndex;
    if (!clusterLagHistory[peer.id]) clusterLagHistory[peer.id] = [];
    clusterLagHistory[peer.id].push({ x: now, y: lag });
    if (clusterLagHistory[peer.id].length > clusterMaxHistoryPoints)
      clusterLagHistory[peer.id].shift();
  }

  // Track lag warning threshold from server
  if (ha.metrics && ha.metrics.lagWarningThreshold > 0)
    clusterLagWarningThreshold = ha.metrics.lagWarningThreshold;

  // Commit index history
  clusterCommitHistory.push({ x: now, y: ha.commitIndex || 0 });
  if (clusterCommitHistory.length > clusterMaxHistoryPoints)
    clusterCommitHistory.shift();

  renderLagChart();
  renderCommitChart();
}

function renderLagChart() {
  var series = [];
  for (var peerId in clusterLagHistory)
    series.push({ name: formatPeerId(peerId), data: clusterLagHistory[peerId].slice() });

  if (series.length === 0) {
    $("#chartReplicationLag").html('<div class="text-muted text-center py-5">No follower replication data (only available on leader)</div>');
    return;
  }

  // Add warning threshold annotation line if configured
  var annotations = {};
  if (clusterLagWarningThreshold > 0) {
    annotations = {
      yaxis: [{
        y: clusterLagWarningThreshold,
        borderColor: "#ef4444",
        strokeDashArray: 4,
        label: { text: "Warning (" + clusterLagWarningThreshold + ")", style: { color: "#fff", background: "#ef4444", fontSize: "10px" }, position: "front" }
      }]
    };
  }

  var options = {
    chart: { type: "line", height: 250, animations: { enabled: true, easing: "linear", dynamicAnimation: { speed: 1000 } }, toolbar: { show: false } },
    series: series,
    annotations: annotations,
    xaxis: { type: "category", labels: { show: false } },
    yaxis: { title: { text: "Entries behind" }, min: 0 },
    stroke: { curve: "smooth", width: 2 },
    colors: ["#ff6384", "#36a2eb", "#ffce56", "#4bc0c0", "#9966ff"],
    legend: { position: "top", fontSize: "11px" },
    tooltip: { y: { formatter: function(val) { return val + " entries"; } } }
  };

  if (clusterLagChart) {
    clusterLagChart.updateSeries(series);
    if (clusterLagWarningThreshold > 0)
      clusterLagChart.updateOptions({ annotations: annotations });
  } else {
    clusterLagChart = new ApexCharts(document.querySelector("#chartReplicationLag"), options);
    clusterLagChart.render();
  }
}

function renderCommitChart() {
  var options = {
    chart: { type: "area", height: 250, animations: { enabled: true }, toolbar: { show: false } },
    series: [{ name: "Commit Index", data: clusterCommitHistory.slice() }],
    xaxis: { type: "category", labels: { show: false } },
    yaxis: { title: { text: "Log Index" } },
    stroke: { curve: "smooth", width: 2 },
    fill: { type: "gradient", gradient: { shadeIntensity: 1, opacityFrom: 0.4, opacityTo: 0.1 } },
    colors: ["var(--color-brand)"],
    tooltip: { y: { formatter: function(val) { return val; } } }
  };

  if (clusterCommitChart) {
    clusterCommitChart.updateSeries([{ name: "Commit Index", data: clusterCommitHistory.slice() }]);
  } else {
    clusterCommitChart = new ApexCharts(document.querySelector("#chartCommitIndex"), options);
    clusterCommitChart.render();
  }
}

function updateMetricsSummary(ha) {
  var m = ha.metrics || {};

  // Election count
  $("#metricElectionCount").text(m.electionCount != null ? m.electionCount : "-");

  // Raft log size
  var logSize = m.raftLogSize != null && m.raftLogSize >= 0 ? m.raftLogSize : "-";
  $("#metricRaftLogSize").text(logSize);

  // Last election time
  if (m.lastElectionTime > 0) {
    var elapsed = Date.now() - m.lastElectionTime;
    $("#metricLastElection").text(formatDuration(elapsed) + " ago");
  } else {
    $("#metricLastElection").text("none");
  }

  // Uptime
  if (m.startTime > 0) {
    var uptime = Date.now() - m.startTime;
    $("#metricUptime").text(formatDuration(uptime));
  } else {
    $("#metricUptime").text("-");
  }
}

function formatDuration(ms) {
  var secs = Math.floor(ms / 1000);
  if (secs < 60) return secs + "s";
  var mins = Math.floor(secs / 60);
  if (mins < 60) return mins + "m " + (secs % 60) + "s";
  var hours = Math.floor(mins / 60);
  if (hours < 24) return hours + "h " + (mins % 60) + "m";
  var days = Math.floor(hours / 24);
  return days + "d " + (hours % 24) + "h";
}

// ==================== MANAGEMENT ACTIONS ====================

function haStepDown() {
  globalConfirm("Step Down Leader", "Are you sure you want the leader to step down?", "warning", function() {
    executeClusterCommand("ha step down", "Leader step down initiated");
  });
}

function haTransferLeader() {
  var target = $("#transferLeaderSelect").val();
  if (!target) { globalNotify("Error", "No target peer selected", "danger"); return; }
  globalConfirm("Transfer Leadership", "Transfer leadership to " + target + "?", "warning", function() {
    executeClusterCommand("ha transfer leader " + target, "Leadership transfer initiated to " + target);
  });
}

function haAddPeer() {
  var addr = $("#addPeerAddrInput").val().trim();
  if (!addr) { globalNotify("Error", "Address is required (e.g. 192.168.1.10:2424)", "danger"); return; }
  // Derive peer ID from address: replace colon with underscore (JMX-safe format)
  var peerId = addr.replace(":", "_");
  executeClusterCommand("ha add peer " + peerId + " " + addr, "Peer " + addr + " added to cluster");
  $("#addPeerAddrInput").val("");
}

function haRemovePeer(peerId) {
  globalConfirm("Remove Peer", "Remove peer " + peerId + " from the cluster?", "warning", function() {
    executeClusterCommand("ha remove peer " + peerId, "Peer " + peerId + " removed");
  });
}

function haVerifyDatabase(dbName) {
  globalNotify("Verifying", "Running verification for " + dbName + "...", "info");
  jQuery.ajax({
    type: "POST",
    url: "api/v1/server",
    data: JSON.stringify({ command: "ha verify database " + dbName }),
    contentType: "application/json",
    beforeSend: function(xhr) { xhr.setRequestHeader("Authorization", globalCredentials); }
  })
  .done(function(data) {
    var resultArea = $("#verifyResultArea");
    resultArea.show();
    $("#verifyResultPre").text(JSON.stringify(data, null, 2));

    if (data.result && data.result.overallStatus) {
      // Leader response with full comparison
      globalNotify("Verification", dbName + ": " + data.result.overallStatus,
        data.result.overallStatus === "ALL_CONSISTENT" ? "success" : "warning");
    } else if (data.localChecksums) {
      // Follower response - show local checksums
      var fileCount = Object.keys(data.localChecksums).length;
      globalNotify("Verification", dbName + ": " + fileCount + " files checksummed on this server. Connect to the leader for cross-node comparison.", "info");
    }
  })
  .fail(function(jqXHR) { globalNotifyError(jqXHR.responseText); });
}

function shutdownServer(serverName) {
  var command = serverName ? "shutdown " + serverName : "shutdown";
  var message = serverName ? "Shut down server '" + serverName + "'?" : "Shut down this server?";
  globalConfirm("Shutdown Server", message, "warning", function() {
    executeClusterCommand(command, "Shutdown request sent");
  });
}

function executeClusterCommand(command, successMessage) {
  if (!command) return;
  jQuery.ajax({
    type: "POST",
    url: "api/v1/server",
    data: JSON.stringify({ command: command }),
    contentType: "application/json",
    beforeSend: function(xhr) { xhr.setRequestHeader("Authorization", globalCredentials); }
  })
  .done(function(data) {
    globalNotify("Success", successMessage, "success");
    setTimeout(function() { updateCluster(); }, 2000);
  })
  .fail(function(jqXHR) {
    var msg = "Unknown error";
    if (jqXHR.responseJSON)
      msg = jqXHR.responseJSON.detail || jqXHR.responseJSON.error || msg;
    else if (jqXHR.responseText)
      try { msg = JSON.parse(jqXHR.responseText).error || msg; } catch(e) { msg = jqXHR.responseText; }
    globalNotify("Error", msg, "danger");
  });
}

// ==================== REFRESH TIMER ====================

function startClusterRefreshTimer(userChange) {
  if (clusterRefreshTimer != null) clearTimeout(clusterRefreshTimer);

  var secs = parseInt($("#clusterRefreshTimeout").val());
  if (secs > 0) {
    clusterRefreshTimer = setTimeout(function() {
      if (studioCurrentTab == "cluster") updateCluster();
    }, secs * 1000);
  }

  if (userChange) globalSetCookie("clusterRefreshTimeoutInSecs", secs, 365);
}

document.addEventListener("DOMContentLoaded", function() {
  var saved = globalGetCookie("clusterRefreshTimeoutInSecs");
  if (saved != null) $("#clusterRefreshTimeout").val(saved);
});
