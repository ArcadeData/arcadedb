var clusterRefreshTimer = null;

function updateCluster(callback) {
  jQuery.ajax({
    type: "GET",
    url: "api/v1/cluster",
    beforeSend: function(xhr) { xhr.setRequestHeader("Authorization", globalCredentials); }
  })
  .done(function(data) {
    renderClusterData(data);
    if (callback) callback();
    startClusterRefreshTimer();
  })
  .fail(function(jqXHR) {
    globalNotifyError(jqXHR.responseText);
  });
}

function renderClusterData(data) {
  // Header
  $("#clusterNameLabel").text(data.clusterName || "");

  var isLeader = data.isLeader === true;
  var leaderId = data.leaderId;
  var localPeerId = data.localPeerId;

  // Determine local role
  var localRole = isLeader ? "LEADER" : "FOLLOWER";
  $("#clusterRoleBadge")
    .text(localRole)
    .removeClass("bg-success bg-warning bg-secondary bg-primary")
    .addClass(isLeader ? "bg-success" : "bg-primary");

  var healthy = leaderId != null && leaderId !== "";
  $("#clusterHealthBadge")
    .text(healthy ? "HEALTHY" : "NO LEADER")
    .removeClass("bg-success bg-warning")
    .addClass(healthy ? "bg-success" : "bg-warning");

  // Info bar
  $("#clusterLeaderName").text(leaderId || "none");
  $("#clusterLeaderHttp").text(data.leaderHttpAddress || "-");
  $("#clusterLocalPeer").text(localPeerId || "-");
  var peers = data.peers || [];
  $("#clusterServerCount").text(peers.length);

  // Node cards
  renderNodeCards(data);

  // Peer management list
  renderPeerManagement(data);

  // Update metrics summary
  updateMetricsSummary(data);
}

function renderNodeCards(data) {
  var container = $("#clusterNodeCards");
  container.empty();

  var peers = data.peers || [];
  var colClass = peers.length <= 3 ? "col-md-4" : "col-md-3";

  for (var i = 0; i < peers.length; i++) {
    var peer = peers[i];
    var isLeader = peer.role === "LEADER";
    var isLocal = peer.id === data.localPeerId;

    var borderColor = isLeader ? "var(--color-brand)" : "var(--border-light)";
    var roleBadge = isLeader
      ? '<span class="badge bg-success">LEADER</span>'
      : '<span class="badge bg-secondary">FOLLOWER</span>';
    var localBadge = isLocal ? ' <span class="badge bg-info" style="font-size:0.6rem;">LOCAL</span>' : '';

    var dotColor = "limegreen";
    var statusDot = '<span style="display:inline-block; width:8px; height:8px; border-radius:50%; background:' + dotColor + '; margin-right:6px;"></span>';

    var card = '<div class="' + colClass + '">'
      + '<div class="card h-100" style="border: 2px solid ' + borderColor + '; border-radius: 10px;">'
      + '<div class="card-body py-2 px-3">'
      + '<div class="d-flex align-items-center justify-content-between">'
      + '<div>' + statusDot + '<b style="font-size:0.85rem;">' + escapeHtml(peer.id) + '</b>' + localBadge + '</div>'
      + '<div>' + roleBadge + '</div>'
      + '</div>'
      + '<div style="font-size:0.78rem; color:var(--text-secondary); margin-top:4px;">'
      + '<i class="fas fa-network-wired" style="margin-right:4px;"></i>' + escapeHtml(peer.address || "")
      + '</div>'
      + '</div></div></div>';

    container.append(card);
  }
}

function renderPeerManagement(data) {
  var container = $("#peerManagementList");
  container.empty();
  var peers = data.peers || [];
  for (var i = 0; i < peers.length; i++) {
    var peer = peers[i];
    var isLeader = peer.role === "LEADER";
    var isLocal = peer.id === data.localPeerId;
    var roleBadge = isLeader
      ? '<span class="badge bg-success" style="font-size:0.65rem;">LEADER</span>'
      : '<span class="badge bg-secondary" style="font-size:0.65rem;">FOLLOWER</span>';
    var localTag = isLocal ? ' <span class="badge bg-info" style="font-size:0.6rem;">LOCAL</span>' : '';

    container.append(
      '<div class="d-flex align-items-center justify-content-between py-1 px-2 mb-1" '
      + 'style="font-size:0.82rem; background:var(--bg-main); border-radius:6px; border:1px solid var(--border-light);">'
      + '<div><i class="fa fa-server" style="color:var(--color-brand); margin-right:6px;"></i>'
      + escapeHtml(peer.id) + ' <span style="color:var(--text-secondary); font-size:0.75rem;">(' + escapeHtml(peer.address || "") + ')</span>'
      + localTag + ' ' + roleBadge + '</div>'
      + '</div>'
    );
  }
}

function updateMetricsSummary(data) {
  // Election count
  $("#metricElectionCount").text(data.electionCount != null ? data.electionCount : "-");

  // Last election time
  if (data.lastElectionTime > 0) {
    var elapsed = Date.now() - data.lastElectionTime;
    $("#metricLastElection").text(formatDuration(elapsed) + " ago");
  } else {
    $("#metricLastElection").text("none");
  }

  // Uptime
  if (data.uptime != null && data.uptime > 0) {
    $("#metricUptime").text(formatDuration(data.uptime));
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
