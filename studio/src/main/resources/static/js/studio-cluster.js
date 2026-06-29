var clusterRefreshTimer = null;
// Last cluster status payload, kept so on-demand actions (e.g. the leadership peer picker) can read the
// current peer list without re-fetching.
var clusterLastData = null;

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
  clusterLastData = data;

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

  // Cluster-level health alerts (e.g. single-bucket types serializing concurrent writes). The
  // section hides itself when the cluster reports no active alerts.
  renderClusterAlerts(data);

  // Node cards
  renderNodeCards(data);

  // Peer management list
  renderPeerManagement(data);

  // Per-database bootstrap baselines (issue #4147 phase 7). Only renders content when at least
  // one database has a committed bootstrap entry; otherwise the section stays empty so older
  // clusters look identical to before.
  renderBootstrapBaselines(data);

  // Emergency recovery controls (resync a diverged database from the leader). Only shown on a
  // follower; the leader holds the authoritative copy and has nothing to resync.
  renderResyncRecovery(data);

  // Leadership controls (transfer / step down) and per-database consistency verification (issue #4727).
  renderLeadershipControls(data);
  renderVerifyConsistency(data);

  // Update metrics summary
  updateMetricsSummary(data);
}

// Renders the cluster health alerts returned by GET /api/v1/cluster ("alerts" array). Each alert is
// a diagnostic with a severity, a headline, an explanation and a concrete remediation. The whole
// section is hidden when there are no active alerts, so a healthy cluster looks unchanged.
function renderClusterAlerts(data) {
  var row = $("#clusterAlertsRow");
  var container = $("#clusterAlerts");
  if (container.length === 0)
    return; // Older cluster.html without the alerts section; degrade silently.

  container.empty();

  var alerts = data.alerts || [];
  if (alerts.length === 0) {
    row.hide();
    return;
  }
  row.show();

  for (var i = 0; i < alerts.length; i++) {
    var a = alerts[i];
    var severity = a.severity || "warning";
    var bsClass = severity === "critical" ? "alert-danger"
      : (severity === "info" ? "alert-info" : "alert-warning");
    var icon = severity === "critical" ? "fa-circle-exclamation"
      : (severity === "info" ? "fa-circle-info" : "fa-triangle-exclamation");

    var html = '<div class="alert ' + bsClass + ' py-2 px-3 mb-2" role="alert" style="font-size: 0.85rem;">'
      + '<div class="d-flex align-items-start gap-2">'
      + '<i class="fa-solid ' + icon + ' mt-1"></i>'
      + '<div class="flex-grow-1">'
      + '<div style="font-weight: 600;">' + escapeHtml(a.title || "Alert") + '</div>';

    if (a.message)
      html += '<div class="mt-1">' + escapeHtml(a.message) + '</div>';
    if (a.recommendation)
      html += '<div class="mt-1"><span class="text-muted">Recommendation:</span> ' + escapeHtml(a.recommendation) + '</div>';

    html += renderAlertDetails(a) + '</div></div></div>';
    container.append(html);
  }
}

// Renders the optional per-alert "details" payload. Currently understands the single-bucket-types
// shape ({ databases: { dbName: [typeName, ...] } }); unknown shapes are ignored so new server-side
// checks never break an older Studio.
function renderAlertDetails(a) {
  var details = a.details;
  if (!details)
    return "";

  // Lagging-follower alert (issue #4812): name each slow node with its lag and how long it has been
  // lagging, so the alert is actionable on its own without cross-referencing the node cards.
  if (details.nodes) {
    var nodeHtml = "";
    for (var n = 0; n < details.nodes.length; n++) {
      var node = details.nodes[n];
      var pieces = ["lag=" + escapeHtml(String(node.replicationLag))];
      if (node.laggingForMs != null && node.laggingForMs > 0)
        pieces.push("for " + escapeHtml(formatClusterDuration(node.laggingForMs)));
      if (node.lastContactMs != null)
        pieces.push("heartbeat " + escapeHtml(formatClusterDuration(node.lastContactMs)));
      nodeHtml += '<div class="mt-1"><code>' + escapeHtml(node.peerId) + '</code> '
        + '<span class="badge bg-' + (node.status === "STALLED" ? "danger" : "warning") + '" style="font-size:0.6rem;">'
        + escapeHtml(node.status) + '</span> '
        + '<span class="text-muted">' + pieces.join(", ") + '</span></div>';
    }
    return nodeHtml;
  }

  if (!details.databases)
    return "";

  var html = "";
  var dbs = details.databases;
  for (var dbName in dbs) {
    if (!Object.prototype.hasOwnProperty.call(dbs, dbName))
      continue;
    var types = dbs[dbName] || [];
    if (types.length === 0)
      continue;
    var escaped = [];
    for (var i = 0; i < types.length; i++)
      escaped.push('<code>' + escapeHtml(types[i]) + '</code>');
    html += '<div class="mt-1"><span class="text-muted">' + escapeHtml(dbName) + ':</span> '
      + escaped.join(", ") + '</div>';
  }
  return html;
}

// Renders one row per database that has a committed bootstrap baseline. Surfaces lastTxId and
// the abbreviated fingerprint so an operator sees at a glance which databases were locally
// bootstrapped vs which still need a leader-shipped catch-up. Issue #4147 phase 7.
function renderBootstrapBaselines(data) {
  var container = $("#clusterBootstrapBaselines");
  var card = $("#clusterBootstrapBaselinesCard");
  if (container.length === 0)
    return; // Older cluster.html that hasn't been updated yet; degrade silently.
  container.empty();

  var dbs = (data.databases || []).filter(function(db) {
    return db.bootstrapLastTxId != null;
  });
  if (dbs.length === 0) {
    card.hide();
    return;
  }
  card.show();

  for (var i = 0; i < dbs.length; i++) {
    var db = dbs[i];
    var fpAbbrev = abbreviateFingerprint(db.bootstrapFingerprint);
    container.append(
      '<div class="d-flex align-items-center justify-content-between py-1 px-2 mb-1" '
      + 'style="font-size:0.82rem; background:var(--bg-main); border-radius:6px; border:1px solid var(--border-light);">'
      + '<div><i class="fa fa-database" style="color:var(--color-brand); margin-right:6px;"></i>'
      + escapeHtml(db.name)
      + ' <span class="badge bg-info" style="font-size:0.6rem;">lastTxId=' + db.bootstrapLastTxId + '</span>'
      + ' <span style="color:var(--text-secondary); font-size:0.75rem; font-family:monospace;">'
      + escapeHtml(fpAbbrev) + '</span>'
      + '</div>'
      + '</div>'
    );
  }
}

// Renders one "Resync from Leader" button per database on a follower node. The button POSTs to the
// local node's resync endpoint, which drops the local copy and re-downloads a full snapshot from the
// leader. Hidden entirely on the leader (nothing to resync) and when no databases are present.
function renderResyncRecovery(data) {
  var card = $("#clusterResyncCard");
  var container = $("#clusterResyncList");
  if (card.length === 0)
    return; // Older cluster.html that hasn't been updated yet; degrade silently.
  container.empty();

  var isFollower = data.isLeader === false && data.leaderId != null && data.leaderId !== "";
  var dbs = data.databases || [];
  if (!isFollower || dbs.length === 0) {
    card.hide();
    return;
  }
  card.show();

  for (var i = 0; i < dbs.length; i++) {
    var name = dbs[i].name;
    var nameAttr = JSON.stringify(name).replace(/"/g, "&quot;");
    container.append(
      '<div class="d-flex align-items-center justify-content-between py-1 px-2 mb-1" '
      + 'style="font-size:0.82rem; background:var(--bg-main); border-radius:6px; border:1px solid var(--border-light);">'
      + '<div><i class="fa fa-database" style="color:var(--color-brand); margin-right:6px;"></i>'
      + escapeHtml(name) + '</div>'
      + '<div><button class="btn btn-sm btn-outline-danger" style="font-size:0.7rem; padding:1px 8px;" '
      + 'onclick="resyncDatabase(' + nameAttr + ')" title="Drop this node\'s copy and re-acquire it from the leader">'
      + '<i class="fa fa-sync-alt"></i> Resync from Leader</button></div>'
      + '</div>'
    );
  }
}

// Confirms then triggers an emergency resync of the given database on the local node.
function resyncDatabase(databaseName) {
  globalConfirm("Resync from Leader",
    "Drop this node's local copy of <b>" + escapeHtml(databaseName) + "</b> and re-download a fresh "
    + "full snapshot from the leader?<br><br>"
    + "<span style='font-size:0.8rem;color:var(--text-secondary);'>"
    + "Use this only when this follower has diverged from the leader. The local copy is replaced by "
    + "the leader's authoritative data; any local-only changes on this node are discarded. The cluster "
    + "keeps serving from the leader and the other followers while this runs."
    + "</span>",
    "warning",
    function() {
      globalNotify("Resync", "Resyncing '" + databaseName + "' from leader, this may take a while...", "info");
      jQuery.ajax({
        type: "POST",
        url: "api/v1/cluster/resync/" + encodeURIComponent(databaseName),
        beforeSend: function(xhr) { xhr.setRequestHeader("Authorization", globalCredentials); }
      })
      .done(function() {
        globalNotify("Success", "Database '" + databaseName + "' resynced from leader", "success");
        updateCluster();
      })
      .fail(function(jqXHR) { globalNotifyError(jqXHR.responseText); });
    });
}

// ==================== LEADERSHIP CONTROLS (issue #4727) ====================

// Shows the transfer-leadership / step-down actions on the leader; followers see a hint instead.
function renderLeadershipControls(data) {
  var card = $("#clusterLeadershipCard");
  if (card.length === 0)
    return; // Older cluster.html; degrade silently.
  var isLeader = data.isLeader === true;
  $("#clusterLeadershipActions").toggle(isLeader);
  $("#clusterLeadershipHint").toggle(!isLeader);
}

// Opens a peer picker and transfers leadership to the chosen peer (POST /api/v1/cluster/leader).
function transferLeadershipPrompt() {
  var data = clusterLastData || {};
  var peers = data.peers || [];
  var localPeerId = data.localPeerId;
  var options = "";
  for (var i = 0; i < peers.length; i++) {
    var id = peers[i].id;
    if (id === localPeerId)
      continue; // cannot transfer to self (the current leader)
    options += '<option value="' + escapeHtml(id) + '">' + escapeHtml(id) + '</option>';
  }
  if (options === "") {
    globalNotify("Transfer leadership", "No other peer is available to receive leadership", "warning");
    return;
  }
  var html =
    '<div class="mb-2">'
    + '<label for="transferPeerId" class="form-label" style="font-size:0.85rem;">Target peer</label>'
    + '<select class="form-select" id="transferPeerId">' + options + '</select>'
    + '</div>';
  globalPrompt("Transfer Leadership", html, "Transfer", function(values) {
    // globalPrompt only captures input/textarea values, not <select>, so read the select directly (the
    // convention used elsewhere in Studio, e.g. studio-database.js).
    var peerId = $("#transferPeerId").val();
    if (!peerId) return;
    globalNotify("Transfer leadership", "Transferring leadership to '" + peerId + "'...", "info");
    jQuery.ajax({
      type: "POST",
      url: "api/v1/cluster/leader",
      data: JSON.stringify({ peerId: peerId }),
      contentType: "application/json",
      beforeSend: function(xhr) { xhr.setRequestHeader("Authorization", globalCredentials); }
    })
    .done(function() {
      globalNotify("Success", "Leadership transferred to '" + peerId + "'", "success");
      setTimeout(function() { updateCluster(); }, 2000);
    })
    .fail(function(jqXHR) { globalNotifyError(jqXHR.responseText); });
  });
}

// Steps the local leader down (POST /api/v1/cluster/stepdown); the cluster elects a new leader.
function stepDownLeader() {
  globalConfirm("Step Down",
    "Relinquish leadership on this node? The cluster will elect a new leader.",
    "warning",
    function() {
      jQuery.ajax({
        type: "POST",
        url: "api/v1/cluster/stepdown",
        beforeSend: function(xhr) { xhr.setRequestHeader("Authorization", globalCredentials); }
      })
      .done(function() {
        globalNotify("Success", "Leadership step-down initiated", "success");
        setTimeout(function() { updateCluster(); }, 2000);
      })
      .fail(function(jqXHR) { globalNotifyError(jqXHR.responseText); });
    });
}

// ==================== CONSISTENCY VERIFICATION (issue #4727) ====================

// On the leader, lists every database with a "Verify" button that fans out a per-file checksum comparison.
function renderVerifyConsistency(data) {
  var card = $("#clusterVerifyCard");
  var container = $("#clusterVerifyList");
  if (card.length === 0)
    return;
  container.empty();

  var isLeader = data.isLeader === true;
  var dbs = data.databases || [];
  if (!isLeader || dbs.length === 0) {
    card.hide();
    return;
  }
  card.show();

  for (var i = 0; i < dbs.length; i++) {
    var name = dbs[i].name;
    var nameAttr = JSON.stringify(name).replace(/"/g, "&quot;");
    container.append(
      '<div class="d-flex align-items-center justify-content-between py-1 px-2 mb-1" '
      + 'style="font-size:0.82rem; background:var(--bg-main); border-radius:6px; border:1px solid var(--border-light);">'
      + '<div><i class="fa fa-database" style="color:var(--color-brand); margin-right:6px;"></i>'
      + escapeHtml(name) + '</div>'
      + '<div><button class="btn btn-sm btn-outline-secondary" style="font-size:0.7rem; padding:1px 8px;" '
      + 'onclick="verifyDatabase(' + nameAttr + ')" title="Compare this database\'s checksums across all nodes">'
      + '<i class="fa fa-check-double"></i> Verify</button></div>'
      + '</div>'
    );
  }
}

// Runs POST /api/v1/cluster/verify/{db} and shows the per-peer result.
function verifyDatabase(databaseName) {
  globalNotify("Verify", "Verifying '" + databaseName + "' across the cluster...", "info");
  jQuery.ajax({
    type: "POST",
    url: "api/v1/cluster/verify/" + encodeURIComponent(databaseName),
    beforeSend: function(xhr) { xhr.setRequestHeader("Authorization", globalCredentials); }
  })
  .done(function(data) {
    var result = (data && data.result) ? data.result : data;
    var status = result.overallStatus || "UNKNOWN";
    var ok = status === "ALL_CONSISTENT";
    var html = '<div style="font-size:0.85rem;">'
      + '<div class="mb-2"><b>Status:</b> <span class="badge ' + (ok ? "bg-success" : "bg-danger") + '">'
      + escapeHtml(status) + '</span></div>';
    var peers = result.peers || [];
    if (peers.length > 0) {
      html += '<table class="table table-sm" style="font-size:0.8rem;"><thead><tr><th>Peer</th><th>Status</th></tr></thead><tbody>';
      for (var i = 0; i < peers.length; i++) {
        var p = peers[i];
        var pStatus = p.status || "-";
        var badge = pStatus === "CONSISTENT" ? "bg-success" : (pStatus === "ERROR" ? "bg-secondary" : "bg-danger");
        html += '<tr><td>' + escapeHtml(p.peerId || p.httpAddress || "-") + '</td>'
          + '<td><span class="badge ' + badge + '">' + escapeHtml(pStatus) + '</span></td></tr>';
      }
      html += '</tbody></table>';
    }
    html += '</div>';
    globalAlert("Consistency: " + escapeHtml(databaseName), html, ok ? "success" : "warning");
  })
  .fail(function(jqXHR) { globalNotifyError(jqXHR.responseText); });
}

// ==================== PRESENCE MATRIX (issue #4727) ====================

// Loads the per-database x per-node presence matrix from the leader (on demand, not on the auto-poll).
function loadPresenceMatrix() {
  jQuery.ajax({
    type: "GET",
    url: "api/v1/cluster?presence=true",
    beforeSend: function(xhr) { xhr.setRequestHeader("Authorization", globalCredentials); }
  })
  .done(function(data) { renderPresenceMatrix(data); })
  .fail(function(jqXHR) { globalNotifyError(jqXHR.responseText); });
}

function renderPresenceMatrix(data) {
  var container = $("#clusterPresenceMatrix");
  if (container.length === 0)
    return;
  container.empty();

  var presence = data.databasePresence;
  if (!presence) {
    container.append('<div style="font-size:0.8rem;color:var(--text-secondary);">'
      + 'The presence matrix is computed on the leader. Open this page on the leader node and click '
      + '"Load presence matrix".</div>');
    return;
  }

  var nodes = presence.nodes || [];
  var dbs = presence.databases || [];
  var unreachable = presence.unreachable || [];

  var html = '<div class="table-responsive"><table class="table table-sm table-bordered" style="font-size:0.8rem;">';
  html += '<thead><tr><th>Database</th>';
  for (var n = 0; n < nodes.length; n++) {
    var isUnreachable = unreachable.indexOf(nodes[n]) >= 0;
    html += '<th title="' + escapeHtml(nodes[n]) + '">' + escapeHtml(nodes[n])
      + (isUnreachable ? ' <span class="badge bg-secondary">unreachable</span>' : '') + '</th>';
  }
  html += '</tr></thead><tbody>';

  for (var d = 0; d < dbs.length; d++) {
    var db = dbs[d];
    var present = db.present || [];
    html += '<tr><td><i class="fa fa-database" style="color:var(--color-brand); margin-right:6px;"></i>'
      + escapeHtml(db.name) + '</td>';
    for (var c = 0; c < nodes.length; c++) {
      var has = present.indexOf(nodes[c]) >= 0;
      var unreach = unreachable.indexOf(nodes[c]) >= 0;
      if (unreach)
        html += '<td class="text-center" style="color:var(--text-secondary);">?</td>';
      else if (has)
        html += '<td class="text-center" style="color:#16a34a;"><i class="fa fa-check"></i></td>';
      else
        html += '<td class="text-center" style="color:#dc2626;"><i class="fa fa-times"></i></td>';
    }
    html += '</tr>';
  }
  html += '</tbody></table></div>';
  container.append(html);
}

function abbreviateFingerprint(fp) {
  if (!fp || fp.length <= 16) return fp || "";
  return fp.substring(0, 8) + "..." + fp.substring(fp.length - 8);
}

// Maps the server-side ClusterMonitor.ReplicaStatus enum to a Bootstrap badge style and a status
// dot color. STALLED is the pre-churn signal so it gets red/danger; HEALTHY stays green.
function replicaStatusStyle(status) {
  switch (status) {
    case "HEALTHY":        return { badge: "bg-success",   dot: "limegreen" };
    case "CATCHING_UP":    return { badge: "bg-info",      dot: "deepskyblue" };
    case "FALLING_BEHIND": return { badge: "bg-warning",   dot: "orange" };
    case "STALLED":        return { badge: "bg-danger",    dot: "crimson" };
    default:               return { badge: "bg-secondary", dot: "limegreen" }; // UNKNOWN / leader / no data
  }
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

    // replicaStatus is only sent for followers and only by a leader's status export.
    // A leader card has no status badge (its color comes from the LEADER role badge already).
    var statusStyle = replicaStatusStyle(peer.replicaStatus);
    var statusBadge = (!isLeader && peer.replicaStatus)
      ? ' <span class="badge ' + statusStyle.badge + '" style="font-size:0.6rem;">' + escapeHtml(peer.replicaStatus) + '</span>'
      : '';

    var dotColor = isLeader ? "limegreen" : statusStyle.dot;
    var statusDot = '<span style="display:inline-block; width:8px; height:8px; border-radius:50%; background:' + dotColor + '; margin-right:6px;"></span>';

    // Per-follower replication health is present only when the local node is the leader (it's the
    // leader who tracks it). Show lag, heartbeat latency and how long the node has been lagging inline
    // so a constantly-slow follower is obvious at a glance (issue #4812).
    var lagLine = "";
    if (!isLeader && peer.matchIndex != null) {
      var parts = ["matchIndex=" + escapeHtml(String(peer.matchIndex))];
      if (peer.replicationLag != null)
        parts.push("lag=" + escapeHtml(String(peer.replicationLag)));
      if (peer.lastContactMs != null)
        parts.push("heartbeat=" + escapeHtml(formatClusterDuration(peer.lastContactMs)));
      lagLine = '<div style="font-size:0.72rem; color:var(--text-secondary); margin-top:2px;">'
        + '<i class="fas fa-tachometer-alt" style="margin-right:4px;"></i>' + parts.join(", ")
        + '</div>';
      // Only show a "lagging for" line once the node has actually been non-healthy for a while, so a
      // healthy follower's card stays clean.
      if (peer.laggingForMs != null && peer.laggingForMs > 0) {
        var lagColor = peer.replicaStatus === "STALLED" ? "crimson" : "orange";
        lagLine += '<div style="font-size:0.72rem; color:' + lagColor + '; margin-top:2px;">'
          + '<i class="fas fa-hourglass-half" style="margin-right:4px;"></i>lagging for '
          + escapeHtml(formatClusterDuration(peer.laggingForMs))
          + '</div>';
      }
    }

    var card = '<div class="' + colClass + '">'
      + '<div class="card h-100" style="border: 2px solid ' + borderColor + '; border-radius: 10px;">'
      + '<div class="card-body py-2 px-3">'
      + '<div class="d-flex align-items-center justify-content-between">'
      + '<div>' + statusDot + '<b style="font-size:0.85rem;">' + escapeHtml(peer.id) + '</b>' + localBadge + '</div>'
      + '<div>' + roleBadge + statusBadge + '</div>'
      + '</div>'
      + '<div style="font-size:0.78rem; color:var(--text-secondary); margin-top:4px;">'
      + '<i class="fas fa-network-wired" style="margin-right:4px;"></i>' + escapeHtml(peer.address || "")
      + '</div>'
      + lagLine
      + '</div></div></div>';

    container.append(card);
  }
}

function renderPeerManagement(data) {
  var container = $("#peerManagementList");
  container.empty();

  // addPeer / removePeer issue Raft setConfiguration, which only the leader can apply. Hide the
  // controls on followers and surface a small hint instead, so the buttons never produce a 500.
  var canManage = data.isLeader === true;
  $("#btnAddPeer").toggle(canManage);
  $("#peerManagementHint").toggle(!canManage);

  var peers = data.peers || [];
  for (var i = 0; i < peers.length; i++) {
    var peer = peers[i];
    var isLeader = peer.role === "LEADER";
    var isLocal = peer.id === data.localPeerId;
    var roleBadge = isLeader
      ? '<span class="badge bg-success" style="font-size:0.65rem;">LEADER</span>'
      : '<span class="badge bg-secondary" style="font-size:0.65rem;">FOLLOWER</span>';
    var localTag = isLocal ? ' <span class="badge bg-info" style="font-size:0.6rem;">LOCAL</span>' : '';
    var statusBadge = (!isLeader && peer.replicaStatus)
      ? ' <span class="badge ' + replicaStatusStyle(peer.replicaStatus).badge + '" style="font-size:0.6rem;">' + escapeHtml(peer.replicaStatus) + '</span>'
      : '';

    // Remove button only when local is leader and target is a follower. The leader cannot remove
    // itself: it must step down (or use Leave Cluster) so the resulting Raft config is valid.
    // JSON.stringify + &quot; escaping keeps any characters in peer.id safe inside the attribute.
    var removeBtn = "";
    if (canManage && !isLeader) {
      var idAttr = JSON.stringify(peer.id).replace(/"/g, "&quot;");
      removeBtn = '<button class="btn btn-sm btn-outline-danger" style="font-size:0.7rem; padding:1px 8px;" '
        + 'onclick="removePeer(' + idAttr + ')" title="Remove peer from the cluster">'
        + '<i class="fa fa-trash"></i></button>';
    }

    container.append(
      '<div class="d-flex align-items-center justify-content-between py-1 px-2 mb-1" '
      + 'style="font-size:0.82rem; background:var(--bg-main); border-radius:6px; border:1px solid var(--border-light);">'
      + '<div><i class="fa fa-server" style="color:var(--color-brand); margin-right:6px;"></i>'
      + escapeHtml(peer.id) + ' <span style="color:var(--text-secondary); font-size:0.75rem;">(' + escapeHtml(peer.address || "") + ')</span>'
      + localTag + ' ' + roleBadge + statusBadge + '</div>'
      + '<div>' + removeBtn + '</div>'
      + '</div>'
    );
  }
}

function addPeerPrompt() {
  var html =
    '<div class="mb-2">'
    + '<label for="addPeerId" class="form-label" style="font-size:0.85rem;">Peer ID</label>'
    + '<input type="text" class="form-control" id="addPeerId" placeholder="e.g. ArcadeDB_2" '
    + 'onkeydown="if (event.which === 13) document.getElementById(\'addPeerAddress\').focus()">'
    + '</div>'
    + '<div class="mb-2">'
    + '<label for="addPeerAddress" class="form-label" style="font-size:0.85rem;">Raft Address</label>'
    + '<input type="text" class="form-control" id="addPeerAddress" placeholder="host:port (e.g. 192.168.1.2:2434)" '
    + 'onkeydown="if (event.which === 13) document.getElementById(\'globalModalConfirmBtn\').click()">'
    + '</div>'
    + '<div class="mb-2">'
    + '<label for="addPeerName" class="form-label" style="font-size:0.85rem;">Friendly Name <span class="text-muted">(optional)</span></label>'
    + '<input type="text" class="form-control" id="addPeerName" placeholder="Optional display name" '
    + 'onkeydown="if (event.which === 13) document.getElementById(\'globalModalConfirmBtn\').click()">'
    + '</div>'
    + '<p class="text-muted" style="font-size:0.75rem; margin-bottom:0;">'
    + 'The new peer must already be running and reachable on the Raft port.'
    + '</p>';

  globalPrompt("Add Peer", html, "Add", function(values) {
    var peerId = (values["addPeerId"] || "").trim();
    var address = (values["addPeerAddress"] || "").trim();
    var name = (values["addPeerName"] || "").trim();

    if (!peerId) {
      globalNotify("Error", "Peer ID is required", "danger");
      return;
    }
    if (!address) {
      globalNotify("Error", "Raft address is required", "danger");
      return;
    }

    var payload = { peerId: peerId, address: address };
    if (name) payload.name = name;

    jQuery.ajax({
      type: "POST",
      url: "api/v1/cluster/peer",
      data: JSON.stringify(payload),
      contentType: "application/json",
      beforeSend: function(xhr) { xhr.setRequestHeader("Authorization", globalCredentials); }
    })
    .done(function() {
      globalNotify("Success", "Peer " + peerId + " added", "success");
      updateCluster();
    })
    .fail(function(jqXHR) { globalNotifyError(jqXHR.responseText); });
  });
}

function removePeer(peerId) {
  globalConfirm("Remove Peer",
    "Remove peer <b>" + escapeHtml(peerId) + "</b> from the cluster?<br><br>"
    + "<span style='font-size:0.8rem;color:var(--text-secondary);'>"
    + "The peer will stop receiving replicated entries. Restarting it will not rejoin the cluster automatically."
    + "</span>",
    "warning",
    function() {
      jQuery.ajax({
        type: "DELETE",
        url: "api/v1/cluster/peer/" + encodeURIComponent(peerId),
        beforeSend: function(xhr) { xhr.setRequestHeader("Authorization", globalCredentials); }
      })
      .done(function() {
        globalNotify("Success", "Peer " + peerId + " removed", "success");
        updateCluster();
      })
      .fail(function(jqXHR) { globalNotifyError(jqXHR.responseText); });
    });
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

// Like formatDuration but keeps sub-second precision, used for heartbeat latency and short lag
// spells where "0s" would be misleading (issue #4812).
function formatClusterDuration(ms) {
  if (ms == null) return "";
  if (ms < 1000) return Math.round(ms) + "ms";
  return formatDuration(ms);
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
