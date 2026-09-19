// Security panel state
//
// Note on securityClusterStatus below: it starts null and its first refresh is asynchronous, so between the
// panel opening and GET /api/v1/cluster answering, the gate treats the cluster as ready and Create stays
// enabled. That window is deliberate and not a bug to be "fixed" into a loading spinner: an unknown answer
// gates nothing ANYWHERE in this feature - a follower, a standalone server and a not-yet-loaded status are
// one case - and the leader's own 409, rendered by clusterCapabilityRefusal(), is the authority that cannot
// be raced. Blocking the form on a status that may never arrive would disable Create on every standalone
// server (PR #7939 review).
var securityInitialized = false;
var usersLoaded = false;
var usersDataTable = null;
var apiTokensLoaded = false;
var apiTokensDataTable = null;
var editingUserName = null;
var groupsLoaded = false;
var groupsDataTable = null;
var groupsData = null;
var editingGroupName = null;
var editingGroupDatabase = null;
// Last GET /api/v1/cluster payload seen by this page, or null when the server is not clustered (the route is
// registered by the HA plugin, so a standalone server simply answers an error and the gate stays silent).
var securityClusterStatus = null;

function initSecurity() {
  if (!securityInitialized) {
    securityInitialized = true;
    loadUsers();
  }
  // Asked every time the panel is opened, not once: a rolling upgrade finishes WHILE Studio is open, and a gate
  // that never refreshed would keep an operator locked out of a form the cluster has since become ready for.
  refreshSecurityClusterReadiness();
}

// ==================== Cluster readiness for replicated security changes ====================

/**
 * Re-reads the cluster status and re-applies the capability gate (issues #7511, #7538, #7548).
 *
 * A group or API-token change is replicated as a Raft log entry whose TYPE a peer that predates it cannot
 * decode, and such a peer HALTS rather than skip a committed entry - so the leader refuses the whole change with
 * a 409 while any peer has not advertised the capability. That refusal is correct and names the peer, but it
 * arrives after the operator has filled in a form, usually inside a change window. This puts the same fact in
 * front of them before the click.
 *
 * Every failure is swallowed on purpose: the route belongs to the HA plugin, so a standalone server answers 404
 * or 400, and a Studio that showed an error toast for "this server is not a cluster" would be worse than one
 * that says nothing.
 */
function refreshSecurityClusterReadiness(callback) {
  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/cluster",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      securityClusterStatus = data;
    })
    .fail(function () {
      // The last successful answer is KEPT, not discarded. A refresh can fail for reasons that say nothing
      // about the cluster - a blip, a proxy, a leader election in progress - and clearing it would report
      // "nothing to gate" and re-enable a control the leader is still refusing, which is the exact failure
      // this gate exists to prevent. A standalone server is unaffected: it never had an answer to keep
      // (PR #7939 review).
    })
    .always(function () {
      renderSecurityCapabilityGate();
      if (callback) callback();
    });
}

/**
 * The capability gap that blocks `capability`, or null when nothing does.
 *
 * Delegates the decision to studio-cluster.js so the Cluster page's banner and this gate cannot drift apart -
 * including the part that matters most, that only a LEADER can answer for its peers (see
 * clusterCapabilityReadiness). A follower, an unclustered server and a cluster that is fully upgraded all
 * produce the same answer here: nothing to report, nothing disabled.
 */
function securityCapabilityGap(capability) {
  if (typeof clusterSecurityCapabilityGaps !== "function") return null;

  var gaps = clusterSecurityCapabilityGaps(securityClusterStatus);
  for (var i = 0; i < gaps.length; i++) if (gaps[i].capability === capability) return gaps[i];
  return null;
}

/** The warning an operator reads in place of the 409 they would otherwise have discovered by pressing the button. */
function securityCapabilityBanner(gap) {
  if (!gap) return "";

  var peers = "";
  for (var i = 0; i < gap.missing.length; i++) {
    peers += "<li><b>" + escapeHtml(gap.missing[i].id) + "</b>: " + escapeHtml(gap.missing[i].reason) + "</li>";
  }

  return (
    '<div class="alert alert-warning py-2 px-3 mb-3" style="font-size:0.82rem;">' +
    '<div><i class="fa fa-exclamation-triangle" style="margin-right:6px;"></i><b>The cluster is not ready for ' +
    escapeHtml(gap.what) +
    ".</b> They are replicated as a <code>" +
    escapeHtml(gap.capability) +
    "</code> entry, and a peer that cannot decode one halts rather than skip it, so the leader refuses the change " +
    "instead of writing it.</div>" +
    '<ul class="mb-1 mt-1">' +
    peers +
    "</ul>" +
    "<div>Finish the rolling upgrade - or restore contact with those peers - and reissue the change; it succeeds " +
    "unchanged, with no sequencing by hand.</div>" +
    "</div>"
  );
}

/**
 * Paints the banner on the Groups and API Tokens tabs and disables the Create buttons while the cluster cannot
 * accept the change. Deletion is NOT disabled, and that is deliberate: a revocation is refused by the same gate,
 * but taking the control away would leave an operator unable even to try, while the banner above the table
 * already says why it would fail - and the 409 is now rendered as an explanation rather than a raw toast.
 */
function renderSecurityCapabilityGate() {
  var gates = [
    { capability: "security-groups-entry", banner: "#groupsCapabilityGate", button: "#btnCreateGroup" },
    { capability: "security-api-tokens-entry", banner: "#tokensCapabilityGate", button: "#btnCreateToken" },
  ];

  for (var i = 0; i < gates.length; i++) {
    var gate = gates[i];
    var gap = securityCapabilityGap(gate.capability);

    $(gate.banner).html(securityCapabilityBanner(gap));
    $(gate.button)
      .prop("disabled", gap != null)
      .attr("title", gap ? "The cluster is not ready: " + gap.missing.length + " peer(s) have not finished upgrading" : null);
  }
}

/**
 * The same gate inside the Create/Edit Group modal, and on its Save button.
 *
 * Needed as well as the tab-level one because Edit Group is reachable from the table while Create Group is
 * disabled, and an EDIT is replicated by the very same entry a create is - so a form that looked usable would
 * hand back a 409 after the permissions had been retyped.
 */
function applyGroupModalCapabilityGate() {
  var gap = securityCapabilityGap("security-groups-entry");
  $("#groupModalCapabilityGate").html(securityCapabilityBanner(gap));
  $("#groupModalSaveBtn").prop("disabled", gap != null);
}

/**
 * Maps the leader's refusal to replicate a security change onto something worth reading (issues #7538, #7548).
 * Returns null for every other failure, which leaves 400/403/412/500 rendering through globalNotifyError()
 * exactly as before.
 *
 * The 409 needs its own handling for the same reason the 412 of issue #7804 did: globalNotifyError() renders
 * json.error as the toast TITLE and json.detail as the body, and in production mode the server CONCEALS detail -
 * so what reaches the operator is the bare heading "Cluster is not ready for this operation" with nothing to act
 * on. The peers ride in exceptionArgs precisely because that field survives production mode, and this is the
 * only thing in Studio that reads them.
 */
function clusterCapabilityRefusal(jqXHR) {
  if (!jqXHR || jqXHR.status !== 409) return null;

  var json;
  try {
    json = JSON.parse(jqXHR.responseText);
  } catch (e) {
    return null; // Not the server's JSON body: some other 409, or a proxy's error page.
  }
  if (!json || json.exception !== "com.arcadedb.server.ClusterCapabilityNotReadyException") return null;

  // "<capability>|<peer>,<peer>,+N more" - bounded on purpose, and the only half production mode still emits.
  var args = typeof json.exceptionArgs === "string" ? json.exceptionArgs : "";
  var separator = args.indexOf("|");
  var capability = separator >= 0 ? args.substring(0, separator) : args;
  var peers = separator >= 0 ? args.substring(separator + 1) : "";

  var message = "The leader refused this change because ";
  message += peers ? "peer(s) " + peers + " have" : "at least one peer has";
  message += " not advertised the '" + (capability || "required") + "' capability, so they cannot decode the entry " +
    "it would be replicated as and would halt on applying it. Nothing was submitted, so nothing has changed " +
    "anywhere in the cluster. Finish the rolling upgrade - or restore contact with those peers - and reissue it.";

  if (json.detail) message += " Server: " + json.detail;

  return { title: "Cluster is not ready for this change", message: message };
}

// ==================== Users & Permissions ====================

function loadUsers() {
  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/server/users",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      usersLoaded = true;
      renderUsersTable(data.result || []);
    })
    .fail(function (jqXHR) {
      globalNotifyError(jqXHR.responseText);
    });
}

function renderUsersTable(users) {
  if (usersDataTable) {
    usersDataTable.destroy();
    usersDataTable = null;
  }

  var tableData = [];
  for (var i = 0; i < users.length; i++) {
    var u = users[i];
    var databases = u.databases || {};
    var dbKeys = Object.keys(databases);

    // Build a readable summary of database access
    var dbSummary = "";
    for (var j = 0; j < dbKeys.length; j++) {
      var dbName = dbKeys[j];
      var groups = databases[dbName];
      if (Array.isArray(groups))
        groups = groups.join(", ");
      else
        groups = String(groups);

      if (j > 0) dbSummary += "<br>";
      dbSummary += '<code>' + escapeHtml(dbName) + '</code>: <span class="badge bg-secondary">' + escapeHtml(groups) + '</span>';
    }
    if (!dbSummary) dbSummary = '<span class="text-muted">No access</span>';

    tableData.push([u.name, dbSummary, u.name]);
  }

  usersDataTable = $("#securityUsersTable").DataTable({
    paging: false,
    searching: false,
    data: tableData,
    columns: [
      { title: "Username", width: "180px" },
      { title: "Database Access" },
      {
        title: "Actions",
        width: "120px",
        orderable: false,
        render: function (data, type, row) {
          var name = data;
          var html = '<button class="btn btn-sm btn-outline-primary me-1" data-action="edit-user" data-name="' + escapeHtml(name) + '"><i class="fa fa-edit"></i></button>';
          if (name !== "root")
            html += '<button class="btn btn-sm btn-outline-danger" data-action="delete-user" data-name="' + escapeHtml(name) + '"><i class="fa fa-trash"></i></button>';
          return html;
        },
      },
    ],
  });
}

function showCreateUserForm() {
  editingUserName = null;
  $("#userModalTitle").text("Create User");
  $("#userModalSaveBtn").text("Create");
  $("#userName").val("").prop("disabled", false);
  $("#userPassword").val("").attr("placeholder", "Min 4 characters");
  $("#userDatabasesTable tbody tr:not([data-db='*'])").remove();

  ensureGroupsLoaded(function () {
    populateWildcardGroupSelect("admin");
    loadDatabasesForUserForm();
    new bootstrap.Modal(document.getElementById("createUserModal")).show();
  });
}

function editUser(name) {
  editingUserName = name;
  $("#userModalTitle").text("Edit User: " + name);
  $("#userModalSaveBtn").text("Save");
  $("#userName").val(name).prop("disabled", true);
  $("#userPassword").val("").attr("placeholder", "Leave blank to keep current");
  $("#userDatabasesTable tbody tr:not([data-db='*'])").remove();

  // Load the available groups first so every assigned group (including custom ones) can be
  // pre-selected in the per-database selects, then load the user data.
  ensureGroupsLoaded(function () {
    jQuery
      .ajax({
        type: "GET",
        url: "api/v1/server/users",
        beforeSend: function (xhr) {
          xhr.setRequestHeader("Authorization", globalCredentials);
        },
      })
      .done(function (data) {
        var users = data.result || [];
        var userData = null;
        for (var i = 0; i < users.length; i++) {
          if (users[i].name === name) {
            userData = users[i];
            break;
          }
        }

        if (!userData) return;

        var databases = userData.databases || {};
        var dbKeys = Object.keys(databases);

        // Set the * row
        var hasWildcard = false;
        for (var j = 0; j < dbKeys.length; j++) {
          var dbName = dbKeys[j];
          var groups = databases[dbName];
          if (!Array.isArray(groups)) groups = [groups];

          if (dbName === "*") {
            hasWildcard = true;
            populateWildcardGroupSelect(groups[0] || "admin");
          } else {
            appendDatabaseRow(dbName, groups[0] || "admin");
          }
        }

        if (!hasWildcard)
          populateWildcardGroupSelect("*");

        loadDatabasesForUserForm();
        new bootstrap.Modal(document.getElementById("createUserModal")).show();
      });
  });
}

// Fetches the latest group definitions so the user form can offer every assignable group
// (including custom, per-database groups) instead of a hardcoded admin/* list. See issue #4638.
function ensureGroupsLoaded(callback) {
  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/server/groups",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      groupsData = data.result || {};
      callback();
    })
    .fail(function () {
      // Fall back to whatever is already cached (or hardcoded defaults) so the form still opens
      callback();
    });
}

// Returns the group names assignable for a database: the default groups defined under "*"
// (e.g. admin) plus any groups defined specifically for that database.
function getGroupNamesForDatabase(dbName) {
  var names = [];
  var databases = (groupsData && groupsData.databases) ? groupsData.databases : {};

  var collect = function (entry) {
    if (!entry || !entry.groups) return;
    var keys = Object.keys(entry.groups);
    for (var i = 0; i < keys.length; i++)
      if (names.indexOf(keys[i]) < 0) names.push(keys[i]);
  };

  // Default groups under "*" apply to every database
  collect(databases["*"]);

  // Database-specific groups
  if (dbName && dbName !== "*")
    collect(databases[dbName]);

  // Fallback when the group list could not be loaded
  if (names.length === 0) {
    names.push("admin");
    names.push("*");
  }

  return names;
}

// Builds the <option> list for a user group <select>, ensuring the currently assigned group
// is always present even if it has since been removed from the group definitions.
function buildGroupOptionsHtml(dbName, selectedGroup) {
  var names = getGroupNamesForDatabase(dbName);
  if (selectedGroup && names.indexOf(selectedGroup) < 0)
    names.push(selectedGroup);

  var html = "";
  for (var i = 0; i < names.length; i++) {
    var name = names[i];
    var label = name === "*" ? "* (default)" : name;
    html += '<option value="' + escapeHtml(name) + '"' + (name === selectedGroup ? " selected" : "") + ">" + escapeHtml(label) + "</option>";
  }
  return html;
}

// Populates the static "* (all databases)" row group select with the available groups.
function populateWildcardGroupSelect(selectedGroup) {
  $("#userDatabasesTable tbody tr[data-db='*'] .user-group-select").html(buildGroupOptionsHtml("*", selectedGroup));
}

function loadDatabasesForUserForm() {
  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/databases",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      var select = $("#userNewDatabase");
      select.find("option:not(:first)").remove();
      var dbs = data.result || [];
      for (var i = 0; i < dbs.length; i++)
        select.append('<option value="' + escapeHtml(dbs[i]) + '">' + escapeHtml(dbs[i]) + "</option>");
    });
}

function appendDatabaseRow(dbName, selectedGroup) {
  var row =
    '<tr data-db="' + escapeHtml(dbName) + '">' +
    "<td><code>" + escapeHtml(dbName) + "</code></td>" +
    '<td><div class="input-group input-group-sm">' +
    '<select class="form-select user-group-select">' +
    buildGroupOptionsHtml(dbName, selectedGroup) +
    "</select></div></td>" +
    '<td><button class="btn btn-sm btn-outline-danger remove-db-row"><i class="fa fa-times"></i></button></td>' +
    "</tr>";
  $("#userDatabasesTable tbody").append(row);
}

function addUserDatabaseRow() {
  var dbName = $("#userNewDatabase").val();
  if (!dbName) return;
  if ($("#userDatabasesTable tbody tr[data-db='" + dbName + "']").length > 0) return;
  appendDatabaseRow(dbName, "admin");
  $("#userNewDatabase").val("");
}

function removeUserDatabaseRow(btn) {
  $(btn).closest("tr").remove();
}

function saveUser() {
  var name = editingUserName || $("#userName").val().trim();
  if (!name) {
    globalNotify("Error", "Username is required", "danger");
    return;
  }

  var password = $("#userPassword").val();

  // Build databases object
  var databases = {};
  $("#userDatabasesTable tbody tr").each(function () {
    var dbName = $(this).data("db");
    var group = $(this).find(".user-group-select").val();
    databases[dbName] = [group];
  });

  if (editingUserName) {
    // Update existing user
    var payload = { databases: databases };
    if (password) payload.password = password;

    jQuery
      .ajax({
        type: "PUT",
        url: "api/v1/server/users?name=" + encodeURIComponent(name),
        data: JSON.stringify(payload),
        contentType: "application/json",
        beforeSend: function (xhr) {
          xhr.setRequestHeader("Authorization", globalCredentials);
        },
      })
      .done(function () {
        bootstrap.Modal.getInstance(document.getElementById("createUserModal")).hide();
        globalNotify("Success", "User '" + name + "' updated", "success");
        loadUsers();
      })
      .fail(function (jqXHR) {
        globalNotifyError(jqXHR.responseText);
      });
  } else {
    // Create new user
    if (!password || password.length < 4) {
      globalNotify("Error", "Password must be at least 4 characters", "danger");
      return;
    }

    var payload = {
      name: name,
      password: password,
      databases: databases,
    };

    jQuery
      .ajax({
        type: "POST",
        url: "api/v1/server/users",
        data: JSON.stringify(payload),
        contentType: "application/json",
        beforeSend: function (xhr) {
          xhr.setRequestHeader("Authorization", globalCredentials);
        },
      })
      .done(function () {
        bootstrap.Modal.getInstance(document.getElementById("createUserModal")).hide();
        globalNotify("Success", "User '" + name + "' created", "success");
        loadUsers();
      })
      .fail(function (jqXHR) {
        globalNotifyError(jqXHR.responseText);
      });
  }
}

function deleteUser(name) {
  if (!confirm("Are you sure you want to delete user '" + name + "'?")) return;

  jQuery
    .ajax({
      type: "DELETE",
      url: "api/v1/server/users?name=" + encodeURIComponent(name),
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function () {
      globalNotify("Deleted", "User '" + name + "' deleted", "success");
      loadUsers();
    })
    .fail(function (jqXHR) {
      globalNotifyError(jqXHR.responseText);
    });
}

// ==================== API Tokens ====================

function loadApiTokens() {
  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/server/api-tokens",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      apiTokensLoaded = true;
      renderApiTokensTable(data.result || []);
      renderSecurityCapabilityGate();
    })
    .fail(function (jqXHR) {
      globalNotifyError(jqXHR.responseText);
    });
}

function renderApiTokensTable(tokens) {
  if (apiTokensDataTable) {
    apiTokensDataTable.destroy();
    apiTokensDataTable = null;
  }

  var tableData = [];
  for (var i = 0; i < tokens.length; i++) {
    var t = tokens[i];
    // An expired token stays in the list until the next token change retires it (issue #7601), so the row says
    // so instead of leaving the reader to compare the date with today's.
    var expiration = t.expiresAt > 0 ? new Date(t.expiresAt).toLocaleString() : "Never";
    if (t.expired)
      expiration = '<span class="text-danger">' + escapeHtml(expiration) + ' (expired)</span>';
    var created = t.createdAt > 0 ? new Date(t.createdAt).toLocaleString() : "-";
    var tokenDisplay = 'at-...' + (t.tokenSuffix ? escapeHtml(t.tokenSuffix) : '');
    tableData.push([t.name, t.database, created, expiration, '<code>' + tokenDisplay + '</code>', t.tokenHash]);
  }

  apiTokensDataTable = $("#apiTokensTable").DataTable({
    paging: false,
    searching: false,
    data: tableData,
    columns: [
      { title: "Name" },
      { title: "Database" },
      { title: "Created" },
      { title: "Expiration" },
      { title: "Token" },
      {
        title: "Actions",
        orderable: false,
        render: function (data) {
          return '<button class="btn btn-sm btn-outline-danger" data-action="delete-token" data-hash="' + escapeHtml(data) + '"><i class="fa fa-trash"></i></button>';
        },
      },
    ],
  });
}

function showCreateTokenForm() {
  $("#tokenName").val("");
  $("#tokenExpiration").val("");
  $("#tokenPermSchema").prop("checked", false);
  $("#tokenPermSettings").prop("checked", false);
  $("#tokenPermissionsTable tbody tr:not([data-type='*'])").remove();
  $("#tokenPermissionsTable tbody tr[data-type='*'] .perm-create").prop("checked", false);
  $("#tokenPermissionsTable tbody tr[data-type='*'] .perm-read").prop("checked", true);
  $("#tokenPermissionsTable tbody tr[data-type='*'] .perm-update").prop("checked", false);
  $("#tokenPermissionsTable tbody tr[data-type='*'] .perm-delete").prop("checked", false);

  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/databases",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      var select = $("#tokenDatabase");
      select.find("option:not(:first)").remove();
      var dbs = data.result || [];
      for (var i = 0; i < dbs.length; i++)
        select.append('<option value="' + escapeHtml(dbs[i]) + '">' + escapeHtml(dbs[i]) + "</option>");
    });

  $("#tokenNewType").html('<option value="">Add type...</option>');
  new bootstrap.Modal(document.getElementById("createTokenModal")).show();
}

function loadTypesForTokenDatabase() {
  var db = $("#tokenDatabase").val();
  var select = $("#tokenNewType");
  select.html('<option value="">Add type...</option>');
  if (!db || db === "*") return;

  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/query/" + encodeDatabaseName(db) + "/sql/SELECT%20name%20FROM%20schema%3Atypes",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      var records = data.result || [];
      for (var i = 0; i < records.length; i++)
        select.append('<option value="' + escapeHtml(records[i].name) + '">' + escapeHtml(records[i].name) + "</option>");
    });
}

function addTokenTypeRow() {
  var typeName = $("#tokenNewType").val();
  if (!typeName) return;
  if ($("#tokenPermissionsTable tbody tr[data-type='" + typeName + "']").length > 0) return;

  var row =
    '<tr data-type="' + escapeHtml(typeName) + '">' +
    "<td><code>" + escapeHtml(typeName) + "</code></td>" +
    '<td class="text-center"><input type="checkbox" class="form-check-input perm-create"></td>' +
    '<td class="text-center"><input type="checkbox" class="form-check-input perm-read" checked></td>' +
    '<td class="text-center"><input type="checkbox" class="form-check-input perm-update"></td>' +
    '<td class="text-center"><input type="checkbox" class="form-check-input perm-delete"></td>' +
    '<td><button class="btn btn-sm btn-outline-danger remove-token-type"><i class="fa fa-times"></i></button></td>' +
    "</tr>";
  $("#tokenPermissionsTable tbody").append(row);
  $("#tokenNewType").val("");
}

function removeTokenTypeRow(btn) {
  $(btn).closest("tr").remove();
}

function createApiToken() {
  var name = $("#tokenName").val().trim();
  if (!name) {
    globalNotify("Error", "Token name is required", "danger");
    return;
  }

  var database = $("#tokenDatabase").val();
  var expirationStr = $("#tokenExpiration").val();
  var expiresAt = 0;
  if (expirationStr)
    expiresAt = new Date(expirationStr + "T23:59:59").getTime();

  var types = {};
  $("#tokenPermissionsTable tbody tr").each(function () {
    var typeName = $(this).data("type");
    var access = [];
    if ($(this).find(".perm-create").is(":checked")) access.push("createRecord");
    if ($(this).find(".perm-read").is(":checked")) access.push("readRecord");
    if ($(this).find(".perm-update").is(":checked")) access.push("updateRecord");
    if ($(this).find(".perm-delete").is(":checked")) access.push("deleteRecord");
    types[typeName] = { access: access };
  });

  var dbPerms = [];
  if ($("#tokenPermSchema").is(":checked")) dbPerms.push("updateSchema");
  if ($("#tokenPermSettings").is(":checked")) dbPerms.push("updateDatabaseSettings");

  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/server/api-tokens",
      data: JSON.stringify({
        name: name,
        database: database,
        expiresAt: expiresAt,
        permissions: { types: types, database: dbPerms },
      }),
      contentType: "application/json",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      bootstrap.Modal.getInstance(document.getElementById("createTokenModal")).hide();
      $("#createdTokenValue").val(data.result.token);
      new bootstrap.Modal(document.getElementById("tokenCreatedModal")).show();
      loadApiTokens();
    })
    .fail(function (jqXHR) {
      // The create modal is deliberately left open on every failure: the form still holds what was typed,
      // and for the 412 below the fix is to reopen Studio elsewhere, not to retype the permissions.
      var refusal = apiTokenTransportRefusal(jqXHR) || clusterCapabilityRefusal(jqXHR);
      if (refusal) {
        globalNotify(refusal.title, refusal.message, "danger");
        // The refusal IS new information about the cluster: it says a peer fell behind since the last poll, so
        // the gate has to catch up with it rather than wait for the next tab reopen (PR #7939 review).
        refreshSecurityClusterReadiness();
      } else globalNotifyError(jqXHR.responseText);
    });
}

/**
 * Maps the server's refusal to mint a token over an unprotected transport onto something worth showing
 * (issue #7804). Returns null for every other failure, which leaves 400/403/409/500 rendering through
 * globalNotifyError() exactly as before.
 *
 * The 412 gets its own handling because globalNotifyError() puts json.error in the TITLE and a fixed
 * "Error on execution of the command" in the body, so the one sentence explaining what to do about it
 * was being rendered as a heading. It also never mentioned that the connection at fault is the one this
 * Studio page is loaded over - which is the part the operator has to act on.
 *
 * PostApiTokenHandler.checkTransport() is the only thing that answers 412 on this route; a 412 with a
 * body that is not the server's JSON (an intermediate proxy's HTML error page) still means the mint was
 * refused, so the status alone drives the message and the body is only ever an optional detail.
 */
function apiTokenTransportRefusal(jqXHR) {
  if (!jqXHR || jqXHR.status !== 412) return null;

  var serverDetail = "";
  try {
    var json = JSON.parse(jqXHR.responseText);
    if (json && typeof json.error === "string") serverDetail = json.error;
  } catch (e) {
    // Not the server's JSON body. The status already told us everything we need to say.
  }

  var message =
    "The server refused to mint this token because the connection it arrived on is not encrypted, and the " +
    "token would be readable on the wire. Studio sends this request over the very connection this page is " +
    "loaded on, so open Studio over HTTPS, or from the server host itself, and try again.";

  if (serverDetail) message += " Server: " + serverDetail;

  return { title: "API token not minted: connection is not secure", message: message };
}

function copyCreatedToken() {
  navigator.clipboard.writeText($("#createdTokenValue").val()).then(function () {
    globalNotify("Copied", "Token copied to clipboard", "success");
  });
}

function deleteApiToken(tokenHash) {
  if (!confirm("Are you sure you want to delete this API token?")) return;

  jQuery
    .ajax({
      type: "DELETE",
      url: "api/v1/server/api-tokens?token=" + encodeURIComponent(tokenHash),
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function () {
      globalNotify("Deleted", "API token deleted", "success");
      loadApiTokens();
    })
    .fail(function (jqXHR) {
      // A revocation is replicated by the same entry a mint is, so the same gate refuses it - and a revocation
      // that silently did not happen is the more dangerous of the two.
      var refusal = clusterCapabilityRefusal(jqXHR);
      if (refusal) {
        globalNotify(refusal.title, refusal.message, "danger");
        refreshSecurityClusterReadiness();
      } else globalNotifyError(jqXHR.responseText);
    });
}

// ==================== Groups ====================

function loadGroups() {
  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/server/groups",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      groupsLoaded = true;
      groupsData = data.result || {};

      // Populate database filter dropdown
      var filterSelect = $("#groupDatabaseFilter");
      var currentVal = filterSelect.val();
      filterSelect.find("option").remove();
      filterSelect.append('<option value="*">* (default)</option>');

      var databases = groupsData.databases || {};
      var dbNames = Object.keys(databases);
      for (var i = 0; i < dbNames.length; i++) {
        if (dbNames[i] !== "*")
          filterSelect.append('<option value="' + escapeHtml(dbNames[i]) + '">' + escapeHtml(dbNames[i]) + '</option>');
      }

      if (currentVal && filterSelect.find('option[value="' + currentVal + '"]').length > 0)
        filterSelect.val(currentVal);
      else
        filterSelect.val("*");

      renderGroupsTable();
      renderSecurityCapabilityGate();
    })
    .fail(function (jqXHR) {
      globalNotifyError(jqXHR.responseText);
    });
}

function renderGroupsTable() {
  if (groupsDataTable) {
    groupsDataTable.destroy();
    groupsDataTable = null;
  }

  var selectedDb = $("#groupDatabaseFilter").val() || "*";
  var databases = (groupsData && groupsData.databases) ? groupsData.databases : {};
  var dbEntry = databases[selectedDb];
  var groups = (dbEntry && dbEntry.groups) ? dbEntry.groups : {};
  var groupNames = Object.keys(groups);

  var tableData = [];
  for (var i = 0; i < groupNames.length; i++) {
    var name = groupNames[i];
    var g = groups[name];

    // Build record perms summary
    var types = g.types || {};
    var typeNames = Object.keys(types);
    var recordSummary = "";
    for (var j = 0; j < typeNames.length; j++) {
      var tName = typeNames[j];
      var tAccess = (types[tName] && types[tName].access) ? types[tName].access : [];
      var perms = [];
      if (tAccess.indexOf("createRecord") >= 0) perms.push("C");
      if (tAccess.indexOf("readRecord") >= 0) perms.push("R");
      if (tAccess.indexOf("updateRecord") >= 0) perms.push("U");
      if (tAccess.indexOf("deleteRecord") >= 0) perms.push("D");
      if (j > 0) recordSummary += "<br>";
      recordSummary += '<code>' + escapeHtml(tName) + '</code>: ' + (perms.length > 0 ? perms.join("") : '<span class="text-muted">none</span>');
    }
    if (!recordSummary) recordSummary = '<span class="text-muted">none</span>';

    // Build db perms summary
    var access = g.access || [];
    var dbPerms = [];
    if (access.indexOf("updateSchema") >= 0) dbPerms.push("Schema");
    if (access.indexOf("updateDatabaseSettings") >= 0) dbPerms.push("Settings");
    if (access.indexOf("updateSecurity") >= 0) dbPerms.push("Security");
    var dbPermsSummary = dbPerms.length > 0 ? dbPerms.join(", ") : '<span class="text-muted">none</span>';

    // Limits summary
    var rsl = g.resultSetLimit !== undefined ? g.resultSetLimit : -1;
    var rt = g.readTimeout !== undefined ? g.readTimeout : -1;
    var limitsSummary = "RSL: " + (rsl === -1 ? "unlimited" : rsl) + ", RT: " + (rt === -1 ? "unlimited" : rt);

    tableData.push([name, recordSummary, dbPermsSummary, limitsSummary, selectedDb + "|" + name]);
  }

  groupsDataTable = $("#groupsTable").DataTable({
    paging: false,
    searching: false,
    data: tableData,
    columns: [
      { title: "Name", width: "130px" },
      { title: "Record Permissions" },
      { title: "Database Permissions" },
      { title: "Limits", width: "200px" },
      {
        title: "Actions",
        width: "120px",
        orderable: false,
        render: function (data) {
          var parts = data.split("|");
          var db = parts[0];
          var name = parts[1];
          var html = '<button class="btn btn-sm btn-outline-primary me-1" data-action="edit-group" data-db="' + escapeHtml(db) + '" data-name="' + escapeHtml(name) + '"><i class="fa fa-edit"></i></button>';
          if (!(name === "admin" && db === "*"))
            html += '<button class="btn btn-sm btn-outline-danger" data-action="delete-group" data-db="' + escapeHtml(db) + '" data-name="' + escapeHtml(name) + '"><i class="fa fa-trash"></i></button>';
          return html;
        },
      },
    ],
  });
}

function onGroupDatabaseFilterChange() {
  renderGroupsTable();
}

function showCreateGroupForm() {
  editingGroupName = null;
  editingGroupDatabase = null;
  $("#groupModalTitle").text("Create Group");
  $("#groupModalSaveBtn").text("Create");
  $("#groupName").val("").prop("disabled", false);
  $("#groupDatabase").prop("disabled", false);
  $("#groupResultSetLimit").val("-1");
  $("#groupReadTimeout").val("-1");
  $("#groupPermSchema").prop("checked", false);
  $("#groupPermSettings").prop("checked", false);
  $("#groupPermSecurity").prop("checked", false);
  $("#groupPermissionsTable tbody tr:not([data-type='*'])").remove();
  $("#groupPermissionsTable tbody tr[data-type='*'] .grp-perm-create").prop("checked", false);
  $("#groupPermissionsTable tbody tr[data-type='*'] .grp-perm-read").prop("checked", true);
  $("#groupPermissionsTable tbody tr[data-type='*'] .grp-perm-update").prop("checked", false);
  $("#groupPermissionsTable tbody tr[data-type='*'] .grp-perm-delete").prop("checked", false);

  // Populate database selector
  var select = $("#groupDatabase");
  select.find("option:not(:first)").remove();
  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/databases",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      var dbs = data.result || [];
      for (var i = 0; i < dbs.length; i++)
        select.append('<option value="' + escapeHtml(dbs[i]) + '">' + escapeHtml(dbs[i]) + '</option>');
    });

  $("#groupNewType").html('<option value="">Add type...</option>');
  applyGroupModalCapabilityGate();
  new bootstrap.Modal(document.getElementById("createGroupModal")).show();
}

function editGroup(db, name) {
  editingGroupName = name;
  editingGroupDatabase = db;
  $("#groupModalTitle").text("Edit Group: " + name);
  $("#groupModalSaveBtn").text("Save");
  $("#groupName").val(name).prop("disabled", true);
  $("#groupDatabase").val(db).prop("disabled", true);

  // Populate database selector (even though disabled, needs the value)
  var select = $("#groupDatabase");
  select.find("option:not(:first)").remove();
  if (db !== "*")
    select.append('<option value="' + escapeHtml(db) + '">' + escapeHtml(db) + '</option>');
  select.val(db);

  // Load group data
  var databases = (groupsData && groupsData.databases) ? groupsData.databases : {};
  var dbEntry = databases[db];
  var groups = (dbEntry && dbEntry.groups) ? dbEntry.groups : {};
  var g = groups[name] || {};

  $("#groupResultSetLimit").val(g.resultSetLimit !== undefined ? g.resultSetLimit : -1);
  $("#groupReadTimeout").val(g.readTimeout !== undefined ? g.readTimeout : -1);

  var access = g.access || [];
  $("#groupPermSchema").prop("checked", access.indexOf("updateSchema") >= 0);
  $("#groupPermSettings").prop("checked", access.indexOf("updateDatabaseSettings") >= 0);
  $("#groupPermSecurity").prop("checked", access.indexOf("updateSecurity") >= 0);

  // Reset type permission rows
  $("#groupPermissionsTable tbody tr:not([data-type='*'])").remove();

  var types = g.types || {};
  var typeNames = Object.keys(types);
  for (var i = 0; i < typeNames.length; i++) {
    var tName = typeNames[i];
    var tAccess = (types[tName] && types[tName].access) ? types[tName].access : [];

    if (tName === "*") {
      $("#groupPermissionsTable tbody tr[data-type='*'] .grp-perm-create").prop("checked", tAccess.indexOf("createRecord") >= 0);
      $("#groupPermissionsTable tbody tr[data-type='*'] .grp-perm-read").prop("checked", tAccess.indexOf("readRecord") >= 0);
      $("#groupPermissionsTable tbody tr[data-type='*'] .grp-perm-update").prop("checked", tAccess.indexOf("updateRecord") >= 0);
      $("#groupPermissionsTable tbody tr[data-type='*'] .grp-perm-delete").prop("checked", tAccess.indexOf("deleteRecord") >= 0);
    } else {
      appendGroupTypeRow(tName, tAccess);
    }
  }

  // If no * type entry, reset defaults
  if (!types["*"]) {
    $("#groupPermissionsTable tbody tr[data-type='*'] .grp-perm-create").prop("checked", false);
    $("#groupPermissionsTable tbody tr[data-type='*'] .grp-perm-read").prop("checked", false);
    $("#groupPermissionsTable tbody tr[data-type='*'] .grp-perm-update").prop("checked", false);
    $("#groupPermissionsTable tbody tr[data-type='*'] .grp-perm-delete").prop("checked", false);
  }

  loadTypesForGroupDatabase();
  applyGroupModalCapabilityGate();
  new bootstrap.Modal(document.getElementById("createGroupModal")).show();
}

function appendGroupTypeRow(typeName, accessArray) {
  var row =
    '<tr data-type="' + escapeHtml(typeName) + '">' +
    '<td><code>' + escapeHtml(typeName) + '</code></td>' +
    '<td class="text-center"><input type="checkbox" class="form-check-input grp-perm-create"' + (accessArray.indexOf("createRecord") >= 0 ? " checked" : "") + '></td>' +
    '<td class="text-center"><input type="checkbox" class="form-check-input grp-perm-read"' + (accessArray.indexOf("readRecord") >= 0 ? " checked" : "") + '></td>' +
    '<td class="text-center"><input type="checkbox" class="form-check-input grp-perm-update"' + (accessArray.indexOf("updateRecord") >= 0 ? " checked" : "") + '></td>' +
    '<td class="text-center"><input type="checkbox" class="form-check-input grp-perm-delete"' + (accessArray.indexOf("deleteRecord") >= 0 ? " checked" : "") + '></td>' +
    '<td><button class="btn btn-sm btn-outline-danger remove-group-type"><i class="fa fa-times"></i></button></td>' +
    '</tr>';
  $("#groupPermissionsTable tbody").append(row);
}

function loadTypesForGroupDatabase() {
  var db = $("#groupDatabase").val();
  var select = $("#groupNewType");
  select.html('<option value="">Add type...</option>');
  if (!db || db === "*") return;

  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/query/" + encodeDatabaseName(db) + "/sql/SELECT%20name%20FROM%20schema%3Atypes",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      var records = data.result || [];
      for (var i = 0; i < records.length; i++)
        select.append('<option value="' + escapeHtml(records[i].name) + '">' + escapeHtml(records[i].name) + '</option>');
    });
}

function addGroupTypeRow() {
  var typeName = $("#groupNewType").val();
  if (!typeName) return;
  if ($("#groupPermissionsTable tbody tr[data-type='" + typeName + "']").length > 0) return;
  appendGroupTypeRow(typeName, ["readRecord"]);
  $("#groupNewType").val("");
}

function removeGroupTypeRow(btn) {
  $(btn).closest("tr").remove();
}

function saveGroup() {
  var database = editingGroupDatabase || $("#groupDatabase").val();
  if (!database) {
    globalNotify("Error", "Database is required", "danger");
    return;
  }

  var name = editingGroupName || $("#groupName").val().trim();
  if (!name) {
    globalNotify("Error", "Group name is required", "danger");
    return;
  }

  var types = {};
  $("#groupPermissionsTable tbody tr").each(function () {
    var typeName = $(this).data("type");
    var access = [];
    if ($(this).find(".grp-perm-create").is(":checked")) access.push("createRecord");
    if ($(this).find(".grp-perm-read").is(":checked")) access.push("readRecord");
    if ($(this).find(".grp-perm-update").is(":checked")) access.push("updateRecord");
    if ($(this).find(".grp-perm-delete").is(":checked")) access.push("deleteRecord");
    types[typeName] = { access: access };
  });

  var dbPerms = [];
  if ($("#groupPermSchema").is(":checked")) dbPerms.push("updateSchema");
  if ($("#groupPermSettings").is(":checked")) dbPerms.push("updateDatabaseSettings");
  if ($("#groupPermSecurity").is(":checked")) dbPerms.push("updateSecurity");

  var payload = {
    database: database,
    name: name,
    resultSetLimit: parseInt($("#groupResultSetLimit").val()) || -1,
    readTimeout: parseInt($("#groupReadTimeout").val()) || -1,
    access: dbPerms,
    types: types,
  };

  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/server/groups",
      data: JSON.stringify(payload),
      contentType: "application/json",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function () {
      bootstrap.Modal.getInstance(document.getElementById("createGroupModal")).hide();
      globalNotify("Success", "Group '" + name + "' saved", "success");
      usersLoaded = false;
      loadGroups();
    })
    .fail(function (jqXHR) {
      // The modal is left open: the form still holds what was typed, and the fix for a 409 is to finish the
      // rolling upgrade and press Create again, not to retype the permissions.
      var refusal = clusterCapabilityRefusal(jqXHR);
      if (refusal) {
        globalNotify(refusal.title, refusal.message, "danger");
        refreshSecurityClusterReadiness();
      } else globalNotifyError(jqXHR.responseText);
    });
}

function deleteGroup(db, name) {
  if (!confirm("Are you sure you want to delete group '" + name + "' from database '" + db + "'?")) return;

  jQuery
    .ajax({
      type: "DELETE",
      url: "api/v1/server/groups?database=" + encodeURIComponent(db) + "&name=" + encodeURIComponent(name),
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function () {
      globalNotify("Deleted", "Group '" + name + "' deleted", "success");
      usersLoaded = false;
      loadGroups();
    })
    .fail(function (jqXHR) {
      var refusal = clusterCapabilityRefusal(jqXHR);
      if (refusal) {
        globalNotify(refusal.title, refusal.message, "danger");
        refreshSecurityClusterReadiness();
      } else globalNotifyError(jqXHR.responseText);
    });
}

function refreshCurrentSecurityTab() {
  var activeTab = $("#tabs-security .nav-link.active").attr("id");
  if (activeTab === "tab-security-groups-sel")
    loadGroups();
  else if (activeTab === "tab-security-tokens-sel")
    loadApiTokens();
  else
    loadUsers();
}

// ==================== Tab Switching & Delegated Event Handlers ====================

document.addEventListener("DOMContentLoaded", function () {
  $('a[data-toggle="tab"]').on("shown.bs.tab", function (e) {
    var activeTab = this.id;
    if (activeTab == "tab-security-tokens-sel") {
      if (!apiTokensLoaded) loadApiTokens();
    } else if (activeTab == "tab-security-users-sel") {
      if (!usersLoaded) loadUsers();
    } else if (activeTab == "tab-security-groups-sel") {
      if (!groupsLoaded) loadGroups();
    }
  });

  // Delegated handlers for dynamically-rendered buttons (replaces inline onclick)
  $(document).on("click", "[data-action='edit-user']", function () {
    editUser($(this).data("name"));
  });
  $(document).on("click", "[data-action='delete-user']", function () {
    deleteUser($(this).data("name"));
  });
  $(document).on("click", "[data-action='delete-token']", function () {
    deleteApiToken($(this).data("hash"));
  });
  $(document).on("click", "[data-action='edit-group']", function () {
    editGroup($(this).data("db"), $(this).data("name"));
  });
  $(document).on("click", "[data-action='delete-group']", function () {
    deleteGroup($(this).data("db"), $(this).data("name"));
  });
  $(document).on("click", ".remove-db-row", function () {
    removeUserDatabaseRow(this);
  });
  $(document).on("click", ".remove-token-type", function () {
    removeTokenTypeRow(this);
  });
  $(document).on("click", ".remove-group-type", function () {
    removeGroupTypeRow(this);
  });
});
