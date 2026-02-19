// Security panel state
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

function initSecurity() {
  if (!securityInitialized) {
    securityInitialized = true;
    loadUsers();
  }
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
  $("#userDatabasesTable tbody tr[data-db='*'] .user-group-select").val("admin");

  loadDatabasesForUserForm();
  new bootstrap.Modal(document.getElementById("createUserModal")).show();
}

function editUser(name) {
  editingUserName = name;
  $("#userModalTitle").text("Edit User: " + name);
  $("#userModalSaveBtn").text("Save");
  $("#userName").val(name).prop("disabled", true);
  $("#userPassword").val("").attr("placeholder", "Leave blank to keep current");
  $("#userDatabasesTable tbody tr:not([data-db='*'])").remove();

  // Load user data
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
        var groupStr = groups.join(", ");

        if (dbName === "*") {
          hasWildcard = true;
          $("#userDatabasesTable tbody tr[data-db='*'] .user-group-select").val(groups[0] || "admin");
        } else {
          appendDatabaseRow(dbName, groups[0] || "admin");
        }
      }

      if (!hasWildcard)
        $("#userDatabasesTable tbody tr[data-db='*'] .user-group-select").val("*");

      loadDatabasesForUserForm();
      new bootstrap.Modal(document.getElementById("createUserModal")).show();
    });
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
    '<option value="admin"' + (selectedGroup === "admin" ? " selected" : "") + '>admin</option>' +
    '<option value="*"' + (selectedGroup === "*" ? " selected" : "") + '>* (default)</option>' +
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
    var expiration = t.expiresAt > 0 ? new Date(t.expiresAt).toLocaleString() : "Never";
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
      url: "api/v1/query/" + encodeURIComponent(db) + "/sql/SELECT%20name%20FROM%20schema%3Atypes",
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
      globalNotifyError(jqXHR.responseText);
    });
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
      globalNotifyError(jqXHR.responseText);
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
      url: "api/v1/query/" + encodeURIComponent(db) + "/sql/SELECT%20name%20FROM%20schema%3Atypes",
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
      globalNotifyError(jqXHR.responseText);
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
      globalNotifyError(jqXHR.responseText);
    });
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
