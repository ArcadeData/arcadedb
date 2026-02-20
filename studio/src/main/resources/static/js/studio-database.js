var editor = null;
var globalResultset = null;
var globalGraphMaxResult = 1000;
var globalCredentials = null;
var globalBasicAuth = null;
var globalUsername = null;
var globalSchemaTypes = null;

var SESSION_STORAGE_KEY = "arcadedb-session";
var USERNAME_STORAGE_KEY = "arcadedb-username";

function make_base_auth(user, password) {
  var tok = user + ":" + password;
  var hash = btoa(tok);
  return "Basic " + hash;
}

function getStoredSession() {
  return localStorage.getItem(SESSION_STORAGE_KEY);
}

function getStoredUsername() {
  return localStorage.getItem(USERNAME_STORAGE_KEY);
}

function storeSession(token, username) {
  localStorage.setItem(SESSION_STORAGE_KEY, token);
  localStorage.setItem(USERNAME_STORAGE_KEY, username);
}

function clearSession() {
  localStorage.removeItem(SESSION_STORAGE_KEY);
  localStorage.removeItem(USERNAME_STORAGE_KEY);
}

function initSessionFromStorage() {
  var storedToken = getStoredSession();
  var storedUsername = getStoredUsername();
  if (storedToken) {
    globalCredentials = "Bearer " + storedToken;
    globalUsername = storedUsername;
    return true;
  }
  return false;
}

function login() {
  var userName = $("#inputUserName").val().trim();
  if (userName.length == 0) {
    console.warn("Username is empty");
    return;
  }

  var userPassword = $("#inputUserPassword").val().trim();
  if (userPassword.length == 0) {
    console.warn("Password is empty");
    return;
  }

  console.log("Starting login process for user:", userName);
  $("#loginSpinner").show();

  var basicAuth = make_base_auth(userName, userPassword);

  // Call the login endpoint to get a session token
  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/login",
      timeout: 30000,
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", basicAuth);
      },
    })
    .done(function (data) {
      console.log("Login successful, received token");

      // Store the session token
      var token = data.token;
      var username = data.user;
      storeSession(token, username);

      // Set global credentials to use Bearer token
      globalCredentials = "Bearer " + token;
      globalBasicAuth = basicAuth;
      globalUsername = username;

      console.log("Session stored, calling updateDatabases");

      updateDatabases(function () {
        console.log("Login complete, initializing query editor");
        initQuery();
      });
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      console.error("Login failed:", {
        status: jqXHR.status,
        statusText: jqXHR.statusText,
        textStatus: textStatus,
        errorThrown: errorThrown,
        responseText: jqXHR.responseText
      });

      var errorMessage = "Login failed";
      if (jqXHR.status === 401) {
        errorMessage = "Invalid username or password";
      } else if (jqXHR.status === 403) {
        errorMessage = "Access denied";
      } else if (jqXHR.status === 0) {
        errorMessage = "Cannot connect to server. Please check if ArcadeDB is running.";
      } else if (textStatus === "timeout") {
        errorMessage = "Connection timeout. Please try again.";
      } else if (jqXHR.responseText) {
        try {
          var json = JSON.parse(jqXHR.responseText);
          errorMessage = json.error || json.detail || errorMessage;
        } catch (e) {
          errorMessage = jqXHR.responseText;
        }
      }

      if (typeof globalNotify === 'function') {
        globalNotify("Login Error", errorMessage, "danger");
      } else {
        alert("Login Error: " + errorMessage);
      }

      $("#inputUserName").focus().select();
    })
    .always(function () {
      $("#loginSpinner").hide();
    });
}

function logout() {
  globalConfirm(
    "Log Out",
    "Are you sure you want to log out?",
    "warning",
    function () {
      // Call the logout endpoint to invalidate the session
      jQuery
        .ajax({
          type: "POST",
          url: "api/v1/logout",
          timeout: 10000,
          beforeSend: function (xhr) {
            if (globalCredentials) {
              xhr.setRequestHeader("Authorization", globalCredentials);
            }
          },
        })
        .always(function () {
          // Clear session regardless of server response
          clearSession();
          globalCredentials = null;
          globalUsername = null;

          // Show login popup and hide studio panel
          $("#studioPanel").hide();
          $("#welcomePanel").show();
          $("#loginPopup").modal("show");

          // Clear form fields
          $("#inputUserName").val("");
          $("#inputUserPassword").val("");
        });
    }
  );
}

function showLoginPopup() {
  $("#loginPopup").modal("show");
}

function editorFocus() {
  let value = editor.getValue();
  editor.setValue("");
  editor.setValue(value);

  editor.setCursor(editor.lineCount(), 0);
  editor.focus();
}

function updateDatabases(callback) {
  let selected = getCurrentDatabase();
  if (selected == null || selected == "") selected = globalStorageLoad("database.current");

  console.log("Making AJAX request to api/v1/databases");
  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/databases",
      timeout: 30000, // 30 second timeout for slow servers
      beforeSend: function (xhr) {
        console.log("Setting authorization header");
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      console.log("Login successful, received data:", data);

      if (!data) {
        console.error("No data received from server");
        globalNotifyError("No data received from server");
        return;
      }

      if (!data.version) {
        console.error("Invalid response data - missing version:", data);
        globalNotifyError("Invalid response from server - missing version");
        return;
      }

      let version = data.version;
      let pos = data.version.indexOf("(build");
      if (pos > -1) {
        version = version.substring(0, pos);
      }

      $("#version").html(version);
      console.log("Set version to:", version);

      let databases = "";
      if (data.result && Array.isArray(data.result)) {
        for (let i in data.result) {
          let dbName = data.result[i];
          databases += "<option value='" + dbName + "'>" + dbName + "</option>";
        }
        console.log("Populated database options:", data.result);
      } else {
        console.warn("No databases found in response:", data);
        databases = "<option value=''>No databases available</option>";
      }
      $(".inputDatabase").html(databases);
      $("#schemaInputDatabase").html(databases);

      if (selected != null && selected != "" && data.result && Array.isArray(data.result) && data.result.length > 0) {
        //check if selected database exists
        if (data.result.includes(selected)) {
          $(".inputDatabase").val(selected);
          $("#schemaInputDatabase").val(selected);
        } else {
          $(".inputDatabase").val(data.result[0]);
          $("#schemaInputDatabase").val(data.result[0]);
        }
      } else if (data.result && Array.isArray(data.result) && data.result.length > 0) {
        // Default to first database if no selection
        $(".inputDatabase").val(data.result[0]);
        $("#schemaInputDatabase").val(data.result[0]);
      }

      // Update current database display
      try {
        $("#currentDatabase").html(getCurrentDatabase());
      } catch (e) {
        console.warn("Error updating current database display:", e);
      }

      // This is the critical part - set the user info for "Connected as" text
      // Handle both query and database user elements
      let username = data.user || globalUsername || 'unknown';
      $("#queryUser").html(username);
      $("#databaseUser").html(username);
      console.log("Set user to:", username);

      // CRITICAL: Always hide login and show studio, even if other operations fail
      var loginModal = bootstrap.Modal.getInstance(document.getElementById("loginPopup"));
      if (loginModal)
        loginModal.hide();
      // Ensure no stale backdrop remains (Bootstrap 5 can leave orphaned backdrops)
      setTimeout(function() {
        document.querySelectorAll(".modal-backdrop").forEach(function(el) { el.remove(); });
        document.body.classList.remove("modal-open");
        document.body.style.removeProperty("overflow");
        document.body.style.removeProperty("padding-right");
      }, 300);
      $("#welcomePanel").hide();
      $("#studioPanel").show();
      console.log("UI updated - login popup hidden, studio panel shown");

      // These operations should not block login completion
      try {
        displaySchema();
      } catch (e) {
        console.warn("Error displaying schema:", e);
      }

      try {
        displayDatabaseSettings();
      } catch (e) {
        console.warn("Error displaying database settings:", e);
      }

      // Execute callback if provided
      if (callback) {
        console.log("Executing callback");
        try {
          callback();
        } catch (e) {
          console.warn("Error in login callback:", e);
        }
      }
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      console.error("Database fetch failed:", {
        status: jqXHR.status,
        statusText: jqXHR.statusText,
        textStatus: textStatus,
        errorThrown: errorThrown,
        responseText: jqXHR.responseText
      });

      // Handle session expiration (401 = invalid/expired token)
      if (jqXHR.status === 401) {
        console.log("Session expired or invalid, clearing session");
        clearSession();
        globalCredentials = null;
        globalUsername = null;

        // Silently redirect to login - no error popup needed
        $("#studioPanel").hide();
        $("#welcomePanel").show();
        showLoginPopup();
        return;
      }

      let errorMessage = "Failed to fetch databases";
      if (jqXHR.status === 403) {
        errorMessage = "Access denied";
      } else if (jqXHR.status === 0) {
        errorMessage = "Cannot connect to server. Please check if ArcadeDB is running.";
      } else if (textStatus === "timeout") {
        errorMessage = "Connection timeout. Please try again.";
      } else if (jqXHR.responseText) {
        try {
          let json = JSON.parse(jqXHR.responseText);
          errorMessage = json.error || json.detail || errorMessage;
        } catch (e) {
          errorMessage = jqXHR.responseText;
        }
      }

      if (typeof globalNotify === 'function') {
        globalNotify("Error", errorMessage, "danger");
      } else {
        alert("Error: " + errorMessage);
      }
    })
    .always(function (data) {
      console.log("Database fetch completed, hiding spinner");
      $("#loginSpinner").hide();
    });
}

function createDatabase() {
  let html = "<label for='inputCreateDatabaseName'>Enter the database name:</label>" +
    "<input class='form-control mt-2' id='inputCreateDatabaseName' onkeydown='if (event.which === 13) document.getElementById(\"globalModalConfirmBtn\").click()'>";

  globalPrompt("Create a new database", html, "Create", function() {
    let database = encodeURI($("#inputCreateDatabaseName").val().trim());
    if (database == "") {
      globalNotify("Error", "Database name empty", "danger");
      return;
    }

    jQuery
      .ajax({
        type: "POST",
        url: "api/v1/server",
        data: "{ 'command': 'create database " + database + "' }",
        beforeSend: function (xhr) {
          xhr.setRequestHeader("Authorization", globalCredentials);
        },
      })
      .done(function (data) {
        $(".inputDatabase").val(database);
        $("#schemaInputDatabase").val(database);
        updateDatabases();
      })
      .fail(function (jqXHR, textStatus, errorThrown) {
        globalNotifyError(jqXHR.responseText);
      });
  });
}

function dropDatabase() {
  let database = escapeHtml(getCurrentDatabase());
  if (database == "") {
    globalNotify("Error", "Database not selected", "danger");
    return;
  }

  globalConfirm(
    "Drop database",
    "Are you sure you want to drop the database '" + database + "'?<br>WARNING: The operation cannot be undone.",
    "warning",
    function () {
      jQuery
        .ajax({
          type: "POST",
          url: "api/v1/server",
          data: "{ 'command': 'drop database " + database + "' }",
          beforeSend: function (xhr) {
            xhr.setRequestHeader("Authorization", globalCredentials);
          },
        })
        .done(function (data) {
          updateDatabases();
        })
        .fail(function (jqXHR, textStatus, errorThrown) {
          globalNotifyError(jqXHR.responseText);
        });
    },
  );
}

function resetDatabase() {
  let database = escapeHtml(getCurrentDatabase());
  if (database == "") {
    globalNotify("Error", "Database not selected", "danger");
    return;
  }

  globalConfirm(
    "Reset database",
    "Are you sure you want to reset the database '" + database + "' (All data will be deleted)?<br>WARNING: The operation cannot be undone.",
    "warning",
    function () {
      jQuery
        .ajax({
          type: "POST",
          url: "api/v1/server",
          data: "{ 'command': 'drop database " + database + "' }",
          beforeSend: function (xhr) {
            xhr.setRequestHeader("Authorization", globalCredentials);
          },
        })
        .done(function (data) {
          jQuery
            .ajax({
              type: "POST",
              url: "api/v1/server",
              data: "{ 'command': 'create database " + database + "' }",
              beforeSend: function (xhr) {
                xhr.setRequestHeader("Authorization", globalCredentials);
              },
            })
            .done(function (data) {
              $(".inputDatabase").val(database);
              $("#schemaInputDatabase").val(database);
              updateDatabases();
            })
            .fail(function (jqXHR, textStatus, errorThrown) {
              globalNotifyError(jqXHR.responseText);
            });
        })
        .fail(function (jqXHR, textStatus, errorThrown) {
          globalNotifyError(jqXHR.responseText);
        });
    },
  );
}

function backupDatabase() {
  let database = getCurrentDatabase();
  if (database == "") {
    globalNotify("Error", "Database not selected", "danger");
    return;
  }

  globalConfirm(
    "Backup database",
    "Are you sure you want to backup the database '" + database + "'?<br>The database archive will be created under the 'backup' directory of the server.",
    "info",
    function () {
      jQuery
        .ajax({
          type: "POST",
          url: "api/v1/command/" + database,
          data: JSON.stringify({
            language: "sql",
            command: "backup database",
            serializer: "record",
          }),
          beforeSend: function (xhr) {
            xhr.setRequestHeader("Authorization", globalCredentials);
          },
        })
        .done(function (data) {
          globalNotify("Backup completed", "File: " + escapeHtml(data.result[0].backupFile), "success");
        })
        .fail(function (jqXHR, textStatus, errorThrown) {
          globalNotifyError(jqXHR.responseText);
        });
    },
  );
}

function dropProperty(type, property) {
  let database = getCurrentDatabase();
  if (database == "") {
    globalNotify("Error", "Database not selected", "danger");
    return;
  }

  globalConfirm(
    "Drop property",
    "Are you sure you want to drop the property '" + property + "' on type '" + type + "'?<br>WARNING: The operation cannot be undone.",
    "warning",
    function () {
      jQuery
        .ajax({
          type: "POST",
          url: "api/v1/command/" + database,
          data: JSON.stringify({
            language: "sql",
            command: "drop property `" + type + "`.`" + property + "`",
            serializer: "record",
          }),
          beforeSend: function (xhr) {
            xhr.setRequestHeader("Authorization", globalCredentials);
          },
        })
        .done(function (data) {
          updateDatabases();
        })
        .fail(function (jqXHR, textStatus, errorThrown) {
          globalNotifyError(jqXHR.responseText);
        });
    },
  );
}

function dropIndex(indexName) {
  let database = getCurrentDatabase();
  if (database == "") {
    globalNotify("Error", "Database not selected", "danger");
    return;
  }

  globalConfirm(
    "Drop index",
    "Are you sure you want to drop the index '" + indexName + "'?<br>WARNING: The operation cannot be undone.",
    "warning",
    function () {
      jQuery
        .ajax({
          type: "POST",
          url: "api/v1/command/" + database,
          data: JSON.stringify({
            language: "sql",
            command: "drop index `" + indexName + "`",
            serializer: "record",
          }),
          beforeSend: function (xhr) {
            xhr.setRequestHeader("Authorization", globalCredentials);
          },
        })
        .done(function (data) {
          updateDatabases();
        })
        .fail(function (jqXHR, textStatus, errorThrown) {
          globalNotifyError(jqXHR.responseText);
        });
    },
  );
}

function getCurrentDatabase() {
  let db = $(".inputDatabase").val();
  return db != null ? db.trim() : null;
}

function setCurrentDatabase(dbName) {
  $(".inputDatabase").val(dbName);
  $("#schemaInputDatabase").val(dbName);

  globalStorageSave("database.current", dbName);
}

function getQueryHistory() {
  let queryHistory = globalStorageLoad("database.query.history");
  if (queryHistory == null) queryHistory = [];
  else {
    try {
      queryHistory = JSON.parse(queryHistory);
    } catch (e) {
      // RESET HISTORY
      globalStorageSave("database.query.history", "[]");
      queryHistory = [];
    }
  }

  return queryHistory;
}

// Sidebar panel state
var sidebarPanelsLoaded = {};
var activeSidebarPanel = "overview";

function initSidebarPanels() {
  sidebarPanelsLoaded = { overview: true };
  activeSidebarPanel = "overview";

  // Restore collapsed state from localStorage
  if (globalStorageLoad("querySidebarCollapsed", false))
    toggleSidebarCollapsed(false);
}

function switchSidebarPanel(panelName) {
  // Toggle collapse if clicking the already-active panel
  if (panelName == activeSidebarPanel) {
    toggleSidebarCollapsed();
    return;
  }

  // Expand if collapsed
  var sidebar = $("#querySidebar");
  if (sidebar.hasClass("collapsed"))
    toggleSidebarCollapsed(false);

  $(".sidebar-panel").removeClass("active");
  $(".icon-sidebar-btn").removeClass("active");

  $("#sidebarPanel" + panelName.charAt(0).toUpperCase() + panelName.slice(1)).addClass("active");
  $(".icon-sidebar-btn[data-panel='" + panelName + "']").addClass("active");

  activeSidebarPanel = panelName;

  // Update header title and refresh button
  var titles = { overview: "Database Info", saved: "Saved Queries", history: "History", reference: "Reference", settings: "Settings" };
  $("#sidebarPanelTitle").text(titles[panelName] || panelName);

  // Show refresh button only for overview
  if (panelName == "overview")
    $("#sidebarRefreshBtn").show();
  else
    $("#sidebarRefreshBtn").hide();

  // Lazy-load panel content
  if (!sidebarPanelsLoaded[panelName]) {
    sidebarPanelsLoaded[panelName] = true;
    if (panelName == "saved") populateSavedQueriesPanel();
    else if (panelName == "history") populateHistoryPanel();
    else if (panelName == "reference") populateReferencePanel();
    else if (panelName == "settings") populateSettingsPanel();
  }
}

function toggleSidebarCollapsed(forceState) {
  var sidebar = $("#querySidebar");
  var collapsed = (forceState !== undefined) ? forceState : !sidebar.hasClass("collapsed");

  if (collapsed)
    sidebar.addClass("collapsed");
  else
    sidebar.removeClass("collapsed");

  globalStorageSave("querySidebarCollapsed", collapsed);

  // Refresh CodeMirror after transition completes
  if (typeof editor !== "undefined" && editor != null)
    setTimeout(function() { editor.refresh(); }, 250);
}

function refreshActiveSidebarPanel() {
  if (activeSidebarPanel == "overview") {
    populateQuerySidebar();
  } else if (activeSidebarPanel == "history") {
    sidebarPanelsLoaded.history = true;
    populateHistoryPanel();
  }
}

// --- Saved Queries ---

function getSavedQueries() {
  let raw = globalStorageLoad("database.saved.queries");
  if (raw == null) return [];
  try { return JSON.parse(raw); } catch (e) { return []; }
}

function storeSavedQueries(arr) {
  globalStorageSave("database.saved.queries", JSON.stringify(arr));
}

function populateSavedQueriesPanel() {
  let container = $("#sidebarPanelSaved");
  let queries = getSavedQueries();

  let html = "<div class='sidebar-action-bar'>";
  html += "<span style='font-size:0.75rem;color:#777;'>" + queries.length + " saved</span>";
  html += "</div>";

  if (queries.length == 0) {
    html += "<div class='sidebar-empty'><i class='fa fa-bookmark' style='font-size:1.5rem;color:#ccc !important;margin-bottom:8px;display:block'></i>No saved queries yet.<br><small>Use the <i class='fa fa-bookmark'></i> button in the editor to save a query.</small></div>";
    container.html(html);
    return;
  }

  for (let i = 0; i < queries.length; i++) {
    let q = queries[i];
    let name = escapeHtml(q.name);
    let cmd = escapeHtml(q.c || "");
    let lang = escapeHtml(q.l || "sql");
    html += "<div class='saved-query-entry' onclick='executeSavedQuery(" + i + ")'>";
    html += "<div class='saved-query-name'><span>" + name + "<span class='saved-query-lang'>" + lang + "</span></span>";
    html += "<span class='saved-query-delete' onclick='event.stopPropagation(); deleteSavedQuery(" + i + ")' title='Delete'><i class='fa fa-times'></i></span></div>";
    html += "<div class='saved-query-preview'>" + cmd + "</div>";
    html += "</div>";
  }

  container.html(html);
}

function saveCurrentQuery() {
  let command = editor.getValue();
  let language = $("#inputLanguage").val();
  let database = getCurrentDatabase();

  if (!command || command.trim() == "") {
    globalNotify("Save Query", "Editor is empty - write a query first.", "warning");
    return;
  }

  let html = "<label>Query name</label>" +
    "<input class='form-control mt-2' id='inputSaveQueryName' placeholder='e.g. Get all users' " +
    "onkeydown='if (event.which === 13) document.getElementById(\"globalModalConfirmBtn\").click()'>";

  globalPrompt("Save Query", html, "Save", function() {
    let name = $("#inputSaveQueryName").val();
    if (!name || name.trim() == "") {
      globalNotify("Error", "Please enter a name", "warning");
      return;
    }
    let queries = getSavedQueries();
    queries.unshift({ name: name.trim(), l: language, c: command, d: database });
    storeSavedQueries(queries);
    populateSavedQueriesPanel();
    globalNotify("Saved", "Query saved as '" + escapeHtml(name.trim()) + "'", "success");
  });
}

function executeSavedQuery(index) {
  let queries = getSavedQueries();
  let q = queries[index];
  if (q) executeCommand(q.l, q.c);
}

function deleteSavedQuery(index) {
  let queries = getSavedQueries();
  let name = queries[index] ? queries[index].name : "";
  queries.splice(index, 1);
  storeSavedQueries(queries);
  populateSavedQueriesPanel();
}

// --- History Panel ---

function populateHistoryPanel() {
  let container = $("#sidebarPanelHistory");
  let queryHistory = getQueryHistory();
  let database = escapeHtml(getCurrentDatabase());

  // Filter for current database
  let filtered = [];
  for (let i = 0; i < queryHistory.length; i++) {
    let q = queryHistory[i];
    if (q != null && q.d == database && q.l != null && q.c != null)
      filtered.push({ index: i, q: q });
  }

  let html = "<div class='history-toolbar'>";
  html += "<div class='form-check'><input class='form-check-input history-checkbox' type='checkbox' id='historySelectAll' onchange='toggleSelectAllHistory()'>";
  html += "<label class='form-check-label' for='historySelectAll' style='font-size:0.72rem;'>All</label></div>";
  html += "<button class='sidebar-action-btn' onclick='deleteSelectedHistory()' title='Delete selected'><i class='fa fa-trash'></i></button>";
  html += "</div>";
  html += "<input type='text' class='history-search' id='historySearchInput' placeholder='Search history...' oninput='filterHistoryEntries()'>";

  if (filtered.length == 0) {
    html += "<div class='sidebar-empty'><i class='fa fa-clock' style='font-size:1.5rem;color:#ccc !important;margin-bottom:8px;display:block'></i>No history yet.<br><small>Run a query to see it here.</small></div>";
    container.html(html);
    return;
  }

  // Group by date
  let groups = {};
  let noDateEntries = [];
  for (let i = 0; i < filtered.length; i++) {
    let entry = filtered[i];
    if (entry.q.t) {
      let d = new Date(entry.q.t);
      let dateKey = d.toDateString();
      if (!groups[dateKey]) groups[dateKey] = [];
      groups[dateKey].push(entry);
    } else
      noDateEntries.push(entry);
  }

  let today = new Date().toDateString();
  let yesterday = new Date(Date.now() - 86400000).toDateString();

  // Render grouped entries
  let dateKeys = Object.keys(groups);
  for (let di = 0; di < dateKeys.length; di++) {
    let dateKey = dateKeys[di];
    let label = dateKey;
    if (dateKey == today) label = "Today";
    else if (dateKey == yesterday) label = "Yesterday";
    html += "<div class='history-date-group'>" + escapeHtml(label) + "</div>";
    html += renderHistoryEntries(groups[dateKey]);
  }

  if (noDateEntries.length > 0) {
    if (dateKeys.length > 0)
      html += "<div class='history-date-group'>Older</div>";
    html += renderHistoryEntries(noDateEntries);
  }

  container.html(html);
}

function renderHistoryEntries(entries) {
  let html = "";
  for (let i = 0; i < entries.length; i++) {
    let entry = entries[i];
    let q = entry.q;
    let idx = entry.index;
    let lang = escapeHtml(q.l || "sql");
    let cmd = escapeHtml(q.c || "");
    let time = "";
    if (q.t) {
      let d = new Date(q.t);
      time = ("0" + d.getHours()).slice(-2) + ":" + ("0" + d.getMinutes()).slice(-2);
    }

    html += "<div class='history-entry' data-index='" + idx + "' data-cmd='" + cmd.toLowerCase() + "'>";
    html += "<input type='checkbox' class='history-checkbox history-item-check' data-index='" + idx + "' onclick='event.stopPropagation()'>";
    html += "<div class='history-entry-content' onclick='executeHistoryEntry(" + idx + ")'>";
    html += "<div class='history-meta'>";
    if (time) html += "<span class='history-time'>" + time + "</span>";
    html += "<span class='history-lang'>" + lang + "</span>";
    html += "</div>";
    html += "<div class='history-cmd'>" + cmd + "</div>";
    html += "</div></div>";
  }
  return html;
}

function executeHistoryEntry(index) {
  let queryHistory = getQueryHistory();
  let q = queryHistory[index];
  if (q) executeCommand(q.l, q.c);
}

function filterHistoryEntries() {
  let search = ($("#historySearchInput").val() || "").toLowerCase();
  $("#sidebarPanelHistory .history-entry").each(function () {
    let cmd = $(this).attr("data-cmd") || "";
    if (search == "" || cmd.indexOf(search) >= 0)
      $(this).show();
    else
      $(this).hide();
  });
}

function toggleSelectAllHistory() {
  let checked = $("#historySelectAll").prop("checked");
  $(".history-item-check:visible").prop("checked", checked);
}

function deleteSelectedHistory() {
  let indices = [];
  $(".history-item-check:checked").each(function () {
    indices.push(parseInt($(this).attr("data-index")));
  });
  if (indices.length == 0) {
    globalNotify("History", "No entries selected.", "warning");
    return;
  }

  // Sort descending so splicing doesn't shift indices
  indices.sort(function (a, b) { return b - a; });

  let queryHistory = getQueryHistory();
  for (let i = 0; i < indices.length; i++)
    queryHistory.splice(indices[i], 1);

  globalStorageSave("database.query.history", JSON.stringify(queryHistory));
  populateHistoryPanel();
}

// --- Reference Panel ---

function populateReferencePanel() {
  let container = $("#sidebarPanelReference");
  let sections = [
    {
      title: "SQL", lang: "sql",
      examples: [
        { label: "SELECT", code: "SELECT * FROM MyType" },
        { label: "SELECT with WHERE", code: "SELECT * FROM MyType WHERE name = 'value'" },
        { label: "INSERT", code: "INSERT INTO MyType SET name = 'value', age = 25" },
        { label: "UPDATE", code: "UPDATE MyType SET name = 'new' WHERE name = 'old'" },
        { label: "DELETE", code: "DELETE FROM MyType WHERE name = 'value'" },
        { label: "CREATE TYPE (Vertex)", code: "CREATE VERTEX TYPE MyVertex" },
        { label: "CREATE TYPE (Edge)", code: "CREATE EDGE TYPE MyEdge" },
        { label: "CREATE TYPE (Document)", code: "CREATE DOCUMENT TYPE MyDoc" },
        { label: "CREATE PROPERTY", code: "CREATE PROPERTY MyType.name STRING" },
        { label: "CREATE INDEX", code: "CREATE INDEX ON MyType (name) UNIQUE" },
        { label: "CREATE EDGE", code: "CREATE EDGE MyEdge FROM #1:0 TO #2:0 SET weight = 1.0" },
        { label: "MATCH", code: "MATCH {type: Person, as: p} -FriendOf-> {as: f} RETURN p, f" }
      ]
    },
    {
      title: "Cypher", lang: "opencypher",
      examples: [
        { label: "MATCH all", code: "MATCH (n) RETURN n" },
        { label: "MATCH with label", code: "MATCH (n:Person) RETURN n" },
        { label: "MATCH relationship", code: "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b" },
        { label: "CREATE node", code: "CREATE (n:Person {name: 'John', age: 30}) RETURN n" },
        { label: "WHERE clause", code: "MATCH (n:Person) WHERE n.age > 25 RETURN n.name" }
      ]
    },
    {
      title: "Gremlin", lang: "gremlin",
      examples: [
        { label: "All vertices", code: "g.V()" },
        { label: "All edges", code: "g.E()" },
        { label: "Filter by property", code: "g.V().has('name', 'John')" },
        { label: "Outgoing edges", code: "g.V().has('name', 'John').out('knows')" },
        { label: "Incoming edges", code: "g.V().has('name', 'John').in('knows')" },
        { label: "Path traversal", code: "g.V().has('name', 'John').out().out().path()" }
      ]
    },
    {
      title: "GraphQL", lang: "graphql",
      examples: [
        { label: "Query type", code: "{ bookByName(name: \"Harry\") { name, author } }" }
      ]
    }
  ];

  let html = "";
  for (let s = 0; s < sections.length; s++) {
    let sec = sections[s];
    html += "<div class='reference-section'>";
    html += "<div class='reference-section-header' onclick='toggleReferenceSection(this)'><span>" + sec.title + "</span><i class='fa fa-chevron-right'></i></div>";
    html += "<div class='reference-section-body'>";
    for (let e = 0; e < sec.examples.length; e++) {
      let ex = sec.examples[e];
      let escapedCode = escapeHtml(ex.code).replace(/'/g, "&#39;");
      html += "<div class='reference-example' title='" + escapeHtml(ex.label) + "' onclick='pasteReferenceExample(\"" + escapedCode.replace(/"/g, "&quot;") + "\", \"" + sec.lang + "\")'>";
      html += "<small style='color:#999;font-family:inherit;'>" + escapeHtml(ex.label) + "</small><br>";
      html += escapeHtml(ex.code);
      html += "</div>";
    }
    html += "</div></div>";
  }

  container.html(html);
}

function toggleReferenceSection(header) {
  let body = $(header).next(".reference-section-body");
  let icon = $(header).find("i.fa-chevron-right");
  body.toggleClass("open");
  icon.toggleClass("open");
}

function pasteReferenceExample(code, lang) {
  // Decode HTML entities back to plain text
  let tmp = document.createElement("textarea");
  tmp.innerHTML = code;
  let decoded = tmp.value;
  if (lang) {
    $("#inputLanguage").val(lang);
    editor.setOption("mode", getEditorMode());
  }
  editor.setValue(decoded);
  editor.focus();
}

// --- Settings Panel ---

function populateSettingsPanel() {
  let container = $("#sidebarPanelSettings");

  let truncateSaved = globalStorageLoad("table.truncateColumns");
  let truncateChecked = (truncateSaved == null || truncateSaved == "true") ? "checked" : "";

  let fitSaved = globalStorageLoad("table.fitInPage");
  let fitChecked = (fitSaved == null || fitSaved == "true") ? "checked" : "";

  let currentLimit = $("#inputLimit").val() || "20";

  let html = "<div class='settings-section'>";
  html += "<div class='settings-section-header'>Table Settings</div>";
  html += "<div class='settings-row'><label>Truncate long values</label>";
  html += "<input type='checkbox' class='form-check-input' id='settingTruncate' " + truncateChecked + " onchange='applySidebarSetting(\"table.truncateColumns\", this.checked)'></div>";
  html += "<div class='settings-row'><label>Fit table in page</label>";
  html += "<input type='checkbox' class='form-check-input' id='settingFitPage' " + fitChecked + " onchange='applySidebarSetting(\"table.fitInPage\", this.checked)'></div>";
  html += "</div>";

  html += "<div class='settings-section'>";
  html += "<div class='settings-section-header'>Query Defaults</div>";
  html += "<div class='settings-row'><label>Default auto-limit</label>";
  html += "<select id='settingDefaultLimit' onchange='applyDefaultLimit(this.value)'>";
  let limits = [{ v: "20", l: "20" }, { v: "100", l: "100" }, { v: "500", l: "500" }, { v: "-1", l: "No limit" }];
  for (let i = 0; i < limits.length; i++) {
    let sel = limits[i].v == currentLimit ? " selected" : "";
    html += "<option value='" + limits[i].v + "'" + sel + ">" + limits[i].l + "</option>";
  }
  html += "</select></div>";
  html += "</div>";

  html += "<div class='settings-section'>";
  html += "<div class='settings-section-header'>Keyboard Shortcuts</div>";
  html += "<div style='font-size:0.75rem;color:#666;padding:0 2px;'>";
  html += "<div style='margin-bottom:4px;'><kbd>Ctrl+Enter</kbd> Execute query</div>";
  html += "<div style='margin-bottom:4px;'><kbd>Ctrl+Up</kbd> Previous history</div>";
  html += "<div><kbd>Ctrl+Down</kbd> Next history</div>";
  html += "</div></div>";

  container.html(html);
}

function applySidebarSetting(key, value) {
  globalStorageSave(key, value.toString());
  // Re-render table if it's currently visible
  let activeTab = $("#tabs-command .active").attr("id");
  if (activeTab == "tab-table-sel") renderTable();
}

function applyDefaultLimit(value) {
  $("#inputLimit").val(value);
}

function executeCommand(language, query) {
  globalResultset = null;

  if (language != null) $("#inputLanguage").val(language);
  else language = $("#inputLanguage").val();

  if (query != null) editor.setValue(query);
  else {
    query = editor.getSelection();
    if (query == null || query == "") query = editor.getValue();
  }

  let database = getCurrentDatabase();

  if (database == "") return;
  if (escapeHtml($("#inputLanguage").val()) == "") return;
  if (escapeHtml(query) == "") return;

  globalActivateTab("tab-query");

  let activeTab = $("#tabs-command .active").attr("id");
  if (activeTab == "tab-graph-sel") executeCommandGraph();
  else executeCommandTable();

  let queryHistory = getQueryHistory();

  for (index in queryHistory) {
    let q = queryHistory[index];
    if (q == null || (q.d == database && q.l == language && q.c == query)) {
      // RE-EXECUTED OLD QUERY, REMOVE OLD ENTRY AND INSERT AT THE TOP OF THE LIST
      queryHistory.splice(index, 1);
    }
  }

  // REMOVE OLD QUERIES
  while (queryHistory.length > 25) queryHistory.pop();

  queryHistory = [{ d: database, l: language, c: query, t: Date.now() }].concat(queryHistory);
  globalStorageSave("database.query.history", JSON.stringify(queryHistory));

  // Refresh history panel if visible
  if (activeSidebarPanel == "history") populateHistoryPanel();
}

function executeCommandTable() {
  let database = getCurrentDatabase();
  let language = escapeHtml($("#inputLanguage").val());

  let command = editor.getSelection();
  if (command == null || command.length <= 4) command = editor.getValue();
  command = escapeHtml(command);

  let limit = parseInt($("#inputLimit").val());
  let profileExecution = $("#profileCommand").prop("checked") ? "detailed" : "basic";

  $("#executeSpinner").show();

  let beginTime = new Date();

  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/command/" + database,
      data: JSON.stringify({
        language: language,
        command: command,
        limit: limit,
        profileExecution: profileExecution,
        serializer: "studio",
      }),
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      let elapsed = new Date() - beginTime;
      $("#result-elapsed").html(elapsed);

      $("#result-num").html(data.result.records.length);
      $("#resultJson").val(JSON.stringify(data, null, 2));
      $("#resultExplain").val(data.explain != null ? data.explain : "No profiler data found");

      globalResultset = data.result;
      globalCy = null;
      renderTable();
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      globalNotifyError(jqXHR.responseText);
    })
    .always(function (data) {
      $("#executeSpinner").hide();
    });
}

function executeCommandGraph() {
  let database = getCurrentDatabase();
  let language = escapeHtml($("#inputLanguage").val());

  let command = editor.getSelection();

  if (command == null || command.length <= 4) command = editor.getValue();
  command = escapeHtml(command);

  let limit = parseInt($("#inputLimit").val());
  let profileExecution = $("#profileCommand").prop("checked") ? "detailed" : "basic";

  $("#executeSpinner").show();

  let beginTime = new Date();

  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/command/" + database,
      data: JSON.stringify({
        language: language,
        command: command,
        limit: limit,
        profileExecution: profileExecution,
        serializer: "studio",
      }),
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      let elapsed = new Date() - beginTime;
      $("#result-elapsed").html(elapsed);

      $("#result-num").html(data.result.records.length);
      $("#resultJson").val(JSON.stringify(data, null, 2));
      $("#resultExplain").val(data.explain != null ? data.explain : "'");

      globalResultset = data.result;
      globalCy = null;

      let activeTab = $("#tabs-command .active").attr("id");

      if (data.result.vertices.length == 0 && data.result.records.length > 0) {
        if (activeTab == "tab-table-sel") renderTable();
        else globalActivateTab("tab-table");
      } else {
        if (activeTab == "tab-graph-sel") renderGraph();
        else globalActivateTab("tab-graph");
      }

      // FORCE RESET OF THE SEARCH FIELD
      $("#inputGraphSearch").val("");
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      globalNotifyError(jqXHR.responseText);
    })
    .always(function (data) {
      $("#executeSpinner").hide();
    });
}

function fetchSchemaTypes(callback) {
  let database = getCurrentDatabase();
  if (database == null || database == "") return;

  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/query/" + database,
      data: JSON.stringify({
        language: "sql",
        command: "select from schema:types",
      }),
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      globalSchemaTypes = data.result;
      if (callback)
        callback(data.result);
      populateQuerySidebar();
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      globalNotifyError(jqXHR.responseText);
    })
    .always(function (data) {
      $("#executeSpinner").hide();
    });
}

function displaySchema() {
  fetchSchemaTypes(function (types) {
    // Build sub-types map
    let subTypes = {};
    for (let i in types) {
      let row = types[i];
      for (let ptidx in row.parentTypes) {
        let pt = row.parentTypes[ptidx];
        if (subTypes[pt] == null) subTypes[pt] = [];
        subTypes[pt].push(row.name);
      }
    }

    // Store for later use by showTypeDetail
    window._schemaSubTypes = subTypes;
    window._schemaTypes = types;

    // Group types by category
    let groups = { vertex: [], edge: [], document: [] };
    for (let i in types) {
      let row = types[i];
      let cat = row.type == "vertex" ? "vertex" : (row.type == "edge" ? "edge" : "document");
      groups[cat].push(row);
    }

    // Render badge sidebar
    let sections = [
      { key: "vertex",   label: "Vertices",  icon: "fa-circle-nodes" },
      { key: "edge",     label: "Edges",     icon: "fa-right-left" },
      { key: "document", label: "Documents", icon: "fa-file-lines" }
    ];

    let html = "";
    let palette = sidebarBadgeColors;

    for (let s = 0; s < sections.length; s++) {
      let sec = sections[s];
      let items = groups[sec.key];
      if (items.length == 0) continue;

      let total = 0;
      for (let j = 0; j < items.length; j++) total += (items[j].records || 0);

      html += "<div class='sidebar-section'>";
      html += "<div class='sidebar-section-header'><i class='fa " + sec.icon + "'></i> " + sec.label + " <span class='sidebar-count'>(" + total.toLocaleString() + ")</span></div>";
      html += "<div class='sidebar-badges'>";

      let colors = palette[sec.key];
      for (let j = 0; j < items.length; j++) {
        let row = items[j];
        let color = colors[j % colors.length];
        let name = escapeHtml(row.name);
        let records = (row.records || 0).toLocaleString();
        html += "<a class='sidebar-badge' href='#' style='background-color: " + color + "' ";
        html += "onclick='showTypeDetail(\"" + row.name.replace(/"/g, "&quot;") + "\"); return false;' ";
        html += "title='" + name + " (" + records + " records)'>";
        html += "<span class='sidebar-badge-name'>" + name + "</span>";
        html += "<span class='sidebar-badge-count'>" + records + "</span>";
        html += "</a>";
      }

      html += "</div></div>";
    }

    if (html == "")
      html = "<div class='sidebar-empty'>No types defined.<br><small>Create vertex or document types to see them here.</small></div>";

    $("#dbTypeBadges").html(html);

    // Reset detail panel
    $("#dbTypeDetail").html("<div class='db-type-empty'><i class='fa fa-database' style='font-size: 2rem; color: #ddd; margin-bottom: 12px; display: block;'></i>Select a type from the sidebar to view its schema.</div>");
  });
}

function showTypeDetail(typeName) {
  let types = window._schemaTypes;
  let subTypes = window._schemaSubTypes;
  if (!types) return;

  let row = null;
  for (let i in types)
    if (types[i].name == typeName) { row = types[i]; break; }
  if (row == null) return;

  // Mark active badge
  $("#dbTypeBadges .sidebar-badge").removeClass("db-badge-active");
  $("#dbTypeBadges .sidebar-badge").each(function () {
    if ($(this).find(".sidebar-badge-name").text() == typeName)
      $(this).addClass("db-badge-active");
  });

  let catLabel = row.type == "vertex" ? "Vertex" : (row.type == "edge" ? "Edge" : "Document");
  let catColor = row.type == "vertex" ? "#3b82f6" : (row.type == "edge" ? "#f97316" : "#22c55e");

  let html = "";
  html += "<div class='d-flex align-items-center gap-3 mb-3'>";
  html += "<h4 style='margin:0;'>" + escapeHtml(row.name) + "</h4>";
  html += "<span class='db-type-category-badge' style='background-color:" + catColor + ";'>" + catLabel + "</span>";
  html += "<span style='color:#888; font-size:0.9rem;'>" + (row.records || 0).toLocaleString() + " records</span>";
  html += "</div>";

  // Parent types
  if (row.parentTypes && row.parentTypes.length > 0 && row.parentTypes[0] != "") {
    html += "<div class='db-type-meta'>Super Types: ";
    for (let ptidx in row.parentTypes) {
      if (ptidx > 0) html += ", ";
      html += "<a class='link' href='#' onclick='showTypeDetail(\"" + row.parentTypes[ptidx] + "\"); return false;'>" + escapeHtml(row.parentTypes[ptidx]) + "</a>";
    }
    html += "</div>";
  }

  // Sub types
  let typeSubTypes = subTypes[row.name];
  if (typeSubTypes != null && typeSubTypes.length > 0) {
    html += "<div class='db-type-meta'>Sub Types: ";
    for (let stidx in typeSubTypes) {
      if (stidx > 0) html += ", ";
      html += "<a class='link' href='#' onclick='showTypeDetail(\"" + typeSubTypes[stidx] + "\"); return false;'>" + escapeHtml(typeSubTypes[stidx]) + "</a>";
    }
    html += "</div>";
  }

  // Properties section
  html += "<div class='db-detail-section'>";
  html += "<h6><i class='fa fa-list'></i> Properties</h6>";
  let propHtml = renderProperties(row, types);
  if (propHtml == "") {
    html += "<p class='text-muted' style='font-size:0.85rem;'>No properties defined.</p>";
  } else {
    html += "<div class='table-responsive'>";
    html += "<table class='table table-sm db-detail-table'>";
    html += "<thead><tr><th>Name</th><th>Defined In</th><th>Type</th><th>Mandatory</th><th>Not Null</th><th>Hidden</th><th>Read Only</th><th>Default</th><th>Min</th><th>Max</th><th>Regexp</th><th>Indexes</th><th>Actions</th></tr></thead>";
    html += "<tbody>" + propHtml + "</tbody></table></div>";
  }
  html += "</div>";

  // Indexes section
  if (row.indexes && row.indexes.length > 0) {
    html += "<div class='db-detail-section'>";
    html += "<h6><i class='fa fa-bolt'></i> Indexes</h6>";
    html += "<div class='table-responsive'>";
    html += "<table class='table table-sm db-detail-table'>";
    html += "<thead><tr><th>Name</th><th>Defined In</th><th>Properties</th><th>Type</th><th>Unique</th><th>Automatic</th><th>Actions</th></tr></thead>";
    html += "<tbody>" + renderIndexes(row, types) + "</tbody></table></div>";
    html += "</div>";
  }

  // Actions section
  html += "<div class='db-detail-section'>";
  html += "<h6><i class='fa fa-play-circle'></i> Quick Actions</h6>";
  html += "<div class='d-flex flex-wrap gap-2'>";
  html += "<button class='btn btn-sm db-action-btn' onclick='executeCommand(\"sql\", \"select from \\`" + row.name + "\\`\")'><i class='fa fa-table'></i> Browse records</button>";
  if (row.type == "vertex")
    html += "<button class='btn btn-sm db-action-btn' onclick='executeCommand(\"sql\", \"select *, bothE() as \\`@edges\\` from \\`" + row.name + "\\`\")'><i class='fa fa-project-diagram'></i> With connections</button>";
  html += "<button class='btn btn-sm db-action-btn' onclick='executeCommand(\"sql\", \"select count(*) from \\`" + row.name + "\\`\")'><i class='fa fa-calculator'></i> Count records</button>";
  html += "</div>";
  html += "</div>";

  $("#dbTypeDetail").html(html);
}

var sidebarBadgeColors = {
  vertex:   ["#3b82f6", "#0ea5e9", "#6366f1", "#8b5cf6", "#06b6d4", "#2563eb", "#4f46e5", "#0284c7"],
  edge:     ["#f97316", "#f59e0b", "#ef4444", "#e11d48", "#ea580c", "#d97706", "#dc2626", "#be123c"],
  document: ["#22c55e", "#10b981", "#14b8a6", "#059669", "#16a34a", "#0d9488", "#15803d", "#047857"]
};

function populateQuerySidebar() {
  let container = $("#sidebarPanelOverview");
  if (container.length == 0) return;

  if (globalSchemaTypes == null || globalSchemaTypes.length == 0) {
    container.html("<div class='sidebar-empty'>No types defined.<br><small>Create vertex or document types to see them here.</small></div>");
    return;
  }

  let groups = { vertex: [], edge: [], document: [] };
  let totals = { vertex: 0, edge: 0, document: 0 };

  for (let i in globalSchemaTypes) {
    let row = globalSchemaTypes[i];
    let cat = row.type == "vertex" ? "vertex" : (row.type == "edge" ? "edge" : "document");
    groups[cat].push(row);
    totals[cat] += (row.records || 0);
  }

  let html = "";
  let sections = [
    { key: "vertex",   label: "Vertices",  icon: "fa-circle-nodes" },
    { key: "edge",     label: "Edges",     icon: "fa-right-left" },
    { key: "document", label: "Documents", icon: "fa-file-lines" }
  ];

  for (let s = 0; s < sections.length; s++) {
    let sec = sections[s];
    let items = groups[sec.key];
    if (items.length == 0) continue;

    let totalFormatted = totals[sec.key].toLocaleString();
    html += "<div class='sidebar-section'>";
    html += "<div class='sidebar-section-header'><i class='fa " + sec.icon + "'></i> " + sec.label + " <span class='sidebar-count'>(" + totalFormatted + ")</span></div>";
    html += "<div class='sidebar-badges'>";

    let palette = sidebarBadgeColors[sec.key];
    for (let j = 0; j < items.length; j++) {
      let row = items[j];
      let color = palette[j % palette.length];
      let name = escapeHtml(row.name);
      let records = (row.records || 0).toLocaleString();
      html += "<a class='sidebar-badge' href='#' style='background-color: " + color + "' ";
      html += "onclick='executeCommand(\"sql\", \"select from \\`" + row.name + "\\`\"); return false;' ";
      html += "title='" + name + " (" + records + " records)'>";
      html += "<span class='sidebar-badge-name'>" + name + "</span>";
      html += "<span class='sidebar-badge-count'>" + records + "</span>";
      html += "</a>";
    }

    html += "</div></div>";
  }

  container.html(html);
}

function refreshQuerySidebar() {
  globalSchemaTypes = null;
  fetchSchemaTypes();
}

function findTypeInResult(name, results) {
  for (i in results) if (results[i].name == name) return results[i];
  return null;
}

function renderProperties(row, results) {
  let panelHtml = "";

  for (let k in row.properties) {
    let property = row.properties[k];
    panelHtml += "<tr><td>" + property.name + "</td><td>" + row.name + "</td><td>" + property.type + "</td>";

    panelHtml += "<td>" + (property.mandatory ? true : false) + "</td>";
    panelHtml += "<td>" + (property.notNull ? true : false) + "</td>";
    panelHtml += "<td>" + (property.hidden ? true : false) + "</td>";
    panelHtml += "<td>" + (property.readOnly ? true : false) + "</td>";
    panelHtml += "<td>" + (property["default"] != null ? property["default"] : "") + "</td>";
    panelHtml += "<td>" + (property.min != null ? property.min : "") + "</td>";
    panelHtml += "<td>" + (property.max != null ? property.max : "") + "</td>";
    panelHtml += "<td>" + (property.regexp != null ? property.regexp : "") + "</td>";

    let totalIndexes = 0;
    if (row.indexes != null && row.indexes.length > 0) {
      for (i in row.indexes) {
        if (row.indexes[i].properties.includes(property.name)) ++totalIndexes;
      }
    }

    panelHtml += "<td>" + (totalIndexes > 0 ? totalIndexes : "None") + "</td>";
    panelHtml +=
      "<td><button class='btn btn-sm db-action-btn db-action-btn-danger' onclick='dropProperty(\"" +
      row.name +
      '", "' +
      property.name +
      "\")'><i class='fa fa-minus'></i> Drop Property</button></td></tr>";

    if (property.custom != null && Object.keys(property.custom).length > 0) {
      panelHtml += "<td></td>";
      panelHtml += "<td colspan='10'><b>Custom Properties</b><br>";
      panelHtml += "<div class='table-responsive'>";
      panelHtml += "<table style='width: 100%'>";
      for (c in property.custom) panelHtml += "<tr><td width='30%'>" + c + "</td><td width='70%'>" + property.custom[c] + "</td></tr>";
      panelHtml += "</table></div></td>";
    }
  }

  if (row.parentTypes != "") {
    for (ptidx in row.parentTypes) {
      let pt = row.parentTypes[ptidx];
      let type = findTypeInResult(pt, results);
      panelHtml += renderProperties(type, results);
    }
  }

  return panelHtml;
}

function renderIndexes(row, results) {
  let panelHtml = "";

  for (let k in row.indexes) {
    let index = row.indexes[k];
    panelHtml += "<tr><td>" + index.name + "</td><td>" + index.typeName + "</td>";
    panelHtml += "<td>" + index.properties + "</td>";
    panelHtml += "<td>" + index.type + "</td>";

    panelHtml += "<td>" + (index.unique ? true : false) + "</td>";
    panelHtml += "<td>" + (index.automatic ? true : false) + "</td>";
    panelHtml += "<td><button class='btn btn-sm db-action-btn db-action-btn-danger' onclick='dropIndex(\"" + index.name + "\")'><i class='fa fa-minus'></i> Drop Index</button></td></tr>";
  }
  return panelHtml;
}

function displayDatabaseSettings() {
  let database = getCurrentDatabase();
  if (database == null || database == "") return;

  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/query/" + database,
      data: JSON.stringify({
        language: "sql",
        command: "select expand( settings ) from schema:database",
      }),
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      if ($.fn.dataTable.isDataTable("#dbSettings"))
        try {
          $("#dbSettings").DataTable().destroy();
          $("#serverMetrics").empty();
        } catch (e) {}

      var tableRecords = [];

      for (let i in data.result) {
        let row = data.result[i];

        let record = [];
        record.push(row.key);
        record.push(row.value);
        record.push(row.description);
        record.push(row.default);
        record.push(row.overridden);
        tableRecords.push(record);
      }

      $("#dbSettings").DataTable({
        paging: false,
        ordering: false,
        autoWidth: false,
        columns: [
          { title: "Key", width: "25%" },
          {
            title: "Value",
            width: "20%",
            render: function (data, type, row) {
              return "<a href='#' onclick='updateDatabaseSetting(\"" + row[0] + '", "' + row[1] + "\")' style='color: green;'><b>" + data + "</b></a>";
            },
          },
          { title: "Description", width: "33%" },
          { title: "Default", width: "15%" },
          { title: "Overridden", width: "7%" },
        ],
        data: tableRecords,
      });
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      globalNotifyError(jqXHR.responseText);
    })
    .always(function (data) {
      $("#executeSpinner").hide();
    });
}

function updateDatabaseSetting(key, value) {
  let html = "<b>" + escapeHtml(key) + "</b> = <input class='form-control mt-2' id='updateSettingInput' value='" + escapeHtml(value) + "' " +
    "onkeydown='if (event.which === 13) document.getElementById(\"globalModalConfirmBtn\").click()'>";
  html += "<br><p><i>The setting will be saved in the database configuration.</i></p>";

  globalPrompt("Update Database Setting", html, "Update", function() {
    jQuery
      .ajax({
        type: "POST",
        url: "api/v1/server",
        data: JSON.stringify({
          command: "set database setting " + getCurrentDatabase() + " " + key + " " + $("#updateSettingInput").val(),
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
        displayDatabaseSettings();
        return true;
      });
  });
}

// Database backup functionality
var dbBackupsLoaded = false;

function loadDatabaseBackups() {
  let database = getCurrentDatabase();
  if (database == null || database == "") {
    globalNotify("Error", "No database selected", "danger");
    return;
  }

  // Reset loaded flag when explicitly refreshing
  dbBackupsLoaded = true;

  // Load backup config first
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
      if (data.config == null || data.config === "null") {
        $("#dbBackupConfigInfo").html(
          '<p class="text-warning">Auto-backup is not configured. Go to Server &gt; Backup tab to configure it.</p>'
        );
      } else {
        displayDbBackupConfig(data.config, data.enabled, data.message);
      }
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      $("#dbBackupConfigInfo").html('<p class="text-danger">Error loading configuration</p>');
    });

  // Load backup list
  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/server",
      data: JSON.stringify({ command: "list backups " + database }),
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      displayDbBackupList(data);
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      globalNotifyError(jqXHR.responseText);
    });
}

function displayDbBackupConfig(config, pluginEnabled, message) {
  let html = "";

  if (!pluginEnabled && message) {
    html += '<div class="alert alert-warning mb-2"><i class="fa fa-exclamation-triangle"></i> ' + escapeHtml(message) + "</div>";
  } else if (!pluginEnabled) {
    html += '<div class="alert alert-info mb-2"><i class="fa fa-info-circle"></i> Configuration saved. Restart server to enable auto-backup.</div>';
  }

  html += "<table class='table table-sm'>";
  html += "<tr><td><strong>Enabled</strong></td><td>" + (config.enabled ? "Yes" : "No") + "</td></tr>";
  html += "<tr><td><strong>Backup Directory</strong></td><td>" + escapeHtml(config.backupDirectory || "./backups") + "</td></tr>";

  if (config.defaults) {
    let defaults = config.defaults;
    html += "<tr><td><strong>Run On Server</strong></td><td>" + escapeHtml(defaults.runOnServer || "$leader") + "</td></tr>";

    if (defaults.schedule) {
      let sched = defaults.schedule;
      if (sched.type === "frequency") {
        html += "<tr><td><strong>Schedule</strong></td><td>Every " + sched.frequencyMinutes + " minutes</td></tr>";
      } else if (sched.type === "cron") {
        html += "<tr><td><strong>Schedule</strong></td><td>CRON: " + escapeHtml(sched.expression || "") + "</td></tr>";
      }

      if (sched.timeWindow) {
        html +=
          "<tr><td><strong>Time Window</strong></td><td>" +
          escapeHtml(sched.timeWindow.start || "") +
          " - " +
          escapeHtml(sched.timeWindow.end || "") +
          "</td></tr>";
      }
    }

    if (defaults.retention) {
      let ret = defaults.retention;
      html += "<tr><td><strong>Max Files</strong></td><td>" + ret.maxFiles + "</td></tr>";
      if (ret.tiered) {
        html +=
          "<tr><td><strong>Tiered Retention</strong></td><td>Hourly: " +
          ret.tiered.hourly +
          ", Daily: " +
          ret.tiered.daily +
          ", Weekly: " +
          ret.tiered.weekly +
          ", Monthly: " +
          ret.tiered.monthly +
          ", Yearly: " +
          ret.tiered.yearly +
          "</td></tr>";
      }
    }
  }

  html += "</table>";
  $("#dbBackupConfigInfo").html(html);
}

function displayDbBackupList(data) {
  // Update statistics
  $("#dbBackupTotalCount").text(data.totalCount || data.backups.length);
  $("#dbBackupTotalSize").text(globalFormatSpace(data.totalSize || 0));

  // Setup DataTable
  if ($.fn.dataTable.isDataTable("#dbBackupList")) {
    try {
      $("#dbBackupList").DataTable().destroy();
      $("#dbBackupList").empty();
    } catch (e) {}
  }

  let tableRecords = [];

  for (let i in data.backups) {
    let backup = data.backups[i];

    let record = [];
    record.push(escapeHtml(backup.fileName));
    record.push(backup.timestamp ? formatBackupTimestamp(backup.timestamp) : "-");
    record.push(globalFormatSpace(backup.size));
    tableRecords.push(record);
  }

  $("#dbBackupList").DataTable({
    paging: true,
    ordering: true,
    order: [[1, "desc"]],
    pageLength: 25,
    columns: [
      { title: "File Name", width: "50%" },
      { title: "Timestamp", width: "30%" },
      { title: "Size", width: "20%" },
    ],
    data: tableRecords,
  });
}

function formatBackupTimestamp(timestamp) {
  if (!timestamp) return "-";
  try {
    let date = new Date(timestamp);
    return (
      date.toLocaleDateString() +
      " " +
      date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })
    );
  } catch (e) {
    return timestamp;
  }
}

function triggerDatabaseBackup() {
  let database = getCurrentDatabase();
  if (database == null || database == "") {
    globalNotify("Error", "No database selected", "danger");
    return;
  }

  globalConfirm(
    "Trigger Backup",
    "Are you sure you want to trigger an immediate backup for database '" + escapeHtml(database) + "'?",
    "info",
    function () {
      jQuery
        .ajax({
          type: "POST",
          url: "api/v1/server",
          data: JSON.stringify({ command: "trigger backup " + database }),
          beforeSend: function (xhr) {
            xhr.setRequestHeader("Authorization", globalCredentials);
          },
        })
        .done(function (data) {
          let msg = "Backup completed successfully for database '" + escapeHtml(database) + "'";
          if (data.backupFile) {
            msg += "<br><small>File: " + escapeHtml(data.backupFile) + "</small>";
          }
          globalNotify("Backup", msg, "success");
          // Reload the backup list to show the new backup
          loadDatabaseBackups();
        })
        .fail(function (jqXHR, textStatus, errorThrown) {
          globalNotifyError(jqXHR.responseText);
        });
    }
  );
}

// Register tab change handler for database backup tab
$(document).ready(function () {
  $('a[data-toggle="tab"]').on("shown.bs.tab", function (e) {
    var activeTab = this.id;
    if (activeTab == "tab-db-backup-sel") {
      if (!dbBackupsLoaded) {
        loadDatabaseBackups();
      }
    }
  });
});
