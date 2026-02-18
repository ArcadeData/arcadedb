var editor = null;
var globalResultset = null;
var globalGraphMaxResult = 1000;
var globalCredentials = null;
var globalBasicAuth = null;
var globalUsername = null;

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
      $("#loginPopup").modal("hide");
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

        // Show login popup
        $("#studioPanel").hide();
        $("#welcomePanel").show();
        $("#loginPopup").modal("show");

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
  let html =
    "<label for='inputCreateDatabaseName'>Enter the database name:&nbsp;&nbsp;</label><input onkeydown='if (event.which === 13) Swal.clickConfirm()' id='inputCreateDatabaseName'>";

  Swal.fire({
    title: "Create a new database",
    html: html,
    inputAttributes: {
      autocapitalize: "off",
    },
    confirmButtonColor: "#3ac47d",
    cancelButtonColor: "red",
    showCancelButton: true,
    confirmButtonText: "Send",
  }).then((result) => {
    if (result.value) {
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
    }
  });

  $("#inputCreateDatabaseName").focus();
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

function loadQueryHistory() {
  $("#inputHistory").html("");
  $("#inputHistory").append("<option value='-1'>History</option>");

  let queryHistory = getQueryHistory();
  if (queryHistory != null && queryHistory.length > 0) {
    let database = escapeHtml(getCurrentDatabase());
    for (let index = 0; index < queryHistory.length; ++index) {
      let q = queryHistory[index];
      if (q != null && q.d == database && q.l != null && q.c != null)
        $("#inputHistory").append("<option value='" + index + "'>(" + q.l + ") " + q.c + "</option>");
    }
  }
}

function copyQueryFromHistory() {
  let index = $("#inputHistory").val();
  if (index == -1) return;

  if (index != "") {
    let queryHistory = getQueryHistory();
    let q = queryHistory[index];
    if (q != null) {
      setCurrentDatabase(q.d);
      $("#inputLanguage").val(q.l);
      editor.setValue(q.c);
    }
  }

  $("#inputHistory").val(-1);
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

  queryHistory = [{ d: database, l: language, c: query }].concat(queryHistory);
  globalStorageSave("database.query.history", JSON.stringify(queryHistory));

  loadQueryHistory();
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

function displaySchema() {
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
      let tabVHtml = "";
      let tabEHtml = "";
      let tabDHtml = "";

      let panelVHtml = "";
      let panelEHtml = "";
      let panelDHtml = "";

      // BUILD SUB TYPES
      let subTypes = {};
      for (let i in data.result) {
        let row = data.result[i];

        for (ptidx in row.parentTypes) {
          let pt = row.parentTypes[ptidx];

          let array = subTypes[pt];
          if (array == null) {
            array = [];
            subTypes[pt] = array;
          }
          array.push(row.name);
        }
      }

      for (let i in data.result) {
        let row = data.result[i];
        let tabName = row.name.replaceAll(":", "-");

        let tabHtml =
          "<li class='nav-item' style='height: 32px; width: 240px'><a data-bs-toggle='tab' href='#tab-" +
          tabName +
          "' class='nav-link vertical-tab" +
          (i == 0 ? " active show" : "");
        tabHtml += "' id='tab-" + tabName + "-sel'>" + row.name + "</a></li>";

        let panelHtml = "<div class='tab-pane fade" + (i == 0 ? " active show" : "") + "' id='tab-" + tabName + "' role='tabpanel'>";

        panelHtml += "<h3>" + row.name + " <span style='font-size: 60%'>(" + row.records + " records)</span></h3>";
        if (row.parentTypes != "") {
          panelHtml += "Super Types: <b>";
          for (ptidx in row.parentTypes) {
            if (ptidx > 0) panelHtml += ", ";
            let pt = row.parentTypes[ptidx];
            let ptName = pt.replaceAll(":", "-");

            panelHtml += "<b><a href='#' onclick=\"globalActivateTab('tab-" + ptName + "')\">" + pt + "</a></b>";
          }
          panelHtml += "</b>";
        }

        let typeSubTypes = subTypes[row.name];
        if (typeSubTypes != null) {
          panelHtml += "<br>Sub Types: <b>";
          for (stidx in typeSubTypes) {
            if (stidx > 0) panelHtml += ", ";
            let st = typeSubTypes[stidx];
            let stName = st.replaceAll(":", "-");

            panelHtml += "<b><a href='#' onclick=\"globalActivateTab('tab-" + stName + "')\">" + st + "</a></b>";
          }
          panelHtml += "</b>";
        }

        panelHtml += "<br>";

        if (row.indexes != "") {
          panelHtml += "<br><h6>Indexes</h6>";
          panelHtml += "<div class='table-responsive'>";
          panelHtml += "<table class='table table-striped table-sm' style='border: 0px; width: 100%'>";
          panelHtml += "<thead><tr><th scope='col'>Name</th>";
          panelHtml += "<th scope='col'>Defined In</th>";
          panelHtml += "<th scope='col'>Properties</th>";
          panelHtml += "<th scope='col'>Type</th>";
          panelHtml += "<th scope='col'>Unique</th>";
          panelHtml += "<th scope='col'>Automatic</th>";
          panelHtml += "<th scope='col'>Actions</th>";
          panelHtml += "<tbody>";

          panelHtml += renderIndexes(row, data.result);

          panelHtml += "</tbody></table></div>";
        }

        panelHtml += "<br><h6>Properties</h6>";
        //panelHtml += "<button class='btn btn-pill' onclick='createProperty()'><i class='fa fa-plus'></i> Create Property</button>";
        panelHtml += "<div class='table-responsive'>";
        panelHtml += "<table class='table table-striped table-sm' style='border: 0px; width: 100%'>";
        panelHtml += "<thead><tr><th scope='col'>Name</th>";
        panelHtml += "<th scope='col'>Defined In</th>";
        panelHtml += "<th scope='col'>Type</th>";
        panelHtml += "<th scope='col'>Mandatory</th>";
        panelHtml += "<th scope='col'>Not Null</th>";
        panelHtml += "<th scope='col'>Hidden</th>";
        panelHtml += "<th scope='col'>Read Only</th>";
        panelHtml += "<th scope='col'>Default Value</th>";
        panelHtml += "<th scope='col'>Min</th>";
        panelHtml += "<th scope='col'>Max</th>";
        panelHtml += "<th scope='col'>Regexp</th>";
        panelHtml += "<th scope='col'>Indexes</th>";
        panelHtml += "<th scope='col'>Actions</th>";
        panelHtml += "<tbody>";

        panelHtml += renderProperties(row, data.result);

        panelHtml += "</tbody></table></div>";

        panelHtml += "<br><h6>Actions</h6>";
        panelHtml += "<ul>";
        panelHtml +=
          "<li><a class='link' href='#' onclick='executeCommand(\"sql\", \"select from `" +
          row.name +
          "` limit 30\")'>Display the first 30 records of " +
          row.name +
          "</a>";
        if (row.type == "vertex")
          panelHtml +=
            "<li><a class='link' href='#' onclick='executeCommand(\"sql\", \"select *, bothE() as `@edges` from `" +
            row.name +
            "` limit 30\")'>Display the first 30 records of " +
            row.name +
            " together with all the vertices that are directly connected</a>";
        panelHtml +=
          "<li><a class='link' href='#' onclick='executeCommand(\"sql\", \"select count(*) from `" +
          row.name +
          "`\")'>Count the records of type " +
          row.name +
          "</a>";
        panelHtml += "</ul>";

        panelHtml += "</div>";

        if (row.type == "vertex") {
          tabVHtml += tabHtml;
          panelVHtml += panelHtml;
        } else if (row.type == "edge") {
          tabEHtml += tabHtml;
          panelEHtml += panelHtml;
        } else {
          tabDHtml += tabHtml;
          panelDHtml += panelHtml;
        }
      }

      $("#vTypesTabs").html(tabVHtml);
      $("#eTypesTabs").html(tabEHtml);
      $("#dTypesTabs").html(tabDHtml);

      $("#vTypesPanels").html(panelVHtml != "" ? panelVHtml : "Not defined.");
      $("#eTypesPanels").html(panelEHtml != "" ? panelEHtml : "Not defined.");
      $("#dTypesPanels").html(panelDHtml != "" ? panelDHtml : "Not defined.");
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      globalNotifyError(jqXHR.responseText);
    })
    .always(function (data) {
      $("#executeSpinner").hide();
    });
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
      "<td><button class='btn btn-pill' onclick='dropProperty(\"" +
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
    panelHtml += "<td><button class='btn btn-pill' onclick='dropIndex(\"" + index.name + "\")'><i class='fa fa-minus'></i> Drop Index</button></td></tr>";
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
  let html = "<b>" + key + "</b> = <input id='updateSettingInput' value='" + value + "'>";
  html += "<br><p><i>The setting will be saved in the database configuration.</i></p>";

  Swal.fire({
    title: "Update Database Setting",
    html: html,
    showCancelButton: true,
    width: 600,
    confirmButtonColor: "#3ac47d",
    cancelButtonColor: "red",
  }).then((result) => {
    if (result.value) {
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
    }
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
