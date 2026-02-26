var editor = null;
var globalResultset = null;
var globalExplainPlan = null;
var globalGraphMaxResult = 1000;
var globalCredentials = null;
var globalBasicAuth = null;
var globalUsername = null;
var globalSchemaTypes = null;
var globalFunctionReference = null;

// Register arcadedb-sql MIME type eagerly so the editor can use it before JSON loads
(function() {
  if (typeof CodeMirror !== "undefined") {
    var basicKw = "select insert update delete create alter drop truncate from where and or not in is null set into values as asc desc order by group having limit offset distinct join left right inner outer on between like exists count sum avg min max begin commit rollback return match traverse let if else vertex edge document type property index bucket";
    var kwObj = {};
    basicKw.split(" ").forEach(function(w) { kwObj[w] = true; });
    CodeMirror.defineMIME("arcadedb-sql", {
      name: "sql",
      keywords: kwObj,
      builtin: {},
      atoms: { "false": true, "true": true, "null": true },
      operatorChars: /^[*+\-%<>!=&|~^\/\?]/
    });
  }
})();

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

          // Show login page and hide studio panel
          $("#studioPanel").hide();
          showLoginPopup();

          // Clear form fields
          $("#inputUserName").val("");
          $("#inputUserPassword").val("");
        });
    }
  );
}

function showLoginPopup() {
  $("#studioPanel").hide();
  $("#loginPage").show();
  if (typeof loginNetworkInit === 'function') loginNetworkInit();
  setTimeout(function() { $("#inputUserName").focus().select(); }, 400);
}

function hideLoginPage() {
  if (typeof loginNetworkStop === 'function') loginNetworkStop();
  $("#loginPage").hide();
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
      $("#tsUser").html(username);
      console.log("Set user to:", username);

      // CRITICAL: Always hide login and show studio, even if other operations fail
      hideLoginPage();
      $("#welcomePanel").hide();
      $("#studioPanel").show();
      console.log("UI updated - login page hidden, studio panel shown");

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

function collectTypeProperties(typeName) {
  let props = [];
  let visited = {};

  function collect(name) {
    if (visited[name]) return;
    visited[name] = true;
    let type = findTypeInResult(name, window._schemaTypes || []);
    if (!type) return;
    for (let k in type.properties) {
      let p = type.properties[k];
      if (!props.find(function (x) { return x.name == p.name; }))
        props.push({ name: p.name, type: p.type || "" });
    }
    for (let ptidx in type.parentTypes)
      if (type.parentTypes[ptidx] != "")
        collect(type.parentTypes[ptidx]);
  }
  collect(typeName);
  return props;
}

function refreshSchemaAndShowType(typeName) {
  fetchSchemaTypes(function (types) {
    let subTypes = {};
    for (let i in types) {
      let t = types[i];
      for (let ptidx in t.parentTypes) {
        let pt = t.parentTypes[ptidx];
        if (subTypes[pt] == null) subTypes[pt] = [];
        subTypes[pt].push(t.name);
      }
    }
    window._schemaSubTypes = subTypes;
    window._schemaTypes = types;
    showTypeDetail(typeName);
  });
}

function createProperty(typeName) {
  let database = getCurrentDatabase();
  if (database == "") {
    globalNotify("Error", "Database not selected", "danger");
    return;
  }

  let allTypes = [
    "BOOLEAN", "INTEGER", "SHORT", "LONG", "FLOAT", "DOUBLE", "BYTE",
    "STRING", "BINARY", "DATE", "DATETIME", "DATETIME_SECOND", "DATETIME_MICROS", "DATETIME_NANOS",
    "DECIMAL", "LIST", "MAP", "LINK", "EMBEDDED",
    "ARRAY_OF_SHORTS", "ARRAY_OF_INTEGERS", "ARRAY_OF_LONGS", "ARRAY_OF_FLOATS", "ARRAY_OF_DOUBLES"
  ];

  let linkedTypes = [
    "BOOLEAN", "INTEGER", "SHORT", "LONG", "FLOAT", "DOUBLE", "BYTE",
    "STRING", "BINARY", "DATE", "DATETIME", "DATETIME_SECOND", "DATETIME_MICROS", "DATETIME_NANOS",
    "DECIMAL", "LINK", "EMBEDDED"
  ];
  if (window._schemaTypes) {
    for (let i in window._schemaTypes)
      linkedTypes.push(window._schemaTypes[i].name);
  }

  var dlg = document.getElementById("globalModalDialog");
  dlg.classList.add("modal-lg");
  var modalEl = document.getElementById("globalModal");
  modalEl.addEventListener("hidden.bs.modal", function restorePropSize() {
    dlg.classList.remove("modal-lg");
    modalEl.removeEventListener("hidden.bs.modal", restorePropSize);
  });

  let html = "";

  html += "<label for='inputCreatePropName'>Property name <span style='color:#dc3545'>*</span></label>";
  html += "<input class='form-control mt-1 mb-3' id='inputCreatePropName' placeholder='e.g. myProperty'>";

  html += "<label for='inputCreatePropType'>Type <span style='color:#dc3545'>*</span></label>";
  html += "<select class='form-select mt-1 mb-3' id='inputCreatePropType'>";
  for (let i = 0; i < allTypes.length; i++)
    html += "<option value='" + allTypes[i] + "'" + (allTypes[i] == "STRING" ? " selected" : "") + ">" + allTypes[i] + "</option>";
  html += "</select>";

  html += "<div id='createPropOfTypeRow' style='display:none;'>";
  html += "<label for='inputCreatePropOfType'>Of Type <small class='text-muted'>(element / linked type)</small></label>";
  html += "<input class='form-control mt-1 mb-3' id='inputCreatePropOfType' list='createPropOfTypeList' placeholder='e.g. STRING'>";
  html += "<datalist id='createPropOfTypeList'>";
  for (let i = 0; i < linkedTypes.length; i++)
    html += "<option value='" + escapeHtml(linkedTypes[i]) + "'>";
  html += "</datalist>";
  html += "</div>";

  html += "<div class='row mb-3'>";
  html += "<div class='col-6'>";
  html += "<label for='inputCreatePropDefault'>Default value</label>";
  html += "<input class='form-control mt-1' id='inputCreatePropDefault' placeholder='(none)'>";
  html += "</div>";
  html += "<div class='col-6' id='createPropRegexpRow'>";
  html += "<label for='inputCreatePropRegexp'>Regexp</label>";
  html += "<input class='form-control mt-1' id='inputCreatePropRegexp' placeholder='e.g. [A-Za-z0-9]+'>";
  html += "</div>";
  html += "</div>";

  html += "<div class='row mb-3'>";
  html += "<div class='col-6'>";
  html += "<label for='inputCreatePropMin'>Min</label>";
  html += "<input class='form-control mt-1' id='inputCreatePropMin' placeholder='(none)'>";
  html += "</div>";
  html += "<div class='col-6'>";
  html += "<label for='inputCreatePropMax'>Max</label>";
  html += "<input class='form-control mt-1' id='inputCreatePropMax' placeholder='(none)'>";
  html += "</div>";
  html += "</div>";

  html += "<div class='d-flex flex-wrap gap-3'>";
  html += "<div class='form-check'><input class='form-check-input' type='checkbox' id='inputCreatePropMandatory'><label class='form-check-label' for='inputCreatePropMandatory'>Mandatory</label></div>";
  html += "<div class='form-check'><input class='form-check-input' type='checkbox' id='inputCreatePropNotNull'><label class='form-check-label' for='inputCreatePropNotNull'>Not Null</label></div>";
  html += "<div class='form-check'><input class='form-check-input' type='checkbox' id='inputCreatePropHidden'><label class='form-check-label' for='inputCreatePropHidden'>Hidden</label></div>";
  html += "<div class='form-check'><input class='form-check-input' type='checkbox' id='inputCreatePropReadOnly'><label class='form-check-label' for='inputCreatePropReadOnly'>Read Only</label></div>";
  html += "<div class='form-check'><input class='form-check-input' type='checkbox' id='inputCreatePropIfNotExists'><label class='form-check-label' for='inputCreatePropIfNotExists'>If not exists</label></div>";
  html += "</div>";

  globalPrompt("Add Property to " + escapeHtml(typeName), html, "Create", function () {
    let name = $("#inputCreatePropName").val().trim();
    if (name == "") {
      globalNotify("Error", "Property name is required", "danger");
      return;
    }

    let type = $("#inputCreatePropType").val();
    let ofType = $("#inputCreatePropOfType").val().trim();
    let defaultVal = $("#inputCreatePropDefault").val().trim();
    let regexp = $("#inputCreatePropRegexp").val().trim();
    let min = $("#inputCreatePropMin").val().trim();
    let max = $("#inputCreatePropMax").val().trim();
    let mandatory = $("#inputCreatePropMandatory").prop("checked");
    let notNull = $("#inputCreatePropNotNull").prop("checked");
    let hidden = $("#inputCreatePropHidden").prop("checked");
    let readOnly = $("#inputCreatePropReadOnly").prop("checked");
    let ifNotExists = $("#inputCreatePropIfNotExists").prop("checked");

    let command = "CREATE PROPERTY `" + typeName + "`.`" + name + "`";
    if (ifNotExists) command += " IF NOT EXISTS";
    command += " " + type;
    if (ofType != "") command += " OF " + ofType;

    let constraints = [];
    if (mandatory) constraints.push("MANDATORY true");
    if (notNull) constraints.push("NOTNULL true");
    if (hidden) constraints.push("HIDDEN true");
    if (readOnly) constraints.push("READONLY true");
    if (defaultVal != "") constraints.push("DEFAULT " + defaultVal);
    if (min != "") constraints.push("MIN " + min);
    if (max != "") constraints.push("MAX " + max);
    if (regexp != "") constraints.push("REGEXP '" + regexp.replace(/'/g, "''") + "'");
    if (constraints.length > 0) command += " (" + constraints.join(", ") + ")";

    jQuery.ajax({
      type: "POST",
      url: "api/v1/command/" + database,
      data: JSON.stringify({ language: "sql", command: command, serializer: "record" }),
      beforeSend: function (xhr) { xhr.setRequestHeader("Authorization", globalCredentials); }
    }).done(function (data) {
      globalNotify("Success", "Property '" + escapeHtml(name) + "' created on '" + escapeHtml(typeName) + "'", "success");
      refreshSchemaAndShowType(typeName);
    }).fail(function (jqXHR) {
      globalNotifyError(jqXHR.responseText);
    });
  });

  setTimeout(function () {
    function updatePropVisibility() {
      let sel = $("#inputCreatePropType").val();
      if (sel == "LIST" || sel == "MAP" || sel == "LINK" || sel == "EMBEDDED")
        $("#createPropOfTypeRow").show();
      else
        $("#createPropOfTypeRow").hide();
      if (sel == "STRING")
        $("#createPropRegexpRow").show();
      else
        $("#createPropRegexpRow").hide();
    }
    $("#inputCreatePropType").on("change", updatePropVisibility);
    updatePropVisibility();
  }, 100);
}

function createIndex(typeName) {
  let database = getCurrentDatabase();
  if (database == "") {
    globalNotify("Error", "Database not selected", "danger");
    return;
  }

  let properties = collectTypeProperties(typeName);

  let vectorProps = properties.filter(function (p) {
    return p.type == "ARRAY_OF_FLOATS" || p.type == "ARRAY_OF_DOUBLES";
  });

  let html = "";

  // Normal properties (LSM_TREE / FULL_TEXT)
  html += "<div id='createIdxPropsNormal'>";
  html += "<label>Properties <span style='color:#dc3545'>*</span></label>";
  if (properties.length > 0) {
    html += "<small class='text-muted d-block mt-1'>Hold Ctrl/Cmd to select multiple for a composite index.</small>";
    html += "<select class='form-select mt-1 mb-3' id='inputCreateIdxProps' multiple style='height:auto;min-height:60px;max-height:140px;'>";
    for (let i = 0; i < properties.length; i++)
      html += "<option value='" + escapeHtml(properties[i].name) + "'>" + escapeHtml(properties[i].name) + " <small>(" + escapeHtml(properties[i].type) + ")</small></option>";
    html += "</select>";
  } else {
    html += "<input class='form-control mt-1 mb-3' id='inputCreateIdxPropsText' placeholder='Property names, comma-separated (e.g. name, age)'>";
  }
  html += "</div>";

  // Vector property (LSM_VECTOR only — single property of type ARRAY_OF_FLOATS or ARRAY_OF_DOUBLES)
  html += "<div id='createIdxPropsVector' style='display:none;'>";
  html += "<label>Property <span style='color:#dc3545'>*</span></label>";
  html += "<small class='text-muted d-block mt-1'>LSM_VECTOR requires exactly one property of type ARRAY_OF_FLOATS or ARRAY_OF_DOUBLES.</small>";
  if (vectorProps.length > 0) {
    html += "<select class='form-select mt-1 mb-3' id='inputCreateIdxPropsVector'>";
    for (let i = 0; i < vectorProps.length; i++)
      html += "<option value='" + escapeHtml(vectorProps[i].name) + "'>" + escapeHtml(vectorProps[i].name) + " (" + escapeHtml(vectorProps[i].type) + ")</option>";
    html += "</select>";
  } else {
    html += "<input class='form-control mt-1 mb-3' id='inputCreateIdxPropsVectorText' placeholder='Property name (ARRAY_OF_FLOATS or ARRAY_OF_DOUBLES)'>";
  }
  html += "</div>";

  html += "<label for='inputCreateIdxAlgorithm'>Index Algorithm <span style='color:#dc3545'>*</span></label>";
  html += "<select class='form-select mt-1 mb-3' id='inputCreateIdxAlgorithm'>";
  html += "<option value='LSM_TREE' selected>LSM_TREE — default, supports range queries</option>";
  html += "<option value='HASH'>HASH — O(1) equality lookups, no range queries</option>";
  html += "<option value='FULL_TEXT'>FULL_TEXT — full-text search index</option>";
  html += "<option value='LSM_VECTOR'>LSM_VECTOR — vector/embedding similarity index</option>";
  html += "</select>";

  html += "<div id='createIdxLsmOptions'>";
  html += "<div class='row mb-3'>";
  html += "<div class='col-6'>";
  html += "<label for='inputCreateIdxNullStrategy'>Null Strategy <small class='text-muted'>(optional)</small></label>";
  html += "<select class='form-select mt-1' id='inputCreateIdxNullStrategy'>";
  html += "<option value='' selected>(default)</option>";
  html += "<option value='SKIP'>SKIP — skip null values</option>";
  html += "<option value='INDEX'>INDEX — index null values</option>";
  html += "</select>";
  html += "</div>";
  html += "<div class='col-6 d-flex align-items-end pb-1'>";
  html += "<div class='form-check'><input class='form-check-input' type='checkbox' id='inputCreateIdxUnique'><label class='form-check-label' for='inputCreateIdxUnique'>Unique</label></div>";
  html += "</div>";
  html += "</div>";
  html += "</div>";

  html += "<div class='form-check'>";
  html += "<input class='form-check-input' type='checkbox' id='inputCreateIdxIfNotExists'>";
  html += "<label class='form-check-label' for='inputCreateIdxIfNotExists'>If not exists</label>";
  html += "</div>";

  globalPrompt("Add Index to " + escapeHtml(typeName), html, "Create", function () {
    let algorithm = $("#inputCreateIdxAlgorithm").val();
    let selectedProps = [];
    if (algorithm == "LSM_VECTOR") {
      let vectorEl = document.getElementById("inputCreateIdxPropsVector");
      if (vectorEl) {
        let val = $("#inputCreateIdxPropsVector").val();
        if (val) selectedProps = [val];
      } else {
        let text = $("#inputCreateIdxPropsVectorText").val().trim();
        if (text != "") selectedProps = [text];
      }
    } else {
      let multiEl = document.getElementById("inputCreateIdxProps");
      if (multiEl) {
        let vals = $("#inputCreateIdxProps").val();
        if (vals && vals.length > 0) selectedProps = vals;
      } else {
        let text = $("#inputCreateIdxPropsText").val().trim();
        if (text != "")
          selectedProps = text.split(",").map(function (p) { return p.trim(); }).filter(function (p) { return p != ""; });
      }
    }

    if (selectedProps.length == 0) {
      globalNotify("Error", "At least one property is required", "danger");
      return;
    }
    let unique = $("#inputCreateIdxUnique").prop("checked");
    let nullStrategy = $("#inputCreateIdxNullStrategy").val();
    let ifNotExists = $("#inputCreateIdxIfNotExists").prop("checked");

    let indexTypeSql;
    if (algorithm == "LSM_TREE")
      indexTypeSql = unique ? "UNIQUE" : "NOTUNIQUE";
    else if (algorithm == "HASH")
      indexTypeSql = unique ? "UNIQUE_HASH" : "NOTUNIQUE_HASH";
    else
      indexTypeSql = algorithm;

    let command = "CREATE INDEX";
    if (ifNotExists) command += " IF NOT EXISTS";
    command += " ON `" + typeName + "` (";
    command += selectedProps.map(function (p) { return "`" + p + "`"; }).join(", ");
    command += ") " + indexTypeSql;
    if ((algorithm == "LSM_TREE" || algorithm == "HASH") && nullStrategy != "") command += " NULL_STRATEGY " + nullStrategy;

    jQuery.ajax({
      type: "POST",
      url: "api/v1/command/" + database,
      data: JSON.stringify({ language: "sql", command: command, serializer: "record" }),
      beforeSend: function (xhr) { xhr.setRequestHeader("Authorization", globalCredentials); }
    }).done(function (data) {
      globalNotify("Success", "Index created on '" + escapeHtml(typeName) + "'", "success");
      refreshSchemaAndShowType(typeName);
    }).fail(function (jqXHR) {
      globalNotifyError(jqXHR.responseText);
    });
  });

  setTimeout(function () {
    function updateIdxVisibility() {
      let alg = $("#inputCreateIdxAlgorithm").val();
      if (alg == "LSM_VECTOR") {
        $("#createIdxPropsNormal").hide();
        $("#createIdxPropsVector").show();
        $("#createIdxLsmOptions").hide();
      } else {
        $("#createIdxPropsNormal").show();
        $("#createIdxPropsVector").hide();
        $("#createIdxLsmOptions").toggle(alg == "LSM_TREE" || alg == "HASH");
      }
    }
    $("#inputCreateIdxAlgorithm").on("change", updateIdxVisibility);
  }, 100);
}

function validateTypeName(name) {
  if (name == null || name.trim() == "")
    return "Type name is required";
  if (name.indexOf(",") >= 0)
    return "Type name cannot contain commas";
  if (name.indexOf("`") >= 0)
    return "Type name cannot contain backticks";
  // Check uniqueness against existing types
  if (window._schemaTypes) {
    for (let i in window._schemaTypes)
      if (window._schemaTypes[i].name == name)
        return "A type named '" + name + "' already exists";
  }
  return null;
}

function createType(category) {
  let catLabel = category == "vertex" ? "Vertex" : (category == "edge" ? "Edge" : "Document");
  let sqlKeyword = category == "vertex" ? "VERTEX" : (category == "edge" ? "EDGE" : "DOCUMENT");

  // Collect existing types of the same category for the "Extends" multi-select
  let compatibleTypes = [];
  if (window._schemaTypes) {
    for (let i in window._schemaTypes) {
      let t = window._schemaTypes[i];
      if (t.type == category)
        compatibleTypes.push(t.name);
    }
    compatibleTypes.sort(function (a, b) { return a.localeCompare(b); });
  }

  let html = "<label for='inputCreateTypeName'>Type name <span style='color:#dc3545'>*</span></label>";
  html += "<input class='form-control mt-1' id='inputCreateTypeName' placeholder='e.g. My" + catLabel + "'>";
  html += "<div id='createTypeNameFeedback' style='font-size:0.78rem;min-height:20px;margin-bottom:8px;'></div>";

  // Extends multi-select
  html += "<label>Extends (optional, multiple selection)</label>";
  html += "<select class='form-select mt-1 mb-3' id='inputCreateTypeParents' multiple style='height:auto;min-height:38px;max-height:120px;'>";
  for (let i = 0; i < compatibleTypes.length; i++)
    html += "<option value='" + escapeHtml(compatibleTypes[i]) + "'>" + escapeHtml(compatibleTypes[i]) + "</option>";
  html += "</select>";
  if (compatibleTypes.length == 0)
    html += "<small class='text-muted' style='display:block;margin-top:-10px;margin-bottom:10px;'>No existing " + escapeHtml(catLabel.toLowerCase()) + " types to extend from.</small>";

  // Buckets + Page Size
  html += "<div class='row mb-3'>";
  html += "<div class='col-6'>";
  html += "<label for='inputCreateTypeBuckets'>Buckets (optional)</label>";
  html += "<input class='form-control mt-1' id='inputCreateTypeBuckets' type='number' min='1' max='32' placeholder='default'>";
  html += "</div>";
  html += "<div class='col-6'>";
  html += "<label for='inputCreateTypePageSize'>Page Size (optional)</label>";
  html += "<input class='form-control mt-1' id='inputCreateTypePageSize' type='number' min='1' placeholder='default'>";
  html += "</div>";
  html += "</div>";

  // Options row
  html += "<div class='d-flex flex-wrap gap-3'>";

  // Unidirectional (edge only)
  if (category == "edge") {
    html += "<div class='form-check'>";
    html += "<input class='form-check-input' type='checkbox' id='inputCreateTypeUnidirectional'>";
    html += "<label class='form-check-label' for='inputCreateTypeUnidirectional'>Unidirectional</label>";
    html += "</div>";
  }

  // If not exists
  html += "<div class='form-check'>";
  html += "<input class='form-check-input' type='checkbox' id='inputCreateTypeIfNotExists'>";
  html += "<label class='form-check-label' for='inputCreateTypeIfNotExists'>If not exists</label>";
  html += "</div>";

  html += "</div>";

  globalPrompt("Create " + catLabel + " Type", html, "Create", function () {
    let name = $("#inputCreateTypeName").val().trim();
    let error = validateTypeName(name);
    if (error && !$("#inputCreateTypeIfNotExists").prop("checked")) {
      globalNotify("Error", error, "danger");
      return;
    }

    let command = "CREATE " + sqlKeyword + " TYPE `" + name + "`";

    if ($("#inputCreateTypeIfNotExists").prop("checked"))
      command += " IF NOT EXISTS";

    // Multiple parents
    let parents = $("#inputCreateTypeParents").val();
    if (parents && parents.length > 0)
      command += " EXTENDS " + parents.map(function (p) { return "`" + p + "`"; }).join(", ");

    let buckets = $("#inputCreateTypeBuckets").val();
    if (buckets && parseInt(buckets) > 0)
      command += " BUCKETS " + parseInt(buckets);

    let pageSize = $("#inputCreateTypePageSize").val();
    if (pageSize && parseInt(pageSize) > 0)
      command += " PAGESIZE " + parseInt(pageSize);

    if (category == "edge" && $("#inputCreateTypeUnidirectional").prop("checked"))
      command += " UNIDIRECTIONAL";

    let database = getCurrentDatabase();
    if (database == "") {
      globalNotify("Error", "Database not selected", "danger");
      return;
    }

    jQuery
      .ajax({
        type: "POST",
        url: "api/v1/command/" + database,
        data: JSON.stringify({
          language: "sql",
          command: command,
          serializer: "record",
        }),
        beforeSend: function (xhr) {
          xhr.setRequestHeader("Authorization", globalCredentials);
        },
      })
      .done(function (data) {
        globalNotify("Success", catLabel + " type '" + escapeHtml(name) + "' created", "success");
        displaySchema();
      })
      .fail(function (jqXHR, textStatus, errorThrown) {
        globalNotifyError(jqXHR.responseText);
      });
  });

  // Wire up live validation on the name field
  setTimeout(function () {
    $("#inputCreateTypeName").on("input", function () {
      let name = $(this).val().trim();
      let feedback = $("#createTypeNameFeedback");
      if (name == "") {
        feedback.html("");
        $(this).removeClass("is-invalid is-valid");
        return;
      }
      let error = validateTypeName(name);
      if (error) {
        feedback.html("<span style='color:#dc3545;'><i class='fa fa-circle-exclamation'></i> " + escapeHtml(error) + "</span>");
        $(this).addClass("is-invalid").removeClass("is-valid");
      } else {
        feedback.html("<span style='color:#28a745;'><i class='fa fa-circle-check'></i> Name is available</span>");
        $(this).addClass("is-valid").removeClass("is-invalid");
      }
    });
  }, 200);
}

function createTimeSeriesType() {
  // Widen the modal for the TimeSeries form
  var dlg = document.getElementById("globalModalDialog");
  dlg.classList.add("modal-lg");
  var modalEl = document.getElementById("globalModal");
  modalEl.addEventListener("hidden.bs.modal", function restoreSize() {
    dlg.classList.remove("modal-lg");
    modalEl.removeEventListener("hidden.bs.modal", restoreSize);
  });

  let html = "<label for='tsCreateName'>Type Name <span style='color:#dc3545'>*</span></label>";
  html += "<input class='form-control mt-1' id='tsCreateName' placeholder='e.g. sensor_data'>";
  html += "<div id='tsCreateNameFeedback' style='font-size:0.78rem;min-height:20px;margin-bottom:8px;'></div>";

  html += "<label for='tsCreateTimestamp'>Timestamp Column Name <span style='color:#dc3545'>*</span></label>";
  html += "<input class='form-control mt-1 mb-3' id='tsCreateTimestamp' value='ts' placeholder='e.g. ts'>";

  // Tags section
  html += "<label>Tags <small class='text-muted'>(indexed metadata columns for filtering)</small></label>";
  html += "<div id='tsCreateTags' class='mb-2'></div>";
  html += "<button type='button' class='btn btn-sm btn-outline-secondary mb-3' onclick='tsAddColumnRow(\"tsCreateTags\", \"TAG\")'><i class='fa fa-plus'></i> Add Tag</button>";

  // Fields section
  html += "<label>Fields <small class='text-muted'>(measurement value columns)</small></label>";
  html += "<div id='tsCreateFields' class='mb-2'></div>";
  html += "<button type='button' class='btn btn-sm btn-outline-secondary mb-3' onclick='tsAddColumnRow(\"tsCreateFields\", \"FIELD\")'><i class='fa fa-plus'></i> Add Field</button>";

  // Advanced options
  html += "<div class='mt-2 mb-2'>";
  html += "<a href='#' onclick='$(\"#tsAdvancedOptions\").toggle(); return false;' style='font-size:0.85rem;'><i class='fa fa-cog'></i> Advanced Options</a>";
  html += "</div>";
  html += "<div id='tsAdvancedOptions' style='display:none;'>";

  html += "<div class='row mb-2'>";
  html += "<div class='col-4'>";
  html += "<label for='tsCreateShards'>Shards</label>";
  html += "<input class='form-control form-control-sm mt-1' id='tsCreateShards' type='number' min='1' max='64' placeholder='1'>";
  html += "</div>";
  html += "<div class='col-4'>";
  html += "<label for='tsCreateRetention'>Retention</label>";
  html += "<div class='d-flex gap-1 mt-1'>";
  html += "<input class='form-control form-control-sm' id='tsCreateRetention' type='number' min='0' placeholder='0' style='width:70px;'>";
  html += "<select class='form-select form-select-sm' id='tsCreateRetentionUnit' style='width:auto;'><option value='DAYS' selected>Days</option><option value='HOURS'>Hours</option><option value='MINUTES'>Minutes</option></select>";
  html += "</div>";
  html += "</div>";
  html += "<div class='col-4'>";
  html += "<label for='tsCreateCompaction'>Compaction Interval</label>";
  html += "<div class='d-flex gap-1 mt-1'>";
  html += "<input class='form-control form-control-sm' id='tsCreateCompaction' type='number' min='0' placeholder='0' style='width:70px;'>";
  html += "<select class='form-select form-select-sm' id='tsCreateCompactionUnit' style='width:auto;'><option value='HOURS' selected>Hours</option><option value='DAYS'>Days</option><option value='MINUTES'>Minutes</option></select>";
  html += "</div>";
  html += "</div>";
  html += "</div>";

  // If not exists
  html += "<div class='form-check mb-2'>";
  html += "<input class='form-check-input' type='checkbox' id='tsCreateIfNotExists'>";
  html += "<label class='form-check-label' for='tsCreateIfNotExists'>If not exists</label>";
  html += "</div>";
  html += "</div>";

  globalPrompt("Create TimeSeries Type", html, "Create", function () {
    let name = $("#tsCreateName").val().trim();
    let error = validateTypeName(name);
    if (error && !$("#tsCreateIfNotExists").prop("checked")) {
      globalNotify("Error", error, "danger");
      return;
    }

    let timestamp = $("#tsCreateTimestamp").val().trim();
    if (!timestamp) {
      globalNotify("Error", "Timestamp column name is required", "danger");
      return;
    }

    // Collect tags
    let tags = [];
    $("#tsCreateTags .ts-col-row").each(function () {
      let colName = $(this).find(".ts-col-name").val().trim();
      let colType = $(this).find(".ts-col-type").val();
      if (colName)
        tags.push(colName + " " + colType);
    });

    // Collect fields
    let fields = [];
    $("#tsCreateFields .ts-col-row").each(function () {
      let colName = $(this).find(".ts-col-name").val().trim();
      let colType = $(this).find(".ts-col-type").val();
      if (colName)
        fields.push(colName + " " + colType);
    });

    if (tags.length == 0 && fields.length == 0) {
      globalNotify("Error", "At least one tag or field is required", "danger");
      return;
    }

    let command = "CREATE TIMESERIES TYPE `" + name + "`";

    if ($("#tsCreateIfNotExists").prop("checked"))
      command += " IF NOT EXISTS";

    command += " TIMESTAMP " + timestamp;

    if (tags.length > 0)
      command += " TAGS (" + tags.join(", ") + ")";

    if (fields.length > 0)
      command += " FIELDS (" + fields.join(", ") + ")";

    let shards = $("#tsCreateShards").val();
    if (shards && parseInt(shards) > 0)
      command += " SHARDS " + parseInt(shards);

    let retention = $("#tsCreateRetention").val();
    if (retention && parseInt(retention) > 0)
      command += " RETENTION " + parseInt(retention) + " " + $("#tsCreateRetentionUnit").val();

    let compaction = $("#tsCreateCompaction").val();
    if (compaction && parseInt(compaction) > 0)
      command += " COMPACTION_INTERVAL " + parseInt(compaction) + " " + $("#tsCreateCompactionUnit").val();

    let database = getCurrentDatabase();
    if (!database) {
      globalNotify("Error", "Database not selected", "danger");
      return;
    }

    jQuery.ajax({
      type: "POST",
      url: "api/v1/command/" + database,
      data: JSON.stringify({ language: "sql", command: command, serializer: "record" }),
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      }
    })
    .done(function (data) {
      globalNotify("Success", "TimeSeries type '" + escapeHtml(name) + "' created", "success");
      displaySchema();
    })
    .fail(function (jqXHR) {
      globalNotifyError(jqXHR.responseText);
    });
  });

  // Wire up live validation
  setTimeout(function () {
    $("#tsCreateName").on("input", function () {
      let name = $(this).val().trim();
      let feedback = $("#tsCreateNameFeedback");
      if (name == "") {
        feedback.html("");
        $(this).removeClass("is-invalid is-valid");
        return;
      }
      let error = validateTypeName(name);
      if (error) {
        feedback.html("<span style='color:#dc3545;'><i class='fa fa-circle-exclamation'></i> " + escapeHtml(error) + "</span>");
        $(this).addClass("is-invalid").removeClass("is-valid");
      } else {
        feedback.html("<span style='color:#28a745;'><i class='fa fa-circle-check'></i> Name is available</span>");
        $(this).addClass("is-valid").removeClass("is-invalid");
      }
    });

    // Add one default tag and one default field row
    tsAddColumnRow("tsCreateTags", "TAG");
    tsAddColumnRow("tsCreateFields", "FIELD");
  }, 200);
}

function tsAddColumnRow(containerId, role) {
  let typeOptions = role == "TAG"
    ? "<option value='STRING' selected>STRING</option><option value='INTEGER'>INTEGER</option><option value='LONG'>LONG</option>"
    : "<option value='DOUBLE' selected>DOUBLE</option><option value='LONG'>LONG</option><option value='FLOAT'>FLOAT</option><option value='INTEGER'>INTEGER</option><option value='STRING'>STRING</option>";

  let html = "<div class='d-flex gap-2 mb-1 ts-col-row'>";
  html += "<input class='form-control form-control-sm ts-col-name' placeholder='Column name' style='flex:2;'>";
  html += "<select class='form-select form-select-sm ts-col-type' style='flex:1;'>" + typeOptions + "</select>";
  html += "<button type='button' class='btn btn-sm btn-outline-danger' onclick='$(this).closest(\".ts-col-row\").remove()'><i class='fa fa-times'></i></button>";
  html += "</div>";
  $("#" + containerId).append(html);
}

function formatTsDuration(ms) {
  if (ms <= 0) return "none";
  let days = ms / (24 * 60 * 60 * 1000);
  if (days >= 1 && days == Math.floor(days))
    return days + " day" + (days > 1 ? "s" : "");
  let hours = ms / (60 * 60 * 1000);
  if (hours >= 1 && hours == Math.floor(hours))
    return hours + " hour" + (hours > 1 ? "s" : "");
  let minutes = ms / (60 * 1000);
  if (minutes >= 1)
    return Math.round(minutes) + " min";
  let seconds = ms / 1000;
  return Math.round(seconds) + " sec";
}

function formatTsTimestamp(ms) {
  if (ms == null) return "—";
  try {
    return new Date(ms).toISOString().replace("T", " ").replace("Z", "");
  } catch (e) {
    return ms.toLocaleString();
  }
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

// --- Function Reference ---

function loadFunctionReference() {
  $.ajax({
    url: "js/function-reference.json",
    type: "GET",
    dataType: "json",
    success: function(data) {
      globalFunctionReference = data;
      initArcadeDBModes();
    },
    error: function() {
      globalFunctionReference = null;
    }
  });
}

function initArcadeDBModes() {
  var sqlKw = (globalFunctionReference && globalFunctionReference.sqlKeywords) || [];
  var sqlKwObj = {};
  for (var i = 0; i < sqlKw.length; i++)
    sqlKwObj[sqlKw[i]] = true;

  CodeMirror.defineMIME("arcadedb-sql", {
    name: "sql",
    keywords: sqlKwObj,
    builtin: {},
    atoms: { "false": true, "true": true, "null": true },
    operatorChars: /^[*+\-%<>!=&|~^\/\?]/
  });

  // Refresh the editor mode so it picks up the new keywords
  if (typeof editor !== "undefined" && editor) {
    var language = $("#inputLanguage").val();
    if (language === "sql" || language === "sqlScript")
      editor.setOption("mode", "arcadedb-sql");
  }
}

function buildSchemaTablesForHint() {
  var tables = {};
  if (!globalSchemaTypes)
    return tables;
  for (var i = 0; i < globalSchemaTypes.length; i++) {
    var row = globalSchemaTypes[i];
    var typeName = row.name;
    if (!typeName)
      continue;
    var props = [];
    if (row.properties) {
      for (var k in row.properties) {
        var property = row.properties[k];
        if (property && property.name)
          props.push(property.name);
      }
    }
    tables[typeName] = props;
  }
  return tables;
}

function resolveGrammarContext(cm, grammarTree) {
  if (!grammarTree)
    return null;

  var cur = cm.getCursor();
  var fullText = cm.getRange({ line: 0, ch: 0 }, cur);
  var lastSemicolon = fullText.lastIndexOf(";");
  var stmtText = (lastSemicolon >= 0) ? fullText.substring(lastSemicolon + 1) : fullText;

  var tokens = stmtText.trim().toUpperCase().match(/[A-Z_]+/g) || [];
  if (tokens.length === 0)
    return grammarTree["_start"];

  var state = "_start";
  var node = grammarTree[state];
  for (var i = 0; i < tokens.length; i++) {
    var token = tokens[i];
    // Try compound keywords: check if this + next form a compound like "GROUP BY", "ORDER BY", etc.
    if (i + 1 < tokens.length) {
      var compound = token + " " + tokens[i + 1];
      var compoundKey = token + "_" + tokens[i + 1];
      // Check if compound matches a keyword in current node
      if (node && node.keywords) {
        var isCompound = false;
        for (var k = 0; k < node.keywords.length; k++) {
          if (node.keywords[k] === compound) {
            isCompound = true;
            break;
          }
        }
        if (isCompound) {
          // Try state transition with compound key
          var compoundCandidates = [
            state + "." + compoundKey + "._target",
            state + "." + compoundKey + "._name",
            state + "." + compoundKey,
            compoundKey
          ];
          for (var c = 0; c < compoundCandidates.length; c++) {
            if (grammarTree[compoundCandidates[c]]) {
              state = compoundCandidates[c];
              node = grammarTree[state];
              i++; // skip next token
              break;
            }
          }
          continue;
        }
      }
    }
    // Try progressively specific paths
    var candidates = [
      state + "." + token + "._target",
      state + "." + token + "._name",
      state + "." + token + "._after",
      state + "." + token + "._projection",
      state + "." + token + "._pattern",
      state + "." + token,
      token
    ];
    for (var c = 0; c < candidates.length; c++) {
      if (grammarTree[candidates[c]]) {
        state = candidates[c];
        node = grammarTree[state];
        break;
      }
    }
  }
  return node;
}

function arcadedbHint(cm) {
  var language = $("#inputLanguage").val();
  var isSql = (language === "sql" || language === "sqlScript");
  var isCypher = (language === "cypher" || language === "opencypher");

  if (isSql)
    return arcadedbSqlHint(cm);
  else if (isCypher)
    return arcadedbCypherHint(cm);
  else
    return arcadedbFunctionHint(cm);
}

function arcadedbSqlHint(cm) {
  var ref = globalFunctionReference;
  var grammarTree = ref && ref.sqlGrammar;
  var ctx = resolveGrammarContext(cm, grammarTree);

  var cur = cm.getCursor();
  var line = cm.getLine(cur.line);
  var end = cur.ch;
  var start = end;
  while (start > 0 && /[\w._]/.test(line.charAt(start - 1)))
    start--;
  var word = line.substring(start, end);
  var upperWord = word.toUpperCase();

  // Check if we're in a TypeName.property context (dot completion)
  var tables = buildSchemaTablesForHint();
  var sqlResult = null;
  if (CodeMirror.hint && CodeMirror.hint.sql && Object.keys(tables).length > 0) {
    try {
      sqlResult = CodeMirror.hint.sql(cm, { tables: tables, completeSingle: false, disableKeywords: true });
    } catch (e) {
      // sql-hint may fail if mode config is not available
    }
  }

  var completions = [];
  var seen = {};

  // Add grammar-contextual keywords
  if (ctx && ctx.keywords) {
    for (var i = 0; i < ctx.keywords.length; i++) {
      var kw = ctx.keywords[i];
      if (upperWord.length === 0 || kw.toUpperCase().indexOf(upperWord) === 0) {
        if (!seen[kw.toUpperCase()]) {
          seen[kw.toUpperCase()] = true;
          completions.push({ text: kw, displayText: kw, className: "arcadedb-hint-keyword" });
        }
      }
    }
  }

  // Add dynamic completions based on context expectations
  if (ctx && ctx.expect) {
    var expectTypes = false, expectFunctions = false, expectProperties = false;
    for (var i = 0; i < ctx.expect.length; i++) {
      if (ctx.expect[i] === "type" || ctx.expect[i] === "expression")
        expectTypes = true;
      if (ctx.expect[i] === "function" || ctx.expect[i] === "expression")
        expectFunctions = true;
      if (ctx.expect[i] === "property" || ctx.expect[i] === "expression")
        expectProperties = true;
    }

    if (expectTypes && globalSchemaTypes) {
      for (var i = 0; i < globalSchemaTypes.length; i++) {
        var typeName = globalSchemaTypes[i].name;
        if (typeName && (upperWord.length === 0 || typeName.toUpperCase().indexOf(upperWord) === 0)) {
          if (!seen[typeName.toUpperCase()]) {
            seen[typeName.toUpperCase()] = true;
            completions.push({ text: typeName, displayText: typeName, className: "arcadedb-hint-type" });
          }
        }
      }
    }

    if (expectFunctions && ref && ref.autocomplete) {
      var funcs = ref.autocomplete;
      for (var i = 0; i < funcs.length; i++) {
        if (upperWord.length === 0 || funcs[i].toUpperCase().indexOf(upperWord) === 0) {
          if (!seen[funcs[i].toUpperCase()]) {
            seen[funcs[i].toUpperCase()] = true;
            var info = findFunctionInfo(funcs[i]);
            completions.push({
              text: funcs[i],
              displayText: info ? funcs[i] + " - " + info.syntax : funcs[i],
              className: "arcadedb-hint-function"
            });
          }
        }
      }
    }

    if (expectProperties) {
      for (var tName in tables) {
        var props = tables[tName];
        for (var j = 0; j < props.length; j++) {
          if (upperWord.length === 0 || props[j].toUpperCase().indexOf(upperWord) === 0) {
            if (!seen[props[j].toUpperCase()]) {
              seen[props[j].toUpperCase()] = true;
              completions.push({ text: props[j], displayText: props[j], className: "arcadedb-hint-property" });
            }
          }
        }
      }
    }
  }

  // Add sql-hint results (handles TypeName.property via sql-hint.js)
  if (sqlResult && sqlResult.list) {
    for (var j = 0; j < sqlResult.list.length; j++) {
      var item = sqlResult.list[j];
      var text = (typeof item === "string") ? item : item.text;
      if (!seen[text.toUpperCase()]) {
        seen[text.toUpperCase()] = true;
        completions.push(typeof item === "string" ? { text: item, displayText: item } : item);
      }
    }
  }

  if (completions.length === 0)
    return;

  return { list: completions, from: CodeMirror.Pos(cur.line, start), to: CodeMirror.Pos(cur.line, end) };
}

function arcadedbCypherHint(cm) {
  var ref = globalFunctionReference;
  var grammarTree = ref && ref.cypherGrammar;
  var ctx = resolveGrammarContext(cm, grammarTree);

  var cur = cm.getCursor();
  var line = cm.getLine(cur.line);
  var end = cur.ch;
  var start = end;
  while (start > 0 && /[\w._]/.test(line.charAt(start - 1)))
    start--;
  var word = line.substring(start, end);
  var upperWord = word.toUpperCase();

  var completions = [];
  var seen = {};

  // Add grammar-contextual keywords
  if (ctx && ctx.keywords) {
    for (var i = 0; i < ctx.keywords.length; i++) {
      var kw = ctx.keywords[i];
      if (upperWord.length === 0 || kw.toUpperCase().indexOf(upperWord) === 0) {
        if (!seen[kw.toUpperCase()]) {
          seen[kw.toUpperCase()] = true;
          completions.push({ text: kw, displayText: kw, className: "arcadedb-hint-keyword" });
        }
      }
    }
  }

  // Add dynamic completions based on context expectations
  if (ctx && ctx.expect) {
    for (var i = 0; i < ctx.expect.length; i++) {
      if ((ctx.expect[i] === "expression" || ctx.expect[i] === "property") && globalSchemaTypes) {
        for (var j = 0; j < globalSchemaTypes.length; j++) {
          var typeName = globalSchemaTypes[j].name;
          if (typeName && (upperWord.length === 0 || typeName.toUpperCase().indexOf(upperWord) === 0)) {
            if (!seen[typeName.toUpperCase()]) {
              seen[typeName.toUpperCase()] = true;
              completions.push({ text: typeName, displayText: typeName, className: "arcadedb-hint-type" });
            }
          }
        }
      }
    }
  }

  // Add function names
  if (ref && ref.autocomplete) {
    var funcs = ref.autocomplete;
    for (var i = 0; i < funcs.length; i++) {
      if (word.length >= 2 && funcs[i].toUpperCase().indexOf(upperWord) === 0) {
        if (!seen[funcs[i].toUpperCase()]) {
          seen[funcs[i].toUpperCase()] = true;
          var info = findFunctionInfo(funcs[i]);
          completions.push({
            text: funcs[i],
            displayText: info ? funcs[i] + " - " + info.syntax : funcs[i],
            className: "arcadedb-hint-function"
          });
        }
      }
    }
  }

  if (completions.length === 0)
    return;

  return { list: completions, from: CodeMirror.Pos(cur.line, start), to: CodeMirror.Pos(cur.line, end) };
}

function arcadedbFunctionHint(cm) {
  if (!globalFunctionReference || !globalFunctionReference.autocomplete)
    return;

  var cur = cm.getCursor();
  var line = cm.getLine(cur.line);
  var end = cur.ch;
  var start = end;
  while (start > 0 && /[\w._]/.test(line.charAt(start - 1)))
    start--;
  var word = line.substring(start, end);
  if (word.length < 2)
    return;

  var lowerWord = word.toLowerCase();
  var matches = [];
  var list = globalFunctionReference.autocomplete;

  for (var i = 0; i < list.length; i++) {
    if (list[i].toLowerCase().indexOf(lowerWord) === 0)
      matches.push(list[i]);
  }

  if (matches.length === 0)
    return;

  var completions = [];
  for (var i = 0; i < matches.length; i++) {
    var info = findFunctionInfo(matches[i]);
    completions.push({
      text: matches[i],
      displayText: info ? matches[i] + " - " + info.syntax : matches[i],
      className: "arcadedb-hint-item"
    });
  }

  return {
    list: completions,
    from: CodeMirror.Pos(cur.line, start),
    to: CodeMirror.Pos(cur.line, end)
  };
}

function findFunctionInfo(name) {
  if (!globalFunctionReference || !globalFunctionReference.categories)
    return null;

  var cats = globalFunctionReference.categories;
  for (var section in cats) {
    var subcats = cats[section];
    for (var cat in subcats) {
      var funcs = subcats[cat];
      for (var i = 0; i < funcs.length; i++) {
        if (funcs[i].name === name)
          return funcs[i];
      }
    }
  }
  return null;
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
        { label: "MATCH", code: "MATCH {type: Person, as: p} -FriendOf-> {as: f} RETURN p, f" },
        { label: "CREATE MATERIALIZED VIEW", code: "CREATE MATERIALIZED VIEW MyView REFRESH MANUAL AS SELECT name, count(*) AS cnt FROM MyType GROUP BY name" },
        { label: "REFRESH MATERIALIZED VIEW", code: "REFRESH MATERIALIZED VIEW MyView" },
        { label: "ALTER MATERIALIZED VIEW", code: "ALTER MATERIALIZED VIEW MyView REFRESH PERIODIC EVERY 5 MINUTE" },
        { label: "DROP MATERIALIZED VIEW", code: "DROP MATERIALIZED VIEW MyView" }
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

  // --- Function Reference ---
  if (globalFunctionReference && globalFunctionReference.categories) {
    html += "<div style='margin-top:12px;border-top:1px solid var(--border-main);padding-top:8px;'>";
    html += "<div style='font-weight:600;font-size:0.85rem;margin-bottom:6px;'>Function Reference</div>";
    html += "<input type='text' id='functionRefFilter' class='form-control form-control-sm' placeholder='Filter functions...' oninput='filterFunctionReference(this.value)' style='margin-bottom:8px;' />";

    var cats = globalFunctionReference.categories;
    for (var section in cats) {
      html += "<div class='reference-section fn-ref-section'>";
      html += "<div class='reference-section-header' onclick='toggleReferenceSection(this)'><span>" + escapeHtml(section) + "</span><i class='fa fa-chevron-right'></i></div>";
      html += "<div class='reference-section-body'>";
      var subcats = cats[section];
      for (var cat in subcats) {
        html += "<div class='fn-ref-category' data-category='" + escapeHtml(cat) + "'>";
        html += "<div style='font-size:0.75rem;font-weight:600;color:var(--color-brand);margin:4px 0 2px;'>" + escapeHtml(cat) + "</div>";
        var funcs = subcats[cat];
        for (var i = 0; i < funcs.length; i++) {
          var fn = funcs[i];
          var syntax = escapeHtml(fn.syntax);
          var desc = fn.description ? escapeHtml(fn.description) : "";
          var title = desc ? desc : syntax;
          html += "<div class='reference-example fn-ref-item' data-name='" + escapeHtml(fn.name) + "' title='" + title + "' onclick='insertFunctionSyntax(\"" + syntax.replace(/"/g, "&quot;") + "\")'>";
          html += "<code style='font-size:0.72rem;'>" + syntax + "</code>";
          if (desc)
            html += "<br><small style='color:#888;font-size:0.65rem;'>" + desc + "</small>";
          html += "</div>";
        }
        html += "</div>";
      }
      html += "</div></div>";
    }
    html += "</div>";
  }

  container.html(html);
}

function filterFunctionReference(query) {
  var lowerQuery = query.toLowerCase();
  $(".fn-ref-item").each(function() {
    var name = $(this).attr("data-name") || "";
    var text = $(this).text().toLowerCase();
    if (lowerQuery === "" || name.indexOf(lowerQuery) >= 0 || text.indexOf(lowerQuery) >= 0)
      $(this).show();
    else
      $(this).hide();
  });
  $(".fn-ref-category").each(function() {
    var visibleItems = $(this).find(".fn-ref-item:visible").length;
    if (visibleItems > 0)
      $(this).show();
    else
      $(this).hide();
  });
  // Auto-expand sections with matching results when filtering
  if (lowerQuery.length > 0) {
    $(".fn-ref-section .reference-section-body").addClass("open");
    $(".fn-ref-section .reference-section-header i").addClass("open");
  }
}

function insertFunctionSyntax(syntax) {
  var tmp = document.createElement("textarea");
  tmp.innerHTML = syntax;
  var decoded = tmp.value;
  var cur = editor.getCursor();
  editor.replaceRange(decoded, cur);
  editor.focus();
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

  let spacingVal = globalGraphSettings ? (globalGraphSettings.graphSpacing || 20) : 20;
  let cumulativeChecked = (globalGraphSettings && globalGraphSettings.cumulativeSelection) ? "checked" : "";

  html += "<div class='settings-section'>";
  html += "<div class='settings-section-header'>Graph Settings</div>";
  html += "<div class='settings-row'><label>Graph Spacing: <span id='settingGraphSpacingVal'>" + spacingVal + "</span></label>";
  html += "<input type='range' class='form-range' id='settingGraphSpacing' min='10' max='150' step='10' value='" + spacingVal + "' onchange='applyGraphSpacing(this.value)'></div>";
  html += "<div class='settings-row'><label>Cumulative Selection</label>";
  html += "<input type='checkbox' class='form-check-input' id='settingCumulativeSelection' " + cumulativeChecked + " onchange='applyCumulativeSelection(this.checked)'></div>";
  html += "</div>";

  html += "<div class='settings-section'>";
  html += "<div class='settings-section-header'>Keyboard Shortcuts</div>";
  html += "<div style='font-size:0.75rem;color:#666;padding:0 2px;'>";
  html += "<div style='margin-bottom:4px;'><kbd>Ctrl+Enter</kbd> Execute query</div>";
  html += "<div style='margin-bottom:4px;'><kbd>Ctrl+Up</kbd> Previous history</div>";
  html += "<div style='margin-bottom:4px;'><kbd>Ctrl+Down</kbd> Next history</div>";
  html += "<div><kbd>Alt+/</kbd> Autocomplete (keywords, types, functions)</div>";
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

function applyGraphSpacing(value) {
  globalGraphSettings.graphSpacing = parseInt(value);
  $("#settingGraphSpacingVal").text(value);
  if (globalCy != null) {
    globalGraphSettings._spacingChanged = true;
    renderGraph();
  }
}

function applyCumulativeSelection(checked) {
  globalGraphSettings.cumulativeSelection = checked;
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

      globalExplainPlan = data.explainPlan || null;
      renderFlameGraph(globalExplainPlan);

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
      $("#resultExplain").val(data.explain != null ? data.explain : "No profiler data found");

      globalExplainPlan = data.explainPlan || null;
      renderFlameGraph(globalExplainPlan);

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
    let groups = { vertex: [], edge: [], document: [], timeseries: [] };
    for (let i in types) {
      let row = types[i];
      let cat = row.type == "vertex" ? "vertex" : (row.type == "edge" ? "edge" : (row.type == "t" ? "timeseries" : "document"));
      groups[cat].push(row);
    }

    // Render badge sidebar
    let sections = [
      { key: "vertex",     label: "Vertices",    icon: "fa-circle-nodes" },
      { key: "edge",       label: "Edges",        icon: "fa-right-left" },
      { key: "document",   label: "Documents",    icon: "fa-file-lines" },
      { key: "timeseries", label: "TimeSeries",   icon: "fa-chart-line" }
    ];

    let html = "";
    let palette = sidebarBadgeColors;

    for (let s = 0; s < sections.length; s++) {
      let sec = sections[s];
      let items = groups[sec.key];

      let total = 0;
      for (let j = 0; j < items.length; j++) total += (items[j].records || 0);

      html += "<div class='sidebar-section'>";
      html += "<div class='sidebar-section-header'><i class='fa " + sec.icon + "'></i> " + sec.label + " <span class='sidebar-count'>(" + total.toLocaleString() + ")</span>";
      if (sec.key == "timeseries")
        html += "<span class='sidebar-section-header-actions'><button onclick='createTimeSeriesType(); return false;' title='Create timeseries type'><i class='fa fa-plus'></i></button></span>";
      else
        html += "<span class='sidebar-section-header-actions'><button onclick='createType(\"" + sec.key + "\"); return false;' title='Create " + sec.key + " type'><i class='fa fa-plus'></i></button></span>";
      html += "</div>";
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

    // Fetch materialized views and append to sidebar
    fetchMaterializedViews(function (views) {
      html += renderMaterializedViewsSidebarSection(views || [], false);
      $("#dbTypeBadges").html(html);
    });

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

  let catLabel = row.type == "vertex" ? "Vertex" : (row.type == "edge" ? "Edge" : (row.type == "t" ? "TimeSeries" : "Document"));
  let catColor = row.type == "vertex" ? "#3b82f6" : (row.type == "edge" ? "#f97316" : (row.type == "t" ? "#ec4899" : "#22c55e"));

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

  // TimeSeries columns section
  if (row.type == "t" && row.tsColumns && row.tsColumns.length > 0) {
    html += "<div class='db-detail-section'>";
    html += "<h6><i class='fa fa-chart-line'></i> TimeSeries Columns</h6>";
    html += "<div class='table-responsive'>";
    html += "<table class='table table-sm db-detail-table'>";
    html += "<thead><tr><th>Name</th><th>Data Type</th><th>Role</th></tr></thead><tbody>";
    for (let c = 0; c < row.tsColumns.length; c++) {
      let col = row.tsColumns[c];
      let roleBadge = col.role == "TIMESTAMP" ? "<span class='badge bg-primary'>TIMESTAMP</span>" :
                      (col.role == "TAG" ? "<span class='badge bg-warning text-dark'>TAG</span>" :
                       "<span class='badge bg-success'>FIELD</span>");
      html += "<tr><td>" + escapeHtml(col.name) + "</td><td>" + escapeHtml(col.dataType || "") + "</td><td>" + roleBadge + "</td></tr>";
    }
    html += "</tbody></table></div>";

    // TimeSeries configuration
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

  // Properties section (skip for timeseries — already shown in TimeSeries Columns)
  if (row.type != "t") {
    html += "<div class='db-detail-section'>";
    html += "<div class='d-flex align-items-center justify-content-between mb-2'>";
    html += "<h6 class='mb-0'><i class='fa fa-list'></i> Properties</h6>";
    html += "<button class='btn btn-sm btn-outline-primary' onclick='createProperty(\"" + row.name.replace(/"/g, "&quot;") + "\"); return false;'><i class='fa fa-plus'></i> Add Property</button>";
    html += "</div>";
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
  }

  // Indexes section
  if (row.type != "t") {
    html += "<div class='db-detail-section'>";
    html += "<div class='d-flex align-items-center justify-content-between mb-2'>";
    html += "<h6 class='mb-0'><i class='fa fa-bolt'></i> Indexes</h6>";
    html += "<button class='btn btn-sm btn-outline-primary' onclick='createIndex(\"" + row.name.replace(/"/g, "&quot;") + "\"); return false;'><i class='fa fa-plus'></i> Add Index</button>";
    html += "</div>";
    if (row.indexes && row.indexes.length > 0) {
      html += "<div class='table-responsive'>";
      html += "<table class='table table-sm db-detail-table'>";
      html += "<thead><tr><th>Name</th><th>Defined In</th><th>Properties</th><th>Type</th><th>Unique</th><th>Automatic</th><th>Actions</th></tr></thead>";
      html += "<tbody>" + renderIndexes(row, types) + "</tbody></table></div>";
    } else {
      html += "<p class='text-muted' style='font-size:0.85rem;'>No indexes defined.</p>";
    }
    html += "</div>";
  } else if (row.indexes && row.indexes.length > 0) {
    html += "<div class='db-detail-section'>";
    html += "<h6><i class='fa fa-bolt'></i> Indexes</h6>";
    html += "<div class='table-responsive'>";
    html += "<table class='table table-sm db-detail-table'>";
    html += "<thead><tr><th>Name</th><th>Defined In</th><th>Properties</th><th>Type</th><th>Unique</th><th>Automatic</th><th>Actions</th></tr></thead>";
    html += "<tbody>" + renderIndexes(row, types) + "</tbody></table></div>";
    html += "</div>";
  }

  // TimeSeries diagnostics section
  if (row.type == "t") {
    html += "<div class='db-detail-section'>";
    html += "<h6><i class='fa fa-stethoscope'></i> TimeSeries Diagnostics</h6>";

    // Global stats
    html += "<div class='row mb-3'>";
    html += "<div class='col-md-3'><div class='card'><div class='card-body py-2 px-3'><small class='text-muted'>Total Samples</small><div style='font-size:1.2rem;font-weight:600;'>" + (row.records || 0).toLocaleString() + "</div></div></div></div>";
    html += "<div class='col-md-3'><div class='card'><div class='card-body py-2 px-3'><small class='text-muted'>Shards</small><div style='font-size:1.2rem;font-weight:600;'>" + (row.shardCount || 1) + "</div></div></div></div>";
    html += "<div class='col-md-3'><div class='card'><div class='card-body py-2 px-3'><small class='text-muted'>Time Range (min)</small><div style='font-size:1.0rem;font-weight:600;'>" + (row.globalMinTimestamp != null ? formatTsTimestamp(row.globalMinTimestamp) : "—") + "</div></div></div></div>";
    html += "<div class='col-md-3'><div class='card'><div class='card-body py-2 px-3'><small class='text-muted'>Time Range (max)</small><div style='font-size:1.0rem;font-weight:600;'>" + (row.globalMaxTimestamp != null ? formatTsTimestamp(row.globalMaxTimestamp) : "—") + "</div></div></div></div>";
    html += "</div>";

    // Configuration
    html += "<h6 style='font-size:0.82rem;margin-top:12px;'>Configuration</h6>";
    html += "<div class='table-responsive'>";
    html += "<table class='table table-sm db-detail-table'>";
    html += "<tbody>";
    html += "<tr><td style='width:200px;'>Timestamp Column</td><td><code>" + escapeHtml(row.timestampColumn || "ts") + "</code></td></tr>";
    html += "<tr><td>Retention</td><td>" + (row.retentionMs > 0 ? formatTsDuration(row.retentionMs) : "<span class='text-muted'>unlimited</span>") + "</td></tr>";
    html += "<tr><td>Compaction Interval</td><td>" + (row.compactionBucketIntervalMs > 0 ? formatTsDuration(row.compactionBucketIntervalMs) : "<span class='text-muted'>none</span>") + "</td></tr>";
    html += "</tbody></table></div>";

    // Downsampling tiers
    if (row.downsamplingTiers && row.downsamplingTiers.length > 0) {
      html += "<h6 style='font-size:0.82rem;margin-top:12px;'>Downsampling Tiers</h6>";
      html += "<div class='table-responsive'>";
      html += "<table class='table table-sm db-detail-table'>";
      html += "<thead><tr><th>After</th><th>Granularity</th></tr></thead><tbody>";
      for (let d = 0; d < row.downsamplingTiers.length; d++) {
        let tier = row.downsamplingTiers[d];
        html += "<tr><td>" + formatTsDuration(tier.afterMs) + "</td><td>" + formatTsDuration(tier.granularityMs) + "</td></tr>";
      }
      html += "</tbody></table></div>";
    }

    // Per-shard stats
    if (row.shards && row.shards.length > 0) {
      html += "<h6 style='font-size:0.82rem;margin-top:12px;'>Shard Details</h6>";
      html += "<div class='table-responsive'>";
      html += "<table class='table table-sm db-detail-table'>";
      html += "<thead><tr><th>Shard</th><th>Sealed Blocks</th><th>Sealed Samples</th><th>Mutable Samples</th><th>Total Samples</th><th>Min Timestamp</th><th>Max Timestamp</th></tr></thead><tbody>";
      for (let s = 0; s < row.shards.length; s++) {
        let sh = row.shards[s];
        if (sh.error) {
          html += "<tr><td>" + sh.shard + "</td><td colspan='6' class='text-danger'>" + escapeHtml(sh.error) + "</td></tr>";
        } else {
          html += "<tr>";
          html += "<td>" + sh.shard + "</td>";
          html += "<td>" + (sh.sealedBlocks || 0).toLocaleString() + "</td>";
          html += "<td>" + (sh.sealedSamples || 0).toLocaleString() + "</td>";
          html += "<td>" + (sh.mutableSamples || 0).toLocaleString() + "</td>";
          html += "<td><strong>" + (sh.totalSamples || 0).toLocaleString() + "</strong></td>";
          html += "<td>" + (sh.minTimestamp != null ? formatTsTimestamp(sh.minTimestamp) : (sh.mutableMinTimestamp != null ? formatTsTimestamp(sh.mutableMinTimestamp) : "—")) + "</td>";
          html += "<td>" + (sh.maxTimestamp != null ? formatTsTimestamp(sh.maxTimestamp) : (sh.mutableMaxTimestamp != null ? formatTsTimestamp(sh.mutableMaxTimestamp) : "—")) + "</td>";
          html += "</tr>";
        }
      }
      html += "</tbody></table></div>";
    }

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
  document: ["#22c55e", "#10b981", "#14b8a6", "#059669", "#16a34a", "#0d9488", "#15803d", "#047857"],
  materializedView: ["#a855f7", "#9333ea", "#7c3aed", "#8b5cf6", "#a78bfa", "#7c3aed", "#6d28d9", "#c084fc"],
  timeseries: ["#ec4899", "#db2777", "#be185d", "#f472b6", "#e879a8", "#d946ef", "#c026d3", "#a21caf"]
};

function populateQuerySidebar() {
  let container = $("#sidebarPanelOverview");
  if (container.length == 0) return;

  if (globalSchemaTypes == null || globalSchemaTypes.length == 0) {
    container.html("<div class='sidebar-empty'>No types defined.<br><small>Create vertex or document types to see them here.</small></div>");
    return;
  }

  let groups = { vertex: [], edge: [], document: [], timeseries: [] };
  let totals = { vertex: 0, edge: 0, document: 0, timeseries: 0 };

  for (let i in globalSchemaTypes) {
    let row = globalSchemaTypes[i];
    let cat = row.type == "vertex" ? "vertex" : (row.type == "edge" ? "edge" : (row.type == "t" ? "timeseries" : "document"));
    groups[cat].push(row);
    totals[cat] += (row.records || 0);
  }

  let html = "";
  let sections = [
    { key: "vertex",     label: "Vertices",    icon: "fa-circle-nodes" },
    { key: "edge",       label: "Edges",        icon: "fa-right-left" },
    { key: "document",   label: "Documents",    icon: "fa-file-lines" },
    { key: "timeseries", label: "TimeSeries",   icon: "fa-chart-line" }
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

  // Fetch materialized views and append section, then render
  fetchMaterializedViews(function(views) {
    let finalHtml = html;
    if (views && views.length > 0)
      finalHtml += renderMaterializedViewsSidebarSection(views, true);
    container.html(finalHtml);
  });
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

// Register tab change handler for database sub-tabs
$(document).ready(function () {
  $('a[data-toggle="tab"]').on("shown.bs.tab", function (e) {
    var activeTab = this.id;
    if (activeTab == "tab-db-backup-sel") {
      if (!dbBackupsLoaded)
        loadDatabaseBackups();
    } else if (activeTab == "tab-db-metrics-sel") {
      loadDatabaseMetrics();
    } else if (activeTab == "tab-db-buckets-sel") {
      loadStorageBuckets();
    } else if (activeTab == "tab-db-indexes-sel") {
      loadStorageIndexes();
    } else if (activeTab == "tab-db-dictionary-sel") {
      loadStorageDictionary();
    }
  });

  $("#dbMetricsAutoRefresh").on("change", function () {
    if (this.checked)
      startDbMetricsAutoRefresh();
    else
      stopDbMetricsAutoRefresh();
  });
});

// ===== Materialized Views =====

var _mvAutoRefreshInterval = null;

function fetchMaterializedViews(callback) {
  let database = getCurrentDatabase();
  if (database == null || database == "") {
    if (callback) callback([]);
    return;
  }

  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/query/" + database,
      data: JSON.stringify({
        language: "sql",
        command: "select from schema:materializedViews",
      }),
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      window._schemaMaterializedViews = data.result || [];
      if (callback) callback(data.result || []);
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      window._schemaMaterializedViews = [];
      if (callback) callback([]);
    });
}

function renderMaterializedViewsSidebarSection(views, isQuerySidebar) {
  let html = "";
  let palette = sidebarBadgeColors.materializedView;

  html += "<div class='sidebar-section'>";
  html += "<div class='sidebar-section-header'><i class='fa fa-layer-group'></i> Materialized Views <span class='sidebar-count'>(" + views.length + ")</span>";
  if (!isQuerySidebar)
    html += "<span class='sidebar-section-header-actions'><button onclick='createMaterializedView(); return false;' title='Create materialized view'><i class='fa fa-plus'></i></button></span>";
  html += "</div>";
  html += "<div class='sidebar-badges'>";

  for (let j = 0; j < views.length; j++) {
    let view = views[j];
    let color = palette[j % palette.length];
    let name = escapeHtml(view.name);
    let statusClass = "mv-status-dot-" + (view.status || "valid").toLowerCase();

    if (isQuerySidebar) {
      html += "<a class='sidebar-badge' href='#' style='background-color: " + color + "' ";
      html += "onclick='executeCommand(\"sql\", \"select from \\`" + view.name + "\\`\"); return false;' ";
      html += "title='" + name + " (Materialized View)'>";
      html += "<span class='mv-status-dot " + statusClass + "'></span>";
      html += "<span class='sidebar-badge-name'>" + name + "</span>";
      html += "</a>";
    } else {
      html += "<a class='sidebar-badge' href='#' style='background-color: " + color + "' ";
      html += "onclick='showMaterializedViewDetail(\"" + view.name.replace(/"/g, "&quot;") + "\"); return false;' ";
      html += "title='" + name + " (Materialized View)'>";
      html += "<span class='mv-status-dot " + statusClass + "'></span>";
      html += "<span class='sidebar-badge-name'>" + name + "</span>";
      html += "</a>";
    }
  }

  html += "</div></div>";
  return html;
}

function mvStatusDotClass(status) {
  let s = (status || "VALID").toUpperCase();
  if (s == "VALID") return "mv-status-dot-valid";
  if (s == "BUILDING") return "mv-status-dot-building";
  if (s == "STALE") return "mv-status-dot-stale";
  if (s == "ERROR") return "mv-status-dot-error";
  return "mv-status-dot-valid";
}

function mvStatusBadgeClass(status) {
  let s = (status || "VALID").toUpperCase();
  if (s == "VALID") return "mv-status-badge-valid";
  if (s == "BUILDING") return "mv-status-badge-building";
  if (s == "STALE") return "mv-status-badge-stale";
  if (s == "ERROR") return "mv-status-badge-error";
  return "mv-status-badge-valid";
}

function mvFormatRelativeTime(timestamp) {
  if (!timestamp || timestamp <= 0) return "Never";
  let diff = Date.now() - timestamp;
  if (diff < 0) diff = 0;
  let secs = Math.floor(diff / 1000);
  if (secs < 60) return secs + "s ago";
  let mins = Math.floor(secs / 60);
  if (mins < 60) return mins + "m ago";
  let hours = Math.floor(mins / 60);
  if (hours < 24) return hours + "h ago";
  let days = Math.floor(hours / 24);
  return days + "d ago";
}

function mvFormatInterval(ms) {
  if (!ms || ms <= 0) return "—";
  let secs = Math.floor(ms / 1000);
  if (secs < 60) return secs + " seconds";
  let mins = Math.floor(secs / 60);
  if (mins < 60) return mins + " minutes";
  let hours = Math.floor(mins / 60);
  return hours + " hours";
}

function showMaterializedViewDetail(viewName) {
  // Clear any existing auto-refresh
  if (_mvAutoRefreshInterval) {
    clearInterval(_mvAutoRefreshInterval);
    _mvAutoRefreshInterval = null;
  }

  let views = window._schemaMaterializedViews;
  if (!views) return;

  let view = null;
  for (let i = 0; i < views.length; i++)
    if (views[i].name == viewName) { view = views[i]; break; }
  if (view == null) return;

  // Mark active badge
  $("#dbTypeBadges .sidebar-badge").removeClass("db-badge-active");
  $("#dbTypeBadges .sidebar-badge").each(function () {
    if ($(this).find(".sidebar-badge-name").text() == viewName)
      $(this).addClass("db-badge-active");
  });

  let status = (view.status || "VALID").toUpperCase();
  let statusBadgeClass = mvStatusBadgeClass(status);
  let statusDotClass = mvStatusDotClass(status);

  let html = "";

  // Header
  html += "<div class='d-flex align-items-center gap-3 mb-3'>";
  html += "<h4 style='margin:0;'>" + escapeHtml(view.name) + "</h4>";
  html += "<span class='db-type-category-badge' style='background-color:#a855f7;'>Materialized View</span>";
  html += "<span class='mv-status-badge " + statusBadgeClass + "'><span class='mv-status-dot " + statusDotClass + "'></span>" + escapeHtml(status) + "</span>";
  html += "</div>";

  // Defining Query
  html += "<div class='db-detail-section'>";
  html += "<h6><i class='fa fa-code'></i> Defining Query</h6>";
  html += "<div class='mv-query-block'>" + escapeHtml(view.query || "") + "</div>";
  html += "</div>";

  // Source Types
  if (view.sourceTypes && view.sourceTypes.length > 0) {
    html += "<div class='db-detail-section'>";
    html += "<h6><i class='fa fa-link'></i> Source Types</h6>";
    html += "<div class='d-flex flex-wrap gap-2'>";
    for (let i = 0; i < view.sourceTypes.length; i++) {
      let st = view.sourceTypes[i];
      html += "<a class='link' href='#' onclick='showTypeDetail(\"" + escapeHtml(st) + "\"); return false;' style='font-weight:600;'>" + escapeHtml(st) + "</a>";
    }
    html += "</div></div>";
  }

  // Refresh Info
  html += "<div class='db-detail-section'>";
  html += "<h6><i class='fa fa-sync'></i> Refresh Info</h6>";
  html += "<div class='mv-info-row'><span class='mv-info-label'>Mode:</span> " + escapeHtml(view.refreshMode || "MANUAL") + "</div>";
  if ((view.refreshMode || "").toUpperCase() == "PERIODIC" && view.refreshInterval > 0)
    html += "<div class='mv-info-row'><span class='mv-info-label'>Interval:</span> " + escapeHtml(mvFormatInterval(view.refreshInterval)) + "</div>";
  html += "<div class='mv-info-row'><span class='mv-info-label'>Last Refresh:</span> " + escapeHtml(mvFormatRelativeTime(view.lastRefreshTime)) + "</div>";
  html += "<div class='mv-info-row'><span class='mv-info-label'>Status:</span> <span class='mv-status-badge " + statusBadgeClass + "'><span class='mv-status-dot " + statusDotClass + "'></span>" + escapeHtml(status) + "</span></div>";
  html += "<div class='mv-info-row'><span class='mv-info-label'>Backing Type:</span> " + escapeHtml(view.backingType || "") + "</div>";
  html += "</div>";

  // Properties & Indexes from backing type
  let backingType = null;
  if (view.backingType && window._schemaTypes) {
    for (let i in window._schemaTypes)
      if (window._schemaTypes[i].name == view.backingType) { backingType = window._schemaTypes[i]; break; }
  }

  if (backingType) {
    html += "<div class='db-detail-section'>";
    html += "<h6><i class='fa fa-list'></i> Properties</h6>";
    let propHtml = renderProperties(backingType, window._schemaTypes);
    if (propHtml == "")
      html += "<p class='text-muted' style='font-size:0.85rem;'>No properties defined.</p>";
    else {
      html += "<div class='table-responsive'>";
      html += "<table class='table table-sm db-detail-table'>";
      html += "<thead><tr><th>Name</th><th>Defined In</th><th>Type</th><th>Mandatory</th><th>Not Null</th><th>Hidden</th><th>Read Only</th><th>Default</th><th>Min</th><th>Max</th><th>Regexp</th><th>Indexes</th><th>Actions</th></tr></thead>";
      html += "<tbody>" + propHtml + "</tbody></table></div>";
    }
    html += "</div>";

    if (backingType.indexes && backingType.indexes.length > 0) {
      html += "<div class='db-detail-section'>";
      html += "<h6><i class='fa fa-bolt'></i> Indexes</h6>";
      html += "<div class='table-responsive'>";
      html += "<table class='table table-sm db-detail-table'>";
      html += "<thead><tr><th>Name</th><th>Defined In</th><th>Properties</th><th>Type</th><th>Unique</th><th>Automatic</th><th>Actions</th></tr></thead>";
      html += "<tbody>" + renderIndexes(backingType, window._schemaTypes) + "</tbody></table></div>";
      html += "</div>";
    }
  }

  // Quick Actions
  html += "<div class='db-detail-section'>";
  html += "<h6><i class='fa fa-play-circle'></i> Quick Actions</h6>";
  html += "<div class='d-flex flex-wrap gap-2'>";
  html += "<button class='btn btn-sm db-action-btn' onclick='refreshMaterializedView(\"" + escapeHtml(view.name) + "\")'><i class='fa fa-sync'></i> Refresh Now</button>";
  html += "<button class='btn btn-sm db-action-btn' onclick='executeCommand(\"sql\", \"select from \\`" + view.name + "\\`\")'><i class='fa fa-table'></i> Browse Records</button>";
  html += "<button class='btn btn-sm db-action-btn' onclick='alterMaterializedView(\"" + escapeHtml(view.name) + "\")'><i class='fa fa-pen'></i> Alter Refresh Mode</button>";
  html += "<button class='btn btn-sm db-action-btn db-action-btn-danger' onclick='dropMaterializedView(\"" + escapeHtml(view.name) + "\")'><i class='fa fa-trash'></i> Drop View</button>";
  html += "</div></div>";

  $("#dbTypeDetail").html(html);

  // Auto-refresh if status is BUILDING
  if (status == "BUILDING") {
    _mvAutoRefreshInterval = setInterval(function () {
      fetchMaterializedViews(function (updatedViews) {
        let updatedView = null;
        for (let i = 0; i < updatedViews.length; i++)
          if (updatedViews[i].name == viewName) { updatedView = updatedViews[i]; break; }
        if (updatedView && (updatedView.status || "").toUpperCase() != "BUILDING") {
          clearInterval(_mvAutoRefreshInterval);
          _mvAutoRefreshInterval = null;
          showMaterializedViewDetail(viewName);
        } else if (updatedView) {
          // Update status display inline
          let newStatus = (updatedView.status || "VALID").toUpperCase();
          let newDotClass = mvStatusDotClass(newStatus);
          let newBadgeClass = mvStatusBadgeClass(newStatus);
          $(".mv-status-badge").attr("class", "mv-status-badge " + newBadgeClass).html("<span class='mv-status-dot " + newDotClass + "'></span>" + escapeHtml(newStatus));
        } else {
          clearInterval(_mvAutoRefreshInterval);
          _mvAutoRefreshInterval = null;
        }
      });
    }, 3000);
  }
}

function validateMaterializedViewName(name) {
  if (name == null || name.trim() == "")
    return "View name is required";
  if (name.indexOf(",") >= 0)
    return "View name cannot contain commas";
  if (name.indexOf("`") >= 0)
    return "View name cannot contain backticks";
  // Check against existing types
  if (window._schemaTypes) {
    for (let i in window._schemaTypes)
      if (window._schemaTypes[i].name == name)
        return "A type named '" + name + "' already exists";
  }
  // Check against existing materialized views
  if (window._schemaMaterializedViews) {
    for (let i = 0; i < window._schemaMaterializedViews.length; i++)
      if (window._schemaMaterializedViews[i].name == name)
        return "A materialized view named '" + name + "' already exists";
  }
  return null;
}

function createMaterializedView() {
  let html = "<label for='mvCreateName'>View Name <span style='color:#dc3545'>*</span></label>";
  html += "<input class='form-control mt-1' id='mvCreateName' placeholder='e.g. my_view'>";
  html += "<div id='mvCreateNameFeedback' style='font-size:0.78rem;min-height:20px;margin-bottom:8px;'></div>";
  html += "<label for='mvCreateQuery'>SQL Query <span style='color:#dc3545'>*</span></label>";
  html += "<textarea class='form-control mt-1 mb-2' id='mvCreateQuery' rows='3' placeholder='SELECT ... FROM ...' style='font-family:monospace;font-size:0.85rem;'></textarea>";
  html += "<label>Refresh Mode</label>";
  html += "<div class='d-flex gap-3 mt-1 mb-2'>";
  html += "<div class='form-check'><input class='form-check-input' type='radio' name='mvRefreshMode' id='mvModeManual' value='MANUAL' checked><label class='form-check-label' for='mvModeManual'>Manual</label></div>";
  html += "<div class='form-check'><input class='form-check-input' type='radio' name='mvRefreshMode' id='mvModeIncremental' value='INCREMENTAL'><label class='form-check-label' for='mvModeIncremental'>Incremental</label></div>";
  html += "<div class='form-check'><input class='form-check-input' type='radio' name='mvRefreshMode' id='mvModePeriodic' value='PERIODIC'><label class='form-check-label' for='mvModePeriodic'>Periodic</label></div>";
  html += "</div>";
  html += "<div id='mvPeriodicOptions' style='display:none;'>";
  html += "<label for='mvInterval'>Interval</label>";
  html += "<div class='d-flex gap-2 mt-1'>";
  html += "<input class='form-control' id='mvInterval' type='number' value='5' min='1' style='width:100px;'>";
  html += "<select class='form-select' id='mvIntervalUnit' style='width:auto;'><option value='SECOND'>Seconds</option><option value='MINUTE' selected>Minutes</option><option value='HOUR'>Hours</option></select>";
  html += "</div></div>";

  globalPrompt("Create Materialized View", html, "Create", function () {
    let name = $("#mvCreateName").val().trim();
    let error = validateMaterializedViewName(name);
    if (error) {
      globalNotify("Error", error, "danger");
      return;
    }
    let query = $("#mvCreateQuery").val().trim();
    if (query == "") {
      globalNotify("Error", "SQL query is required", "danger");
      return;
    }
    let mode = $("input[name='mvRefreshMode']:checked").val();
    let command = "CREATE MATERIALIZED VIEW `" + name + "` AS " + query;
    if (mode != "MANUAL") {
      command += " REFRESH";
      if (mode == "PERIODIC") {
        let interval = parseInt($("#mvInterval").val()) || 5;
        let unit = $("#mvIntervalUnit").val() || "MINUTE";
        command += " EVERY " + interval + " " + unit;
      } else {
        command += " " + mode;
      }
    }

    let database = getCurrentDatabase();
    jQuery
      .ajax({
        type: "POST",
        url: "api/v1/command/" + database,
        data: JSON.stringify({
          language: "sql",
          command: command,
          serializer: "record",
        }),
        beforeSend: function (xhr) {
          xhr.setRequestHeader("Authorization", globalCredentials);
        },
      })
      .done(function (data) {
        globalNotify("Success", "Materialized view '" + escapeHtml(name) + "' created", "success");
        displaySchema();
      })
      .fail(function (jqXHR, textStatus, errorThrown) {
        globalNotifyError(jqXHR.responseText);
      });
  });

  // Wire up live validation + periodic toggle
  setTimeout(function () {
    $("#mvCreateName").on("input", function () {
      let name = $(this).val().trim();
      let feedback = $("#mvCreateNameFeedback");
      if (name == "") {
        feedback.html("");
        $(this).removeClass("is-invalid is-valid");
        return;
      }
      let error = validateMaterializedViewName(name);
      if (error) {
        feedback.html("<span style='color:#dc3545;'><i class='fa fa-circle-exclamation'></i> " + escapeHtml(error) + "</span>");
        $(this).addClass("is-invalid").removeClass("is-valid");
      } else {
        feedback.html("<span style='color:#28a745;'><i class='fa fa-circle-check'></i> Name is available</span>");
        $(this).addClass("is-valid").removeClass("is-invalid");
      }
    });
    $("input[name='mvRefreshMode']").on("change", function () {
      if ($(this).val() == "PERIODIC")
        $("#mvPeriodicOptions").show();
      else
        $("#mvPeriodicOptions").hide();
    });
  }, 200);
}

function refreshMaterializedView(name) {
  globalConfirm(
    "Refresh Materialized View",
    "Are you sure you want to refresh the materialized view '" + escapeHtml(name) + "'?",
    "info",
    function () {
      let database = getCurrentDatabase();
      jQuery
        .ajax({
          type: "POST",
          url: "api/v1/command/" + database,
          data: JSON.stringify({
            language: "sql",
            command: "REFRESH MATERIALIZED VIEW `" + name + "`",
            serializer: "record",
          }),
          beforeSend: function (xhr) {
            xhr.setRequestHeader("Authorization", globalCredentials);
          },
        })
        .done(function (data) {
          globalNotify("Success", "Materialized view '" + escapeHtml(name) + "' refreshed", "success");
          displaySchema();
        })
        .fail(function (jqXHR, textStatus, errorThrown) {
          globalNotifyError(jqXHR.responseText);
        });
    }
  );
}

function alterMaterializedView(name) {
  let html = "<label>New Refresh Mode</label>";
  html += "<div class='d-flex gap-3 mt-1 mb-2'>";
  html += "<div class='form-check'><input class='form-check-input' type='radio' name='mvAlterMode' id='mvAlterManual' value='MANUAL' checked><label class='form-check-label' for='mvAlterManual'>Manual</label></div>";
  html += "<div class='form-check'><input class='form-check-input' type='radio' name='mvAlterMode' id='mvAlterIncremental' value='INCREMENTAL'><label class='form-check-label' for='mvAlterIncremental'>Incremental</label></div>";
  html += "<div class='form-check'><input class='form-check-input' type='radio' name='mvAlterMode' id='mvAlterPeriodic' value='PERIODIC'><label class='form-check-label' for='mvAlterPeriodic'>Periodic</label></div>";
  html += "</div>";
  html += "<div id='mvAlterPeriodicOptions' style='display:none;'>";
  html += "<label for='mvAlterInterval'>Interval</label>";
  html += "<div class='d-flex gap-2 mt-1'>";
  html += "<input class='form-control' id='mvAlterInterval' type='number' value='5' min='1' style='width:100px;'>";
  html += "<select class='form-select' id='mvAlterIntervalUnit' style='width:auto;'><option value='SECOND'>Seconds</option><option value='MINUTE' selected>Minutes</option><option value='HOUR'>Hours</option></select>";
  html += "</div></div>";

  globalPrompt("Alter Materialized View: " + escapeHtml(name), html, "Alter", function () {
    let mode = $("input[name='mvAlterMode']:checked").val();
    let command = "ALTER MATERIALIZED VIEW `" + name + "` REFRESH " + mode;
    if (mode == "PERIODIC") {
      let interval = parseInt($("#mvAlterInterval").val()) || 5;
      let unit = $("#mvAlterIntervalUnit").val() || "MINUTE";
      command += " EVERY " + interval + " " + unit;
    }

    let database = getCurrentDatabase();
    jQuery
      .ajax({
        type: "POST",
        url: "api/v1/command/" + database,
        data: JSON.stringify({
          language: "sql",
          command: command,
          serializer: "record",
        }),
        beforeSend: function (xhr) {
          xhr.setRequestHeader("Authorization", globalCredentials);
        },
      })
      .done(function (data) {
        globalNotify("Success", "Materialized view '" + escapeHtml(name) + "' updated", "success");
        displaySchema();
      })
      .fail(function (jqXHR, textStatus, errorThrown) {
        globalNotifyError(jqXHR.responseText);
      });
  });

  // Toggle periodic options visibility
  setTimeout(function () {
    $("input[name='mvAlterMode']").on("change", function () {
      if ($(this).val() == "PERIODIC")
        $("#mvAlterPeriodicOptions").show();
      else
        $("#mvAlterPeriodicOptions").hide();
    });
  }, 200);
}

function dropMaterializedView(name) {
  globalConfirm(
    "Drop Materialized View",
    "Are you sure you want to drop the materialized view '" + escapeHtml(name) + "'?<br>WARNING: The operation cannot be undone.",
    "warning",
    function () {
      let database = getCurrentDatabase();
      jQuery
        .ajax({
          type: "POST",
          url: "api/v1/command/" + database,
          data: JSON.stringify({
            language: "sql",
            command: "DROP MATERIALIZED VIEW `" + name + "`",
            serializer: "record",
          }),
          beforeSend: function (xhr) {
            xhr.setRequestHeader("Authorization", globalCredentials);
          },
        })
        .done(function (data) {
          globalNotify("Success", "Materialized view '" + escapeHtml(name) + "' dropped", "success");
          displaySchema();
        })
        .fail(function (jqXHR, textStatus, errorThrown) {
          globalNotifyError(jqXHR.responseText);
        });
    }
  );
}

// ===== Flame Graph Visualization =====

function renderFlameGraph(plan) {
  var container = $("#flameGraphContainer");
  if (!plan) {
    container.html("");
    return;
  }

  var steps = plan.steps;
  if (!steps || steps.length == 0) {
    container.html("");
    return;
  }

  // Check if any step has cost data (cost != -1)
  var hasCostData = stepsHaveCost(steps);

  if (!hasCostData) {
    container.html("<div class='flame-no-data'><i class='fa fa-info-circle'></i> Enable <b>Profile Execution</b> to see cost data in the flame graph.</div>");
    return;
  }

  var totalCost = computeTotalCost(steps);
  if (totalCost <= 0) {
    container.html("<div class='flame-no-data'><i class='fa fa-info-circle'></i> No measurable cost recorded.</div>");
    return;
  }

  var html = "<div class='flame-graph'>";
  html += "<div class='flame-graph-header'><i class='fa fa-fire'></i> Execution Flame Graph</div>";
  // Root bar spanning full width showing total cost
  html += "<div class='flame-bar flame-depth-0' style='width:100%'";
  html += " data-flame-tip='Total execution &mdash; " + escapeHtml(formatCostNanos(totalCost)) + "'>";
  html += "<span class='flame-label'>Total <small>" + formatCostNanos(totalCost) + "</small></span>";
  html += "</div>";
  // Render top-level steps as children, all relative to totalCost
  html += renderFlameRow(steps, totalCost, 1);
  html += "</div>";
  container.html(html);
  initFlameTooltips();
}

/**
 * Renders one level of the icicle chart. Each step bar's width is proportional
 * to the rootCost (the total), so a 248µs step next to a 58ms step is visually
 * tiny — which is correct. Sub-steps are rendered recursively below their parent,
 * each nested inside a wrapper whose width matches the parent bar.
 */
function renderFlameRow(steps, rootCost, depth) {
  if (!steps || steps.length == 0) return "";

  var html = "<div class='flame-row'>";
  for (var i = 0; i < steps.length; i++) {
    var step = steps[i];
    var cost = step.cost != null ? step.cost : -1;
    var pctOfRoot = rootCost > 0 && cost > 0 ? (cost / rootCost * 100) : 0;
    var widthPct = Math.max(pctOfRoot, 1.5); // minimum 1.5% for visibility
    var depthClass = "flame-depth-" + (depth % 5);
    var name = simplifyStepName(step.name || "Step");
    var desc = step.description || "";
    var costLabel = formatCostNanos(cost);

    // Wrapper holds the bar + its children (so children sit below, constrained to parent width)
    html += "<div class='flame-cell' style='width:" + widthPct + "%'>";
    var tipHtml = escapeHtml(name) + " &mdash; " + escapeHtml(costLabel) + " (" + pctOfRoot.toFixed(1) + "% of total)";
    if (desc) tipHtml += "<br>" + escapeHtml(desc);
    html += "<div class='flame-bar " + depthClass + "'";
    html += " data-flame-tip='" + tipHtml.replace(/'/g, "&#39;") + "'>";
    html += "<span class='flame-label'>" + escapeHtml(name) + " <small>" + costLabel + "</small></span>";
    html += "</div>";
    // Recursively render sub-steps inside this cell (inherits parent width)
    if (step.subSteps && step.subSteps.length > 0)
      html += renderFlameRow(step.subSteps, rootCost, depth + 1);
    html += "</div>";
  }
  html += "</div>";
  return html;
}

function initFlameTooltips() {
  var tip = $("#flameTooltip");
  if (tip.length == 0) {
    $("body").append("<div id='flameTooltip' class='flame-tooltip'></div>");
    tip = $("#flameTooltip");
  }
  $(".flame-bar[data-flame-tip]").on("mouseenter", function(e) {
    tip.html($(this).attr("data-flame-tip")).css({
      left: e.pageX + 12,
      top: e.pageY - 10
    }).addClass("visible");
  }).on("mousemove", function(e) {
    tip.css({ left: e.pageX + 12, top: e.pageY - 10 });
  }).on("mouseleave", function() {
    tip.removeClass("visible");
  });
}

function stepsHaveCost(steps) {
  if (!steps) return false;
  for (var i = 0; i < steps.length; i++) {
    if (steps[i].cost != null && steps[i].cost > 0) return true;
    if (steps[i].subSteps && stepsHaveCost(steps[i].subSteps)) return true;
  }
  return false;
}

function computeTotalCost(steps) {
  var total = 0;
  if (!steps) return 0;
  for (var i = 0; i < steps.length; i++) {
    var cost = steps[i].cost;
    if (cost != null && cost > 0) total += cost;
  }
  return total;
}

function simplifyStepName(name) {
  if (!name) return "Step";
  return name.replace(/ExecutionStep$/, "").replace(/Step$/, "");
}

function formatCostNanos(ns) {
  if (ns == null || ns < 0) return "n/a";
  if (ns < 1000) return ns + " ns";
  if (ns < 1000000) return (ns / 1000).toFixed(1) + " \u00B5s";
  if (ns < 1000000000) return (ns / 1000000).toFixed(1) + " ms";
  return (ns / 1000000000).toFixed(2) + " s";
}

// ===== Database Metrics Tab =====

var _dbMetricsAutoRefreshInterval = null;
var _dbMetricsCrudChart = null;

function startDbMetricsAutoRefresh() {
  stopDbMetricsAutoRefresh();
  _dbMetricsAutoRefreshInterval = setInterval(function () {
    if ($("#tab-db-metrics").hasClass("active"))
      loadDatabaseMetrics();
  }, 5000);
}

function stopDbMetricsAutoRefresh() {
  if (_dbMetricsAutoRefreshInterval) {
    clearInterval(_dbMetricsAutoRefreshInterval);
    _dbMetricsAutoRefreshInterval = null;
  }
}

function loadDatabaseMetrics() {
  let database = getCurrentDatabase();
  if (!database) return;

  jQuery.ajax({
    type: "POST",
    url: "api/v1/query/" + database,
    data: JSON.stringify({ language: "sql", command: "SELECT FROM schema:stats" }),
    beforeSend: function (xhr) { xhr.setRequestHeader("Authorization", globalCredentials); },
  }).done(function (data) {
    if (data.result && data.result.length > 0)
      displayDatabaseStats(data.result[0]);
  }).fail(function (jqXHR) {
    globalNotifyError(jqXHR.responseText);
  });

  jQuery.ajax({
    type: "POST",
    url: "api/v1/query/" + database,
    data: JSON.stringify({ language: "sql", command: "SELECT FROM schema:materializedViews" }),
    beforeSend: function (xhr) { xhr.setRequestHeader("Authorization", globalCredentials); },
  }).done(function (data) {
    displayMvHealth(data.result || []);
  });
}

function formatNumber(n) {
  if (n == null) return "-";
  return Number(n).toLocaleString();
}

function displayDatabaseStats(stats) {
  $("#dbStatTxCommits").text(formatNumber((stats.writeTx || 0) + (stats.readTx || 0)));
  $("#dbStatTxRollbacks").text(formatNumber(stats.txRollbacks));
  $("#dbStatQueries").text(formatNumber(stats.queries));
  $("#dbStatCommands").text(formatNumber(stats.commands));
  $("#dbStatScanType").text(formatNumber(stats.scanType));
  $("#dbStatScanBucket").text(formatNumber(stats.scanBucket));
  $("#dbStatIterateType").text(formatNumber(stats.iterateType));
  $("#dbStatIterateBucket").text(formatNumber(stats.iterateBucket));
  $("#dbStatCountType").text(formatNumber(stats.countType));
  $("#dbStatCountBucket").text(formatNumber(stats.countBucket));

  // CRUD pie chart
  let crudData = [
    stats.createRecord || 0,
    stats.readRecord || 0,
    stats.updateRecord || 0,
    stats.deleteRecord || 0
  ];

  if (_dbMetricsCrudChart) {
    _dbMetricsCrudChart.updateSeries(crudData);
  } else {
    _dbMetricsCrudChart = new ApexCharts(document.querySelector("#dbStatCrudChart"), {
      chart: { type: "donut", height: 200 },
      series: crudData,
      labels: ["Create", "Read", "Update", "Delete"],
      colors: ["#28a745", "#00aeee", "#ffc107", "#dc3545"],
      legend: { position: "bottom", fontSize: "11px" },
      plotOptions: { pie: { donut: { size: "50%" } } },
      dataLabels: { enabled: true, formatter: function (val) { return val.toFixed(0) + "%"; } },
      noData: { text: "No CRUD operations yet" }
    });
    _dbMetricsCrudChart.render();
  }
}

function mvStatusBadge(status) {
  let cls = "secondary";
  if (status === "VALID") cls = "success";
  else if (status === "BUILDING") cls = "info";
  else if (status === "STALE") cls = "warning";
  else if (status === "ERROR") cls = "danger";
  return '<span class="badge bg-' + cls + '">' + escapeHtml(status) + '</span>';
}

function formatMs(ms) {
  if (ms == null || ms === 0) return "-";
  if (ms < 1000) return ms + " ms";
  return (ms / 1000).toFixed(2) + " s";
}

function displayMvHealth(views) {
  let container = $("#dbMetricsMvContainer");
  if (!views || views.length === 0) {
    container.html('<p class="text-muted">No materialized views found.</p>');
    return;
  }

  let html = '<div class="table-responsive"><table class="table table-sm table-striped">';
  html += "<thead><tr>";
  html += "<th>Name</th><th>Status</th><th>Refreshes</th>";
  html += "<th>Avg Time</th><th>Min</th><th>Max</th>";
  html += "<th>Last Duration</th><th>Errors</th><th></th>";
  html += "</tr></thead><tbody>";

  for (let i = 0; i < views.length; i++) {
    let v = views[i];
    html += "<tr>";
    html += "<td>" + escapeHtml(v.name) + "</td>";
    html += "<td>" + mvStatusBadge(v.status) + "</td>";
    html += "<td class='text-end'>" + formatNumber(v.refreshCount) + "</td>";
    html += "<td class='text-end'>" + formatMs(v.refreshAvgTimeMs) + "</td>";
    html += "<td class='text-end'>" + formatMs(v.refreshMinTimeMs) + "</td>";
    html += "<td class='text-end'>" + formatMs(v.refreshMaxTimeMs) + "</td>";
    html += "<td class='text-end'>" + formatMs(v.lastRefreshDurationMs) + "</td>";
    html += "<td class='text-end'>" + (v.errorCount > 0 ? '<span class="text-danger">' + formatNumber(v.errorCount) + '</span>' : '0') + "</td>";
    html += '<td><button class="btn btn-sm db-action-btn" onclick="refreshMvFromMetrics(\'' + escapeHtml(v.name).replace(/'/g, "\\'") + '\')"><i class="fa fa-sync"></i></button></td>';
    html += "</tr>";
  }

  html += "</tbody></table></div>";
  container.html(html);
}

function refreshMvFromMetrics(name) {
  let database = getCurrentDatabase();
  if (!database) return;

  jQuery.ajax({
    type: "POST",
    url: "api/v1/command/" + database,
    data: JSON.stringify({ language: "sql", command: "REFRESH MATERIALIZED VIEW `" + name + "`" }),
    beforeSend: function (xhr) { xhr.setRequestHeader("Authorization", globalCredentials); },
  }).done(function () {
    globalNotify("Success", "Materialized view '" + escapeHtml(name) + "' refreshed", "success");
    loadDatabaseMetrics();
  }).fail(function (jqXHR) {
    globalNotifyError(jqXHR.responseText);
  });
}

// ===== Buckets Tab =====

var _bucketDetails = {};

function loadStorageBuckets() {
  var database = getCurrentDatabase();
  if (!database) return;

  $("#dbBucketDetailPanel").hide();
  _bucketDetails = {};

  if ($.fn.dataTable.isDataTable("#dbStorageBuckets"))
    try { $("#dbStorageBuckets").DataTable().destroy(); $("#dbStorageBuckets").empty(); } catch (e) {}

  jQuery.ajax({
    type: "POST",
    url: "api/v1/query/" + database,
    data: JSON.stringify({ language: "sql", command: "SELECT FROM schema:buckets" }),
    beforeSend: function (xhr) { xhr.setRequestHeader("Authorization", globalCredentials); },
  }).done(function (data) {
    if (!data.result || data.result.length === 0) {
      $("#dbStorageBuckets").DataTable({
        paging: false, ordering: true, data: [],
        columns: [{ title: "Name" }, { title: "File ID" }, { title: "Page Size" }, { title: "Pages" }, { title: "Active" }, { title: "Deleted" }, { title: "Errors" }]
      });
      return;
    }

    var buckets = data.result;
    var pending = buckets.length;
    var detailResults = {};

    for (var i = 0; i < buckets.length; i++) {
      (function (bucket) {
        jQuery.ajax({
          type: "POST",
          url: "api/v1/query/" + database,
          data: JSON.stringify({ language: "sql", command: "SELECT FROM schema:bucket:" + bucket.name }),
          beforeSend: function (xhr) { xhr.setRequestHeader("Authorization", globalCredentials); },
        }).done(function (detailData) {
          if (detailData.result && detailData.result.length > 0)
            detailResults[bucket.name] = detailData.result[0];
        }).always(function () {
          pending--;
          if (pending === 0)
            renderBucketsDataTable(buckets, detailResults);
        });
      })(buckets[i]);
    }
  }).fail(function (jqXHR) {
    globalNotifyError(jqXHR.responseText);
  });
}

function renderBucketsDataTable(buckets, details) {
  _bucketDetails = details;
  var tableData = [];

  for (var i = 0; i < buckets.length; i++) {
    var b = buckets[i];
    var d = details[b.name] || {};
    tableData.push([
      escapeHtml(b.name),
      b.fileId != null ? b.fileId : 0,
      d.pageSize != null ? d.pageSize : 0,
      d.totalPages != null ? d.totalPages : 0,
      d.totalActiveRecords != null ? d.totalActiveRecords : 0,
      d.totalDeletedRecords != null ? d.totalDeletedRecords : 0,
      d.totalErrors != null ? d.totalErrors : 0
    ]);
  }

  if ($.fn.dataTable.isDataTable("#dbStorageBuckets"))
    try { $("#dbStorageBuckets").DataTable().destroy(); $("#dbStorageBuckets").empty(); } catch (e) {}

  var dt = $("#dbStorageBuckets").DataTable({
    paging: false,
    ordering: true,
    order: [[0, "asc"]],
    data: tableData,
    columns: [
      { title: "Name" },
      { title: "File ID" },
      { title: "Page Size", render: function (d) { return d != 0 ? Number(d).toLocaleString() : "-"; } },
      { title: "Pages", render: function (d) { return d != 0 ? Number(d).toLocaleString() : "-"; } },
      { title: "Active", render: function (d) { return Number(d).toLocaleString(); } },
      { title: "Deleted", render: function (d) { return Number(d).toLocaleString(); } },
      { title: "Errors" }
    ]
  });

  $("#dbStorageBuckets tbody").on("click", "tr", function () {
    $("#dbStorageBuckets tbody tr").removeClass("table-active");
    $(this).addClass("table-active");
    var rowData = dt.row(this).data();
    if (!rowData) return;
    var name = $("<div>").html(rowData[0]).text();
    var detail = _bucketDetails[name] || {};
    $("#dbBucketDetailTitle").text(name);
    $("#dbBucketDetailContent").text(JSON.stringify(detail, null, 2));
    $("#dbBucketDetailPanel").show();
  });
}

// ===== Indexes Tab =====

var _indexDetails = {};

function loadStorageIndexes() {
  var database = getCurrentDatabase();
  if (!database) return;

  $("#dbIndexDetailPanel").hide();
  _indexDetails = {};

  if ($.fn.dataTable.isDataTable("#dbStorageIndexes"))
    try { $("#dbStorageIndexes").DataTable().destroy(); $("#dbStorageIndexes").empty(); } catch (e) {}

  jQuery.ajax({
    type: "POST",
    url: "api/v1/query/" + database,
    data: JSON.stringify({ language: "sql", command: "SELECT FROM schema:indexes" }),
    beforeSend: function (xhr) { xhr.setRequestHeader("Authorization", globalCredentials); },
  }).done(function (data) {
    if (!data.result || data.result.length === 0) {
      $("#dbStorageIndexes").DataTable({
        paging: false, ordering: true, data: [],
        columns: [{ title: "Name" }, { title: "Type" }, { title: "Type Name" }, { title: "File ID" }, { title: "Size" }, { title: "Unique" }, { title: "Compacting" }, { title: "Valid" }]
      });
      return;
    }

    var indexes = data.result;
    var pending = indexes.length;
    var detailResults = {};

    for (var i = 0; i < indexes.length; i++) {
      (function (index) {
        jQuery.ajax({
          type: "POST",
          url: "api/v1/query/" + database,
          data: JSON.stringify({ language: "sql", command: "SELECT FROM schema:index:" + index.name }),
          beforeSend: function (xhr) { xhr.setRequestHeader("Authorization", globalCredentials); },
        }).done(function (detailData) {
          if (detailData.result && detailData.result.length > 0)
            detailResults[index.name] = detailData.result[0];
        }).always(function () {
          pending--;
          if (pending === 0)
            renderIndexesDataTable(indexes, detailResults);
        });
      })(indexes[i]);
    }
  }).fail(function (jqXHR) {
    globalNotifyError(jqXHR.responseText);
  });
}

function renderIndexesDataTable(indexes, details) {
  _indexDetails = details;
  var tableData = [];

  for (var i = 0; i < indexes.length; i++) {
    var idx = indexes[i];
    var d = details[idx.name] || {};
    tableData.push([
      escapeHtml(idx.name),
      escapeHtml(idx.indexType || "-"),
      escapeHtml(idx.typeName || "-"),
      d.fileId != null ? d.fileId : "-",
      escapeHtml(d.size || "-"),
      idx.unique ? "Yes" : "No",
      d.compacting ? "Yes" : "No",
      d.valid != null ? (d.valid ? "Yes" : "No") : "-"
    ]);
  }

  if ($.fn.dataTable.isDataTable("#dbStorageIndexes"))
    try { $("#dbStorageIndexes").DataTable().destroy(); $("#dbStorageIndexes").empty(); } catch (e) {}

  var dt = $("#dbStorageIndexes").DataTable({
    paging: false,
    ordering: true,
    order: [[0, "asc"]],
    data: tableData,
    columns: [
      { title: "Name" },
      { title: "Type" },
      { title: "Type Name" },
      { title: "File ID" },
      { title: "Size" },
      { title: "Unique" },
      { title: "Compacting" },
      { title: "Valid" }
    ]
  });

  $("#dbStorageIndexes tbody").on("click", "tr", function () {
    $("#dbStorageIndexes tbody tr").removeClass("table-active");
    $(this).addClass("table-active");
    var rowData = dt.row(this).data();
    if (!rowData) return;
    var name = $("<div>").html(rowData[0]).text();
    var detail = _indexDetails[name] || {};
    $("#dbIndexDetailTitle").text(name);
    $("#dbIndexDetailContent").text(JSON.stringify(detail, null, 2));
    $("#dbIndexDetailPanel").show();
  });
}

// ===== Dictionary Tab =====

function loadStorageDictionary() {
  var database = getCurrentDatabase();
  if (!database) return;

  if ($.fn.dataTable.isDataTable("#dbStorageDictTable"))
    try { $("#dbStorageDictTable").DataTable().destroy(); $("#dbStorageDictTable").empty(); } catch (e) {}

  jQuery.ajax({
    type: "POST",
    url: "api/v1/query/" + database,
    data: JSON.stringify({ language: "sql", command: "SELECT FROM schema:dictionary" }),
    beforeSend: function (xhr) { xhr.setRequestHeader("Authorization", globalCredentials); },
  }).done(function (data) {
    if (data.result && data.result.length > 0) {
      var dict = data.result[0];
      $("#dbStorageDictEntries").text(dict.totalEntries != null ? dict.totalEntries.toLocaleString() : "-");
      $("#dbStorageDictPages").text(dict.totalPages != null ? dict.totalPages.toLocaleString() : "-");

      var entries = dict.entries || {};
      var tableData = [];
      var keys = Object.keys(entries);
      for (var i = 0; i < keys.length; i++)
        tableData.push([escapeHtml(keys[i]), entries[keys[i]]]);

      $("#dbStorageDictTable").DataTable({
        paging: false,
        ordering: true,
        order: [[1, "asc"]],
        data: tableData,
        columns: [
          { title: "Name" },
          { title: "ID" }
        ]
      });
    }
  }).fail(function (jqXHR) {
    globalNotifyError(jqXHR.responseText);
  });
}
