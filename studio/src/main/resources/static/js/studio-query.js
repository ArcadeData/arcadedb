/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */

// Initialize Bootstrap 5 tooltips
document.querySelectorAll('[data-toggle="tooltip"]').forEach(function(el) {
  new bootstrap.Tooltip(el);
});

var editor = null;
var historyNavIndex = -1;
var historyNavBuffer = "";

function getFilteredHistory() {
  let queryHistory = getQueryHistory();
  // THE STORED ENTRIES CARRY THE RAW NAME (ISSUE #6830), SO THE ARROW-KEY NAVIGATION COMPARES AGAINST THE RAW NAME TOO
  let database = getCurrentDatabase();
  let filtered = [];
  for (let i = 0; i < queryHistory.length; i++) {
    let q = queryHistory[i];
    if (q != null && q.d == database && q.l != null && q.c != null)
      filtered.push(q);
  }
  return filtered;
}

function historyPrevious() {
  let filtered = getFilteredHistory();
  if (filtered.length == 0) return;

  // Save current input when starting navigation
  if (historyNavIndex == -1)
    historyNavBuffer = editor.getValue();
  if (historyNavIndex < filtered.length - 1) {
    historyNavIndex++;
    let q = filtered[historyNavIndex];
    setEditorLanguage(q.l);
    editor.setValue(q.c);
    editor.setCursor(editor.lineCount(), 0);
  }
}

function historyNext() {
  if (historyNavIndex == -1) return;
  let filtered = getFilteredHistory();
  if (historyNavIndex > 0) {
    historyNavIndex--;
    let q = filtered[historyNavIndex];
    setEditorLanguage(q.l);
    editor.setValue(q.c);
    editor.setCursor(editor.lineCount(), 0);
  } else {
    historyNavIndex = -1;
    editor.setValue(historyNavBuffer);
    editor.setCursor(editor.lineCount(), 0);
  }
}

function resetHistoryNavigation() {
  historyNavIndex = -1;
  historyNavBuffer = "";
}

// The editor's Run button.
function runQueryFromEditor() {
  resetHistoryNavigation();
  executeCommand();
}

function clearQuery() {
  if (editor == null) return;
  if (editor.getValue() === "") {
    editor.focus();
    return;
  }
  editor.setValue("");
  editor.setCursor(0, 0);
  resetHistoryNavigation();
  editor.focus();
}

function initQuery() {
  document.querySelectorAll('[data-toggle="tooltip"]').forEach(function(el) {
    new bootstrap.Tooltip(el);
  });

  $('a[data-toggle="tab"]').on("shown.bs.tab", function (e) {
    let activeTab = $("#tabs-command .active").attr("id");
    if (activeTab == "tab-table-sel") renderTable();
    else if (activeTab == "tab-graph-sel" && globalCy == null) renderGraph();
    else if (activeTab == "tab-chart-sel") renderQueryChart();

    if ($(e.target).closest("#tabs-command").length > 0 && activeTab != null)
      globalStorageSave("query.tab", activeTab);
  });

  $("#inputLanguage").change(function () {
    editor.setOption("mode", getEditorMode());
    vecLanguageChanged();
    chartLanguageChanged();
  });

  $(".dropdown-menu").on("click.bs.dropdown", function (e) {
    e.stopPropagation();
    e.preventDefault();
  });

  $("#jsonToClipboard").off("click").on("click", async function(e) {
    e.preventDefault();
    const text = $("#resultJson").val() || $("#resultJson").text();
    try {
      await copyTextToClipboard(text);
      globalNotify("Content", "successfully copied in the clipboard", "success");
    } catch (err) {
      globalNotify("Error", "Failed to copy to clipboard", "danger");
    }
  });

  renderCodeEditor();
  loadFunctionReference();
  initSidebarPanels();
  populateQuerySidebar();

  let savedQueryTab = globalStorageLoad("query.tab", null);
  if (savedQueryTab != null && savedQueryTab != "tab-table-sel" && $("#" + savedQueryTab).length > 0)
    $("#" + savedQueryTab).tab("show");

  executeCommand();

  var dbPar = getURLParameter("db");
  if (dbPar != null && dbPar != "null") setCurrentDatabase(dbPar);

  var limitPar = getURLParameter("limit");
  if (limitPar != null && limitPar != "null") {
    if (limitPar != "20" && limitPar != "100" && limitPar != "500" && limitPar != "-1") {
      var sanitizedLimitPar = escapeHtml(limitPar);
      $("#inputLimit").append("<option value='" + sanitizedLimitPar + "' selected='selected'>" + sanitizedLimitPar + "</option>");
    }
    $("#inputLimit").val(escapeHtml(limitPar));
  }

  $(document).on("databaseChanged", function (e, dbName) {
    if (dbName) {
      globalStorageSave("database.current", dbName);
      resetHistoryNavigation();
      displaySchema();
      displayDatabaseSettings();
      // Refresh active sidebar panel on database change
      refreshActiveSidebarPanel();
      // Reload timeseries types if on that tab
      if (typeof tsLoadTypes === "function")
        tsLoadTypes();
    }
  });
}

function renderCodeEditor() {
  // Dispose any previous CodeMirror instance before creating a new one. initQuery()
  // (and therefore renderCodeEditor()) runs on every login, so without this a fresh
  // CodeMirror wrapper div would be stacked on top of the old one at each re-login
  // (see issue #4893). toTextArea() removes the wrapper and restores the original
  // hidden <textarea id="inputCommand"> so fromTextArea() below can run cleanly again.
  if (editor != null) {
    editor.toTextArea();
    editor = null;
  }

  editor = CodeMirror.fromTextArea(document.getElementById("inputCommand"), {
    mode: getEditorMode(),
    lineWrapping: true,
    indentWithTabs: false,
    smartIndent: true,
    lineNumbers: true,
    matchBrackets: true,
    autofocus: true,
    viewportMargin: Infinity,
    theme: "neo",
  });

  editor.addKeyMap({
    "Ctrl-Enter": function (cm) {
      resetHistoryNavigation();
      executeCommand();
    },
    "Cmd-Enter": function (cm) {
      resetHistoryNavigation();
      executeCommand();
    },
    "Ctrl-Up": function (cm) { historyPrevious(); },
    "Ctrl-Down": function (cm) { historyNext(); },
    "Cmd-Up": function (cm) { historyPrevious(); },
    "Cmd-Down": function (cm) { historyNext(); },
    "Alt-/": function (cm) {
      CodeMirror.showHint(cm, arcadedbHint, { completeSingle: false });
    },
    "Ctrl-Space": function (cm) {
      CodeMirror.showHint(cm, arcadedbHint, { completeSingle: false });
    }
  });

  // HISTORY, SAVED QUERIES AND REPLAY SET THE EDITOR TEXT: THE INDEX SEARCH FORM FOLLOWS IT
  editor.on("change", function () {
    vecLoadFormFromEditor();
  });

  editor.on("inputRead", function (cm, event) {
    if (event.origin !== "+input")
      return;
    var cur = cm.getCursor();
    var line = cm.getLine(cur.line);
    var end = cur.ch;
    var start = end;
    while (start > 0 && /[\w._]/.test(line.charAt(start - 1)))
      start--;
    if (end - start >= 2)
      CodeMirror.showHint(cm, arcadedbHint, { completeSingle: false });
  });

  setTimeout(function () { editor.refresh(); }, 1);
}

function getEditorMode() {
  let language = $("#inputLanguage").val();
  let mime = null;
  if (vecModeForLanguage(language) != null) mime = "application/json";
  else if (chartIsPromQL(language)) mime = "text/plain";
  if (language == "sql") mime = "arcadedb-sql";
  else if (language == "sqlScript") mime = "arcadedb-sql";
  else if (language == "cypher") mime = "application/x-cypher-query";
  else if (language == "opencypher") mime = "application/x-cypher-query";
  else if (language == "gremlin") mime = "sql";
  else if (language == "mongodb") mime = "application/json";
  else if (language == "graphql") mime = "application/json";
  else if (language == "redis") mime = "text/plain";
  return mime;
}
