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

/*
 * Delegated event handlers for Studio markup (issue #9681).
 *
 * The server sends Studio with a Content-Security-Policy whose script-src carries no 'unsafe-inline', so the browser
 * refuses every inline on*="..." attribute: that is what stops an HTML-injection bug from running script. Markup names
 * its handler in data attributes instead, and one listener per event type on the document calls it.
 *
 * The listener runs in the CAPTURE phase, before any handler of the elements on the event's path. An inline handler ran
 * at its own element whatever the elements around it did; a bubbling listener on the document would not, because a
 * handler that stops the event on its way up hides it from the document. Studio has such handlers: the one on every
 * .dropdown-menu (studio-query.js) stops each click inside a menu, so the menu items' actions would never run.
 *
 *
 *   data-on-click="name"       calls the action on click; likewise data-on-change, data-on-input, data-on-keydown
 *   data-on-enter="name"       calls it on a keydown of Enter
 *   data-args='[...]'          the arguments, as a JSON array. Four objects stand for what an inline handler could
 *                              reach: {"$":"this"} the element, {"$":"event"}, {"$":"value"} and {"$":"checked"}
 *   data-stop-propagation      a click stops at this element: no data-on-click of an ancestor runs. Only the
 *                              data-on-* handlers stop; the event itself goes on, since stopping it in the capture
 *                              phase would keep it from the element that was clicked
 *   data-prevent-default       the click's default action is cancelled (the "return false" of an inline handler)
 *   data-enter-click="id"      Enter clicks the element with that id
 *   data-enter-focus="id"      Enter moves the focus to the element with that id
 *
 * Markup built in JavaScript gets these attributes from studioAction(), which escapes them for either quote style.
 *
 * Only the functions listed in STUDIO_ACTIONS can be called. Injected markup can still name one of them, so the list is
 * what bounds that: an existing Studio action, run only on a gesture of the user, never arbitrary script.
 */

/** Hides the element with the given id, as jQuery's hide() does. */
function studioHideById(id) {
  var el = document.getElementById(id);
  if (el) el.style.display = "none";
}

/** Shows or hides the element with the given id, as jQuery's toggle() does. */
function studioToggleById(id) {
  $("#" + CSS.escape(id)).toggle();
}

/** Removes the closest ancestor of `el` (or `el` itself) carrying the CSS class `className`. */
function studioRemoveClosest(el, className) {
  var target = el.closest("." + CSS.escape(className));
  if (target) target.remove();
}

var STUDIO_ACTION_EVENTS = ["click", "change", "input", "keydown"];

var STUDIO_ARG_THIS = { $: "this" };
var STUDIO_ARG_EVENT = { $: "event" };
var STUDIO_ARG_VALUE = { $: "value" };
var STUDIO_ARG_CHECKED = { $: "checked" };

var STUDIO_ACTIONS = new Set([
  "addGroupTypeRow", "addMCPUser", "addPeerPrompt", "addPropertyToRecord", "addTokenTypeRow", "addUserDatabaseRow",
  "aiActivate", "aiChartOpenInQuery", "aiCopyCode", "aiDeleteChat", "aiDeleteMessage", "aiExecuteAll",
  "aiExecuteCommand", "aiHandleInputKeydown", "aiLoadChat", "aiNewChat", "aiOpenInQuery", "aiSendMessage",
  "aiSetMode", "aiStopResponse", "aiToggleResults", "applyCumulativeSelection", "applyDefaultLabel",
  "applyDefaultLimit", "applyGraphSpacing", "applyMaxLabelLength", "applyNodeSize", "applySidebarSetting",
  "applyTheme", "backupDatabase", "cancelRecordEditor", "chartSetType", "clearQuery", "copyApiResponse",
  "copyCreatedToken", "copyInstanceId", "copyMCPConfig", "createApiToken", "createDatabase",
  "createGraphAnalyticalView", "createMaterializedView", "createTimeSeriesType", "createType", "cropSelection",
  "cutSelection", "deleteBackupAction", "deletePropertyFromRecord", "deleteRecord", "deleteSavedQuery",
  "deleteSelectedHistory", "displaySchema", "dropDatabase", "enableBackupConfig", "executeSavedQuery", "exportGraph",
  "exportSettings", "filterFunctionReference", "filterHistoryEntries", "globalActivateTab",
  "hideSelectedGraphElements", "importGraph", "importSampleDatabase", "importSettings", "insertFunctionSyntax",
  "invertVertexSelection", "loadDatabaseBackups", "loadDatabaseMetrics", "loadHistoryEntry", "loadNodeNeighbors",
  "loadPresenceMatrix", "loadQuickHistoryEntry", "loadSavedQuery", "loadStorageBuckets", "loadStorageDictionary",
  "loadStorageIndexes", "loadTypesForGroupDatabase", "loadTypesForTokenDatabase", "login", "logout",
  "onApiAuthChange", "onGroupDatabaseFilterChange", "onMcpAuthMethodChange", "openApiDocs",
  "openRecordEditorFromTable", "pasteReferenceExample", "profilerAiExecuteAll", "profilerAiExecuteCommand",
  "profilerAnalyzeWithAi", "profilerBackToList", "profilerRefresh", "profilerReset", "profilerShowDetail",
  "profilerStart", "profilerStop", "refreshCurrentSecurityTab", "refreshCurrentServerTab", "refreshQuerySidebar",
  "removePeer", "renderGraph", "resetDatabase", "resetGraphTypeStyle", "restoreBackupAction", "resyncDatabase",
  "runQueryFromEditor", "saveBackupConfig", "saveCurrentQuery", "saveGroup", "saveMCPConfig", "saveRecordEditor",
  "saveUser", "searchInGraph", "selectGraphElementByType", "selectNeighbors", "selectOrphanVertices",
  "sendApiRequest", "shortestPath", "showApiDetail", "showCreateGroupForm", "showCreateTokenForm",
  "showCreateUserForm", "showImportDatasetModal", "showRecordInGraph", "shutdownServer", "startClusterRefreshTimer",
  "startRunningQueriesRefreshTimer", "startServerRefreshTimer", "stepDownLeader", "studioHideById",
  "studioRemoveClosest", "studioToggleById", "supportDownloadBundle", "supportOpenGithub", "supportPreview",
  "supportRegister", "supportSendIssue", "supportUnregister", "supportVerify", "switchSidebarPanel",
  "toggleApiPlayground", "toggleApiRespHeaders", "toggleBackupScheduleFields", "toggleExportMenu",
  "toggleGraphAppearanceSection", "toggleReferenceSection", "toggleSelectAllHistory", "toggleServerInfoPopover",
  "toggleTieredRetention", "toggleTocSection", "toggleWorkareaFullscreen", "transferLeadershipPrompt",
  "triggerDatabaseBackup", "tsAddColumnRow", "tsAddDownsamplingPolicy", "tsAddDownsamplingTierRow",
  "tsDropDownsamplingPolicy", "tsDropType", "tsExecuteQuery", "tsGetLatest", "tsLoadSchema", "tsQueryEditorKeydown",
  "tsStartAutoRefresh", "tsTimeRangeChanged", "tsToggleChartType", "tsTogglePanel", "tsToggleQueryMode",
  "tsTypeChanged", "updateCluster", "updateDatabaseSetting", "updateMCPConnectionInfo", "updateServer",
  "updateServerSetting", "verifyDatabase"
]);

/**
 * Returns the attributes that bind action `name` to `event` (default "click") with `args`, starting with a space, ready
 * to concatenate into an opening tag whichever quote style it uses.
 */
function studioAction(name, args, event) {
  if (!STUDIO_ACTIONS.has(name)) throw new Error("Not a Studio action: " + name);
  var html = " data-on-" + (event || "click") + '="' + studioEscapeAttr(name) + '"';
  if (args != null && args.length > 0) html += ' data-args="' + studioEscapeAttr(JSON.stringify(args)) + '"';
  return html;
}

function studioEscapeAttr(text) {
  return String(text).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
}

function studioResolveActionArgs(el, event) {
  var raw = el.getAttribute("data-args");
  if (raw == null || raw === "") return [];
  var args;
  try {
    args = JSON.parse(raw);
  } catch (e) {
    console.error("Invalid data-args on", el, e);
    return null;
  }
  if (!Array.isArray(args)) return null;
  for (var i = 0; i < args.length; i++) {
    var a = args[i];
    if (a === null || typeof a !== "object" || Array.isArray(a) || Object.keys(a).length !== 1 || typeof a.$ !== "string") continue;
    if (a.$ === "this") args[i] = el;
    else if (a.$ === "event") args[i] = event;
    else if (a.$ === "value") args[i] = el.value;
    else if (a.$ === "checked") args[i] = el.checked;
  }
  return args;
}

function studioRunAction(el, attribute, event) {
  var name = el.getAttribute(attribute);
  if (!name) return;
  if (!STUDIO_ACTIONS.has(name)) {
    console.error("Refused a call to '" + name + "': not a Studio action");
    return;
  }
  var fn = window[name];
  if (typeof fn !== "function") {
    console.error("Studio action '" + name + "' is not defined");
    return;
  }
  var args = studioResolveActionArgs(el, event);
  if (args === null) return;
  // Called as an inline handler called it: `this` is not the element. A handler that needs it gets {"$":"this"}.
  fn.apply(window, args);
}

/**
 * Dispatches `event` to the data-on-* handlers of its target and of the target's ancestors, innermost first, the order
 * inline handlers ran in.
 */
function studioDispatchAction(event) {
  var type = event.type;
  var attribute = "data-on-" + type;
  for (var el = event.target; el && el.nodeType === 1; el = el.parentElement) {
    if (el.hasAttribute(attribute)) studioRunAction(el, attribute, event);

    if (type === "keydown" && event.key === "Enter") {
      if (el.hasAttribute("data-on-enter")) studioRunAction(el, "data-on-enter", event);
      var clickId = el.getAttribute("data-enter-click");
      if (clickId) {
        var toClick = document.getElementById(clickId);
        if (toClick) toClick.click();
      }
      var focusId = el.getAttribute("data-enter-focus");
      if (focusId) {
        var toFocus = document.getElementById(focusId);
        if (toFocus) toFocus.focus();
      }
    }

    if (type === "click") {
      if (el.hasAttribute("data-prevent-default")) event.preventDefault();
      if (el.hasAttribute("data-stop-propagation")) return;
    }
  }
}

// Unit tests load this file outside a browser, for studioAction()
if (typeof document !== "undefined")
  STUDIO_ACTION_EVENTS.forEach(function (type) {
    document.addEventListener(type, studioDispatchAction, true);
  });
