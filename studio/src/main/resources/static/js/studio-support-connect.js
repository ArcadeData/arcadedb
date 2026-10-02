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

// "Connect to ArcadeDB Portal" in the Support tab: one click, approve in the portal, done. The server asks the portal for a code
// (api/v1/server/support/connect), this page shows the code and opens the portal in a new tab, and once a workspace owner or admin
// approves it there the SERVER receives the workspace key, stores it and registers itself as an installation. The browser never
// sees the key or the device code: it only gets the code to compare, the portal address and the state, which it polls.
//
// The manual Client ID and key form stays under "Advanced / offline server" for a server that cannot reach a browser.

var SUPPORT_CONNECT_POLL_MS = 2000;
var SUPPORT_AUTOSYNC_MS = 24 * 3600 * 1000;

var supportConnect = null; // the last answer of GET /connect: {status, userCode, verifyUrl, expiresOn, workspaceName, registration, error}
var supportConnectBusy = false;
var supportConnectError = null; // a failure of starting, as supportError() returns it
var supportConnectTimer = null;
var supportConnectBlocked = false; // the browser did not open the new tab: the link is shown instead

/** The portal address the user is sent to, only when it is a plain http(s) URL: never a javascript: or data: one. */
function supportConnectSafeUrl(url) {
  return typeof url === "string" && /^https?:\/\/[^\s]+$/i.test(url) ? url : null;
}

/** The sentence for what happened to the installation when the server registered itself, or "" when there is nothing to say. */
function supportConnectRegistrationText(reg) {
  if (!reg) return "";
  if (reg.error) return "The server could not be added to your installations: " + (reg.message || reg.error) + " You can retry with Synchronize.";
  var what =
    reg.status === "created"
      ? "Registered as installation"
      : reg.status === "updated"
        ? "Your installation was completed"
        : "Already registered as installation";
  var text = what + (reg.name ? " " + reg.name : "") + ".";
  if (reg.differs && reg.differs.length === 1) text += " 1 field differs from the portal and was left as it is: " + reg.differs[0] + ".";
  else if (reg.differs && reg.differs.length > 1)
    text += " " + reg.differs.length + " fields differ from the portal and were left as they are: " + reg.differs.join(", ") + ".";
  return text;
}

/** The message of a connection that ended without connecting. */
function supportConnectEndText(status) {
  if (status.status === "denied") return "The connection was denied in the portal. Nothing was connected.";
  if (status.status === "expired") return "The code expired before it was approved. Start again.";
  if (status.status === "error") return (status.error && status.error.message) || "The connection failed.";
  return "";
}

/** The card: the button, or the code while waiting, or the outcome. All values are escaped: they come from the portal. */
function supportConnectHtml() {
  var st = supportConnect;
  var html = "";
  if (st && st.status === "pending") {
    var url = supportConnectSafeUrl(st.verifyUrl);
    html += '<div class="support-connect-wait">';
    html += '<div class="support-hint">Check that the portal shows this code, then approve:</div>';
    html += '<div class="support-connect-code" id="supportConnectCode">' + supportEsc(st.userCode) + "</div>";
    html += "<div>" + supportSpinner("Waiting for approval...") + "</div>";
    if (url)
      html +=
        '<div class="support-hint mt-1">' + (supportConnectBlocked ? "The new tab was blocked. " : "Did the portal not open? ") +
        '<a href="' + supportEsc(url) + '" target="_blank" rel="noopener noreferrer" id="supportConnectLink">Open the portal</a>.</div>';
    html += '<button class="btn btn-sm btn-outline-secondary mt-2" id="supportConnectCancelBtn">Cancel</button>';
    html += "</div>";
    return html;
  }
  if (st && st.status === "connected") {
    html += '<div class="alert alert-success py-2 mb-0" style="font-size: 0.86rem;" id="supportConnectDone"><i class="fa fa-circle-check"></i> Connected' +
      (st.workspaceName ? " to <b>" + supportEsc(st.workspaceName) + "</b>" : "") + ". " + supportEsc(supportConnectRegistrationText(st.registration)) + "</div>";
    return html;
  }
  var ended = st ? supportConnectEndText(st) : "";
  if (ended) html += '<div class="alert alert-warning py-2" style="font-size: 0.86rem;" id="supportConnectEnded"><i class="fa fa-triangle-exclamation"></i> ' + supportEsc(ended) + "</div>";
  if (supportConnectError) html += supportAlertHtml(supportConnectError);
  html +=
    '<button class="btn btn-primary" id="supportConnectBtn"' + (supportConnectBusy ? " disabled" : "") + '><i class="fa fa-plug"></i> Connect to ArcadeDB Portal</button>';
  html += '<div class="support-hint mt-2">Opens the customer portal in a new tab: sign in if needed, check the code and approve. The key goes straight to this server.</div>';
  return html;
}

function supportConnectRender() {
  $("#supportConnect").html(supportConnectHtml());
}

function supportConnectStopPolling() {
  if (supportConnectTimer) clearTimeout(supportConnectTimer);
  supportConnectTimer = null;
}

/** Applies one answer of GET /connect: keeps polling while pending, and on success shows the outcome and reloads the registration. */
function supportConnectApply(status) {
  var wasPending = supportConnect && supportConnect.status === "pending";
  supportConnect = status && status.status !== "none" ? status : null;
  if (supportConnect && supportConnect.status === "pending") {
    supportConnectSchedule();
  } else {
    supportConnectStopPolling();
    if (wasPending && supportConnect && supportConnect.status === "connected") {
      var reg = supportConnect.registration;
      supportInstallation = reg && reg.status ? reg : null;
      supportInstallationError = reg && reg.error ? { code: reg.error, message: reg.message || reg.error } : null;
      globalNotify("Support", "This server is connected to the support portal", "success");
      if (supportStatus && supportStatus.instanceId) supportAutoSyncMark(supportStatus.instanceId);
      loadSupport(true);
      supportIssues = null;
      return;
    }
  }
  supportConnectRender();
}

function supportConnectSchedule() {
  supportConnectStopPolling();
  supportConnectTimer = setTimeout(supportConnectPoll, SUPPORT_CONNECT_POLL_MS);
}

function supportConnectPoll() {
  supportApi("GET", "/connect")
    .done(function (text) {
      supportConnectApply(supportParse(text) || { status: "none" });
    })
    .fail(function () {
      // A moment without an answer is not the end of the wait: the server keeps polling the portal on its own
      if (supportConnect && supportConnect.status === "pending") supportConnectSchedule();
    });
}

/** Called when the unregistered overview is drawn: a wait that was started before a reload is picked up again. */
function supportConnectResume() {
  supportApi("GET", "/connect").done(function (text) {
    var st = supportParse(text);
    if (st && st.status === "pending") supportConnectApply(st);
  });
}

function supportConnectStart() {
  if (supportConnectBusy) return;
  // Opened now, inside the click, so the browser lets it through; pointed at the portal once the server has the code
  var tab = null;
  try {
    tab = window.open("", "_blank");
  } catch (e) {
    tab = null;
  }
  supportConnectBusy = true;
  supportConnectError = null;
  supportConnectBlocked = !tab;
  supportConnectRender();
  supportApi("POST", "/connect", {})
    .done(function (text) {
      var r = supportParse(text) || {};
      var url = supportConnectSafeUrl(r.verifyUrl);
      if (tab && url) {
        try {
          tab.opener = null;
          tab.location.href = url;
        } catch (e) {
          supportConnectBlocked = true;
        }
      } else if (tab) tab.close();
      supportConnectApply({ status: "pending", userCode: r.userCode, verifyUrl: r.verifyUrl, expiresOn: Date.now() + (r.expiresIn || 600) * 1000 });
    })
    .fail(function (jqXHR) {
      if (tab) tab.close();
      supportConnectError = supportError(jqXHR);
      supportConnectRender();
    })
    .always(function () {
      supportConnectBusy = false;
      if (!supportConnect || supportConnect.status !== "pending") supportConnectRender();
    });
}

function supportConnectCancel() {
  supportConnectStopPolling();
  supportApi("DELETE", "/connect").always(function () {
    supportConnect = null;
    supportConnectRender();
  });
}

/** Whether the daily re-registration of the installation is due; `last` is the epoch milliseconds of the previous one, or null. */
function supportAutoSyncDue(last, now) {
  return !(last > 0) || now - last >= SUPPORT_AUTOSYNC_MS || last > now;
}

/** Records that the installation was just registered (by the server itself after a connect), so the daily call is not made right away. */
function supportAutoSyncMark(instanceId) {
  try {
    window.localStorage.setItem("arcadedb.support.installationSyncedAt." + instanceId, String(Date.now()));
  } catch (e) {
    // no storage: nothing to remember
  }
}

/** At most once a day per server, when the Support tab loads a registration: completes blank fields of the installation, never overwrites. */
function supportAutoSync(instanceId) {
  var key = "arcadedb.support.installationSyncedAt." + instanceId;
  var last = null;
  try {
    last = parseInt(window.localStorage.getItem(key), 10);
  } catch (e) {
    return false; // no storage (a private window): no automatic call, the button is there
  }
  var now = Date.now();
  if (!supportAutoSyncDue(last, now)) return false;
  try {
    window.localStorage.setItem(key, String(now));
  } catch (e) {
    return false;
  }
  supportRegisterInstallation(true);
  return true;
}

$(document).on("click", "#supportConnectBtn", function () {
  supportConnectStart();
});

$(document).on("click", "#supportConnectCancelBtn", function () {
  supportConnectCancel();
});
