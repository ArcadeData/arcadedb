/*
 * Server > Support tab: the AI Assistant and the Support page. The Support page shows the registration with the ArcadeData
 * customer portal (collapsed to "Support Active" once registered) and the issues of the workspace; opening a support issue
 * (redacted logs and a diagnostics snapshot attached) and the public GitHub path for users without a plan are popups.
 *
 * The browser never sees the Client key: the server holds it and proxies the portal (api/v1/server/support/...).
 * Everything that comes from the portal is escaped before it is put in the page.
 */

var SUPPORT_API = "api/v1/server/support";
var SUPPORT_BUY_URL = "https://arcadedb.com/pricing.html";
var SUPPORT_GITHUB_NEW_ISSUE = "https://github.com/ArcadeData/arcadedb/issues/new";
var SUPPORT_GITHUB_MAX_URL = 7000;
var SUPPORT_SEVERITIES = [
  { id: "S1", label: "S1 - Critical: production is down" },
  { id: "S2", label: "S2 - High: major impact, no workaround" },
  { id: "S3", label: "S3 - Medium: limited impact or workaround available" },
  { id: "S4", label: "S4 - Low: question or minor issue" },
];
var SUPPORT_LOG_PRESETS = [
  { id: "", label: "Do not send logs" },
  { id: "10m", label: "Last 10 minutes" },
  { id: "30m", label: "Last 30 minutes" },
  { id: "1h", label: "Last 1 hour" },
  { id: "12h", label: "Last 12 hours" },
  { id: "24h", label: "Last 24 hours" },
  { id: "1w", label: "Last 1 week" },
  { id: "custom", label: "Custom" },
];

var supportStatus = null;
var supportTier = null; // the tier of the workspace plan as the portal reports it for the AI Assistant ('assistant', 'silver'...)
var supportLoaded = false;
var supportCurrentView = "ai";
var supportPreviews = {}; // prefix -> { id, expiresAt, description }
var supportIssues = null;
var supportIssuesFilter = "open";
var supportCurrentIssue = null;
var supportDetailsOpen = false; // the registration details under "Support Active"
var supportInstallation = null; // the portal's answer to "register this server": {status, name, filled, differs}
var supportInstallationError = null;
var supportInstallationBusy = false;

function supportEsc(value) {
  return escapeHtml(value == null ? "" : value);
}

// ------------------------------------------------------------------------------------------------ API

function supportApi(method, path, body) {
  var options = {
    type: method,
    url: SUPPORT_API + path,
    dataType: "text",
    beforeSend: function (xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    },
  };
  if (body !== undefined && body !== null) {
    options.data = JSON.stringify(body);
    options.contentType = "application/json";
  }
  return jQuery.ajax(options);
}

function supportParse(text) {
  if (text == null || text === "") return null;
  try {
    return JSON.parse(text);
  } catch (e) {
    return null;
  }
}

/** The error of a failed call as {code, message, status}: the server always answers {error, message}. */
function supportError(jqXHR) {
  var body = supportParse(jqXHR && jqXHR.responseText);
  var code = body && body.error ? body.error : "";
  var message = body && (body.message || body.detail) ? body.message || body.detail : "";
  if (!message) {
    if (jqXHR && jqXHR.status === 0) {
      code = "studio_offline";
      message = "Studio cannot reach this ArcadeDB server. Check the network connection.";
    } else message = "The request failed (HTTP " + (jqXHR ? jqXHR.status : "?") + ").";
  }
  return { code: code, message: message, status: jqXHR ? jqXHR.status : 0 };
}

function supportAlertHtml(err) {
  var icon = "fa-circle-exclamation";
  var kind = "danger";
  var extra = "";
  if (err.code === "support_not_active") {
    kind = "warning";
    icon = "fa-triangle-exclamation";
    extra =
      ' <a href="' +
      SUPPORT_BUY_URL +
      '" target="_blank" rel="noopener noreferrer">Get professional support</a> or use the "Report a public GitHub issue" button.';
  } else if (err.code === "invalid_key" || err.code === "client_mismatch" || err.code === "scope_denied") {
    extra = " Unregister and register again in the Support tab with a valid Client ID and key.";
  } else if (err.code === "portal_unreachable") {
    kind = "warning";
    icon = "fa-plug-circle-xmark";
  } else if (err.code === "rate_limited") {
    kind = "warning";
    icon = "fa-hourglass-half";
  }
  return (
    '<div class="alert alert-' +
    kind +
    ' py-2" style="font-size: 0.86rem;" role="alert"><i class="fa ' +
    icon +
    '"></i> ' +
    supportEsc(err.message) +
    extra +
    "</div>"
  );
}

function supportShowError(jqXHR, target) {
  $(target || "#supportAlert").html(supportAlertHtml(supportError(jqXHR)));
}

function supportClearAlert(target) {
  $(target || "#supportAlert").empty();
}

// ------------------------------------------------------------------------------------------------ helpers

function supportFormatBytes(bytes) {
  if (bytes == null) return "";
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  if (bytes < 1024 * 1024 * 1024) return (bytes / 1024 / 1024).toFixed(1) + " MB";
  return (bytes / 1024 / 1024 / 1024).toFixed(2) + " GB";
}

function supportFormatDate(value) {
  if (value == null || value === "") return "";
  var date = new Date(value);
  if (isNaN(date.getTime())) return String(value);
  return date.toLocaleString();
}

function supportSpinner(text) {
  return '<span class="spinner-border spinner-border-sm" role="status"></span> ' + supportEsc(text);
}

function supportNumber(value) {
  return value == null ? "" : Number(value).toLocaleString();
}

/**
 * The AI Assistant plan has no private support: only the AI Assistant, the news and the public GitHub issue. A UI rule, not a
 * security boundary (the portal is the authority); a tier Studio does not know of keeps the full page.
 */
function supportIsAssistantOnly() {
  return !!(supportStatus && supportStatus.registered && supportTier === "assistant");
}

/** Asks the server for the tier of the plan (it reads it from the portal and caches it), then calls {@code done}. */
function supportLoadTier(done) {
  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/ai/config",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      supportTier = data && data.portal && typeof data.portal.tier === "string" ? data.portal.tier : null;
    })
    .fail(function () {
      supportTier = null;
    })
    .always(done);
}

function supportIsEntitled() {
  return !!(supportStatus && supportStatus.registered && supportStatus.plan && supportStatus.plan.entitled);
}

function supportSlaFor(severity) {
  return supportStatus && supportStatus.sla && supportStatus.sla[severity] ? supportStatus.sla[severity] : null;
}

function supportBuyUrl() {
  return (supportStatus && supportSafeUrl(supportStatus.buyUrl)) || SUPPORT_BUY_URL;
}

/** Only links to the portal or to GitHub that start with https (or http on localhost, for tests) are made clickable. */
function supportSafeUrl(url) {
  if (typeof url !== "string") return null;
  if (/^https:\/\//i.test(url) || /^http:\/\/(localhost|127\.0\.0\.1)(:\d+)?(\/|$)/i.test(url)) return url;
  return null;
}

// ------------------------------------------------------------------------------------------------ load and navigation

function initSupport() {
  if (!supportLoaded) loadSupport(false);
}

function refreshSupport() {
  // The refresh next to the status reloads the status; the one in the Issues tab reloads the issues (renderSupportIssuesShell).
  loadSupport(true);
}

function loadSupport(refresh) {
  $("#supportLoading").show();
  supportClearAlert();
  return supportApi("GET", "?refresh=" + (refresh ? "true" : "false"))
    .done(function (text) {
      supportStatus = supportParse(text) || {};
      supportLoaded = true;
      $("#supportContent").show();
      // The page depends on the tier (the AI Assistant plan has no private issues), so the tier is known before it is drawn
      var drawn = function () {
        renderSupportAll();
        // The installation is completed at most once a day, whoever opens the tab; it only fills blank fields
        if (supportStatus.registered && !supportStatus.portalError && supportStatus.instanceId) supportAutoSync(supportStatus.instanceId);
      };
      if (supportStatus.registered) supportLoadTier(drawn);
      else {
        supportTier = null;
        drawn();
      }
    })
    .fail(function (jqXHR) {
      supportLoaded = false;
      $("#supportContent").hide();
      supportShowError(jqXHR);
    })
    .always(function () {
      $("#supportLoading").hide();
    });
}

function renderSupportAll() {
  renderSupportOverview();
  renderSupportIssuesShell();
  showSupportView(supportCurrentView);
}

function showSupportView(view) {
  // The popups are opened from the Issues tab: their old tabs are gone, their links now open them
  if (view === "issue" || view === "public") {
    showSupportView("issues");
    if (view === "issue") supportOpenIssueModal();
    else supportOpenPublicModal();
    return;
  }
  if (view === "overview") view = "issues";
  if (view !== "ai") view = "issues";

  supportCurrentView = view;
  $("#supportNav .nav-link").removeClass("active");
  $('#supportNav .nav-link[data-support-view="' + view + '"]').addClass("active");

  // The status of the registration is on top of both tabs: load it whichever tab opens first.
  if (!supportLoaded) initSupport();

  if (view === "ai") {
    $("#supportViewIssues").hide();
    $("#supportViewAi").show();
    if (typeof initAi === "function") initAi();
    supportFitAi();
    setTimeout(supportFitAi, 150);
    return;
  }

  $("#supportViewAi").hide();
  $("#supportViewIssues").show();
  if (supportLoaded && supportStatus && supportStatus.registered && supportIssues == null && !supportCurrentIssue) loadSupportIssues();
}

/**
 * The chat fills what is left of the window below the status card and the tabs, so its prompt is always on screen (sticky at
 * the bottom) whatever the height of the card above it (the plan details expand, the connect panel is taller, a narrow window).
 */
function supportFitAi() {
  var layout = document.getElementById("aiLayout");
  if (!layout || !$("#supportViewAi").is(":visible") || !$("#aiActivePanel").is(":visible")) return;
  var top = layout.getBoundingClientRect().top;
  layout.style.height = Math.max(360, Math.floor(window.innerHeight - top - 12)) + "px";
}

$(window).on("resize", supportFitAi);
if (typeof ResizeObserver !== "undefined")
  $(function () {
    var section = document.getElementById("supportSection");
    if (section) new ResizeObserver(supportFitAi).observe(section);
  });

/** What opening the Support tab does: the view that was last shown (the AI Assistant the first time). */
function initSupportTab() {
  showSupportView(supportCurrentView);
}

$(document).on("click", "#supportNav .nav-link", function (e) {
  e.preventDefault();
  supportClearAlert();
  showSupportView($(this).attr("data-support-view"));
});

$(document).on("click", ".support-goto", function (e) {
  e.preventDefault();
  supportClearAlert();
  showSupportView($(this).attr("data-support-view"));
});

$(document).on("click", ".support-open-issue", function (e) {
  e.preventDefault();
  supportOpenIssueModal();
});

$(document).on("click", ".support-open-public", function (e) {
  e.preventDefault();
  supportOpenPublicModal();
});

function supportShowModal(id) {
  var el = document.getElementById(id);
  if (el.parentNode !== document.body) document.body.appendChild(el);
  bootstrap.Modal.getOrCreateInstance(el).show();
}

function supportHideModal(id) {
  var el = document.getElementById(id);
  var modal = el ? bootstrap.Modal.getInstance(el) : null;
  if (modal) modal.hide();
}

function supportOpenIssueModal() {
  if (!supportIsEntitled() || supportIsAssistantOnly()) return;
  renderSupportIssueForm();
  supportShowModal("supportIssueModal");
}

function supportOpenPublicModal() {
  renderSupportPublic();
  supportShowModal("supportPublicModal");
}

// ------------------------------------------------------------------------------------------------ overview / registration

function renderSupportOverview() {
  var s = supportStatus || {};
  var html = "";

  if (!s.registered) {
    html += '<div class="support-card support-register">';
    html += '<div class="support-register-head"><div class="support-reg-icon"><i class="fa fa-headset"></i></div><div>';
    html += '<div class="support-reg-plan">Professional support</div>';
    html +=
      '<div class="support-hint">Guaranteed first-response times, with the logs and a diagnostics snapshot of this server attached to every issue.</div>';
    html += "</div></div>";
    html +=
      '<p style="font-size: 0.88rem;">Connect this server to the <b>workspace</b> of your company in the ArcadeDB customer portal to open issues from here and to follow the replies. The server is also added to your installations.</p>';
    html += '<div id="supportConnect">' + supportConnectHtml() + "</div>";
    html += '<details class="support-advanced mt-3">';
    html +=
      '<summary style="font-size: 0.84rem; cursor: pointer;">Advanced / offline server: paste a Client ID and key</summary>';
    html +=
      '<p class="mt-2" style="font-size: 0.86rem;">For a server that cannot open a browser: create a key in the customer portal (Studio keys) and paste its <b>Client ID</b> and <b>Client key</b>.</p>';
    html += '<div class="row g-2 align-items-end">';
    html +=
      '<div class="col-md-4"><label class="form-label mb-1" for="supportClientId" style="font-size: 0.82rem;">Client ID</label>' +
      '<input type="text" class="form-control" id="supportClientId" autocomplete="off" spellcheck="false" maxlength="100"></div>';
    html +=
      '<div class="col-md-5"><label class="form-label mb-1" for="supportClientKey" style="font-size: 0.82rem;">Client key</label>' +
      '<input type="password" class="form-control" id="supportClientKey" autocomplete="new-password" spellcheck="false" maxlength="256" placeholder="wsk_..."></div>';
    html +=
      '<div class="col-md-3 d-flex gap-2"><button class="btn btn-sm btn-outline-secondary" id="supportVerifyBtn" onclick="supportVerify()">Verify</button>' +
      '<button class="btn btn-sm btn-primary" id="supportRegisterBtn" onclick="supportRegister()"><i class="fa fa-link"></i> Register</button></div>';
    html += "</div>";
    html += '<div id="supportVerifyResult" class="mt-2"></div>';
    html += "</details>";
    if (s.canWriteConfig === false)
      html +=
        '<div class="alert alert-warning py-2 mt-2" style="font-size: 0.84rem;"><i class="fa fa-triangle-exclamation"></i> The configuration directory of this server is not writable. ' +
        "Set the settings <code>arcadedb.support.clientId</code> and <code>arcadedb.support.clientKey</code> (for example from a Kubernetes secret) instead of registering here.</div>";
    html +=
      '<div class="support-hint mt-2">The key is stored on the server only (file <code>support.json</code>, owner-only permissions) and is never shown again: only its last four characters.</div>';
    html += "</div>";

    html += '<div class="support-card support-public-card">';
    html += '<div class="d-flex align-items-center gap-3 flex-wrap">';
    html += '<div style="flex: 1; min-width: 260px;"><h6 class="mb-1">Do not have a support plan?</h6>';
    html +=
      '<div class="support-hint"><a href="' +
      SUPPORT_BUY_URL +
      '" target="_blank" rel="noopener noreferrer"><i class="fa fa-arrow-up-right-from-square"></i> Get professional support</a>. You can still report a problem in public: the same redacted logs and diagnostics are produced for you to attach to a GitHub issue. Nothing is uploaded automatically.</div></div>';
    html += '<button class="btn btn-outline-secondary support-open-public"><i class="fab fa-github"></i> Report a public GitHub issue</button>';
    html += "</div></div>";
  } else html += supportStatusPanelHtml(s);

  $("#supportViewOverview").html(html);
  // A connection started before the page was reloaded is still waiting on the server
  if (!s.registered && !supportConnect) supportConnectResume();
}

/** The registered state: one line ("Support Active") that expands to the details nobody needs after the first minute. */
function supportStatusPanelHtml(s) {
  var plan = s.plan;
  var assistantOnly = supportIsAssistantOnly() && !s.portalError;
  var kind = s.portalError ? "bad" : assistantOnly || (plan && plan.entitled) ? "ok" : "warn";
  var icon = { ok: "fa-circle-check", warn: "fa-triangle-exclamation", bad: "fa-plug-circle-xmark" }[kind];
  var title = { ok: "Support Active", warn: "Support not active", bad: "Cannot reach the support portal" }[kind];
  if (assistantOnly) title = "AI Assistant plan";
  var sub = [];
  if (assistantOnly) {
    if (s.workspaceName) sub.push(supportEsc(s.workspaceName));
    sub.push("The AI Assistant is included. Private support issues need a professional support plan.");
  } else if (kind === "ok") {
    if (plan.label) sub.push(supportEsc(plan.label) + (plan.units ? " &times; " + supportEsc(plan.units) : ""));
    if (s.workspaceName) sub.push(supportEsc(s.workspaceName));
    sub.push(plan.endsOn ? "until " + supportEsc(supportFormatDate(plan.endsOn)) : "no end date");
  } else if (kind === "bad") sub.push(supportEsc(s.portalError.message));
  else sub.push("Your plan expired or there is none: issues cannot be opened in the portal.");

  var html = '<div class="support-status ' + kind + '">';
  html += '<div class="support-status-bar">';
  html +=
    '<button type="button" class="support-status-main" id="supportStatusToggle" aria-expanded="' +
    (supportDetailsOpen ? "true" : "false") +
    '" aria-controls="supportStatusDetails" title="Show the registration details">';
  html += '<span class="support-status-icon"><i class="fa ' + icon + '"></i></span>';
  html += '<span class="support-status-text"><span class="support-status-title">' + title + '</span><span class="support-hint">' + sub.join(" &middot; ") + "</span></span>";
  html += '<span class="support-status-chevron' + (supportDetailsOpen ? " open" : "") + '"><i class="fa fa-chevron-down"></i></span>';
  html += "</button>";
  html += '<div class="support-status-actions">';
  if (kind === "warn" || assistantOnly)
    html +=
      '<a class="btn btn-sm btn-outline-secondary" href="' +
      supportEsc(supportBuyUrl()) +
      '" target="_blank" rel="noopener noreferrer">' +
      (assistantOnly ? "Upgrade for private issues" : "Get professional support") +
      "</a>";
  html += '<button type="button" class="btn btn-sm btn-outline-secondary" id="supportRefreshBtn" title="Check the plan again"><i class="fa fa-sync"></i></button>';
  html += "</div></div>";

  html += '<div class="support-status-details" id="supportStatusDetails"' + (supportDetailsOpen ? "" : ' style="display: none;"') + ">";
  html += '<dl class="support-grid">';
  html += "<div><dt>Workspace</dt><dd>" + supportEsc(s.workspaceName || "") + "</dd></div>";
  if (plan)
    html +=
      "<div><dt>Plan</dt><dd>" +
      supportEsc(plan.label || "") +
      (plan.units ? " &times; " + supportEsc(plan.units) : "") +
      ' <span class="support-badge ' +
      (plan.entitled ? "ok" : "bad") +
      '">' +
      (plan.entitled ? "Active" : "Not active") +
      "</span></dd></div>";
  html +=
    "<div><dt>Client key</dt><dd class='support-mono'>" +
    supportEsc(s.keyHint) +
    (s.keyLabel ? ' <span class="support-hint">(' + supportEsc(s.keyLabel) + ")</span>" : "") +
    "</dd></div>";
  html += "<div><dt>Client ID</dt><dd class='support-mono'>" + supportEsc(s.clientId) + "</dd></div>";
  html += "<div><dt>Instance ID</dt><dd class='support-mono'>" + supportEsc(s.instanceId) + "</dd></div>";
  html += "<div><dt>Portal</dt><dd class='support-mono'>" + supportEsc(s.portalUrl) + "</dd></div>";
  if (s.scopes && s.scopes.length) html += "<div><dt>Scopes</dt><dd>" + supportEsc(s.scopes.join(", ")) + "</dd></div>";
  html += "</dl>";

  html += '<div class="support-installation" id="supportInstallation">' + supportInstallationHtml() + "</div>";

  if (s.sla) {
    html += '<div class="support-sla"><dt class="support-sla-title">First-response times</dt><div class="support-sla-row">';
    SUPPORT_SEVERITIES.forEach(function (sev) {
      html += '<div class="support-sla-cell"><b>' + sev.id + "</b><span>" + supportEsc(s.sla[sev.id] || "") + "</span></div>";
    });
    html += "</div>";
    if (s.sla.coverage) html += '<div class="support-hint mt-1">Coverage: ' + supportEsc(s.sla.coverage) + "</div>";
    html += "</div>";
  }

  html += '<div class="support-reg-foot">';
  if (s.fromSettings) html += '<span class="support-hint">Registered through the server settings: change or remove them in the server configuration.</span>';
  else html += '<span class="support-hint">The key is stored on this server only.</span>';
  html +=
    '<button class="btn btn-sm btn-outline-danger ms-auto" id="supportUnregisterBtn" onclick="supportUnregister()"' +
    (s.fromSettings ? ' disabled title="Configured through the settings arcadedb.support.clientId and arcadedb.support.clientKey"' : "") +
    '><i class="fa fa-unlink"></i> Unregister</button>';
  html += "</div></div></div>";
  return html;
}

$(document).on("click", "#supportStatusToggle", function () {
  supportDetailsOpen = !supportDetailsOpen;
  $(this).attr("aria-expanded", supportDetailsOpen ? "true" : "false");
  $(".support-status-chevron").toggleClass("open", supportDetailsOpen);
  $("#supportStatusDetails").slideToggle(150);
});

$(document).on("click", "#supportRefreshBtn", function () {
  loadSupport(true);
  supportIssues = null;
});

// ------------------------------------------------------------------------------------------------ the server as an installation

/** What the portal said about this server's record in the installations, and the button to do it (again). */
function supportInstallationHtml() {
  var html = '<div class="support-installation-text">';
  var r = supportInstallation;
  if (supportInstallationBusy) html += supportSpinner("Registering this server in the portal...");
  else if (supportInstallationError)
    html += '<i class="fa fa-circle-exclamation text-danger"></i> ' + supportEsc(supportInstallationError.message);
  else if (r && r.status) {
    var what =
      r.status === "created"
        ? "Added to your installations in the portal"
        : r.status === "updated"
          ? "Your installation in the portal was completed (" + supportEsc((r.filled || []).join(", ")) + ")"
          : "Already in your installations in the portal";
    html += '<i class="fa fa-circle-check support-ok"></i> ' + what + (r.name ? ": <b>" + supportEsc(r.name) + "</b>" : "") + ".";
    if (r.differs && r.differs.length)
      html += ' <span class="support-hint">Differs from what the portal has, left as it is: ' + supportEsc(r.differs.join(", ")) + ".</span>";
  } else html += '<span class="support-hint">Synchronize adds this server to the installations of your workspace in the portal, or completes its blank fields.</span>';
  html += "</div>";
  html +=
    '<button class="btn btn-sm btn-outline-primary" id="supportSyncBtn"' +
    (supportInstallationBusy ? " disabled" : "") +
    '><i class="fa fa-cloud-arrow-up"></i> ' +
    "Synchronize" +
    "</button>";
  return html;
}

function supportRenderInstallation() {
  $("#supportInstallation").html(supportInstallationHtml());
}

/** @param auto true right after the key was registered: only a new installation is announced, a failure stays in the details */
function supportRegisterInstallation(auto) {
  if (supportInstallationBusy) return;
  supportInstallationBusy = true;
  supportInstallationError = null;
  supportRenderInstallation();
  return supportApi("POST", "/installation")
    .done(function (text) {
      supportInstallation = supportParse(text) || {};
      if (supportInstallation.status === "created")
        globalNotify("Support", "This server was added to your installations in the portal", "success");
      else if (!auto) globalNotify("Support", "This server is up to date in your installations in the portal", "success");
    })
    .fail(function (jqXHR) {
      supportInstallationError = supportError(jqXHR);
      if (!auto) globalNotify("Support", supportInstallationError.message, "warning");
    })
    .always(function () {
      supportInstallationBusy = false;
      supportRenderInstallation();
    });
}

$(document).on("click", "#supportSyncBtn", function () {
  supportRegisterInstallation(false);
});

function supportCredentials() {
  return { clientId: $("#supportClientId").val().trim(), key: $("#supportClientKey").val().trim() };
}

function supportVerify() {
  var c = supportCredentials();
  if (!c.clientId || !c.key) {
    $("#supportVerifyResult").html(supportAlertHtml({ code: "bad_request", message: "Enter the Client ID and the Client key." }));
    return;
  }
  $("#supportVerifyBtn, #supportRegisterBtn").prop("disabled", true);
  $("#supportVerifyResult").html('<span class="support-hint">' + supportSpinner("Asking the portal...") + "</span>");
  supportApi("POST", "/register", { clientId: c.clientId, key: c.key, verifyOnly: true })
    .done(function (text) {
      var r = supportParse(text) || {};
      var plan = r.plan;
      var html =
        '<div class="alert alert-success py-2" style="font-size: 0.86rem;"><i class="fa fa-circle-check"></i> The key is valid for the workspace <b>' +
        supportEsc(r.workspaceName) +
        "</b>" +
        (r.keyLabel ? " (" + supportEsc(r.keyLabel) + ")" : "") +
        ". ";
      if (plan) html += plan.entitled ? "Plan: " + supportEsc(plan.label || "") + " (active)." : "The support plan is not active.";
      html += " Press Register to store it.</div>";
      $("#supportVerifyResult").html(html);
    })
    .fail(function (jqXHR) {
      supportShowError(jqXHR, "#supportVerifyResult");
    })
    .always(function () {
      $("#supportVerifyBtn, #supportRegisterBtn").prop("disabled", false);
    });
}

function supportRegister() {
  var c = supportCredentials();
  if (!c.clientId || !c.key) {
    $("#supportVerifyResult").html(supportAlertHtml({ code: "bad_request", message: "Enter the Client ID and the Client key." }));
    return;
  }
  $("#supportVerifyBtn, #supportRegisterBtn").prop("disabled", true);
  $("#supportVerifyResult").html('<span class="support-hint">' + supportSpinner("Verifying and registering...") + "</span>");
  supportApi("POST", "/register", { clientId: c.clientId, key: c.key })
    .done(function (text) {
      $("#supportClientKey").val("");
      supportStatus = supportParse(text) || {};
      supportIssues = null;
      supportInstallation = null;
      supportInstallationError = null;
      renderSupportAll();
      globalNotify("Support", "This server is registered with the support portal", "success");
      // Once registered, that's it: the server also becomes an installation of the workspace, without another click
      supportRegisterInstallation(true);
    })
    .fail(function (jqXHR) {
      supportShowError(jqXHR, "#supportVerifyResult");
      $("#supportVerifyBtn, #supportRegisterBtn").prop("disabled", false);
    });
}

function supportUnregister() {
  globalConfirm(
    "Unregister",
    "Remove the Client ID and key from this server? You will not be able to open support issues from Studio until you register again. Existing issues stay in the portal.",
    "warning",
    function () {
      supportApi("DELETE", "/register")
        .done(function () {
          supportIssues = null;
          supportCurrentIssue = null;
          supportInstallation = null;
          supportInstallationError = null;
          supportDetailsOpen = false;
          loadSupport(true);
          globalNotify("Support", "The registration was removed", "success");
        })
        .fail(function (jqXHR) {
          supportShowError(jqXHR);
        });
    }
  );
}

// ------------------------------------------------------------------------------------------------ collect form (logs, diagnostics, preview)

function supportCollectFormHtml(prefix, withTitle) {
  var html = "";
  html += '<div class="support-collect" data-prefix="' + prefix + '">';
  html += '<div class="row g-2 mb-2">';
  html +=
    '<div class="col-md-4"><label class="form-label mb-1" style="font-size: 0.82rem;" for="' +
    prefix +
    'Logs">Logs</label><select class="form-select" id="' +
    prefix +
    'Logs">';
  SUPPORT_LOG_PRESETS.forEach(function (p) {
    html += '<option value="' + p.id + '">' + p.label + "</option>";
  });
  html += "</select></div>";
  html +=
    '<div class="col-md-4 ' +
    prefix +
    'Custom" style="display: none;"><label class="form-label mb-1" style="font-size: 0.82rem;" for="' +
    prefix +
    'From">From</label><input type="datetime-local" step="1" class="form-control" id="' +
    prefix +
    'From"></div>';
  html +=
    '<div class="col-md-4 ' +
    prefix +
    'Custom" style="display: none;"><label class="form-label mb-1" style="font-size: 0.82rem;" for="' +
    prefix +
    'To">To</label><input type="datetime-local" step="1" class="form-control" id="' +
    prefix +
    'To"></div>';
  html += "</div>";
  html += '<div class="support-hint mb-2" id="' + prefix + 'ZoneNote">' + supportZoneNote() + "</div>";
  html +=
    '<div class="form-check"><input class="form-check-input" type="checkbox" id="' +
    prefix +
    'Diag" checked><label class="form-check-label" style="font-size: 0.86rem;" for="' +
    prefix +
    'Diag">Diagnostics snapshot (version, OS, JVM, non-default settings with secrets masked, plugins, database names and sizes, metrics)</label></div>';
  html +=
    '<div class="form-check mb-2"><input class="form-check-input" type="checkbox" id="' +
    prefix +
    'Threads"><label class="form-check-label" style="font-size: 0.86rem;" for="' +
    prefix +
    'Threads">Thread dump (all threads with their stacks: useful for hangs and slowness; it briefly pauses the JVM, longer on a busy server with many threads)</label></div>';
  html +=
    '<button class="btn btn-sm btn-outline-primary" id="' +
    prefix +
    'PreviewBtn" onclick="supportPreview(\'' +
    prefix +
    '\')"><i class="fa fa-paperclip"></i> Prepare the attachments</button>';
  html += '<div id="' + prefix + 'Preview" class="mt-3"></div>';
  html += "</div>";
  return html;
}

function supportZoneNote() {
  var z = supportStatus && supportStatus.logTimeZone ? supportStatus.logTimeZone : null;
  var browserOffset = -new Date().getTimezoneOffset();
  var sign = browserOffset >= 0 ? "+" : "-";
  var abs = Math.abs(browserOffset);
  var browser = "UTC" + sign + String(Math.floor(abs / 60)).padStart(2, "0") + ":" + String(abs % 60).padStart(2, "0");
  if (!z) return "";
  return (
    "Log lines carry no time zone: this server writes them in <b>" +
    supportEsc(z.id) +
    " (UTC" +
    supportEsc(z.offset === "Z" ? "" : z.offset) +
    ")</b>. The window is taken in your browser time (" +
    browser +
    ") and converted to it."
  );
}

function supportWireCollect(prefix, onChange) {
  $("#" + prefix + "Logs").on("change", function () {
    $("." + prefix + "Custom").toggle($(this).val() === "custom");
    if ($(this).val() === "custom" && !$("#" + prefix + "From").val()) {
      var now = new Date();
      var from = new Date(now.getTime() - 3600 * 1000);
      $("#" + prefix + "To").val(supportLocalInput(now));
      $("#" + prefix + "From").val(supportLocalInput(from));
    }
    supportInvalidatePreview(prefix);
    if (onChange) onChange();
  });
  $("#" + prefix + "From, #" + prefix + "To, #" + prefix + "Diag, #" + prefix + "Threads").on("change", function () {
    supportInvalidatePreview(prefix);
    if (onChange) onChange();
  });
}

function supportLocalInput(date) {
  var pad = function (n) {
    return String(n).padStart(2, "0");
  };
  return (
    date.getFullYear() + "-" + pad(date.getMonth() + 1) + "-" + pad(date.getDate()) + "T" + pad(date.getHours()) + ":" + pad(date.getMinutes()) + ":" + pad(date.getSeconds())
  );
}

/** The request of the preview from the form, or null (with a message shown) when it is not valid. */
function supportCollectOptions(prefix) {
  var logs = $("#" + prefix + "Logs").val();
  var options = {
    includeLogs: logs !== "",
    includeDiagnostics: $("#" + prefix + "Diag").is(":checked"),
    includeThreads: $("#" + prefix + "Threads").is(":checked"),
  };
  if (logs === "custom") {
    var from = $("#" + prefix + "From").val();
    var to = $("#" + prefix + "To").val();
    if (!from || !to) {
      $("#" + prefix + "Preview").html(supportAlertHtml({ code: "bad_request", message: "Choose the start and the end of the window." }));
      return null;
    }
    var fromDate = new Date(from);
    var toDate = new Date(to);
    if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime()) || toDate <= fromDate) {
      $("#" + prefix + "Preview").html(supportAlertHtml({ code: "bad_request", message: "The end of the window must be after its start." }));
      return null;
    }
    // The browser's local time, sent as an instant: the server converts it to the zone of the log
    options.window = { from: fromDate.toISOString(), to: toDate.toISOString() };
  } else if (logs !== "") options.window = { preset: logs };
  if (!options.includeLogs && !options.includeDiagnostics && !options.includeThreads) return { none: true };
  return options;
}

function supportInvalidatePreview(prefix) {
  delete supportPreviews[prefix];
  $("#" + prefix + "Preview").empty();
  supportUpdateSendState(prefix);
}

function supportPreview(prefix) {
  var options = supportCollectOptions(prefix);
  if (options == null) return;
  if (options.none) {
    $("#" + prefix + "Preview").html(
      '<div class="support-hint">Nothing is selected to be attached: the issue is sent with your text only.</div>'
    );
    delete supportPreviews[prefix];
    supportUpdateSendState(prefix);
    return;
  }
  var btn = $("#" + prefix + "PreviewBtn");
  btn.prop("disabled", true).html(supportSpinner("Collecting and masking..."));
  $("#" + prefix + "Preview").empty();
  supportApi("POST", "/preview", options)
    .done(function (text) {
      var description = supportParse(text) || {};
      supportPreviews[prefix] = { id: description.previewId, description: description };
      $("#" + prefix + "Preview").html(supportPreviewHtml(description, prefix));
      supportUpdateSendState(prefix);
    })
    .fail(function (jqXHR) {
      delete supportPreviews[prefix];
      supportShowError(jqXHR, "#" + prefix + "Preview");
      supportUpdateSendState(prefix);
    })
    .always(function () {
      btn.prop("disabled", false).html('<i class="fa fa-paperclip"></i> Prepare the attachments');
    });
}

function supportPreviewHtml(d, prefix) {
  var html = "";
  var files = d.files || [];
  if (files.length) {
    html += '<table class="support-table"><thead><tr><th>File</th><th class="support-num">Size</th><th class="support-num">Lines</th><th class="support-num">Secrets masked</th></tr></thead><tbody>';
    var totalSize = 0;
    files.forEach(function (f) {
      totalSize += f.sizeBytes || 0;
      html +=
        '<tr><td class="support-mono">' +
        supportEsc(f.name) +
        '</td><td class="support-num">' +
        supportEsc(supportFormatBytes(f.sizeBytes)) +
        '</td><td class="support-num">' +
        supportEsc(f.lines != null ? supportNumber(f.lines) : "") +
        '</td><td class="support-num">' +
        supportRedactionBadge(f.redactions) +
        "</td></tr>";
      (f.entries || []).forEach(function (e) {
        html +=
          '<tr class="support-sub"><td class="support-mono">' +
          supportEsc(e.name) +
          '</td><td class="support-num">' +
          supportEsc(supportFormatBytes(e.sizeBytes)) +
          ' <span title="uncompressed">(raw)</span></td><td class="support-num">' +
          supportEsc(supportNumber(e.lines)) +
          '</td><td class="support-num">' +
          supportRedactionBadge(e.redactions) +
          "</td></tr>";
      });
    });
    html += "</tbody></table>";
    html += '<div class="support-hint mt-1">Total to send: ' + supportEsc(supportFormatBytes(totalSize)) + ".</div>";
  } else html += '<div class="support-hint">The preview holds no file.</div>';

  if (d.window)
    html +=
      '<div class="support-hint mt-1">Window: ' +
      supportEsc(supportFormatDate(d.window.from)) +
      " - " +
      supportEsc(supportFormatDate(d.window.to)) +
      " (your browser time). Log time zone: " +
      supportEsc(d.logTimeZone ? d.logTimeZone.id : "") +
      ".</div>";
  (d.warnings || []).forEach(function (w) {
    html +=
      '<div class="alert alert-warning py-1 mt-2 mb-0" style="font-size: 0.84rem;"><i class="fa fa-triangle-exclamation"></i> ' +
      supportEsc(w) +
      "</div>";
  });
  // The public GitHub path: the text that goes into the prefilled issue is shown here, before the click, exactly as it will be
  if (prefix === "spPublic" && d.githubSummary)
    html +=
      '<div class="mt-2"><div class="support-hint"><b>This text is added to the public GitHub issue</b> (the logs are not: attach the downloaded bundle by hand, after reading it):</div>' +
      '<pre class="support-mono" style="white-space: pre-wrap; font-size: 0.8rem; max-height: 14rem; overflow: auto; margin: 0.25rem 0 0;">' +
      supportEsc(d.githubSummary) +
      "</pre></div>";
  html +=
    '<div class="support-hint mt-2"><i class="fa fa-lock"></i> Exactly these files are what is sent or downloaded. Passwords, tokens, keys, credentials in URLs and PEM blocks are masked before they are written; ' +
    "the masking cannot recognise free-text secrets, query text, host names, IP addresses or user names in the logs, so review the content. " +
    "The diagnostics also carry the server name, the cluster name, the database names and sizes (never their data) and the JVM arguments. The preview expires at " +
    supportEsc(supportFormatDate(d.expiresAt)) +
    ".</div>";
  return html;
}

function supportRedactionBadge(n) {
  if (n == null) return "";
  return n > 0 ? '<span class="support-badge warn">' + supportEsc(n) + "</span>" : '<span class="support-badge">0</span>';
}

function supportUpdateSendState(prefix) {
  var needsPreview = supportCollectNeedsPreview(prefix);
  var ready = !needsPreview || !!supportPreviews[prefix];
  if (prefix === "spIssue") {
    $("#spIssueSendBtn").prop("disabled", !ready);
    $("#spIssueSendHint").text(ready ? "" : "Prepare the attachments first: you review exactly what leaves the server.");
  } else if (prefix === "spPublic") {
    var has = !!supportPreviews[prefix];
    $("#spPublicDownloadBtn").prop("disabled", !has);
    $("#spPublicGithubBtn").prop("disabled", !has && needsPreview);
  } else if (prefix === "spAttach") {
    $("#spAttachSendBtn").prop("disabled", !supportPreviews[prefix]);
  }
}

function supportCollectNeedsPreview(prefix) {
  return $("#" + prefix + "Logs").val() !== "" || $("#" + prefix + "Diag").is(":checked") || $("#" + prefix + "Threads").is(":checked");
}

// ------------------------------------------------------------------------------------------------ open an issue

function renderSupportIssueForm() {
  var s = supportStatus || {};
  var html = "";
  delete supportPreviews.spIssue;
  supportShotsClear("spIssue");

  if (!s.registered || !supportIsEntitled()) {
    html +=
      '<div class="alert alert-warning py-2" style="font-size: 0.86rem;"><i class="fa fa-triangle-exclamation"></i> ' +
      (s.portalError ? supportEsc(s.portalError.message) : "Your support plan is not active (it expired or there is none), so issues cannot be opened in the portal.") +
      ' <a href="' +
      supportEsc(supportBuyUrl()) +
      '" target="_blank" rel="noopener noreferrer">Get professional support</a> or report a public GitHub issue.</div>';
    $("#supportIssueModalBody").html(html);
    return;
  }

  html += '<p class="support-hint mb-3">Describe the problem and attach the logs and a diagnostics snapshot of this server. Secrets are masked before anything leaves the server, and you review exactly what is sent.</p>';
  html += '<div id="spIssueResult"></div>';
  html += '<div id="spIssueCard" data-shots="spIssue">';
  html +=
    '<div class="mb-2"><label class="form-label mb-1" style="font-size: 0.82rem;" for="spIssueTitle">Title</label><input type="text" class="form-control" id="spIssueTitle" maxlength="200" autocomplete="off"></div>';
  html +=
    '<div class="mb-2"><label class="form-label mb-1" style="font-size: 0.82rem;" for="spIssueBody">Description</label><textarea class="form-control" id="spIssueBody" rows="6" maxlength="20000" placeholder="What happened, what you expected, what changed recently..."></textarea></div>';
  html += '<div class="row g-2 mb-3">';
  html += '<div class="col-md-8"><label class="form-label mb-1" style="font-size: 0.82rem;" for="spIssueSeverity">Severity</label><select class="form-select" id="spIssueSeverity">';
  SUPPORT_SEVERITIES.forEach(function (sev) {
    var sla = supportSlaFor(sev.id);
    html += '<option value="' + sev.id + '"' + (sev.id === "S3" ? " selected" : "") + ">" + sev.label + (sla ? " (first response: " + supportEsc(sla) + ")" : "") + "</option>";
  });
  html += "</select></div>";
  html +=
    '<div class="col-md-4"><label class="form-label mb-1" style="font-size: 0.82rem;" for="spIssueKind">Kind</label><select class="form-select" id="spIssueKind"><option value="">Not specified</option><option value="bug">Bug</option><option value="question">Question</option><option value="performance">Performance</option><option value="other">Other</option></select></div>';
  html += "</div>";
  html += supportShotsHtml("spIssue");
  html += supportCollectFormHtml("spIssue");
  html += '<hr style="border-color: var(--border-light);">';
  html +=
    '<button class="btn btn-primary" id="spIssueSendBtn" onclick="supportSendIssue()"><i class="fa fa-paper-plane"></i> Send to ArcadeData support</button> <span class="support-hint" id="spIssueSendHint"></span>';
  html += "</div>";
  $("#supportIssueModalBody").html(html);
  supportWireCollect("spIssue");
  supportUpdateSendState("spIssue");
}

function supportSendIssue() {
  var title = $("#spIssueTitle").val().trim();
  if (!title) {
    $("#spIssueResult").html(supportAlertHtml({ code: "bad_request", message: "The title is required." }));
    return;
  }
  var preview = supportPreviews.spIssue;
  if (supportCollectNeedsPreview("spIssue") && !preview) {
    $("#spIssueResult").html(supportAlertHtml({ code: "bad_request", message: "Prepare the attachments first." }));
    return;
  }
  var body = {
    title: title,
    body: $("#spIssueBody").val(),
    severity: $("#spIssueSeverity").val(),
  };
  if ($("#spIssueKind").val()) body.kind = $("#spIssueKind").val();
  if (preview) body.previewId = preview.id;
  var shots = supportShotsIds("spIssue");
  if (shots.length) body.screenshots = shots;

  $("#spIssueSendBtn").prop("disabled", true).html(supportSpinner("Sending..."));
  supportClearAlert("#spIssueResult");
  supportApi("POST", "/issues", body)
    .done(function (text) {
      var r = supportParse(text) || {};
      supportShotsClear("spIssue", true);
      supportHideModal("supportIssueModal");
      supportIssues = null;
      globalNotify("Support", "Issue #" + r.number + " was sent to ArcadeData support", "success");
      if (supportCurrentView === "issues") loadSupportIssues();
    })
    .fail(function (jqXHR) {
      supportShowError(jqXHR, "#spIssueResult");
      // a refusal of the plan or of the key does not lose the preview; an expired preview does
      if (supportError(jqXHR).code === "preview_not_found") supportInvalidatePreview("spIssue");
    })
    .always(function () {
      $("#spIssueSendBtn").html('<i class="fa fa-paper-plane"></i> Send to ArcadeData support');
      supportUpdateSendState("spIssue");
    });
}

// ------------------------------------------------------------------------------------------------ my issues

function renderSupportIssuesShell() {
  var s = supportStatus || {};
  if (!s.registered) {
    $("#supportViewIssues").empty();
    return;
  }
  if (supportIsAssistantOnly()) {
    $("#supportViewIssues").html(supportAssistantOnlyHtml());
    return;
  }
  var entitled = supportIsEntitled();
  var html = '<div class="support-actions">';
  html += '<h5 class="support-section-title"><i class="fa fa-life-ring"></i> Support issues</h5>';
  html += '<div class="ms-auto d-flex gap-2 flex-wrap">';
  html += '<button class="btn btn-outline-secondary support-open-public"><i class="fab fa-github"></i> Report a public GitHub issue</button>';
  html +=
    '<button class="btn btn-primary support-open-issue"' +
    (entitled ? "" : ' disabled title="Your support plan is not active"') +
    '><i class="fa fa-plus"></i> Open a new issue</button>';
  html += "</div></div>";
  html += '<div id="spIssuesAlert"></div>';
  html += '<div id="spIssuesList"></div><div id="spIssueDetail" style="display: none;"></div>';
  $("#supportViewIssues").html(html);
  if (supportIssues != null && !supportCurrentIssue) renderSupportIssuesList();
  else if (supportIssues == null && !supportCurrentIssue && supportCurrentView === "issues") loadSupportIssues();
}

/** What the AI Assistant plan shows where the issues would be: the public path and the way up. */
function supportAssistantOnlyHtml() {
  var html = '<div class="support-actions">';
  html += '<h5 class="support-section-title"><i class="fa fa-life-ring"></i> Support issues</h5>';
  html += '<div class="ms-auto d-flex gap-2 flex-wrap">';
  html += '<button class="btn btn-outline-secondary support-open-public"><i class="fab fa-github"></i> Report a public GitHub issue</button>';
  html += "</div></div>";
  html += '<div class="support-empty"><i class="fa fa-lock"></i>';
  html += "<div><b>Private support issues are not part of the AI Assistant plan</b></div>";
  html +=
    '<div class="support-hint">Upgrade to a professional support plan to open private issues with response times, and to follow the replies here. ' +
    '<a href="' +
    SUPPORT_BUY_URL +
    '" target="_blank" rel="noopener noreferrer"><i class="fa fa-arrow-up-right-from-square"></i> See the plans</a>. ' +
    "Until then you can report a problem in public on GitHub.</div></div>";
  return html;
}

function loadSupportIssues() {
  if (supportIsAssistantOnly()) return;
  supportCurrentIssue = null;
  $("#spIssueDetail").hide().empty();
  $("#spIssuesList").show().html('<div class="text-center py-3 support-hint">' + supportSpinner("Loading the issues...") + "</div>");
  supportClearAlert("#spIssuesAlert");
  supportApi("GET", "/issues?status=" + encodeURIComponent(supportIssuesFilter))
    .done(function (text) {
      var data = supportParse(text);
      supportIssues = Array.isArray(data) ? data : data && Array.isArray(data.result) ? data.result : [];
      renderSupportIssuesList();
    })
    .fail(function (jqXHR) {
      supportIssues = null;
      $("#spIssuesList").empty();
      supportShowError(jqXHR, "#spIssuesAlert");
    });
}

function supportIssueStatus(issue) {
  return issue.status || issue.workflowStatus || (issue.open === false ? "closed" : "open");
}

function supportIssueIsClosed(issue) {
  var status = String(supportIssueStatus(issue)).toLowerCase();
  return status === "closed" || status === "done" || status === "resolved" || issue.open === false || !!issue.closedOn;
}

function supportStatusBadge(issue) {
  var status = supportIssueStatus(issue);
  return '<span class="support-badge ' + (supportIssueIsClosed(issue) ? "" : "ok") + '">' + supportEsc(status) + "</span>";
}

function renderSupportIssuesList() {
  var html = '<div class="d-flex align-items-center gap-2 mb-2">';
  html += '<label class="support-hint mb-0" for="spIssuesFilter">Show</label>';
  html += '<select class="form-select form-select-sm" id="spIssuesFilter" style="width: auto;">';
  ["open", "closed", "all"].forEach(function (f) {
    html += '<option value="' + f + '"' + (f === supportIssuesFilter ? " selected" : "") + ">" + f + "</option>";
  });
  html += "</select>";
  html += '<button class="btn btn-sm btn-outline-secondary" id="spIssuesRefresh" title="Reload the issues"><i class="fa fa-sync"></i></button>';
  html += "</div>";
  if (!supportIssues || !supportIssues.length) {
    html += '<div class="support-empty"><i class="fa fa-inbox"></i>';
    html += "<div><b>No " + (supportIssuesFilter === "all" ? "" : supportEsc(supportIssuesFilter) + " ") + "issues</b></div>";
    html += '<div class="support-hint">' + (supportIsEntitled() ? "Open a new issue when something needs ArcadeData support." : "Issues you open with ArcadeData support will show here.") + "</div></div>";
  } else {
    html += '<div class="support-issue-list">';
    supportIssues.forEach(function (issue) {
      var sev = issue.severity || "";
      html +=
        '<div class="support-issue-row support-row-click" role="button" tabindex="0" data-number="' +
        supportEsc(issue.number) +
        '"><span class="support-issue-num">#' +
        supportEsc(issue.number) +
        '</span><span class="support-issue-title">' +
        supportEsc(issue.title) +
        "</span>" +
        (sev ? '<span class="support-badge sev-' + supportEsc(sev) + '">' + supportEsc(sev) + "</span>" : "") +
        supportStatusBadge(issue) +
        '<span class="support-hint support-issue-date">' +
        supportEsc(supportFormatDate(issue.updatedOn || issue.updatedAt || issue.modifiedOn || issue.createdOn)) +
        "</span></div>";
    });
    html += "</div>";
  }
  $("#spIssuesList").show().html(html);
}

$(document).on("change", "#spIssuesFilter", function () {
  supportIssuesFilter = $(this).val();
  loadSupportIssues();
});

$(document).on("click", "#spIssuesRefresh", function () {
  loadSupportIssues();
});

$(document).on("click keydown", "#spIssuesList .support-row-click", function (e) {
  if (e.type === "keydown" && e.key !== "Enter" && e.key !== " ") return;
  e.preventDefault();
  loadSupportIssue($(this).attr("data-number"));
});

function loadSupportIssue(number) {
  if (supportIsAssistantOnly()) return;
  $("#spIssuesList").hide();
  $("#spIssueDetail")
    .show()
    .html('<div class="text-center py-3 support-hint">' + supportSpinner("Loading the issue...") + "</div>");
  supportClearAlert("#spIssuesAlert");
  supportApi("GET", "/issues/" + encodeURIComponent(number))
    .done(function (text) {
      supportCurrentIssue = supportParse(text) || { number: number };
      if (supportCurrentIssue.number == null) supportCurrentIssue.number = number;
      renderSupportIssueDetail();
    })
    .fail(function (jqXHR) {
      supportCurrentIssue = null;
      $("#spIssueDetail").hide().empty();
      $("#spIssuesList").show();
      supportShowError(jqXHR, "#spIssuesAlert");
    });
}

function supportTimeline(issue) {
  var entries = issue.timeline || issue.entries || issue.events || issue.comments || [];
  return Array.isArray(entries) ? entries : [];
}

function renderSupportIssueDetail() {
  var issue = supportCurrentIssue;
  var closed = supportIssueIsClosed(issue);
  delete supportPreviews.spAttach;
  supportShotsClear("spReply");

  var html = '<div class="mb-2"><a href="#" id="spBackToIssues"><i class="fa fa-arrow-left"></i> All issues</a></div>';
  html += '<div class="support-card">';
  html += '<div class="d-flex justify-content-between align-items-start">';
  html +=
    "<h6>#" +
    supportEsc(issue.number) +
    " " +
    supportEsc(issue.title) +
    "</h6>" +
    '<div class="d-flex gap-2"><button class="btn btn-sm btn-outline-secondary" id="spIssueRefresh"><i class="fa fa-sync"></i> Refresh</button>' +
    '<button class="btn btn-sm btn-outline-' +
    (closed ? "success" : "danger") +
    '" id="spIssueToggle">' +
    (closed ? '<i class="fa fa-rotate-left"></i> Reopen' : '<i class="fa fa-circle-check"></i> Close') +
    "</button></div>";
  html += "</div>";
  html += '<div class="support-hint mb-3">' + supportStatusBadge(issue) + " " + (issue.severity ? "Severity " + supportEsc(issue.severity) : "");
  var updated = issue.updatedOn || issue.updatedAt || issue.modifiedOn;
  if (updated) html += " - updated " + supportEsc(supportFormatDate(updated));
  var link = supportSafeUrl(issue.url);
  if (link) html += ' - <a href="' + supportEsc(link) + '" target="_blank" rel="noopener noreferrer">Open in the portal</a>';
  html += "</div>";
  if (issue.body || issue.description)
    html += '<div class="support-timeline-entry client"><div class="support-meta">Description</div><div class="support-body support-md">' + supportMarkdownHtml(issue.body || issue.description) + "</div></div>";

  var timeline = supportTimeline(issue);
  if (!timeline.length) html += '<div class="support-hint mb-2">No reply yet.</div>';
  timeline.forEach(function (entry) {
    var side = entry.side === "client" || entry.side === "studio" ? "client" : "";
    var text = entry.body || entry.text || entry.message || entry.description || "";
    var who = entry.authorLabel || entry.author || (side ? "You" : "ArcadeData support");
    var kind = entry.type && entry.type !== "comment" ? " - " + entry.type : "";
    html +=
      '<div class="support-timeline-entry ' +
      side +
      '"><div class="support-meta"><b>' +
      supportEsc(who) +
      "</b>" +
      (entry.authorRole ? '<span class="support-role">' + supportEsc(entry.authorRole) + "</span>" : "") +
      (kind ? "<span>" + supportEsc(kind) + "</span>" : "") +
      "<span>" +
      supportEsc(supportFormatDate(entry.createdOn || entry.createdAt || entry.at || entry.date)) +
      '</span></div><div class="support-body support-md">' +
      supportMarkdownHtml(text) +
      "</div>" +
      supportRequestsHtml(entry) +
      "</div>";
  });
  html += "</div>";

  html += supportRequestsBarHtml(issue);
  html += '<div class="support-card" data-shots="spReply"><h6>Reply</h6>';
  html += '<div id="spReplyAlert"></div>';
  html += '<textarea class="form-control mb-2" id="spReplyBody" rows="4" maxlength="20000" placeholder="Write a reply to ArcadeData support... (you can paste a screenshot here)"></textarea>';
  html += supportShotsHtml("spReply");
  html += '<button class="btn btn-sm btn-primary" id="spReplySend"><i class="fa fa-reply"></i> Send reply</button>';
  html += "</div>";

  html += '<div class="support-card"><h6>Send more logs or diagnostics to this issue</h6>';
  html += '<div id="spAttachAlert"></div>';
  html += supportCollectFormHtml("spAttach");
  html += '<hr style="border-color: var(--border-light);"><button class="btn btn-sm btn-primary" id="spAttachSendBtn" disabled><i class="fa fa-paperclip"></i> Send to issue #' + supportEsc(issue.number) + "</button>";
  html += "</div>";

  $("#spIssueDetail").show().html(html);
  $("#spIssuesList").hide();
  supportRequestsInit(issue);
  supportWireCollect("spAttach");
  $("#spAttachSendBtn").on("click", supportSendAttachment);
  supportUpdateSendState("spAttach");
}

$(document).on("click", "#spBackToIssues", function (e) {
  e.preventDefault();
  supportCurrentIssue = null;
  $("#spIssueDetail").hide().empty();
  loadSupportIssues();
});

$(document).on("click", "#spIssueRefresh", function () {
  if (supportCurrentIssue) loadSupportIssue(supportCurrentIssue.number);
});

$(document).on("click", "#spReplySend", function () {
  var text = $("#spReplyBody").val().trim();
  if (!text) {
    $("#spReplyAlert").html(supportAlertHtml({ code: "bad_request", message: "The reply is empty." }));
    return;
  }
  var btn = $(this);
  btn.prop("disabled", true).html(supportSpinner("Sending..."));
  var reply = { body: text };
  var replyShots = supportShotsIds("spReply");
  if (replyShots.length) reply.screenshots = replyShots;
  supportApi("POST", "/issues/" + encodeURIComponent(supportCurrentIssue.number) + "/comments", reply)
    .done(function () {
      supportShotsClear("spReply", true);
      loadSupportIssue(supportCurrentIssue.number);
    })
    .fail(function (jqXHR) {
      supportShowError(jqXHR, "#spReplyAlert");
      btn.prop("disabled", false).html('<i class="fa fa-reply"></i> Send reply');
    });
});

$(document).on("click", "#spIssueToggle", function () {
  var closed = supportIssueIsClosed(supportCurrentIssue);
  var btn = $(this);
  btn.prop("disabled", true);
  supportApi("PUT", "/issues/" + encodeURIComponent(supportCurrentIssue.number), { open: closed })
    .done(function () {
      supportIssues = null;
      loadSupportIssue(supportCurrentIssue.number);
    })
    .fail(function (jqXHR) {
      supportShowError(jqXHR, "#spIssuesAlert");
      btn.prop("disabled", false);
    });
});

function supportSendAttachment() {
  var preview = supportPreviews.spAttach;
  if (!preview || !supportCurrentIssue) return;
  var btn = $("#spAttachSendBtn");
  btn.prop("disabled", true).html(supportSpinner("Sending..."));
  supportClearAlert("#spAttachAlert");
  supportApi("POST", "/issues/" + encodeURIComponent(supportCurrentIssue.number) + "/attachments", { previewId: preview.id })
    .done(function () {
      globalNotify("Support", "The files were added to issue #" + supportCurrentIssue.number, "success");
      loadSupportIssue(supportCurrentIssue.number);
    })
    .fail(function (jqXHR) {
      supportShowError(jqXHR, "#spAttachAlert");
      if (supportError(jqXHR).code === "preview_not_found") supportInvalidatePreview("spAttach");
      btn.html('<i class="fa fa-paperclip"></i> Send to issue #' + supportEsc(supportCurrentIssue.number));
      supportUpdateSendState("spAttach");
    });
}

// ------------------------------------------------------------------------------------------------ public GitHub path

function renderSupportPublic() {
  var html = "<div>";
  html +=
    '<p style="font-size: 0.88rem;">Build the same redacted bundle, download it, and open a prefilled public issue on GitHub. ' +
    "<b>Nothing is uploaded anywhere automatically.</b> GitHub issues are public: the issue holds your text and a short environment summary, without logs. " +
    "Attach the downloaded zip to the issue by hand, after reading it.</p>";
  html += '<div id="spPublicAlert"></div>';
  html +=
    '<div class="mb-2"><label class="form-label mb-1" style="font-size: 0.82rem;" for="spPublicTitle">Title</label><input type="text" class="form-control" id="spPublicTitle" maxlength="200" autocomplete="off"></div>';
  html +=
    '<div class="mb-3"><label class="form-label mb-1" style="font-size: 0.82rem;" for="spPublicBody">Description</label><textarea class="form-control" id="spPublicBody" rows="6" placeholder="What happened, what you expected, steps to reproduce..."></textarea></div>';
  html += supportCollectFormHtml("spPublic");
  html += '<hr style="border-color: var(--border-light);">';
  html +=
    '<button class="btn btn-outline-primary" id="spPublicDownloadBtn" disabled onclick="supportDownloadBundle()"><i class="fa fa-download"></i> Download redacted bundle</button> ';
  html +=
    '<button class="btn btn-primary" id="spPublicGithubBtn" onclick="supportOpenGithub()"><i class="fab fa-github"></i> Open GitHub issue</button>';
  html +=
    '<div class="support-hint mt-2">The GitHub issue opens in a new tab with the title, your description and the environment summary filled in (the text is limited in length). Remember to attach the zip: drag it into the issue description.</div>';
  html += "</div>";
  $("#supportPublicModalBody").html(html);
  delete supportPreviews.spPublic;
  supportWireCollect("spPublic");
  supportUpdateSendState("spPublic");
}

function supportDownloadBundle() {
  var preview = supportPreviews.spPublic;
  if (!preview) return;
  var btn = $("#spPublicDownloadBtn");
  btn.prop("disabled", true).html(supportSpinner("Preparing..."));
  supportClearAlert("#spPublicAlert");
  // fetch, not $.ajax: the answer is binary
  fetch(SUPPORT_API + "/bundle", {
    method: "POST",
    headers: { Authorization: globalCredentials, "Content-Type": "application/json" },
    body: JSON.stringify({ previewId: preview.id }),
  })
    .then(function (response) {
      if (!response.ok)
        return response.text().then(function (text) {
          throw { responseText: text, status: response.status };
        });
      var disposition = response.headers.get("Content-Disposition") || "";
      var match = /filename="([^"]+)"/.exec(disposition);
      var name = match ? match[1] : "arcadedb-support-bundle.zip";
      return response.blob().then(function (blob) {
        return { blob: blob, name: name };
      });
    })
    .then(function (file) {
      var url = URL.createObjectURL(file.blob);
      var a = document.createElement("a");
      a.href = url;
      a.download = file.name;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      setTimeout(function () {
        URL.revokeObjectURL(url);
      }, 1000);
      globalNotify("Support", "The redacted bundle was downloaded: attach it to the GitHub issue by hand", "success");
    })
    .catch(function (err) {
      var e = err && err.responseText !== undefined ? err : { responseText: "", status: 0 };
      supportShowError(e, "#spPublicAlert");
      if (supportError(e).code === "preview_not_found") supportInvalidatePreview("spPublic");
    })
    .then(function () {
      btn.html('<i class="fa fa-download"></i> Download redacted bundle');
      supportUpdateSendState("spPublic");
    });
}

function supportBuildGithubUrl(title, body, summary) {
  var footer = "\n\n_The redacted logs and diagnostics bundle is attached to this issue by hand._";
  var text = (body || "").trim();
  if (summary) text += (text ? "\n\n" : "") + summary.trim();
  text += footer;
  var build = function (t) {
    return SUPPORT_GITHUB_NEW_ISSUE + "?title=" + encodeURIComponent(title) + "&body=" + encodeURIComponent(t);
  };
  var url = build(text);
  // Long bodies are cut (keeping the footer) until the URL fits what GitHub accepts
  while (url.length > SUPPORT_GITHUB_MAX_URL && text.length > footer.length + 40) {
    var keep = Math.floor((text.length - footer.length) * 0.9);
    text = text.substring(0, keep).trimEnd() + "\n...(shortened)" + footer;
    url = build(text);
  }
  return url;
}

function supportOpenGithub() {
  var title = $("#spPublicTitle").val().trim();
  if (!title) {
    $("#spPublicAlert").html(supportAlertHtml({ code: "bad_request", message: "The title is required." }));
    return;
  }
  var preview = supportPreviews.spPublic;
  if (supportCollectNeedsPreview("spPublic") && !preview) {
    $("#spPublicAlert").html(supportAlertHtml({ code: "bad_request", message: "Prepare the attachments first, so the summary of your environment can be added." }));
    return;
  }
  var summary = preview && preview.description ? preview.description.githubSummary : "";
  var url = supportBuildGithubUrl(title, $("#spPublicBody").val(), summary);
  window.open(url, "_blank", "noopener,noreferrer");
  if (preview) globalNotify("GitHub", "Download the redacted bundle and attach the zip to the issue by hand", "info");
}
