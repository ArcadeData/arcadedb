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

// Support requests in Studio (the Support page). ArcadeData support can ask, in a reply, for the result of a read-only
// query. Each request shows here as a card with the exact statement; NOTHING runs until the user clicks Run. The result
// is shown as a table the user can mask (a cell, a row or a column) before anything leaves the browser, and is sent back
// with a second click.
//
// Safety does not rest on this file: the statement is run through the ordinary query endpoint (`api/v1/query`), which the
// server refuses for anything that is not idempotent, in SQL and in OpenCypher. The check below (the same rules as the
// customer portal and the platform, one shared vector file) only gives a friendly refusal before the call, and Studio
// re-validates here because the text came from outside.
//
// This version runs a request on the server Studio is connected to, never on the other nodes of a cluster.

// ---------------------------------------------------------------------------------------------- the rules

var SUPPORT_REQ_LANGUAGES = ["sql", "opencypher"];
var SUPPORT_REQ_MAX_STATEMENT = 2000;
var SUPPORT_REQ_SQL_START = ["SELECT", "MATCH", "TRAVERSE"];
var SUPPORT_REQ_CYPHER_START = ["MATCH", "OPTIONAL", "WITH", "UNWIND", "RETURN"];
var SUPPORT_REQ_SQL_DENIED = [
  "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TRUNCATE", "REBUILD", "CHECK", "IMPORT", "EXPORT", "BACKUP", "GRANT",
  "REVOKE", "SLEEP", "JS", "FUNCTION", "EXPLAIN", "PROFILE", "LET", "BEGIN", "COMMIT", "ROLLBACK", "MOVE", "CONSOLE", "SCRIPT",
];
var SUPPORT_REQ_CYPHER_DENIED = [
  "CREATE", "MERGE", "SET", "DELETE", "DETACH", "REMOVE", "DROP", "CALL", "LOAD", "FOREACH", "USING", "START", "GRANT", "REVOKE",
  "ALTER",
];
// eslint-disable-next-line no-control-regex
var SUPPORT_REQ_CONTROL = /[\u0000-\u0008\u000B-\u001F\u007F]/;

function supportReqRefuse(code, message) {
  return { ok: false, code: code, message: message };
}

/** The statement with string literals and quoted names blanked out, so keyword checks see only code; null when a quote is open. */
function supportReqCodeOf(statement) {
  var out = "";
  for (var i = 0; i < statement.length; i++) {
    var c = statement[i];
    if (c === "'" || c === '"' || c === "`") {
      var j = i + 1;
      for (; j < statement.length; j++) {
        if (statement[j] === "\\") j++;
        else if (statement[j] === c) {
          if (statement[j + 1] === c) j++;
          else break;
        }
      }
      if (j >= statement.length) return null;
      out += c + c;
      i = j;
    } else out += c;
  }
  return out;
}

/** {ok: true, statement} (trimmed, no trailing semicolon) or {ok: false, code, message}. */
function supportReqValidate(language, statement) {
  if (SUPPORT_REQ_LANGUAGES.indexOf(language) < 0) return supportReqRefuse("bad_language", "language must be sql or opencypher");
  if (typeof statement !== "string") return supportReqRefuse("bad_statement", "the statement must be text");
  var text = statement.trim();
  if (!text) return supportReqRefuse("bad_statement", "the statement is empty");
  if (text.length > SUPPORT_REQ_MAX_STATEMENT) return supportReqRefuse("bad_statement", "the statement is too long");
  if (SUPPORT_REQ_CONTROL.test(text)) return supportReqRefuse("bad_statement", "the statement contains control characters");

  var code = supportReqCodeOf(text);
  if (code === null) return supportReqRefuse("bad_statement", "a quote is not closed");
  if (code.indexOf("--") >= 0 || code.indexOf("/*") >= 0 || code.indexOf("*/") >= 0 || (language === "opencypher" && code.indexOf("//") >= 0))
    return supportReqRefuse("comments_not_allowed", "comments are not allowed");
  var body = code.replace(/;\s*$/, "").trim();
  if (body.indexOf(";") >= 0) return supportReqRefuse("multiple_statements", "exactly one statement is allowed");
  if (!body) return supportReqRefuse("bad_statement", "the statement is empty");

  var words = body.toUpperCase().match(/[A-Z_][A-Z0-9_]*/g) || [];
  if ((language === "sql" ? SUPPORT_REQ_SQL_START : SUPPORT_REQ_CYPHER_START).indexOf(words[0]) < 0)
    return supportReqRefuse("not_read_only", "it is not a read-only query");
  var denied = (language === "sql" ? SUPPORT_REQ_SQL_DENIED : SUPPORT_REQ_CYPHER_DENIED).filter(function (w) {
    return words.indexOf(w) >= 0;
  })[0];
  if (denied) return supportReqRefuse("not_read_only", denied + " is not allowed in a request");
  if (language === "opencypher" && words.indexOf("RETURN") < 0) return supportReqRefuse("not_read_only", "a query must return something");
  return { ok: true, statement: text.replace(/;\s*$/, "").trim() };
}

// ---------------------------------------------------------------------------------------------- results and masks

var SUPPORT_REQ_MAX_ROWS = 1000;
var SUPPORT_REQ_MASK = "••••";
var SUPPORT_REQ_SENSITIVE_COLUMN = /password|passwd|secret|token|key|email|phone|iban|card/i;
var SUPPORT_REQ_REMEMBER = "support.requests.masked";

/** One cell as text-safe data: null, number, boolean or string. A nested value becomes its JSON text. */
function supportReqCell(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return isFinite(value) ? value : String(value);
  if (typeof value === "boolean" || typeof value === "string") return value;
  try {
    return JSON.stringify(value);
  } catch (e) {
    return String(value);
  }
}

function supportReqType(values) {
  for (var i = 0; i < values.length; i++) {
    var v = values[i];
    if (v === null) continue;
    if (typeof v === "number") return Number.isInteger(v) ? "LONG" : "DOUBLE";
    if (typeof v === "boolean") return "BOOLEAN";
    return "STRING";
  }
  return "STRING";
}

/**
 * The records the query endpoint answered as {columns: [{name, type}], rows: [[...]], truncated}. Properties that start with "@"
 * (record ids, types) are not data and are left out. `node` (this server's name) becomes the first column when the request
 * asked for every node, so staff read the answer the same way a cluster answer will read.
 */
function supportReqTable(records, nodeName, limit) {
  var cap = limit || SUPPORT_REQ_MAX_ROWS;
  var list = Array.isArray(records) ? records : [];
  var truncated = list.length > cap;
  if (truncated) list = list.slice(0, cap);
  var names = [];
  list.forEach(function (r) {
    if (r === null || typeof r !== "object") return;
    Object.keys(r).forEach(function (k) {
      if (k.charAt(0) !== "@" && names.indexOf(k) < 0) names.push(k);
    });
  });
  var rows = list.map(function (r) {
    return names.map(function (n) {
      return supportReqCell(r !== null && typeof r === "object" ? r[n] : null);
    });
  });
  var columns = names.map(function (n, c) {
    return { name: n, type: supportReqType(rows.map(function (row) { return row[c]; })) };
  });
  if (nodeName) {
    columns.unshift({ name: "node", type: "STRING" });
    rows = rows.map(function (row) { return [nodeName].concat(row); });
  }
  return { columns: columns, rows: rows, truncated: truncated };
}

/** A new mask state for a table: the columns whose names look sensitive, or that the user masked before, start masked. */
function supportReqInitialMask(table, remembered) {
  var columns = {};
  table.columns.forEach(function (c) {
    var before = remembered && Object.prototype.hasOwnProperty.call(remembered, c.name) ? remembered[c.name] : null;
    if (before === true || (before === null && SUPPORT_REQ_SENSITIVE_COLUMN.test(c.name))) columns[c.name] = true;
  });
  return { columns: columns, cells: {}, mode: "redact" };
}

function supportReqIsMasked(mask, table, r, c) {
  return !!mask.columns[table.columns[c].name] || !!mask.cells[r + ":" + c];
}

function supportReqToggleCell(mask, r, c) {
  var key = r + ":" + c;
  if (mask.cells[key]) delete mask.cells[key];
  else mask.cells[key] = true;
}

function supportReqToggleColumn(mask, table, c) {
  var name = table.columns[c].name;
  if (mask.columns[name]) delete mask.columns[name];
  else mask.columns[name] = true;
}

/** Masks every cell of a row, or clears them when the whole row is already masked individually. */
function supportReqToggleRow(mask, table, r) {
  var all = true;
  for (var c = 0; c < table.columns.length; c++) if (!mask.cells[r + ":" + c]) all = false;
  for (var k = 0; k < table.columns.length; k++) {
    if (all) delete mask.cells[r + ":" + k];
    else mask.cells[r + ":" + k] = true;
  }
}

function supportReqMaskedCount(mask, table) {
  var count = 0;
  for (var r = 0; r < table.rows.length; r++)
    for (var c = 0; c < table.columns.length; c++) if (supportReqIsMasked(mask, table, r, c)) count++;
  return count;
}

function supportReqHex(bytes) {
  var out = "";
  for (var i = 0; i < bytes.length; i++) out += ("0" + bytes[i].toString(16)).slice(-2);
  return out;
}

/**
 * The answer to send: the masked values are replaced HERE, in the browser, so the original never leaves it. `redact` replaces
 * with dots; `hash` with a short HMAC under a random salt used for this answer only, so equal values stay equal inside it and
 * nothing can be matched across answers. Resolves to the body of the portal call.
 */
function supportReqBuildAnswer(table, mask, durationMs) {
  var subtle = typeof globalThis !== "undefined" && globalThis.crypto && globalThis.crypto.subtle;
  var hashing = mask.mode === "hash" && subtle;
  var pending = Promise.resolve(null);
  var key = null;
  if (hashing) {
    var salt = new Uint8Array(16);
    globalThis.crypto.getRandomValues(salt);
    pending = subtle.importKey("raw", salt, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]).then(function (k) {
      key = k;
    });
  }
  return pending.then(function () {
    var encoder = new TextEncoder();
    var cells = [];
    var columns = [];
    table.columns.forEach(function (col) {
      if (mask.columns[col.name]) columns.push(col.name);
    });
    var jobs = [];
    var rows = table.rows.map(function (row, r) {
      return row.map(function (value, c) {
        var byColumn = !!mask.columns[table.columns[c].name];
        var byCell = !!mask.cells[r + ":" + c];
        if (!byColumn && !byCell) return value;
        if (byCell && !byColumn) cells.push([r, c]);
        if (!hashing) return SUPPORT_REQ_MASK;
        var slot = { r: r, c: c };
        jobs.push(
          subtle.sign("HMAC", key, encoder.encode(value === null ? "\u0000null" : String(value))).then(function (sig) {
            slot.value = "#" + supportReqHex(new Uint8Array(sig)).slice(0, 12);
          }),
        );
        return slot;
      });
    });
    return Promise.all(jobs).then(function () {
      rows = rows.map(function (row) {
        return row.map(function (v) {
          return v !== null && typeof v === "object" ? v.value : v;
        });
      });
      var body = {
        outcome: "answered",
        result: {
          columns: table.columns,
          rows: rows,
          truncated: !!table.truncated,
          masked: { cells: cells, columns: columns, mode: mask.mode },
        },
      };
      if (durationMs > 0) body.durationMs = Math.round(durationMs);
      return body;
    });
  });
}

// ---------------------------------------------------------------------------------------------- state

var supportReqState = {};
var supportReqDatabases = null;
var supportReqNode = "";

function supportReqRemembered() {
  try {
    return JSON.parse(globalStorageLoad(SUPPORT_REQ_REMEMBER) || "{}") || {};
  } catch (e) {
    return {};
  }
}

function supportReqRemember(table, mask) {
  var memory = supportReqRemembered();
  table.columns.forEach(function (c) {
    memory[c.name] = !!mask.columns[c.name];
  });
  try {
    globalStorageSave(SUPPORT_REQ_REMEMBER, JSON.stringify(memory));
  } catch (e) {
    // the memory is a convenience, never a requirement
  }
}

function supportReqStateFor(request) {
  if (!supportReqState[request.id]) supportReqState[request.id] = { request: request, status: "idle", database: request.database || "" };
  return supportReqState[request.id];
}

function supportReqOpen(entry) {
  return (entry.requests || []).filter(function (r) {
    return r.status === "open";
  });
}

function supportReqAllOpen(issue) {
  var out = [];
  supportTimeline(issue).forEach(function (e) {
    supportReqOpen(e).forEach(function (r) { out.push(r); });
  });
  return out;
}

// ---------------------------------------------------------------------------------------------- rendering

var SUPPORT_REQ_STATUS = { open: ["warning", "Waiting for you"], answered: ["success", "Answered"], declined: ["secondary", "Declined"], failed: ["danger", "Failed"] };

function supportRequestsHtml(entry) {
  var list = entry.requests;
  if (!Array.isArray(list) || !list.length) return "";
  var html = '<div class="support-requests">';
  list.forEach(function (request) {
    if (!request || !request.id) return;
    var status = SUPPORT_REQ_STATUS[request.status] || SUPPORT_REQ_STATUS.open;
    html += '<div class="support-request" data-rq="' + supportEsc(request.id) + '">';
    html += '<div class="d-flex flex-wrap align-items-center gap-2 mb-1"><b>' + supportEsc(request.label) + "</b>";
    html += '<span class="badge text-bg-' + status[0] + '">' + status[1] + "</span>";
    html += '<span class="badge text-bg-light border">' + (request.language === "opencypher" ? "OpenCypher" : "SQL") + "</span>";
    if (request.nodes === "all") html += '<span class="badge text-bg-info">Every node</span>';
    html += "</div>";
    html += '<pre class="support-request-statement">' + supportEsc(request.statement) + "</pre>";
    if (request.status === "open") html += '<div class="support-request-body" id="spRq_' + supportEsc(request.id) + '">' + supportRequestBodyHtml(request) + "</div>";
    html += "</div>";
  });
  return html + "</div>";
}

function supportRequestBodyHtml(request) {
  var s = supportReqStateFor(request);
  var html = "";
  if (request.nodes === "all")
    html += '<div class="support-hint mb-2">Support asked for every node of the cluster. This version of Studio runs it on this server only' + (supportReqNode ? " (<b>" + supportEsc(supportReqNode) + "</b>)" : "") + ".</div>";
  var check = supportReqValidate(request.language, request.statement);
  if (!check.ok) {
    html += '<div class="alert alert-warning py-2" style="font-size: 0.84rem;">Studio will not run this statement: ' + supportEsc(check.message) + ". Nothing was run.</div>";
    html += '<button class="btn btn-sm btn-outline-secondary sp-rq-decline" data-rq="' + supportEsc(request.id) + '">I do not want to run it</button>';
    return html;
  }
  if (s.status === "idle" || s.status === "running") {
    html += '<div class="d-flex flex-wrap align-items-center gap-2">';
    html += '<select class="form-select form-select-sm sp-rq-db" style="max-width: 14rem;" data-rq="' + supportEsc(request.id) + '" title="Database"><option value="">Database...</option></select>';
    html += '<button class="btn btn-sm btn-primary sp-rq-run" data-rq="' + supportEsc(request.id) + '"' + (s.status === "running" ? " disabled" : "") + '><i class="fa fa-play"></i> Run</button>';
    html += '<button class="btn btn-sm btn-outline-secondary sp-rq-decline" data-rq="' + supportEsc(request.id) + '">Decline</button>';
    html += "</div>";
    html += '<div id="spRqAlert_' + supportEsc(request.id) + '" class="mt-2"></div>';
    html += '<div class="support-hint mt-1">Nothing runs until you click Run, and nothing is sent until you review the result and click Send.</div>';
    return html;
  }
  if (s.status === "failed") {
    html += '<div class="alert alert-danger py-2" style="font-size: 0.84rem;">' + supportEsc(s.error) + "</div>";
    html += '<div class="support-hint mb-2">You decide whether support sees this error.</div>';
    html += supportReqActionsHtml(request, "Send the error");
    return html;
  }
  if (s.status === "declined") {
    html += '<div class="support-hint mb-1">You declined this request' + (s.reason ? " (" + supportEsc(s.reason) + ")" : "") + ".</div>";
    html += supportReqActionsHtml(request, "Send");
    return html;
  }
  if (s.status === "sending") return '<div class="support-hint">' + supportSpinner("Sending...") + "</div>";
  html += supportReqTableHtml(request, s);
  html += supportReqActionsHtml(request, "Send the result");
  return html;
}

function supportReqActionsHtml(request, label) {
  var id = supportEsc(request.id);
  return (
    '<div class="d-flex flex-wrap gap-2 mt-2 align-items-center"><button class="btn btn-sm btn-primary sp-rq-send" data-rq="' + id + '"><i class="fa fa-paper-plane"></i> ' + supportEsc(label) +
    '</button><button class="btn btn-sm btn-outline-secondary sp-rq-again" data-rq="' + id + '">Back</button><span class="support-rq-alert" id="spRqAlert_' + id + '"></span></div>'
  );
}

function supportReqTableHtml(request, s) {
  var table = s.table;
  var mask = s.mask;
  var html = '<div class="support-hint mb-1">Click a cell, a column title or a row number to mask it. Masked values are replaced in this browser: they are never sent.</div>';
  html += '<div class="support-rq-scroll"><table class="support-table support-rq-table"><thead><tr><th></th>';
  table.columns.forEach(function (c, i) {
    html += '<th class="sp-rq-col' + (mask.columns[c.name] ? " masked" : "") + '" data-rq="' + supportEsc(request.id) + '" data-c="' + i + '" title="Mask the column">' + supportEsc(c.name) + ' <span class="support-hint">' + supportEsc(c.type) + "</span></th>";
  });
  html += "</tr></thead><tbody>";
  var shown = Math.min(table.rows.length, 200);
  for (var r = 0; r < shown; r++) {
    html += '<tr><td class="sp-rq-row support-num" data-rq="' + supportEsc(request.id) + '" data-r="' + r + '" title="Mask the row">' + (r + 1) + "</td>";
    for (var c = 0; c < table.columns.length; c++) {
      var masked = supportReqIsMasked(mask, table, r, c);
      var v = table.rows[r][c];
      html += '<td class="sp-rq-cell' + (masked ? " masked" : "") + (typeof v === "number" ? " support-num" : "") + '" data-rq="' + supportEsc(request.id) + '" data-r="' + r + '" data-c="' + c + '">' +
        (masked ? SUPPORT_REQ_MASK : v === null ? '<span class="support-hint">null</span>' : supportEsc(String(v).slice(0, 400))) + "</td>";
    }
    html += "</tr>";
  }
  html += "</tbody></table></div>";
  var maskedCount = supportReqMaskedCount(mask, table);
  html += '<div class="support-hint mt-1">' + table.rows.length + " row" + (table.rows.length === 1 ? "" : "s") + (table.rows.length > shown ? " (the first " + shown + " are shown, all are sent)" : "") +
    (table.truncated ? " - cut at " + SUPPORT_REQ_MAX_ROWS : "") + " - " + maskedCount + " cell" + (maskedCount === 1 ? "" : "s") + " masked";
  html += ' - masked values become <label class="ms-1"><input type="radio" class="sp-rq-mode" name="spRqMode_' + supportEsc(request.id) + '" value="redact" data-rq="' + supportEsc(request.id) + '"' + (mask.mode === "redact" ? " checked" : "") + "> dots</label>" +
    '<label class="ms-2"><input type="radio" class="sp-rq-mode" name="spRqMode_' + supportEsc(request.id) + '" value="hash" data-rq="' + supportEsc(request.id) + '"' + (mask.mode === "hash" ? " checked" : "") + '> a code that only matches inside this answer</label></div>';
  return html;
}

function supportRequestsBarHtml(issue) {
  if (supportReqAllOpen(issue).length < 2) return "";
  return '<div class="support-card"><h6>Support requests</h6><div class="support-hint mb-2">Run every request, review each result, then send them all in one reply.</div>' +
    '<div class="d-flex flex-wrap gap-2 align-items-center"><button class="btn btn-sm btn-primary" id="spRqRunAll"><i class="fa fa-play"></i> Run all</button>' +
    '<button class="btn btn-sm btn-primary" id="spRqSendAll" disabled><i class="fa fa-paper-plane"></i> Send all</button><span id="spRqAllAlert"></span></div></div>';
}

function supportReqRefresh(request) {
  $("#spRq_" + request.id).html(supportRequestBodyHtml(request));
  supportReqFillDatabases(request);
  supportReqUpdateBar();
}

function supportReqUpdateBar() {
  var ready = 0;
  var open = 0;
  Object.keys(supportReqState).forEach(function (id) {
    var s = supportReqState[id];
    if (s.request.status !== "open") return;
    open++;
    if (s.status === "done" || s.status === "failed" || s.status === "declined") ready++;
  });
  $("#spRqSendAll").prop("disabled", ready === 0);
}

// ---------------------------------------------------------------------------------------------- running

function supportReqLoadDatabases(then) {
  if (supportReqDatabases) return then(supportReqDatabases);
  jQuery
    .ajax({ type: "GET", url: "api/v1/databases", beforeSend: function (xhr) { xhr.setRequestHeader("Authorization", globalCredentials); } })
    .done(function (data) {
      supportReqDatabases = Array.isArray(data.result) ? data.result : [];
      then(supportReqDatabases);
    })
    .fail(function () {
      supportReqDatabases = [];
      then([]);
    });
}

function supportReqFillDatabases(request) {
  var select = $('.sp-rq-db[data-rq="' + request.id + '"]');
  if (!select.length) return;
  supportReqLoadDatabases(function (names) {
    var s = supportReqStateFor(request);
    select.empty().append($("<option>").val("").text("Database..."));
    names.forEach(function (n) {
      select.append($("<option>").val(n).text(n));
    });
    var wanted = s.database && names.indexOf(s.database) >= 0 ? s.database : names.length === 1 ? names[0] : "";
    s.database = wanted;
    select.val(wanted);
  });
}

function supportReqLoadNode() {
  if (supportReqNode) return;
  jQuery
    .ajax({ type: "GET", url: "api/v1/server", beforeSend: function (xhr) { xhr.setRequestHeader("Authorization", globalCredentials); } })
    .done(function (data) {
      supportReqNode = data && data.serverName ? String(data.serverName) : "";
    });
}

/** Runs one request through the idempotent query endpoint. Calls back with nothing; the state says what happened. */
function supportReqRun(request, done) {
  var s = supportReqStateFor(request);
  var check = supportReqValidate(request.language, request.statement);
  if (!check.ok) return done && done();
  if (!s.database) {
    supportShowError({ status: 400, responseText: JSON.stringify({ error: "bad_request", message: "Choose the database to run this request on." }) }, "#spRqAlert_" + request.id);
    return done && done();
  }
  s.status = "running";
  supportReqRefresh(request);
  var began = Date.now();
  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/query/" + encodeDatabaseName(s.database),
      contentType: "application/json",
      data: JSON.stringify({ language: request.language, command: check.statement, limit: SUPPORT_REQ_MAX_ROWS + 1 }),
      beforeSend: function (xhr) { xhr.setRequestHeader("Authorization", globalCredentials); },
    })
    .done(function (data) {
      s.table = supportReqTable(data && data.result, request.nodes === "all" ? supportReqNode || "this server" : "", SUPPORT_REQ_MAX_ROWS);
      s.mask = supportReqInitialMask(s.table, supportReqRemembered());
      s.durationMs = Date.now() - began;
      s.status = "done";
    })
    .fail(function (jqXHR) {
      var body = supportParse(jqXHR && jqXHR.responseText);
      s.error = (body && (body.detail || body.error || body.message)) || "The query failed (HTTP " + (jqXHR ? jqXHR.status : "?") + ").";
      s.status = "failed";
    })
    .always(function () {
      supportReqRefresh(request);
      if (done) done();
    });
}

// ---------------------------------------------------------------------------------------------- sending

function supportReqAnswerOf(request) {
  var s = supportReqStateFor(request);
  if (s.status === "done") return supportReqBuildAnswer(s.table, s.mask, s.durationMs);
  if (s.status === "declined") return Promise.resolve({ outcome: "declined", reason: s.reason || "" });
  if (s.status === "failed") return Promise.resolve({ outcome: "failed", reason: String(s.error || "").slice(0, 500) });
  return Promise.resolve(null);
}

function supportReqSent() {
  supportReqState = {};
  loadSupportIssue(supportCurrentIssue.number);
}

function supportReqSendOne(request) {
  var s = supportReqStateFor(request);
  var alertId = "#spRqAlert_" + request.id;
  supportReqAnswerOf(request).then(function (answer) {
    if (!answer) return;
    if (s.status === "done") supportReqRemember(s.table, s.mask);
    var previous = s.status;
    s.status = "sending";
    supportReqRefresh(request);
    supportApi("POST", "/issues/" + encodeURIComponent(supportCurrentIssue.number) + "/requests/" + encodeURIComponent(request.id) + "/response", answer)
      .done(function () {
        globalNotify("Support", "Your answer was sent to ArcadeData support", "success");
        supportReqSent();
      })
      .fail(function (jqXHR) {
        s.status = previous;
        supportReqRefresh(request);
        supportShowError(jqXHR, alertId);
        if (supportError(jqXHR).code === "already_answered") supportReqSent();
      });
  });
}

function supportReqSendAll() {
  var requests = [];
  supportTimeline(supportCurrentIssue).forEach(function (e) {
    supportReqOpen(e).forEach(function (r) {
      var s = supportReqStateFor(r);
      if (s.status === "done" || s.status === "failed" || s.status === "declined") requests.push(r);
    });
  });
  if (!requests.length) return;
  Promise.all(requests.map(supportReqAnswerOf)).then(function (answers) {
    var responses = answers.map(function (a, i) {
      a.requestId = requests[i].id;
      return a;
    });
    requests.forEach(function (r) {
      var s = supportReqStateFor(r);
      if (s.status === "done") supportReqRemember(s.table, s.mask);
    });
    $("#spRqSendAll").prop("disabled", true).html(supportSpinner("Sending..."));
    supportApi("POST", "/issues/" + encodeURIComponent(supportCurrentIssue.number) + "/responses", { responses: responses })
      .done(function () {
        globalNotify("Support", "Your answers were sent to ArcadeData support", "success");
        supportReqSent();
      })
      .fail(function (jqXHR) {
        supportShowError(jqXHR, "#spRqAllAlert");
        $("#spRqSendAll").html('<i class="fa fa-paper-plane"></i> Send all');
        supportReqUpdateBar();
        if (supportError(jqXHR).code === "already_answered") supportReqSent();
      });
  });
}

// ---------------------------------------------------------------------------------------------- events

function supportReqOf(element) {
  var id = $(element).attr("data-rq");
  var s = supportReqState[id];
  return s ? s.request : null;
}

/** Called after the issue is drawn: the state of requests that are gone is dropped and the selects are filled. */
function supportRequestsInit(issue) {
  var live = {};
  supportReqAllOpen(issue).forEach(function (r) {
    live[r.id] = true;
    supportReqStateFor(r);
    supportReqFillDatabases(r);
  });
  Object.keys(supportReqState).forEach(function (id) {
    if (!live[id]) delete supportReqState[id];
  });
  supportReqLoadNode();
  supportReqUpdateBar();
}

$(document).on("change", ".sp-rq-db", function () {
  var request = supportReqOf(this);
  if (request) supportReqStateFor(request).database = $(this).val();
});

$(document).on("click", ".sp-rq-run", function () {
  var request = supportReqOf(this);
  if (request) supportReqRun(request);
});

$(document).on("click", "#spRqRunAll", function () {
  var button = $(this).prop("disabled", true);
  supportClearAlert("#spRqAllAlert");
  var queue = supportReqAllOpen(supportCurrentIssue).filter(function (r) {
    return supportReqStateFor(r).status === "idle";
  });
  (function next() {
    var request = queue.shift();
    if (!request) return button.prop("disabled", false);
    supportReqRun(request, next);
  })();
});

$(document).on("click", ".sp-rq-decline", function () {
  var request = supportReqOf(this);
  if (!request) return;
  var id = supportEsc(request.id);
  $("#spRq_" + request.id).html(
    '<div class="d-flex flex-wrap gap-2 align-items-center"><input class="form-control form-control-sm sp-rq-reason" style="max-width: 22rem;" maxlength="500" placeholder="Why not? (optional)" data-rq="' + id +
      '"><button class="btn btn-sm btn-outline-secondary sp-rq-decline-ok" data-rq="' + id + '">Decline this request</button><button class="btn btn-sm btn-link sp-rq-again" data-rq="' + id + '">Cancel</button></div>',
  );
});

$(document).on("click", ".sp-rq-decline-ok", function () {
  var request = supportReqOf(this);
  if (!request) return;
  var s = supportReqStateFor(request);
  s.reason = $("#spRq_" + request.id + " .sp-rq-reason").val().trim();
  s.status = "declined";
  supportReqRefresh(request);
});

$(document).on("click", ".sp-rq-again", function () {
  var request = supportReqOf(this);
  if (!request) return;
  supportReqStateFor(request).status = "idle";
  supportReqRefresh(request);
});

$(document).on("click", ".sp-rq-cell", function () {
  var request = supportReqOf(this);
  var s = request && supportReqStateFor(request);
  if (!s || !s.table) return;
  supportReqToggleCell(s.mask, parseInt($(this).attr("data-r"), 10), parseInt($(this).attr("data-c"), 10));
  supportReqRefresh(request);
});

$(document).on("click", ".sp-rq-col", function () {
  var request = supportReqOf(this);
  var s = request && supportReqStateFor(request);
  if (!s || !s.table) return;
  supportReqToggleColumn(s.mask, s.table, parseInt($(this).attr("data-c"), 10));
  supportReqRefresh(request);
});

$(document).on("click", ".sp-rq-row", function () {
  var request = supportReqOf(this);
  var s = request && supportReqStateFor(request);
  if (!s || !s.table) return;
  supportReqToggleRow(s.mask, s.table, parseInt($(this).attr("data-r"), 10));
  supportReqRefresh(request);
});

$(document).on("change", ".sp-rq-mode", function () {
  var request = supportReqOf(this);
  var s = request && supportReqStateFor(request);
  if (s && s.mask) s.mask.mode = $(this).val() === "hash" ? "hash" : "redact";
});

$(document).on("click", ".sp-rq-send", function () {
  var request = supportReqOf(this);
  if (request) supportReqSendOne(request);
});

$(document).on("click", "#spRqSendAll", supportReqSendAll);
